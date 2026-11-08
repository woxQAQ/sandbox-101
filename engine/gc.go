package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sync"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
)

// GCOptions bounds deletions, not retention. Age is never evidence of disuse.
// Callers should also set a context deadline; background passes have two seconds.
type GCOptions struct {
	DryRun       bool `json:"dry_run"`
	MaxArtifacts int  `json:"max_artifacts"`
}

type GCIssue struct {
	Path   string `json:"path"`
	Reason string `json:"reason"`
}

type GCReport struct {
	StartedAt  time.Time `json:"started_at"`
	DryRun     bool      `json:"dry_run"`
	Complete   bool      `json:"complete"`
	Candidates []string  `json:"candidates"`
	Removed    []string  `json:"removed"`
	Retained   []GCIssue `json:"retained"`
}

func (e *Engine) startGC() {
	interval := e.opts.GCInterval
	if interval == 0 {
		interval = time.Minute
	}
	e.wg.Go(func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-e.ctx.Done():
				return
			case <-t.C:
				ctx, cancel := context.WithTimeout(e.ctx, 2*time.Second)
				_, _ = e.CollectGarbage(ctx, GCOptions{MaxArtifacts: 16})
				cancel()
			}
		}
	})
}

// LastGCReport returns the latest background or administrative result.
func (e *Engine) LastGCReport() GCReport {
	e.mu.Lock()
	defer e.mu.Unlock()
	r := e.lastGC
	r.Candidates = append([]string(nil), r.Candidates...)
	r.Removed = append([]string(nil), r.Removed...)
	r.Retained = append([]GCIssue(nil), r.Retained...)
	return r
}

// CollectGarbage holds the import guard and lifecycle locks until every unlink
// and metadata retirement completes. TryLock avoids waiting for lifecycle work
// (whose lock order is VM -> admission). A changing inventory causes a retry,
// never a sweep with an incomplete root set. Operation results are never pruned.
func (e *Engine) CollectGarbage(
	ctx context.Context,
	opts GCOptions,
) (report GCReport, err error) {
	report = GCReport{
		StartedAt:  time.Now().UTC(),
		DryRun:     opts.DryRun,
		Candidates: []string{},
		Removed:    []string{},
		Retained:   []GCIssue{},
	}
	defer func() {
		if err != nil {
			report.Retained = append(
				report.Retained,
				GCIssue{e.opts.StateDir, err.Error()},
			)
		}
		e.mu.Lock()
		e.lastGC = report
		e.lastGC.Candidates = append([]string(nil), report.Candidates...)
		e.lastGC.Removed = append([]string(nil), report.Removed...)
		e.lastGC.Retained = append([]GCIssue(nil), report.Retained...)
		e.mu.Unlock()
	}()
	if opts.MaxArtifacts == 0 {
		opts.MaxArtifacts = 16
	}
	if opts.MaxArtifacts < 1 || opts.MaxArtifacts > 1024 {
		return report, api.Fail(
			api.Invalid,
			"GC batch must be 1..1024 artifacts",
		)
	}
	if !e.artifacts.TryLock() {
		return report, api.Fail(
			api.Conflict,
			"image import or collection in progress",
		)
	}
	defer e.artifacts.Unlock()
	e.mu.Lock()
	closed := e.closing
	e.mu.Unlock()
	if closed {
		return report, api.Fail(api.Conflict, "engine shutting down")
	}
	ids, err := e.gcLockIDs(ctx)
	if err != nil {
		return report, err
	}
	var locks []*sync.Mutex
	defer func() {
		for _, lock := range slices.Backward(locks) {
			lock.Unlock()
		}
	}()
	for _, id := range ids {
		l := e.vmLock(id)
		if !l.TryLock() {
			return report, api.Fail(
				api.Conflict,
				"lifecycle work in progress: %s",
				id,
			)
		}
		locks = append(locks, l)
	}
	if !e.admission.TryLock() {
		return report, api.Fail(api.Conflict, "admission in progress")
	}
	defer e.admission.Unlock()
	current, err := e.gcLockIDs(ctx)
	if err != nil {
		return report, err
	}
	if !reflect.DeepEqual(ids, current) {
		return report, api.Fail(
			api.Conflict,
			"resource inventory changed; retry collection",
		)
	}
	if err = e.gcFence(ctx); err != nil {
		return report, err
	}
	g := gcGraph{
		e:            e,
		ctx:          ctx,
		vms:          map[string]vmRecord{},
		ops:          map[string]opRecord{},
		snapshots:    map[string]snapshotRecord{},
		images:       map[string]ImageManifest{},
		backing:      map[string]string{},
		roots:        map[string]bool{},
		layers:       map[string]bool{},
		versions:     map[string]gcVersion{},
		imports:      map[string]ImportOptions{},
		reservations: map[string]bool{},
		deletions:    map[string]gcDeletion{},
	}
	if err = g.load(); err != nil {
		return report, err
	}
	report.Complete = true
	if err = g.sweep(&report, opts); err != nil {
		report.Complete = false
		return report, err
	}
	return report, nil
}

func (e *Engine) gcLockIDs(ctx context.Context) ([]string, error) {
	rows, err := e.db.QueryContext(
		ctx,
		"SELECT id FROM vms UNION SELECT 'snapshot:' || id FROM snapshots ORDER BY 1",
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	ids := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (e *Engine) gcFence(ctx context.Context) error {
	for _, name := range []string{"RECOVERY_REQUIRED", "RECOVERY_REVIEWED.json"} {
		if _, err := os.Lstat(
			filepath.Join(e.opts.StateDir, name),
		); !os.IsNotExist(
			err,
		) {
			return api.Fail(
				api.ManualRecovery,
				"GC fenced by recovery evidence %s: %v",
				name,
				err,
			)
		}
	}
	tables, err := e.db.QueryContext(
		ctx,
		"SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
	)
	if err != nil {
		return err
	}
	for tables.Next() {
		var name string
		if err = tables.Scan(&name); err != nil {
			break
		}
		switch name {
		case "meta",
			"operations",
			"vms",
			"snapshots",
			"leases",
			"reservations",
			"backing",
			"gc_deletions":
		default:
			err = fmt.Errorf("GC unknown metadata table: %s", name)
		}
		if err != nil {
			break
		}
	}
	if err == nil {
		err = tables.Err()
	}
	err = errors.Join(err, tables.Close())
	if err != nil {
		return err
	}
	var integrity, schema, store string
	var epoch int64
	if err := e.db.QueryRowContext(ctx, "PRAGMA quick_check").
		Scan(&integrity); err != nil ||
		integrity != "ok" {
		return api.Fail(
			api.ManualRecovery,
			"GC database integrity unavailable: %v %s",
			err,
			integrity,
		)
	}
	if err := e.db.QueryRowContext(ctx, "SELECT value FROM meta WHERE key='schema'").
		Scan(&schema); err != nil ||
		schema != "1" {
		return api.Fail(api.ManualRecovery, "GC schema unavailable")
	}
	if err := e.db.QueryRowContext(ctx, "SELECT value FROM meta WHERE key='identity'").
		Scan(&store); err != nil {
		return err
	}
	if err := e.db.QueryRowContext(ctx, "SELECT CAST(value AS INTEGER) FROM meta WHERE key='epoch'").
		Scan(&epoch); err != nil {
		return err
	}
	var head RecoveryPoint
	if err := gcReadJSON(
		filepath.Join(e.opts.StateDir, "HEAD.json"),
		&head,
	); err != nil || head.Store != store ||
		epoch < head.Epoch {
		return api.Fail(
			api.ManualRecovery,
			"GC durable HEAD missing, corrupt or ahead of metadata: %v",
			err,
		)
	}
	return nil
}
