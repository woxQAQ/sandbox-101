package sandboxd

import (
	"encoding/json"
	"time"
)

// Reconstruct the streak from durable terminal intents so a restart cannot reset
// a capacity failure into a burst of clone attempts. A successful refill resets it.
func (s *Service) refillBackoff(template string, now time.Time) (bool, error) {
	rows, err := s.store.db.QueryContext(s.ctx, `SELECT body FROM operations
 WHERE status IN ('failed','succeeded') AND json_extract(body,'$.command.action')='refill'
 AND json_extract(body,'$.command.template_id')=? ORDER BY rowid DESC LIMIT 10`, template)
	if err != nil {
		return true, err
	}
	defer rows.Close()
	failures := 0
	var latest time.Time
	for rows.Next() {
		var raw []byte
		var o Operation
		if err = rows.Scan(&raw); err != nil {
			return true, err
		}
		if err = json.Unmarshal(raw, &o); err != nil {
			return true, err
		}
		if o.Status == "succeeded" {
			break
		}
		if failures == 0 {
			latest = o.FinishedAt
			if latest.IsZero() {
				latest = o.CreatedAt
			}
		}
		failures++
	}
	if err = rows.Err(); err != nil {
		return true, err
	}
	if failures == 0 {
		return false, nil
	}
	delay := time.Second << min(failures-1, 8)
	return now.Before(latest.Add(delay)), nil
}
