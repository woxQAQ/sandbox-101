package sandboxd

import (
	"context"
	"crypto/hmac"
	"encoding/base64"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
)

type previewCapability struct {
	Sandbox string `json:"s"`
	Tenant  string `json:"t"`
	Epoch   string `json:"e"`
	Port    int    `json:"p"`
	Expires int64  `json:"x"`
}

func (s *Service) PreviewURL(
	ctx context.Context,
	p Principal,
	id string,
	port int,
	ttl time.Duration,
) (string, error) {
	b, err := s.Get(ctx, p, id)
	if err != nil {
		return "", err
	}
	if b.State != "running" && b.State != "paused" {
		return "", api.Fail(api.Conflict, "sandbox not available")
	}
	if port < 1 || port > 65535 || ttl <= 0 {
		return "", api.Fail(
			api.Invalid,
			"valid port and positive duration required",
		)
	}
	exp := time.Now().Add(ttl)
	if exp.After(b.Deadline) {
		exp = b.Deadline
	}
	if !exp.After(time.Now()) {
		return "", api.Fail(api.Conflict, "lease expired")
	}
	cap := previewCapability{b.ID, b.TenantID, b.Epoch, port, exp.Unix()}
	body := base64.RawURLEncoding.EncodeToString(encode(cap))
	return "/preview/" + body + "." + s.store.sign(
		"preview-v1",
		body,
	) + "/", nil
}

func (s *Service) createPreview(
	w http.ResponseWriter,
	r *http.Request,
	p Principal,
	id string,
) {
	port, _ := strconv.Atoi(r.URL.Query().Get("port"))
	ttl, _ := strconv.ParseInt(r.URL.Query().Get("ttl_seconds"), 10, 64)
	if ttl == 0 {
		ttl = 300
	}
	u, err := s.PreviewURL(
		r.Context(),
		p,
		id,
		port,
		time.Duration(ttl)*time.Second,
	)
	respond(w, map[string]string{"url": u}, err)
}

func (s *Service) preview(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/preview/")
	captext, tail, _ := strings.Cut(path, "/")
	body, sig, ok := strings.Cut(captext, ".")
	if !ok ||
		!hmac.Equal([]byte(sig), []byte(s.store.sign("preview-v1", body))) {
		writeError(w, api.Fail(api.NotFound, "not found"))
		return
	}
	raw, err := base64.RawURLEncoding.DecodeString(body)
	var cap previewCapability
	if err != nil || json.Unmarshal(raw, &cap) != nil || cap.Port < 1 ||
		cap.Port > 65535 ||
		time.Now().Unix() >= cap.Expires {
		writeError(w, api.Fail(api.NotFound, "not found"))
		return
	}
	p := Principal{TenantID: cap.Tenant, SandboxID: cap.Sandbox}
	b, err := s.Get(r.Context(), p, cap.Sandbox)
	if err != nil || b.Epoch != cap.Epoch || b.State == "terminated" ||
		cap.Expires > b.Deadline.Unix() {
		writeError(w, api.Fail(api.NotFound, "not found"))
		return
	}
	b, err = s.DataReady(r.Context(), p, cap.Sandbox)
	if err != nil {
		writeError(w, err)
		return
	}
	transport := &http.Transport{
		DisableKeepAlives: true,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			unlock := s.lock(b.ID)
			defer unlock()
			current, e := s.Get(ctx, p, b.ID)
			if e != nil {
				return nil, e
			}
			if current.State != "running" ||
				!time.Now().Before(current.Deadline) {
				return nil, api.Fail(api.Conflict, "lease inactive")
			}
			vm, e := s.verifiedVM(ctx, current)
			if e != nil {
				return nil, e
			}
			c, e := s.opts.Agent.Forward(ctx, vm, cap.Port)
			if e != nil {
				return nil, e
			}
			_, publication, e := s.lockDataAdmission(ctx, p, current.ID)
			if e != nil {
				_ = c.Close()
				return nil, e
			}
			tracked := s.trackedConnection(current.ID, c)
			publication()
			return tracked, nil
		},
	}
	defer transport.CloseIdleConnections()
	proxy := &httputil.ReverseProxy{
		Transport: transport,
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(
				&url.URL{
					Scheme: "http",
					Host: net.JoinHostPort(
						"127.0.0.1",
						strconv.Itoa(cap.Port),
					),
				},
			)
			pr.Out.URL.Path = "/" + tail
			pr.Out.URL.RawPath = ""
			pr.Out.Header.Del("Authorization")
			pr.Out.Header.Del("Cookie")
			pr.SetXForwarded()
		},
		ModifyResponse: func(response *http.Response) error {
			response.Header.Set("Referrer-Policy", "no-referrer")
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			w.Header().Set("Referrer-Policy", "no-referrer")
			writeJSON(
				w,
				502,
				&api.Error{
					Code:    api.Internal,
					Message: "preview upstream unavailable",
				},
			)
		},
	}
	proxy.ServeHTTP(w, r)
}
