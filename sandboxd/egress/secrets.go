package egress

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/textproto"
	"net/url"
	"sort"
	"strings"
)

type credential struct {
	value       string
	hosts       map[string]struct{}
	header      string
	placeholder string
}

func compileSecrets(secrets map[string]Secret) ([]credential, error) {
	keys := make([]string, 0, len(secrets))
	for k := range secrets {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var out []credential
	for _, key := range keys {
		s := secrets[key]
		if s.Value == "" || len(s.Value) > 16384 ||
			strings.ContainsAny(s.Value, "\r\n\x00") ||
			strings.ContainsAny(s.Placeholder, "\r\n\x00") ||
			len(s.Placeholder) > 16384 ||
			len(s.Hosts) == 0 ||
			!validHeader(s.Header) {
			return nil, errors.New("egress: invalid credential configuration")
		}
		c := credential{
			value:       s.Value,
			hosts:       make(map[string]struct{}),
			header:      textproto.CanonicalMIMEHeaderKey(s.Header),
			placeholder: s.Placeholder,
		}
		for _, h := range s.Hosts {
			h, err := canonicalHost(h)
			if err != nil {
				return nil, errors.New("egress: invalid credential host")
			}
			c.hosts[h] = struct{}{}
		}
		for _, other := range out {
			if c.header == other.header {
				for h := range c.hosts {
					if _, ok := other.hosts[h]; ok {
						return nil, errors.New(
							"egress: overlapping credential headers",
						)
					}
				}
			}
		}
		out = append(out, c)
	}
	return out, nil
}

func validHeader(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') &&
			(c < '0' || c > '9') &&
			!strings.ContainsRune("!#$%&'*+-.^_`|~", c) {
			return false
		}
	}
	switch strings.ToLower(s) {
	case "host",
		"connection",
		"proxy-connection",
		"proxy-authorization",
		"proxy-authenticate",
		"content-length",
		"transfer-encoding",
		"trailer",
		"upgrade",
		"te",
		"keep-alive",
		"expect",
		"accept-encoding":
		return false
	}
	return true
}

// A placeholder may occur only in its configured header on its authorized
// HTTPS host. Rejecting other occurrences also covers redirect credential reuse.
func (b *binding) validatePlaceholders(r *http.Request, host string) error {
	query, err := url.QueryUnescape(r.URL.RawQuery)
	if err != nil {
		return ErrDenied
	}
	for _, s := range b.secrets {
		_, allowed := s.hosts[host]
		if s.placeholder == "" {
			continue
		}
		if strings.Contains(r.URL.String(), s.placeholder) ||
			strings.Contains(r.URL.Path, s.placeholder) ||
			strings.Contains(query, s.placeholder) ||
			strings.Contains(r.Host, s.placeholder) {
			return ErrDenied
		}
		for k, vs := range r.Header {
			for _, v := range vs {
				if strings.Contains(v, s.placeholder) &&
					(!allowed || r.URL.Scheme != "https" || !strings.EqualFold(k, s.header)) {
					return ErrDenied
				}
			}
		}
	}
	return nil
}

func (b *binding) inject(r *http.Request, host string) error {
	var matches []credential
	for _, s := range b.secrets {
		if _, ok := s.hosts[host]; !ok || r.URL.Scheme != "https" {
			continue
		}
		if s.placeholder != "" &&
			!strings.Contains(
				strings.Join(r.Header.Values(s.header), ", "),
				s.placeholder,
			) {
			continue
		}
		matches = append(matches, s)
	}
	if len(matches) == 0 {
		return nil
	}
	// Persist one event for the complete request before replacing ANY header.
	// A failed sink leaves both explicit and implicit credentials untouched.
	if err := b.audit(host, safeMethod(r.Method), "inject", ""); err != nil {
		return err
	}
	for _, s := range matches {
		value := s.value
		if s.placeholder != "" {
			value = strings.ReplaceAll(
				strings.Join(r.Header.Values(s.header), ", "),
				s.placeholder,
				s.value,
			)
		}
		r.Header.Set(s.header, value)
	}
	return nil
}

// Scrub literal credential reflections, including matches spanning read chunks.
// Authorized upstream endpoints are trusted: no proxy can prevent an endpoint
// from intentionally encoding a credential into an otherwise ordinary response.
func (b *binding) scrubResponse(resp *http.Response) *http.Response {
	if len(b.secrets) == 0 {
		return resp
	}
	values := make([]string, 0, len(b.secrets))
	for _, s := range b.secrets {
		values = append(values, s.value)
	}
	sort.Slice(
		values,
		func(i, j int) bool { return len(values[i]) > len(values[j]) },
	)
	scrub := func(s string) string {
		for _, v := range values {
			s = strings.ReplaceAll(s, v, "[redacted]")
		}
		return s
	}
	for k, vs := range resp.Header {
		for i, v := range vs {
			vs[i] = scrub(v)
		}
		if scrub(k) != k {
			delete(resp.Header, k)
		}
	}
	// Arbitrary compressed responses cannot be inspected safely as plaintext.
	if encoding := resp.Header.Get(
		"Content-Encoding",
	); encoding != "" &&
		!strings.EqualFold(encoding, "identity") {
		_ = resp.Body.Close()
		return deniedResponse(
			resp.Request,
			http.StatusBadGateway,
			"egress: encoded response denied\n",
		)
	}
	resp.Status = scrub(resp.Status)
	resp.Header.Del("Content-Length")
	resp.Header.Del("Trailer")
	resp.ContentLength = -1
	resp.Trailer = nil
	resp.Body = &redactingBody{
		source: resp.Body,
		values: values,
		done:   func() { resp.Trailer = nil },
	}
	return resp
}

type redactingBody struct {
	source  io.ReadCloser
	values  []string
	pending []byte
	output  []byte
	err     error
	done    func()
}

func (r *redactingBody) Close() error {
	err := r.source.Close()
	if r.done != nil {
		r.done()
	}
	return err
}

func (r *redactingBody) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for len(r.output) == 0 {
		if r.err != nil && len(r.pending) == 0 {
			return 0, r.err
		}
		if r.err == nil {
			var chunk [32768]byte
			n, err := r.source.Read(chunk[:])
			r.pending = append(r.pending, chunk[:n]...)
			r.err = err
			if err != nil && r.done != nil {
				r.done()
			}
		}
		i := 0
		for i < len(r.pending) {
			matched, partial := false, false
			for _, v := range r.values {
				if bytes.HasPrefix(r.pending[i:], []byte(v)) {
					r.output = append(r.output, "[redacted]"...)
					i += len(v)
					matched = true
					break
				}
				if r.err == nil && bytes.HasPrefix([]byte(v), r.pending[i:]) {
					partial = true
					break
				}
			}
			// Keep only a suffix that could become a credential. Ordinary
			// streaming/SSE data is released immediately, without lookahead.
			if partial {
				break
			}
			if !matched {
				r.output = append(r.output, r.pending[i])
				i++
			}
		}
		r.pending = append(r.pending[:0], r.pending[i:]...)
	}
	n := copy(p, r.output)
	r.output = r.output[n:]
	return n, nil
}
