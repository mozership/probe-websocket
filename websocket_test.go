package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/linyows/probe/actionref"
	"github.com/linyows/probe/actionrpc"
)

// server upgrades every request and hands the connection to handle, then
// closes it. It records the handshake request and the messages it read.
func server(t *testing.T, handle func(ctx context.Context, c *websocket.Conn, rec *recorded)) (string, *recorded) {
	t.Helper()
	rec := &recorded{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.mu.Lock()
		rec.header = r.Header.Clone()
		rec.mu.Unlock()
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{"chat.v2", "chat.v1"}})
		if err != nil {
			return
		}
		defer func() { _ = c.CloseNow() }()
		handle(r.Context(), c, rec)
	}))
	t.Cleanup(ts.Close)
	return "ws" + strings.TrimPrefix(ts.URL, "http"), rec
}

type recorded struct {
	mu     sync.Mutex
	header http.Header
	got    []string
}

func (r *recorded) read(ctx context.Context, c *websocket.Conn) (websocket.MessageType, []byte, error) {
	typ, data, err := c.Read(ctx)
	if err == nil {
		r.mu.Lock()
		r.got = append(r.got, string(data))
		r.mu.Unlock()
	}
	return typ, data, err
}

func (r *recorded) messages() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.got)
}

// echo writes back each message it reads, until the client closes.
func echo(ctx context.Context, c *websocket.Conn, rec *recorded) {
	for {
		typ, data, err := rec.read(ctx, c)
		if err != nil {
			return
		}
		if err := c.Write(ctx, typ, data); err != nil {
			return
		}
	}
}

func res(t *testing.T, ret map[string]any) map[string]any {
	t.Helper()
	r, ok := ret["res"].(map[string]any)
	if !ok {
		t.Fatalf("ret has no res: %v", ret)
	}
	return r
}

func data(t *testing.T, r map[string]any) []any {
	t.Helper()
	var out []any
	for _, m := range r["messages"].([]any) {
		out = append(out, m.(map[string]any)["data"])
	}
	return out
}

func TestRun(t *testing.T) {
	u, rec := server(t, echo)

	ret, err := (&Action{}).Run(map[string]any{
		"url":          u,
		"headers":      map[string]any{"Authorization": "Bearer x"},
		"subprotocols": []any{"chat.v1"},
		"messages": []any{
			map[string]any{"send": "hello"},
			map[string]any{"receive": nil},
			map[string]any{"send": map[string]any{"type": "ping", "n": 1}},
			map[string]any{"receive": map[string]any{}},
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if got := rec.header.Get("Authorization"); got != "Bearer x" {
		t.Errorf("Authorization = %q", got)
	}
	if got := rec.header.Get("User-Agent"); got != "probe-websocket/dev" {
		t.Errorf("User-Agent = %q", got)
	}
	if got, want := rec.messages(), []string{"hello", `{"n":1,"type":"ping"}`}; !slices.Equal(got, want) {
		t.Errorf("server read %q, want %q", got, want)
	}

	if ret["status"] != 0 {
		t.Errorf("status = %v, want 0", ret["status"])
	}
	r := res(t, ret)
	if r["code"] != 101 || r["subprotocol"] != "chat.v1" || r["error"] != "" || r["close"] != nil {
		t.Errorf("res = %v", r)
	}
	msgs := r["messages"].([]any)
	if len(msgs) != 2 {
		t.Fatalf("messages = %v", msgs)
	}
	first := msgs[0].(map[string]any)
	if first["type"] != "text" || first["data"] != "hello" || first["raw"] != "hello" {
		t.Errorf("messages[0] = %v", first)
	}
	second := msgs[1].(map[string]any)
	if d, _ := second["data"].(map[string]any); d["type"] != "ping" || d["n"] != float64(1) || second["raw"] != `{"n":1,"type":"ping"}` {
		t.Errorf("messages[1] = %v", second)
	}

	req := ret["req"].(map[string]any)
	if req["url"] != u || !reflect.DeepEqual(req["subprotocols"], []string{"chat.v1"}) {
		t.Errorf("req = %v", req)
	}

	// What the step sees crosses the plugin boundary.
	if _, err := actionrpc.Sendable(ret); err != nil {
		t.Errorf("Sendable() error = %v", err)
	}
}

func TestRunConnectOnly(t *testing.T) {
	u, _ := server(t, echo)
	ret, err := (&Action{}).Run(map[string]any{"url": u})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	r := res(t, ret)
	if ret["status"] != 0 || r["code"] != 101 || len(r["messages"].([]any)) != 0 {
		t.Errorf("ret = %v", ret)
	}
	if _, err := actionrpc.Sendable(ret); err != nil {
		t.Errorf("Sendable() error = %v", err)
	}
}

func TestRunBinary(t *testing.T) {
	u, rec := server(t, echo)
	ret, err := (&Action{}).Run(map[string]any{
		"url": u,
		"messages": []any{
			map[string]any{"send_binary": "AAEC"},
			map[string]any{"receive": nil},
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if got := rec.messages(); len(got) != 1 || got[0] != "\x00\x01\x02" {
		t.Errorf("server read %q", got)
	}
	m := res(t, ret)["messages"].([]any)[0].(map[string]any)
	if m["type"] != "binary" || m["data"] != "AAEC" {
		t.Errorf("message = %v", m)
	}
}

// feed writes each of msgs, then closes with code.
func feed(code websocket.StatusCode, msgs ...string) func(context.Context, *websocket.Conn, *recorded) {
	return func(ctx context.Context, c *websocket.Conn, _ *recorded) {
		for _, m := range msgs {
			if err := c.Write(ctx, websocket.MessageText, []byte(m)); err != nil {
				return
			}
		}
		_ = c.Close(code, "bye")
	}
}

func TestRunReceive(t *testing.T) {
	msgs := []string{`{"type":"hello"}`, `{"type":"update","price":10}`, `{"type":"ping"}`, `{"type":"update","price":11}`, "plain"}
	tests := []struct {
		name    string
		receive any
		want    []any
		status  int
		close   bool
		failure string
	}{
		{name: "next", receive: nil, want: []any{map[string]any{"type": "hello"}}},
		{name: "count", receive: map[string]any{"count": 3}, want: []any{
			map[string]any{"type": "hello"},
			map[string]any{"type": "update", "price": float64(10)},
			map[string]any{"type": "ping"},
		}},
		{name: "match", receive: map[string]any{"match": map[string]any{"type": "update"}}, want: []any{
			map[string]any{"type": "update", "price": float64(10)},
		}},
		{name: "match a number by value", receive: map[string]any{"match": map[string]any{"price": 11}}, want: []any{
			map[string]any{"type": "update", "price": float64(11)},
		}},
		{name: "count of matching", receive: map[string]any{"count": 2, "match": map[string]any{"type": "update"}}, want: []any{
			map[string]any{"type": "update", "price": float64(10)},
			map[string]any{"type": "update", "price": float64(11)},
		}},
		{name: "match a text", receive: map[string]any{"match": "plain"}, want: []any{"plain"}},
		{name: "until close", receive: map[string]any{"until_close": true}, want: []any{
			map[string]any{"type": "hello"},
			map[string]any{"type": "update", "price": float64(10)},
			map[string]any{"type": "ping"},
			map[string]any{"type": "update", "price": float64(11)},
			"plain",
		}, close: true},
		{name: "until close with match", receive: map[string]any{"until_close": true, "match": map[string]any{"type": "update"}}, want: []any{
			map[string]any{"type": "update", "price": float64(10)},
			map[string]any{"type": "update", "price": float64(11)},
		}, close: true},
		{name: "closed before the count", receive: map[string]any{"count": 9}, want: []any{
			map[string]any{"type": "hello"},
			map[string]any{"type": "update", "price": float64(10)},
			map[string]any{"type": "ping"},
			map[string]any{"type": "update", "price": float64(11)},
			"plain",
		}, status: 1, close: true, failure: "messages[0] took 5 of 9 messages: the server closed the connection with 4000"},
		{name: "closed before a match", receive: map[string]any{"match": map[string]any{"type": "none"}}, status: 1, close: true,
			failure: "messages[0] took 0 of 1 message: the server closed the connection with 4000"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, _ := server(t, feed(4000, msgs...))
			ret, err := (&Action{}).Run(map[string]any{
				"url":      u,
				"messages": []any{map[string]any{"receive": tt.receive}},
			})
			if err != nil {
				t.Fatalf("Run() error = %v", err)
			}
			r := res(t, ret)
			if got := data(t, r); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("messages = %v, want %v", got, tt.want)
			}
			if ret["status"] != tt.status || r["error"] != tt.failure {
				t.Errorf("status = %v, error = %q; want %v, %q", ret["status"], r["error"], tt.status, tt.failure)
			}
			if tt.close {
				if c, _ := r["close"].(map[string]any); c["code"] != 4000 || c["reason"] != "bye" {
					t.Errorf("close = %v", r["close"])
				}
			} else if r["close"] != nil {
				t.Errorf("close = %v, want nil", r["close"])
			}
		})
	}
}

func TestRunTimeoutWhileReceiving(t *testing.T) {
	u, _ := server(t, func(ctx context.Context, c *websocket.Conn, _ *recorded) {
		_ = c.Write(ctx, websocket.MessageText, []byte("one"))
		<-ctx.Done()
	})
	start := time.Now()
	ret, err := (&Action{}).Run(map[string]any{
		"url":      u,
		"timeout":  "200ms",
		"messages": []any{map[string]any{"receive": map[string]any{"count": 2}}},
	})
	if err != nil {
		t.Fatalf("Run() error = %v, want a result: the server answered", err)
	}
	if time.Since(start) > 5*time.Second {
		t.Errorf("Run() took %s", time.Since(start))
	}
	r := res(t, ret)
	if ret["status"] != 1 || r["error"] != "messages[0] took 1 of 2 messages: timed out after 200ms" {
		t.Errorf("status = %v, error = %q", ret["status"], r["error"])
	}
	if got := data(t, r); !reflect.DeepEqual(got, []any{"one"}) {
		t.Errorf("messages = %v", got)
	}
}

func TestRunSendAfterClose(t *testing.T) {
	u, _ := server(t, feed(websocket.StatusGoingAway))
	ret, err := (&Action{}).Run(map[string]any{
		"url": u,
		"messages": []any{
			map[string]any{"receive": nil},
			map[string]any{"send": "late"},
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	r := res(t, ret)
	if ret["status"] != 1 || !strings.HasPrefix(r["error"].(string), "messages[0] took 0 of 1 message") {
		t.Errorf("status = %v, error = %q", ret["status"], r["error"])
	}
}

func TestRunHandshakeRefused(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("WWW-Authenticate", "Bearer")
		http.Error(w, "no token", http.StatusUnauthorized)
	}))
	t.Cleanup(ts.Close)

	ret, err := (&Action{}).Run(map[string]any{
		"url":      "ws" + strings.TrimPrefix(ts.URL, "http"),
		"messages": []any{map[string]any{"send": "x"}},
	})
	if err != nil {
		t.Fatalf("Run() error = %v, want a result: the server answered", err)
	}
	r := res(t, ret)
	if ret["status"] != 1 || r["code"] != 401 || r["status"] != "401 Unauthorized" || r["error"] == "" {
		t.Errorf("ret = %v", ret)
	}
	if h, _ := r["headers"].(map[string]any); h["Www-Authenticate"] != "Bearer" {
		t.Errorf("headers = %v", r["headers"])
	}
	if _, err := actionrpc.Sendable(ret); err != nil {
		t.Errorf("Sendable() error = %v", err)
	}
}

func TestRunNoResponse(t *testing.T) {
	ts := httptest.NewServer(http.NotFoundHandler())
	u := "ws" + strings.TrimPrefix(ts.URL, "http")
	ts.Close()
	if _, err := (&Action{}).Run(map[string]any{"url": u}); err == nil {
		t.Error("Run() error = nil, want one for a refused connection")
	}
}

func TestRunHandshakeTimeout(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	t.Cleanup(ts.Close)
	_, err := (&Action{}).Run(map[string]any{"url": "ws" + strings.TrimPrefix(ts.URL, "http"), "timeout": 0.2})
	if err == nil {
		t.Error("Run() error = nil, want one for a handshake with no answer")
	}
}

func TestParseRequestErrors(t *testing.T) {
	tests := []struct {
		name string
		with map[string]any
		want string
	}{
		{name: "no url", with: map[string]any{}, want: "requires with.url"},
		{name: "http url", with: map[string]any{"url": "http://example.com"}, want: "ws or wss URL"},
		{name: "unknown key", with: map[string]any{"url": "ws://x", "mesages": []any{}}, want: "not mesages"},
		{name: "headers", with: map[string]any{"url": "ws://x", "headers": "a"}, want: "with.headers must be an object"},
		{name: "subprotocols", with: map[string]any{"url": "ws://x", "subprotocols": "a"}, want: "with.subprotocols must be a list"},
		{name: "subprotocol", with: map[string]any{"url": "ws://x", "subprotocols": []any{1}}, want: "with.subprotocols[0] must be a string"},
		{name: "messages", with: map[string]any{"url": "ws://x", "messages": map[string]any{}}, want: "with.messages must be a list"},
		{name: "empty entry", with: map[string]any{"url": "ws://x", "messages": []any{map[string]any{}}}, want: "messages[0]: an entry must have exactly one"},
		{name: "two kinds", with: map[string]any{"url": "ws://x", "messages": []any{map[string]any{"send": "a", "receive": nil}}}, want: "exactly one"},
		{name: "unknown kind", with: map[string]any{"url": "ws://x", "messages": []any{map[string]any{"recv": nil}}}, want: "not recv"},
		{name: "send nothing", with: map[string]any{"url": "ws://x", "messages": []any{map[string]any{"send": nil}}}, want: "send needs a message"},
		{name: "bad base64", with: map[string]any{"url": "ws://x", "messages": []any{map[string]any{"send_binary": "!!"}}}, want: "base64"},
		{name: "receive not object", with: map[string]any{"url": "ws://x", "messages": []any{map[string]any{"receive": 3}}}, want: "receive must be an object"},
		{name: "receive unknown", with: map[string]any{"url": "ws://x", "messages": []any{map[string]any{"receive": map[string]any{"limit": 1}}}}, want: "not limit"},
		{name: "count zero", with: map[string]any{"url": "ws://x", "messages": []any{map[string]any{"receive": map[string]any{"count": 0}}}}, want: "positive integer"},
		{name: "count fraction", with: map[string]any{"url": "ws://x", "messages": []any{map[string]any{"receive": map[string]any{"count": 1.5}}}}, want: "positive integer"},
		{name: "until_close not bool", with: map[string]any{"url": "ws://x", "messages": []any{map[string]any{"receive": map[string]any{"until_close": "yes"}}}}, want: "true or false"},
		{name: "count and until_close", with: map[string]any{"url": "ws://x", "messages": []any{map[string]any{"receive": map[string]any{"count": 2, "until_close": true}}}}, want: "not both"},
		{name: "until_close not last", with: map[string]any{"url": "ws://x", "messages": []any{
			map[string]any{"receive": map[string]any{"until_close": true}},
			map[string]any{"send": "a"},
		}}, want: "must be the last entry"},
		{name: "timeout", with: map[string]any{"url": "ws://x", "timeout": "soon"}, want: "with.timeout"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseRequest(tt.with)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("parseRequest() error = %v, want one saying %q", err, tt.want)
			}
		})
	}

	// until_close false is a receive of one message.
	r, err := parseRequest(map[string]any{"url": "ws://x", "messages": []any{
		map[string]any{"receive": map[string]any{"until_close": false, "count": 2}},
		map[string]any{"send": "a"},
	}})
	if err != nil || r.exchanges[0].count != 2 || r.exchanges[0].untilClose {
		t.Errorf("parseRequest() = %+v, %v", r, err)
	}
}

func TestMatches(t *testing.T) {
	tests := []struct {
		pattern, value any
		want           bool
	}{
		{map[string]any{"a": 1}, map[string]any{"a": float64(1), "b": "x"}, true},
		{map[string]any{"a": 1}, map[string]any{"a": float64(2)}, false},
		{map[string]any{"a": 1}, map[string]any{"b": float64(1)}, false},
		{map[string]any{"a": map[string]any{"b": "c"}}, map[string]any{"a": map[string]any{"b": "c", "d": "e"}}, true},
		{map[string]any{"a": nil}, map[string]any{"a": nil}, true},
		{map[string]any{"a": nil}, map[string]any{}, false},
		{map[string]any{}, "text", false},
		{[]any{1, "x"}, []any{float64(1), "x"}, true},
		{[]any{1}, []any{float64(1), "x"}, false},
		{"plain", "plain", true},
		{"1", float64(1), false},
		{true, true, true},
	}
	for _, tt := range tests {
		if got := matches(tt.pattern, tt.value); got != tt.want {
			t.Errorf("matches(%v, %v) = %v, want %v", tt.pattern, tt.value, got, tt.want)
		}
	}
}

func TestParseTimeout(t *testing.T) {
	tests := []struct {
		in   any
		want time.Duration
	}{
		{"10s", 10 * time.Second},
		{5, 5 * time.Second},
		{int64(2), 2 * time.Second},
		{1.5, 1500 * time.Millisecond},
	}
	for _, tt := range tests {
		got, err := parseTimeout(tt.in)
		if err != nil || got != tt.want {
			t.Errorf("parseTimeout(%v) = %v, %v; want %v", tt.in, got, err, tt.want)
		}
	}
	if _, err := parseTimeout(true); err == nil {
		t.Error("parseTimeout(true) error = nil")
	}
}

func TestRunStepReadOnly(t *testing.T) {
	u, rec := server(t, feed(websocket.StatusNormalClosure, "news"))
	host := strings.TrimPrefix(u, "ws://")
	guard := actionrpc.Guard{ReadOnly: true, AllowHosts: []string{host}}

	// A step that only receives is run.
	ret, _, err := (&Action{}).RunStep(actionrpc.Call{
		With:  map[string]any{"url": u, "messages": []any{map[string]any{"receive": map[string]any{"until_close": true}}}},
		Guard: guard,
	})
	if err != nil || ret["status"] != 0 {
		t.Fatalf("RunStep() = %v, %v; want the receive run", ret, err)
	}

	for _, kind := range []string{"send", "send_binary"} {
		_, _, err := (&Action{}).RunStep(actionrpc.Call{
			With: map[string]any{"url": u, "messages": []any{
				map[string]any{"receive": nil},
				map[string]any{kind: "AAAA"},
			}},
			Guard: guard,
		})
		if !actionrpc.IsRefused(err) || !strings.Contains(err.Error(), "messages[1] sends to the server") {
			t.Errorf("RunStep() with %s error = %v, want a refusal", kind, err)
		}
	}
	if len(rec.messages()) != 0 {
		t.Errorf("server read %q under read-only", rec.messages())
	}
}

func TestRunStepAllowHost(t *testing.T) {
	u, rec := server(t, echo)
	pu, _ := url.Parse(u)
	with := map[string]any{"url": u}

	_, _, err := (&Action{}).RunStep(actionrpc.Call{With: with, Guard: actionrpc.Guard{AllowHosts: []string{"api.example.com"}}})
	if !actionrpc.IsRefused(err) || rec.header != nil {
		t.Errorf("RunStep() error = %v, connected = %v; want a refusal before connecting", err, rec.header != nil)
	}

	// A host without a port is any port of it.
	if _, _, err := (&Action{}).RunStep(actionrpc.Call{With: with, Guard: actionrpc.Guard{AllowHosts: []string{pu.Hostname()}}}); err != nil {
		t.Errorf("RunStep() error = %v for an allowed host", err)
	}

	// A redirect to a host the run does not allow is refused.
	front := httptest.NewServer(http.RedirectHandler(strings.Replace(u, "ws://", "http://", 1), http.StatusTemporaryRedirect))
	t.Cleanup(front.Close)
	fu, _ := url.Parse(front.URL)
	rec.header = nil
	_, _, err = (&Action{}).RunStep(actionrpc.Call{
		With:  map[string]any{"url": "ws://" + fu.Host},
		Guard: actionrpc.Guard{AllowHosts: []string{fu.Host}},
	})
	if !actionrpc.IsRefused(err) || rec.header != nil {
		t.Errorf("RunStep() error = %v, connected = %v; want the redirect refused", err, rec.header != nil)
	}
}

func TestCheckHostDefaultPorts(t *testing.T) {
	tests := []struct {
		url, allow string
	}{
		{"ws://example.com/x", "example.com:80"},
		{"wss://example.com/x", "example.com:443"},
		{"https://example.com/x", "example.com:443"},
	}
	for _, tt := range tests {
		u, _ := url.Parse(tt.url)
		if err := checkHost(actionrpc.Guard{AllowHosts: []string{tt.allow}}, u); err != nil {
			t.Errorf("checkHost(%s) under %s error = %v", tt.url, tt.allow, err)
		}
	}
}

// TestManifestsDeclare checks that action.yml, and the one the e2e workflows
// run the action by, declare the params and the guard the action has, so
// that probe check and the guard of a run take it as it is. probe manifest
// keeps what action.yml declares in the one of a release.
func TestManifestsDeclare(t *testing.T) {
	for _, name := range []string{"action.yml", "e2e/action/action.yml"} {
		data, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		m, err := actionref.ParseManifest(data)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !slices.Equal(m.Params, params) || !slices.Equal(m.Guard, keeps) {
			t.Errorf("%s declares params %v and guard %v, want %v and %v", name, m.Params, m.Guard, params, keeps)
		}
	}
}
