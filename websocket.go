package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/coder/websocket"
	"github.com/hashicorp/go-hclog"
	"github.com/mozership/probe/actionrpc"
)

// DefaultTimeout bounds a step that does not set with.timeout.
const DefaultTimeout = 30 * time.Second

// maxRedirects is how many redirects the handshake follows, as many as
// net/http follows when it is not told otherwise.
const maxRedirects = 10

// readLimit is the largest message the action reads. A larger one ends the
// exchange.
const readLimit = 16 << 20

// params are the keys the action takes in with. action.yml declares them,
// for probe check to report a key the action does not take.
var params = []string{"url", "headers", "subprotocols", "messages", "timeout"}

// keeps are the kinds of guard the action keeps to, as action.yml declares
// them: it only receives under read-only, and connects only to a host the
// run allows.
var keeps = []string{actionrpc.KindReadOnly, actionrpc.KindAllowHost}

// receiveKeys are the keys a receive entry of with.messages takes.
var receiveKeys = []string{"count", "match", "until_close"}

// Action connects to the WebSocket server a step describes, sends and
// receives its messages in order, and returns what it received.
type Action struct {
	log hclog.Logger
	// client makes the handshake. It defaults to http.DefaultTransport.
	client *http.Client
}

// Run exchanges the messages in with, under no guard.
func (a *Action) Run(with map[string]any) (map[string]any, error) {
	ret, _, err := a.RunStep(actionrpc.Call{With: with})
	return ret, err
}

// RunStep exchanges the messages in call.With under the guard of the run.
// Once the server answers the handshake, what happens is a result, whether
// it upgrades or not, and whether the messages run to the end or not; only
// a handshake that gets no answer, or a step the guard refuses, is an
// error.
func (a *Action) RunStep(call actionrpc.Call) (map[string]any, map[string]any, error) {
	if a.log == nil {
		a.log = hclog.NewNullLogger()
	}
	actionrpc.LogParams(a.log, "received request parameters", call.With)

	req, err := parseRequest(call.With)
	if err != nil {
		return nil, nil, err
	}
	if err := checkReadOnly(call.Guard, req); err != nil {
		return nil, nil, err
	}
	ret, err := a.do(req, call.Guard)
	actionrpc.LogOutcome(a.log, "websocket exchange", ret, err)
	return ret, nil, err
}

// checkReadOnly returns a Refused error when the run is read-only and the
// step sends a message. What a message does on the server cannot be told,
// so only a step that receives is run.
func checkReadOnly(guard actionrpc.Guard, r *request) error {
	if !guard.ReadOnly {
		return nil
	}
	for i, x := range r.exchanges {
		if x.kind != kindReceive {
			return actionrpc.Refuse("the run is read-only, and messages[%d] sends to the server; only a step that receives is run", i)
		}
	}
	return nil
}

// checkHost returns a Refused error when the guard does not allow the host
// of u. A host without a port is taken at the port of its scheme.
func checkHost(guard actionrpc.Guard, u *url.URL) error {
	host := u.Host
	if u.Port() == "" {
		port := "80"
		if u.Scheme == "wss" || u.Scheme == "https" {
			port = "443"
		}
		host = net.JoinHostPort(u.Hostname(), port)
	}
	return guard.CheckHost(host)
}

const (
	kindSend       = "send"
	kindSendBinary = "send_binary"
	kindReceive    = "receive"
)

// exchange is one entry of with.messages.
type exchange struct {
	kind string
	// data is what a send entry sends.
	data []byte
	// count is how many messages a receive entry takes.
	count int
	// untilClose has a receive entry take messages until the server closes
	// the connection.
	untilClose bool
	// match, when hasMatch, is what a message must contain for a receive
	// entry to take it.
	match    any
	hasMatch bool
}

type request struct {
	url          *url.URL
	headers      map[string]string
	subprotocols []string
	messages     any
	exchanges    []exchange
	timeout      time.Duration
}

func parseRequest(with map[string]any) (*request, error) {
	r := &request{timeout: DefaultTimeout, headers: map[string]string{}}

	for k := range with {
		if !slices.Contains(params, k) {
			return nil, fmt.Errorf("websocket action takes %s, not %s", strings.Join(params, ", "), k)
		}
	}

	raw, _ := with["url"].(string)
	if raw == "" {
		return nil, errors.New("websocket action requires with.url")
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "ws" && u.Scheme != "wss") || u.Host == "" {
		return nil, fmt.Errorf("with.url must be a ws or wss URL, not %q", raw)
	}
	r.url = u

	if v, ok := with["headers"]; ok && v != nil {
		headers, ok := v.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("with.headers must be an object, not %T", v)
		}
		for k, v := range headers {
			r.headers[k] = fmt.Sprint(v)
		}
	}

	if v, ok := with["subprotocols"]; ok && v != nil {
		list, ok := v.([]any)
		if !ok {
			return nil, fmt.Errorf("with.subprotocols must be a list, not %T", v)
		}
		for i, p := range list {
			s, ok := p.(string)
			if !ok || s == "" {
				return nil, fmt.Errorf("with.subprotocols[%d] must be a string, not %v", i, p)
			}
			r.subprotocols = append(r.subprotocols, s)
		}
	}

	if v, ok := with["messages"]; ok && v != nil {
		list, ok := v.([]any)
		if !ok {
			return nil, fmt.Errorf("with.messages must be a list, not %T", v)
		}
		r.messages = list
		for i, e := range list {
			x, err := parseExchange(e)
			if err != nil {
				return nil, fmt.Errorf("with.messages[%d]: %w", i, err)
			}
			if x.untilClose && i != len(list)-1 {
				return nil, fmt.Errorf("with.messages[%d]: a receive with until_close must be the last entry, since nothing can follow the close", i)
			}
			r.exchanges = append(r.exchanges, x)
		}
	}

	if v, ok := with["timeout"]; ok {
		if r.timeout, err = parseTimeout(v); err != nil {
			return nil, err
		}
	}

	return r, nil
}

// parseExchange reads an entry of with.messages, which has exactly one of
// send, send_binary and receive.
func parseExchange(e any) (exchange, error) {
	m, ok := e.(map[string]any)
	if !ok || len(m) != 1 {
		return exchange{}, errors.New("an entry must have exactly one of send, send_binary and receive")
	}
	for k, v := range m {
		switch k {
		case kindSend:
			switch s := v.(type) {
			case nil:
				return exchange{}, errors.New("send needs a message")
			case string:
				return exchange{kind: kindSend, data: []byte(s)}, nil
			default:
				b, err := json.Marshal(v)
				if err != nil {
					return exchange{}, fmt.Errorf("send cannot be sent as JSON: %w", err)
				}
				return exchange{kind: kindSend, data: b}, nil
			}
		case kindSendBinary:
			s, ok := v.(string)
			if !ok {
				return exchange{}, fmt.Errorf("send_binary must be a base64 string, not %T", v)
			}
			b, err := base64.StdEncoding.DecodeString(s)
			if err != nil {
				return exchange{}, fmt.Errorf("send_binary must be a base64 string: %w", err)
			}
			return exchange{kind: kindSendBinary, data: b}, nil
		case kindReceive:
			return parseReceive(v)
		}
		return exchange{}, fmt.Errorf("an entry takes send, send_binary or receive, not %s", k)
	}
	panic("unreachable")
}

// parseReceive reads the options of a receive entry. Without any, it takes
// the next message.
func parseReceive(v any) (exchange, error) {
	x := exchange{kind: kindReceive, count: 1}
	if v == nil {
		return x, nil
	}
	opts, ok := v.(map[string]any)
	if !ok {
		return exchange{}, fmt.Errorf("receive must be an object, not %T", v)
	}
	for k := range opts {
		if !slices.Contains(receiveKeys, k) {
			return exchange{}, fmt.Errorf("receive takes %s, not %s", strings.Join(receiveKeys, ", "), k)
		}
	}
	if c, ok := opts["count"]; ok {
		n, ok := toInt(c)
		if !ok || n < 1 {
			return exchange{}, fmt.Errorf("receive.count must be a positive integer, not %v", c)
		}
		x.count = n
	}
	if u, ok := opts["until_close"]; ok {
		b, ok := u.(bool)
		if !ok {
			return exchange{}, fmt.Errorf("receive.until_close must be true or false, not %v", u)
		}
		if b {
			if _, ok := opts["count"]; ok {
				return exchange{}, errors.New("receive takes count or until_close, not both")
			}
			x.untilClose = true
		}
	}
	x.match, x.hasMatch = opts["match"]
	return x, nil
}

// toInt returns v as an int when it is a whole number.
func toInt(v any) (int, bool) {
	f, ok := toFloat(v)
	if !ok || f != math.Trunc(f) || f > math.MaxInt32 || f < math.MinInt32 {
		return 0, false
	}
	return int(f), true
}

func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	}
	return 0, false
}

// parseTimeout accepts a duration string such as "10s" or a number of
// seconds, as the http action does.
func parseTimeout(v any) (time.Duration, error) {
	switch t := v.(type) {
	case string:
		d, err := time.ParseDuration(t)
		if err != nil {
			return 0, fmt.Errorf("with.timeout: %w", err)
		}
		return d, nil
	case int:
		return time.Duration(t) * time.Second, nil
	case int64:
		return time.Duration(t) * time.Second, nil
	case float64:
		return time.Duration(t * float64(time.Second)), nil
	default:
		return 0, fmt.Errorf("with.timeout must be a duration or a number of seconds, not %T", v)
	}
}

func (a *Action) do(r *request, guard actionrpc.Guard) (map[string]any, error) {
	if err := checkHost(guard, r.url); err != nil {
		return nil, err
	}

	ctx := context.Background()
	if r.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.timeout)
		defer cancel()
	}

	header := http.Header{}
	header.Set("User-Agent", "probe-websocket/"+version)
	for k, v := range r.headers {
		header.Set(k, v)
	}

	client := &http.Client{}
	if a.client != nil {
		c := *a.client
		client = &c
	}
	// A redirect is followed only to a host the guard allows.
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) >= maxRedirects {
			return fmt.Errorf("stopped after %d redirects", maxRedirects)
		}
		return checkHost(guard, req.URL)
	}

	reqInfo := map[string]any{
		"url":          r.url.String(),
		"headers":      flattenHeaders(header),
		"subprotocols": r.subprotocols,
		"messages":     r.messages,
	}

	start := time.Now()
	conn, resp, err := websocket.Dial(ctx, r.url.String(), &websocket.DialOptions{
		HTTPClient:   client,
		HTTPHeader:   header,
		Subprotocols: r.subprotocols,
	})
	if err != nil {
		// A redirect the guard refused is a refusal, not a failure to reach
		// the server.
		if refused := (*actionrpc.Refused)(nil); errors.As(err, &refused) {
			return nil, refused
		}
		if resp == nil {
			return nil, err
		}
		// The server answered, but did not upgrade the connection.
		res := handshake(resp)
		res["error"] = err.Error()
		return result(reqInfo, res, time.Since(start)), nil
	}
	conn.SetReadLimit(readLimit)

	res := handshake(resp)
	res["subprotocol"] = conn.Subprotocol()
	messages, closed, failure := exchangeAll(ctx, conn, r.exchanges, r.timeout)
	if closed == nil {
		if failure == "" {
			_ = conn.Close(websocket.StatusNormalClosure, "")
		} else {
			_ = conn.CloseNow()
		}
	}
	rt := time.Since(start)

	res["messages"] = messages
	if closed != nil {
		res["close"] = closed
	}
	res["error"] = failure
	return result(reqInfo, res, rt), nil
}

// exchangeAll sends and receives the messages in order, and stops at the
// first that cannot be done. It returns the messages received, the close
// the server sent when it closed the connection, and why it stopped, or an
// empty string when every entry was done.
func exchangeAll(ctx context.Context, conn *websocket.Conn, exchanges []exchange, timeout time.Duration) ([]any, map[string]any, string) {
	messages := []any{}
	for i, x := range exchanges {
		switch x.kind {
		case kindSend, kindSendBinary:
			typ := websocket.MessageText
			if x.kind == kindSendBinary {
				typ = websocket.MessageBinary
			}
			if err := conn.Write(ctx, typ, x.data); err != nil {
				closed, why := failure(ctx, err, timeout)
				return messages, closed, fmt.Sprintf("messages[%d] was not sent: %s", i, why)
			}
		case kindReceive:
			taken := 0
			for x.untilClose || taken < x.count {
				typ, data, err := conn.Read(ctx)
				if err != nil {
					closed, why := failure(ctx, err, timeout)
					if x.untilClose && closed != nil {
						return messages, closed, ""
					}
					return messages, closed, fmt.Sprintf("messages[%d] took %d of %s: %s", i, taken, want(x), why)
				}
				m := message(typ, data)
				if x.hasMatch && !matches(x.match, m["data"]) {
					continue
				}
				messages = append(messages, m)
				taken++
			}
		}
	}
	return messages, nil, ""
}

// want says how many messages a receive entry takes.
func want(x exchange) string {
	if x.untilClose {
		return "the messages until the close"
	}
	if x.count == 1 {
		return "1 message"
	}
	return fmt.Sprintf("%d messages", x.count)
}

// failure tells why a read or a write failed: the close the server
// sent, if it closed the connection, and a description.
func failure(ctx context.Context, err error, timeout time.Duration) (map[string]any, string) {
	var ce websocket.CloseError
	if errors.As(err, &ce) {
		return map[string]any{"code": int(ce.Code), "reason": ce.Reason}, fmt.Sprintf("the server closed the connection with %d", int(ce.Code))
	}
	if ctx.Err() != nil {
		return nil, fmt.Sprintf("timed out after %s", timeout)
	}
	return nil, err.Error()
}

// message is what a received message looks like to a test. The data of a
// text message is parsed when it is JSON; that of a binary one is base64.
func message(typ websocket.MessageType, data []byte) map[string]any {
	if typ == websocket.MessageBinary {
		s := base64.StdEncoding.EncodeToString(data)
		return map[string]any{"type": "binary", "data": s, "raw": s}
	}
	m := map[string]any{"type": "text", "data": string(data), "raw": string(data)}
	var parsed any
	if err := json.Unmarshal(data, &parsed); err == nil {
		m["data"] = parsed
	}
	return m
}

// matches reports whether value contains pattern: an object has every key
// of the pattern with a value that matches, a list has as many entries,
// each matching, and anything else is equal, numbers by value.
func matches(pattern, value any) bool {
	switch p := pattern.(type) {
	case map[string]any:
		v, ok := value.(map[string]any)
		if !ok {
			return false
		}
		for k, pv := range p {
			vv, ok := v[k]
			if !ok || !matches(pv, vv) {
				return false
			}
		}
		return true
	case []any:
		v, ok := value.([]any)
		if !ok || len(v) != len(p) {
			return false
		}
		for i := range p {
			if !matches(p[i], v[i]) {
				return false
			}
		}
		return true
	}
	if pf, ok := toFloat(pattern); ok {
		vf, ok := toFloat(value)
		return ok && pf == vf
	}
	return reflect.DeepEqual(pattern, value)
}

// handshake is what the server answered to the handshake.
func handshake(resp *http.Response) map[string]any {
	return map[string]any{
		"code":        resp.StatusCode,
		"status":      resp.Status,
		"headers":     flattenHeaders(resp.Header),
		"subprotocol": "",
		"messages":    []any{},
		"close":       nil,
		"error":       "",
	}
}

// result returns what the step sees. Its status is 0 only when the
// connection was upgraded and every entry of with.messages was done.
func result(req, res map[string]any, rt time.Duration) map[string]any {
	status := 0
	if res["code"] != http.StatusSwitchingProtocols || res["error"] != "" {
		status = 1
	}
	return map[string]any{
		"req":    req,
		"res":    res,
		"rt":     rt.String(),
		"status": status,
	}
}

func flattenHeaders(h http.Header) map[string]any {
	m := make(map[string]any, len(h))
	for k, v := range h {
		m[k] = strings.Join(v, ", ")
	}
	return m
}
