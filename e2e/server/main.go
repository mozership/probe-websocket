// The WebSocket server the e2e workflows run probe-websocket against. Its
// connections are kept by gorilla/websocket, an implementation the action
// was not written against.
//
//	go -C e2e/server run . -addr 127.0.0.1:18082
//
// It serves:
//
//	/echo       sends back each message, as text or as binary
//	/ticker     greets who the Authorization header names, and sends the
//	            updates of a channel to one who subscribes to it
//	/countdown  counts down from the number in from, then closes with the
//	            code and the reason of the query, or 1000 and liftoff
//	/silent     sends nothing
//	/moved      redirects to /echo
package main

import (
	"encoding/json"
	"flag"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// closeWait is how long the server waits for the other side to answer a
// close before it drops the connection.
const closeWait = 2 * time.Second

// tick is the time between two updates of a channel.
const tick = 20 * time.Millisecond

var upgrader = websocket.Upgrader{Subprotocols: []string{"ticker.v2", "ticker.v1"}}

// conn is a connection several goroutines write to, one at a time.
type conn struct {
	*websocket.Conn
	mu sync.Mutex
}

func (c *conn) send(v any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.WriteJSON(v)
}

// close sends a close with code and reason, and waits for the answer.
func (c *conn) close(code int, reason string) {
	c.mu.Lock()
	_ = c.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, reason), time.Now().Add(closeWait))
	c.mu.Unlock()
	_ = c.SetReadDeadline(time.Now().Add(closeWait))
	for {
		if _, _, err := c.ReadMessage(); err != nil {
			break
		}
	}
	_ = c.Conn.Close()
}

func upgrade(w http.ResponseWriter, r *http.Request) *conn {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("%s: %v", r.URL.Path, err)
		return nil
	}
	return &conn{Conn: ws}
}

func echo(w http.ResponseWriter, r *http.Request) {
	c := upgrade(w, r)
	if c == nil {
		return
	}
	defer func() { _ = c.Conn.Close() }()
	for {
		typ, data, err := c.ReadMessage()
		if err != nil {
			return
		}
		if err := c.WriteMessage(typ, data); err != nil {
			return
		}
	}
}

func ticker(w http.ResponseWriter, r *http.Request) {
	user, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok {
		w.Header().Set("WWW-Authenticate", "Bearer")
		http.Error(w, "not signed in", http.StatusUnauthorized)
		return
	}
	c := upgrade(w, r)
	if c == nil {
		return
	}
	defer func() { _ = c.Conn.Close() }()

	done := make(chan struct{})
	defer close(done)
	_ = c.send(map[string]any{"type": "hello", "user": user})
	for {
		_, data, err := c.ReadMessage()
		if err != nil {
			return
		}
		var m struct {
			Type    string `json:"type"`
			Channel string `json:"channel"`
		}
		if err := json.Unmarshal(data, &m); err != nil {
			_ = c.send(map[string]any{"type": "error", "message": "the message is not JSON"})
			continue
		}
		switch m.Type {
		case "subscribe":
			_ = c.send(map[string]any{"type": "subscribed", "channel": m.Channel})
			go updates(c, m.Channel, done)
		case "bye":
			c.close(websocket.CloseNormalClosure, "bye")
			return
		default:
			_ = c.send(map[string]any{"type": "error", "message": "no such type: " + m.Type})
		}
	}
}

// updates sends the updates of channel, one each tick, until done is closed
// or one cannot be sent.
func updates(c *conn, channel string, done <-chan struct{}) {
	t := time.NewTicker(tick)
	defer t.Stop()
	for seq := 1; ; seq++ {
		select {
		case <-done:
			return
		case <-t.C:
		}
		if err := c.send(map[string]any{"type": "update", "channel": channel, "seq": seq, "price": 100 + seq}); err != nil {
			return
		}
	}
}

func countdown(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	from, _ := strconv.Atoi(q.Get("from"))
	code, err := strconv.Atoi(q.Get("code"))
	if err != nil {
		code = websocket.CloseNormalClosure
	}
	reason := q.Get("reason")
	if reason == "" {
		reason = "liftoff"
	}
	c := upgrade(w, r)
	if c == nil {
		return
	}
	for n := from; n > 0; n-- {
		if err := c.WriteMessage(websocket.TextMessage, []byte(strconv.Itoa(n))); err != nil {
			_ = c.Conn.Close()
			return
		}
	}
	c.close(code, reason)
}

func silent(w http.ResponseWriter, r *http.Request) {
	c := upgrade(w, r)
	if c == nil {
		return
	}
	defer func() { _ = c.Conn.Close() }()
	for {
		if _, _, err := c.ReadMessage(); err != nil {
			return
		}
	}
}

func main() {
	addr := flag.String("addr", "127.0.0.1:18082", "the address to listen on")
	flag.Parse()

	mux := http.NewServeMux()
	mux.HandleFunc("/echo", echo)
	mux.HandleFunc("/ticker", ticker)
	mux.HandleFunc("/countdown", countdown)
	mux.HandleFunc("/silent", silent)
	mux.HandleFunc("/moved", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/echo", http.StatusTemporaryRedirect)
	})

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("listening on %s", ln.Addr())
	log.Fatal(http.Serve(ln, mux))
}
