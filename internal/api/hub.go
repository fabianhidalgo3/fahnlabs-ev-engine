package api

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/fhidalgo/emob/motor/internal/csms"
)

// hub broadcasts live snapshots to subscribers (the BFF), throttled to ~2 per second.
type hub struct {
	csms    *csms.Csms
	mu      sync.Mutex
	clients map[*client]bool
	pending chan struct{}
}

type client struct {
	ws   *websocket.Conn
	send chan []byte
}

var eventsUpgrader = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}

func newHub(c *csms.Csms) *hub {
	h := &hub{csms: c, clients: map[*client]bool{}, pending: make(chan struct{}, 1)}
	c.OnChange = func() {
		select {
		case h.pending <- struct{}{}:
		default:
		}
	}
	go h.loop()
	return h
}

// event keeps "type" as the first key, so subscribers can classify a message without parsing it.
type event struct {
	Type string `json:"type"`
	Data any    `json:"data"`
}

func (h *hub) message() []byte {
	b, _ := json.Marshal(event{Type: "snapshot", Data: h.csms.Snapshot()})
	return b
}

func (h *hub) loop() {
	for range h.pending {
		time.Sleep(500 * time.Millisecond)
		h.mu.Lock()
		n := len(h.clients)
		h.mu.Unlock()
		if n == 0 {
			continue
		}
		msg := h.message()
		h.mu.Lock()
		for c := range h.clients {
			select {
			case c.send <- msg:
			default: // slow subscriber: drop this snapshot, the next one supersedes it
			}
		}
		h.mu.Unlock()
	}
}

func (h *hub) serve(w http.ResponseWriter, r *http.Request) {
	ws, err := eventsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	c := &client{ws: ws, send: make(chan []byte, 4)}
	c.send <- h.message()
	h.mu.Lock()
	h.clients[c] = true
	h.mu.Unlock()
	go func() { // reader: detects close
		for {
			if _, _, err := ws.ReadMessage(); err != nil {
				h.mu.Lock()
				delete(h.clients, c)
				h.mu.Unlock()
				close(c.send)
				return
			}
		}
	}()
	for msg := range c.send {
		_ = ws.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if err := ws.WriteMessage(websocket.TextMessage, msg); err != nil {
			break
		}
	}
	_ = ws.Close()
}
