// Package gateway is the motor's client of the OCPP gateway: OCPP commands through its REST API and charger events
// (connections, frames, CALLs to answer) through its stream. The motor itself no longer speaks OCPP-J.
package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Error is an OCPP-level error (CALLERROR code, Timeout, NotConnected, ConnectionClosed…).
type Error struct {
	Code    string
	Message string
}

func (e *Error) Error() string { return e.Message }

// NewError builds an OCPP error; returned by a call handler it becomes the CALLERROR sent to the charger.
func NewError(code, msg string) *Error { return &Error{Code: code, Message: msg} }

// Handler receives the stream events. OnCall answers a charger CALL (its result or an *Error).
type Handler interface {
	OnHello(connected []string)
	OnConnected(chargerID string)
	OnDisconnected(chargerID string)
	OnStreamDown()
	OnCall(chargerID, action string, payload json.RawMessage) (any, error)
	OnFrame(chargerID, dir string, frame []json.RawMessage, t int64)
}

// Client talks to one OCPP gateway.
type Client struct {
	base  string
	token string
	http  *http.Client
}

// New creates a client for the gateway at baseURL (e.g. http://localhost:9100).
func New(baseURL, token string) *Client {
	return &Client{base: strings.TrimRight(baseURL, "/"), token: token, http: &http.Client{Timeout: 45 * time.Second}}
}

func (c *Client) req(ctx context.Context, method, path string, body []byte) (*http.Response, error) {
	r, err := http.NewRequestWithContext(ctx, method, c.base+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	if body != nil {
		r.Header.Set("Content-Type", "application/json")
	}
	if c.token != "" {
		r.Header.Set("X-Gateway-Token", c.token)
	}
	return c.http.Do(r)
}

// Command sends an OCPP command to a charger and returns its CALLRESULT payload.
func (c *Client) Command(ctx context.Context, chargerID, action string, payload any) (json.RawMessage, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	res, err := c.req(ctx, http.MethodPost, "/api/v1/chargers/"+url.PathEscape(chargerID)+"/commands/"+url.PathEscape(action), body)
	if err != nil {
		return nil, NewError("GatewayUnavailable", "OCPP gateway not reachable: "+err.Error())
	}
	defer res.Body.Close()
	data, _ := io.ReadAll(res.Body)
	if res.StatusCode == http.StatusOK {
		return data, nil
	}
	var p struct {
		Detail        string `json:"detail"`
		OcppErrorCode string `json:"ocppErrorCode"`
	}
	_ = json.Unmarshal(data, &p)
	switch res.StatusCode {
	case http.StatusBadRequest:
		return nil, NewError("NotSupported", p.Detail)
	case http.StatusNotFound:
		return nil, NewError("NotConnected", p.Detail)
	case http.StatusGatewayTimeout:
		return nil, NewError("Timeout", p.Detail)
	case http.StatusBadGateway:
		return nil, NewError(p.OcppErrorCode, p.Detail)
	}
	return nil, fmt.Errorf("gateway %d: %s", res.StatusCode, p.Detail)
}

// Disconnect closes a charger connection; policy=true closes with "policy violation" (e.g. unregistered charger).
func (c *Client) Disconnect(ctx context.Context, chargerID, reason string, policy bool) error {
	q := url.Values{"reason": {reason}}
	if policy {
		q.Set("policy", "true")
	}
	res, err := c.req(ctx, http.MethodDelete, "/api/v1/connections/"+url.PathEscape(chargerID)+"?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	res.Body.Close()
	return nil
}

// Link is a command channel to one charger.
type Link struct {
	c  *Client
	id string
}

// Link returns the command channel of a charger.
func (c *Client) Link(chargerID string) *Link { return &Link{c: c, id: chargerID} }

// Call sends an OCPP command through the gateway.
func (l *Link) Call(ctx context.Context, action string, payload any) (json.RawMessage, error) {
	return l.c.Command(ctx, l.id, action, payload)
}

type streamMessage struct {
	Type        string            `json:"type"`
	ID          string            `json:"id,omitempty"`
	ChargerID   string            `json:"chargerId,omitempty"`
	Action      string            `json:"action,omitempty"`
	Payload     json.RawMessage   `json:"payload,omitempty"`
	Dir         string            `json:"dir,omitempty"`
	Frame       []json.RawMessage `json:"frame,omitempty"`
	T           int64             `json:"t,omitempty"`
	Code        string            `json:"code,omitempty"`
	Description string            `json:"description,omitempty"`
	Connections []struct {
		ChargerID string `json:"chargerId"`
	} `json:"connections,omitempty"`
}

// Run keeps the stream open until ctx ends, reconnecting with exponential backoff (up to 10 s).
func (c *Client) Run(ctx context.Context, h Handler) {
	retry := 0
	for ctx.Err() == nil {
		up := c.stream(ctx, h)
		if up {
			retry = 0
		}
		wait := min(10*time.Second, time.Duration(500<<min(retry, 5))*time.Millisecond)
		retry++
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

func (c *Client) stream(ctx context.Context, h Handler) (wasUp bool) {
	u := strings.Replace(c.base, "http", "ws", 1) + "/api/v1/stream"
	hdr := http.Header{}
	if c.token != "" {
		hdr.Set("X-Gateway-Token", c.token)
	}
	ws, _, err := websocket.DefaultDialer.DialContext(ctx, u, hdr)
	if err != nil {
		return false
	}
	slog.Info("attached to the OCPP gateway stream", "url", u)
	defer func() {
		_ = ws.Close()
		slog.Warn("OCPP gateway stream closed; retrying", "url", u)
		h.OnStreamDown()
	}()
	var writeMu sync.Mutex
	reply := func(m streamMessage) {
		b, _ := json.Marshal(m)
		writeMu.Lock()
		_ = ws.SetWriteDeadline(time.Now().Add(10 * time.Second))
		_ = ws.WriteMessage(websocket.TextMessage, b)
		writeMu.Unlock()
	}
	go func() {
		<-ctx.Done()
		_ = ws.Close()
	}()
	for {
		_, data, err := ws.ReadMessage()
		if err != nil {
			return true
		}
		var m streamMessage
		if json.Unmarshal(data, &m) != nil {
			continue
		}
		switch m.Type {
		case "hello":
			ids := make([]string, len(m.Connections))
			for i, x := range m.Connections {
				ids[i] = x.ChargerID
			}
			h.OnHello(ids)
		case "connected":
			h.OnConnected(m.ChargerID)
		case "disconnected":
			h.OnDisconnected(m.ChargerID)
		case "frame":
			h.OnFrame(m.ChargerID, m.Dir, m.Frame, m.T)
		case "call":
			go func(m streamMessage) {
				res, err := h.OnCall(m.ChargerID, m.Action, m.Payload)
				if err != nil {
					code := "InternalError"
					var oe *Error
					if errors.As(err, &oe) {
						code = oe.Code
					}
					reply(streamMessage{Type: "error", ID: m.ID, Code: code, Description: err.Error()})
					return
				}
				b, _ := json.Marshal(res)
				reply(streamMessage{Type: "result", ID: m.ID, Payload: b})
			}(m)
		}
	}
}
