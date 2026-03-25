package ws

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

const (
	writeWait  = 10 * time.Second
	pongWait   = 40 * time.Second
	pingPeriod = 30 * time.Second
)

type MessageHandler func(context.Context, string, Frame) error

type Conn struct {
	WS      *websocket.Conn
	UserID  string
	TraceID string
	send    chan []byte
	hub     *Hub
	handler MessageHandler
	logger  *zap.Logger
	closed  atomic.Bool
}

func NewConn(ws *websocket.Conn, hub *Hub, userID, traceID string, logger *zap.Logger, handler MessageHandler) *Conn {
	return &Conn{
		WS:      ws,
		UserID:  userID,
		TraceID: traceID,
		send:    make(chan []byte, 64),
		hub:     hub,
		handler: handler,
		logger:  logger,
	}
}

func (c *Conn) Serve(ctx context.Context) {
	c.Register()
	c.Run(ctx)
}

func (c *Conn) Register() {
	c.hub.Register(c)
}

func (c *Conn) Run(ctx context.Context) {
	go c.writePump(ctx)
	c.readPump(ctx)
}

func (c *Conn) readPump(ctx context.Context) {
	defer c.cleanup()
	c.WS.SetReadLimit(1 << 20)
	_ = c.WS.SetReadDeadline(time.Now().Add(pongWait))
	c.WS.SetPongHandler(func(string) error {
		return c.WS.SetReadDeadline(time.Now().Add(pongWait))
	})
	for {
		_, data, err := c.WS.ReadMessage()
		if err != nil {
			return
		}
		frame, err := UnmarshalFrame(data)
		if err != nil {
			RecordError()
			continue
		}
		RecordMessage()
		if c.handler != nil {
			_ = c.handler(ctx, c.UserID, frame)
		}
	}
}

func (c *Conn) writePump(ctx context.Context) {
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()
	defer c.cleanup()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-c.send:
			_ = c.WS.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				_ = c.WS.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.WS.WriteMessage(websocket.BinaryMessage, msg); err != nil {
				return
			}
		case <-ticker.C:
			_ = c.WS.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.WS.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (c *Conn) Send(frame Frame) error {
	data, err := MarshalFrame(frame)
	if err != nil {
		return err
	}
	select {
	case c.send <- data:
		return nil
	default:
		return fmt.Errorf("ws.Conn.Send: send buffer full")
	}
}

func (c *Conn) cleanup() {
	if c.closed.CompareAndSwap(false, true) {
		c.hub.Unregister(c)
		_ = c.WS.Close()
	}
}
