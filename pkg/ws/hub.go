package ws

import (
	"context"
	"sync"

	"go.uber.org/zap"
)

type Outbound struct {
	Targets []string
	Data    []byte
}

type Hub struct {
	register   chan *Conn
	unregister chan *Conn
	broadcast  chan Outbound
	clients    map[string]map[*Conn]struct{}
	once       sync.Once
	logger     *zap.Logger
	onOffline  func(context.Context, string) error
}

func NewHub(logger *zap.Logger, onOffline func(context.Context, string) error) *Hub {
	return &Hub{
		register:   make(chan *Conn),
		unregister: make(chan *Conn),
		broadcast:  make(chan Outbound, 256),
		clients:    make(map[string]map[*Conn]struct{}),
		logger:     logger,
		onOffline:  onOffline,
	}
}

func (h *Hub) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			h.closeAll()
			return
		case conn := <-h.register:
			set := h.clients[conn.UserID]
			if set == nil {
				set = make(map[*Conn]struct{})
				h.clients[conn.UserID] = set
			}
			set[conn] = struct{}{}
		case conn := <-h.unregister:
			set := h.clients[conn.UserID]
			if set != nil {
				if _, ok := set[conn]; ok {
					delete(set, conn)
					close(conn.send)
				}
				if len(set) == 0 {
					delete(h.clients, conn.UserID)
					if h.onOffline != nil {
						go func(userID string) {
							_ = h.onOffline(context.Background(), userID)
						}(conn.UserID)
					}
				}
			}
		case outbound := <-h.broadcast:
			h.deliver(outbound)
		}
	}
}

func (h *Hub) closeAll() {
	for _, set := range h.clients {
		for conn := range set {
			close(conn.send)
		}
	}
}

func (h *Hub) Register(conn *Conn) {
	h.register <- conn
}

func (h *Hub) Unregister(conn *Conn) {
	h.unregister <- conn
}

func (h *Hub) Broadcast(outbound Outbound) {
	h.broadcast <- outbound
}

func (h *Hub) deliver(outbound Outbound) {
	if len(outbound.Targets) == 0 {
		for _, set := range h.clients {
			for conn := range set {
				select {
				case conn.send <- outbound.Data:
				default:
				}
			}
		}
		return
	}
	for _, userID := range outbound.Targets {
		for conn := range h.clients[userID] {
			select {
			case conn.send <- outbound.Data:
			default:
			}
		}
	}
}

func (h *Hub) ActiveConnections() int {
	total := 0
	for _, set := range h.clients {
		total += len(set)
	}
	return total
}
