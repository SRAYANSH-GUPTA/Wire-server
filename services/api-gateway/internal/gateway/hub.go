package gateway

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"syscall"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"

	"wire-server/pkg/eventbus"
	"wire-server/pkg/middleware"
	presencepb "wire-server/pkg/proto/presencepb"
)

type Hub struct {
	logger       *zap.Logger
	redis        *redis.Client
	eventBus     eventbus.EventBus
	fd           int
	conns        map[int]*Connection
	register     chan *Connection
	unregister   chan *Connection
	broadcast    chan []byte
	groups       chan groupBroadcast
	mu           sync.Mutex
	activeMetric prometheus.Gauge
}

type groupBroadcast struct {
	targets []string
	payload []byte
}

type Connection struct {
	net.Conn
	fd       int
	user     middleware.Claims
	bytesMu  sync.Mutex
	lastPong time.Time
}

func NewHub(logger *zap.Logger, redisClient *redis.Client, eventBus eventbus.EventBus) (*Hub, error) {
	fd, err := unix.EpollCreate1(0)
	if err != nil {
		return nil, fmt.Errorf("epoll create: %w", err)
	}
	hub := &Hub{
		logger:       logger,
		redis:        redisClient,
		eventBus:     eventBus,
		fd:           fd,
		conns:        make(map[int]*Connection),
		register:     make(chan *Connection),
		unregister:   make(chan *Connection),
		broadcast:    make(chan []byte, 128),
		groups:       make(chan groupBroadcast, 32),
		activeMetric: prometheus.NewGauge(prometheus.GaugeOpts{Name: "active_ws_connections", Help: "Open WS connections"}),
	}, nil
}

func (h *Hub) Run(ctx context.Context) error {
	defer unix.Close(h.fd)
	events := make([]unix.EpollEvent, 64)
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case conn := <-h.register:
			h.add(conn)
		case conn := <-h.unregister:
			h.remove(conn)
		case payload := <-h.broadcast:
			h.deliver(payload, nil)
		case gb := <-h.groups:
			h.deliver(gb.payload, gb.targets)
		case <-ticker.C:
			h.checkHeartbeats()
		default:
			n, err := unix.EpollWait(h.fd, events, 50)
			if err != nil && err != syscall.EINTR {
				h.logger.Warn("epoll wait", zap.Error(err))
				continue
			}
			for i := 0; i < n; i++ {
				fd := int(events[i].Fd)
				conn := h.conns[fd]
				if events[i].Events&unix.EPOLLIN != 0 {
					h.handleRead(conn)
				}
			}
		}
	}
	prometheus.MustRegister(hub.activeMetric)
	return hub, nil
}

func (h *Hub) Register(c *Connection) {
	h.register <- c
}

func (h *Hub) Unregister(c *Connection) {
	h.unregister <- c
}

func (h *Hub) Broadcast(payload []byte) {
	h.broadcast <- payload
}

func (h *Hub) GroupBroadcast(targets []string, payload []byte) {
	h.groups <- groupBroadcast{targets: targets, payload: payload}
}

func (h *Hub) add(c *Connection) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.conns[c.fd] = c
	h.activeMetric.Inc()
	ev := &unix.EpollEvent{Events: unix.EPOLLIN | unix.EPOLLET, Fd: int32(c.fd)}
	if err := unix.EpollCtl(h.fd, unix.EPOLL_CTL_ADD, c.fd, ev); err != nil {
		h.logger.Warn("epoll add", zap.Error(err))
	}
}

func (h *Hub) remove(c *Connection) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.conns, c.fd)
	h.activeMetric.Dec()
	_ = unix.EpollCtl(h.fd, unix.EPOLL_CTL_DEL, c.fd, nil)
	if c != nil {
		_ = c.Close()
		_ = h.eventBus.Publish(context.Background(), "events.presence", &presencepb.PresenceRecord{
			UserId: c.user.UserID,
			Status: "offline",
		})
	}
}

func (h *Hub) deliver(payload []byte, targets []string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(targets) == 0 {
		for _, conn := range h.conns {
			conn.Send(payload)
		}
		return
	}
	for _, target := range targets {
		for _, conn := range h.conns {
			if conn.user.UserID == target {
				conn.Send(payload)
			}
		}
	}
}

func (h *Hub) checkHeartbeats() {
	now := time.Now()
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, conn := range h.conns {
		if now.Sub(conn.lastPong) > 40*time.Second {
			h.Unregister(conn)
			continue
		}
		_ = wsutil.WriteServerMessage(conn.Conn, ws.OpPing, nil)
	}
}

func (h *Hub) handleRead(conn *Connection) {
	if conn == nil {
		return
	}
	data, err := conn.readMessage()
	if err != nil {
		h.Unregister(conn)
		return
	}
	if len(data) == 0 {
		return
	}
	if !h.allowWSMessage(conn.user.UserID) {
		return
	}
	h.Broadcast(data)
}

func (h *Hub) allowWSMessage(userID string) bool {
	key := fmt.Sprintf("rl_ws:%s:%d", userID, time.Now().Unix()/60)
	count, err := h.redis.Incr(context.Background(), key).Result()
	if err == nil && count == 1 {
		_ = h.redis.Expire(context.Background(), key, time.Minute).Err()
	}
	return err == nil && count <= 60
}

func (h *Hub) Shutdown() error {
	return unix.Close(h.fd)
}

func NewConnection(conn net.Conn, claims middleware.Claims, redisClient *redis.Client, bus eventbus.EventBus, logger *zap.Logger) (*Connection, error) {
	raw, ok := conn.(syscall.Conn)
	if !ok {
		return nil, fmt.Errorf("unsupported conn type: %T", conn)
	}
	var fd int
	if err := raw.SyscallConn().Control(func(s uintptr) {
		fd = int(s)
		_ = unix.SetNonblock(fd, true)
	}); err != nil {
		return nil, err
	}
	return &Connection{
		Conn:     conn,
		fd:       fd,
		user:     claims,
		lastPong: time.Now(),
	}, nil
}

func (c *Connection) readMessage() ([]byte, error) {
	reader := wsutil.NewReader(c.Conn, ws.StateServerSide)
	op, err := reader.NextFrame()
	if err != nil {
		return nil, err
	}
	switch reader.Header.OpCode {
	case ws.OpPong:
		c.lastPong = time.Now()
		return nil, nil
	case ws.OpPing:
		c.Send(nil)
		return nil, nil
	case ws.OpClose:
		return nil, fmt.Errorf("connection closed")
	}
	return io.ReadAll(reader)
}

func (c *Connection) Send(payload []byte) {
	c.bytesMu.Lock()
	defer c.bytesMu.Unlock()
	_ = wsutil.WriteServerBinary(c.Conn, payload)
}

type PresenceOffline struct {
	UserId string
}
