package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"

	"wire-server/pkg/eventbus"
	"wire-server/pkg/middleware"
	"wire-server/pkg/proto/presencepb"
	"wire-server/pkg/redis"
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
	lookup       func(context.Context, string) (bool, error)
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
	ctx      context.Context
	cancel   context.CancelFunc
}

func NewHub(logger *zap.Logger, redisClient *redis.Client, eventBus eventbus.EventBus, lookup func(context.Context, string) (bool, error)) (*Hub, error) {
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
		lookup:       lookup,
	}
	prometheus.MustRegister(hub.activeMetric)
	return hub, nil
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
			if err != nil {
				if errors.Is(err, syscall.EINTR) {
					continue
				}
				if errors.Is(err, syscall.EBADF) {
					return nil // Exit gracefully on closed fd
				}
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

	// Mark user as online in Redis for chat-service delivery
	presenceKey := "presence:user:" + c.user.Phone
	if err := h.redis.Set(context.Background(), presenceKey, "online", 24*time.Hour); err != nil {
		h.logger.Warn("mark online failed", zap.String("phone", c.user.Phone), zap.Error(err))
	}

	// Subscribe to user's Redis channel for chat messages
	go h.subscribeUser(c)
}

func (h *Hub) subscribeUser(c *Connection) {
	pubsub, err := h.redis.Subscribe(c.ctx, "pod:user:"+c.user.Phone)
	if err != nil {
		h.logger.Warn("redis subscribe failed", zap.String("phone", c.user.Phone), zap.Error(err))
		return
	}
	defer pubsub.Close()

	ch := pubsub.Channel()
	for {
		select {
		case <-c.ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			c.Send([]byte(msg.Payload))
		}
	}
}

func (h *Hub) remove(c *Connection) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.conns, c.fd)
	h.activeMetric.Dec()
	_ = unix.EpollCtl(h.fd, unix.EPOLL_CTL_DEL, c.fd, nil)
	if c != nil {
		c.cancel() // Stop the Redis subscription goroutine
		_ = c.Close()

		// Remove online status from Redis
		presenceKey := "presence:user:" + c.user.Phone
		_ = h.redis.Cmdable().Del(context.Background(), presenceKey).Err()

		_ = h.eventBus.Publish(context.Background(), "user.offline", &presencepb.PresenceRecord{
			UserPhone: c.user.Phone,
			Status:    "offline",
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
			if conn.user.Phone == target {
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
	raw := strings.TrimSpace(string(data))
	if strings.HasPrefix(raw, "lookup:") {
		if h.lookup != nil {
			phone := strings.TrimSpace(strings.TrimPrefix(raw, "lookup:"))
			go h.respondLookup(conn, phone)
		}
		return
	}
	if !h.allowWSMessage(conn.user.Phone) {
		return
	}
	h.Broadcast(data)
}

func (h *Hub) respondLookup(conn *Connection, phone string) {
	exists, err := h.lookup(context.Background(), phone)
	resp := map[string]any{
		"type":   "lookup",
		"phone":  phone,
		"exists": exists,
	}
	if err != nil {
		resp["error"] = err.Error()
	}
	payload, _ := json.Marshal(resp)
	conn.Send(payload)
}

func (h *Hub) allowWSMessage(userPhone string) bool {
	key := fmt.Sprintf("rl_ws:%s:%d", userPhone, time.Now().Unix()/60)
	count, err := h.redis.Incr(context.Background(), key).Result()
	if err == nil && count == 1 {
		_ = h.redis.Expire(context.Background(), key, time.Minute).Err()
	}
	return err == nil && count <= 60
}

func (h *Hub) Shutdown() error {
	return unix.Close(h.fd)
}

func NewConnection(conn net.Conn, claims middleware.Claims) (*Connection, error) {
	raw, ok := conn.(syscall.Conn)
	if !ok {
		return nil, fmt.Errorf("unsupported conn type: %T", conn)
	}
	var fd int
	rawConn, err := raw.SyscallConn()
	if err != nil {
		return nil, err
	}
	if err := rawConn.Control(func(s uintptr) {
		fd = int(s)
		_ = unix.SetNonblock(fd, true)
	}); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Connection{
		Conn:     conn,
		fd:       fd,
		user:     claims,
		lastPong: time.Now(),
		ctx:      ctx,
		cancel:   cancel,
	}, nil
}

func (c *Connection) readMessage() ([]byte, error) {
	reader := wsutil.NewReader(c.Conn, ws.StateServerSide)
	hdr, err := reader.NextFrame()
	if err != nil {
		return nil, err
	}
	switch hdr.OpCode {
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
	if len(payload) == 0 {
		_ = wsutil.WriteServerMessage(c.Conn, ws.OpPong, nil)
		return
	}
	_ = wsutil.WriteServerBinary(c.Conn, payload)
}
