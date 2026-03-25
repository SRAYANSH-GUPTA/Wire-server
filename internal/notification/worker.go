package notification

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

type Message struct {
	Token string            `json:"to"`
	Title string            `json:"title,omitempty"`
	Body  string            `json:"body,omitempty"`
	Data  map[string]string `json:"data,omitempty"`
}

type WorkerPool struct {
	key      string
	client   *http.Client
	jobs     chan Message
	wg       sync.WaitGroup
	workers  int
	shutdown chan struct{}
}

func New(serverKey string, workers int) *WorkerPool {
	if workers <= 0 {
		workers = 4
	}
	return &WorkerPool{
		key:      serverKey,
		client:   &http.Client{Timeout: 10 * time.Second},
		jobs:     make(chan Message, 1024),
		workers:  workers,
		shutdown: make(chan struct{}),
	}
}

func (p *WorkerPool) Start(ctx context.Context) {
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			p.worker(ctx)
		}()
	}
}

func (p *WorkerPool) Stop() {
	close(p.shutdown)
	p.wg.Wait()
}

func (p *WorkerPool) Enqueue(message Message) error {
	select {
	case p.jobs <- message:
		return nil
	default:
		return fmt.Errorf("notification.Enqueue: queue full")
	}
}

func (p *WorkerPool) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.shutdown:
			return
		case job := <-p.jobs:
			_ = p.sendWithRetry(ctx, job)
		}
	}
}

func (p *WorkerPool) sendWithRetry(ctx context.Context, message Message) error {
	var lastErr error
	backoff := 200 * time.Millisecond
	for attempt := 0; attempt < 4; attempt++ {
		if err := p.send(ctx, message); err != nil {
			lastErr = err
			select {
			case <-time.After(backoff):
				backoff *= 2
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}
		return nil
	}
	if lastErr != nil {
		return fmt.Errorf("notification.sendWithRetry: %w", lastErr)
	}
	return nil
}

func (p *WorkerPool) send(ctx context.Context, message Message) error {
	body, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("notification.send marshal: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://fcm.googleapis.com/fcm/send", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("notification.send request: %w", err)
	}
	req.Header.Set("Authorization", "key="+p.key)
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("notification.send do: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("notification.send status: %s", resp.Status)
	}
	return nil
}
