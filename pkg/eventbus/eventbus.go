package eventbus

import "context"

type Event struct {
	Type     string
	Stream   string
	ID       string
	Payload  []byte
	Metadata map[string]string
}

type Bus interface {
	Publish(ctx context.Context, stream string, event Event) error
	Subscribe(ctx context.Context, stream, group string) (<-chan Event, error)
}
