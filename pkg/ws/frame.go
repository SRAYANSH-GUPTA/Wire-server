package ws

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

type Frame struct {
	Type    string
	TraceID string
	UserID  string
	Payload map[string]any
}

func MarshalFrame(frame Frame) ([]byte, error) {
	payload, err := structpb.NewStruct(frame.Payload)
	if err != nil {
		return nil, fmt.Errorf("ws.MarshalFrame payload: %w", err)
	}
	envelope, err := structpb.NewStruct(map[string]any{
		"type":     frame.Type,
		"trace_id": frame.TraceID,
		"user_id":  frame.UserID,
		"payload":  payload.AsMap(),
	})
	if err != nil {
		return nil, fmt.Errorf("ws.MarshalFrame envelope: %w", err)
	}
	return proto.Marshal(envelope)
}

func UnmarshalFrame(data []byte) (Frame, error) {
	envelope := &structpb.Struct{}
	if err := proto.Unmarshal(data, envelope); err != nil {
		return Frame{}, fmt.Errorf("ws.UnmarshalFrame: %w", err)
	}
	raw := envelope.AsMap()
	frame := Frame{}
	if v, ok := raw["type"].(string); ok {
		frame.Type = v
	}
	if v, ok := raw["trace_id"].(string); ok {
		frame.TraceID = v
	}
	if v, ok := raw["user_id"].(string); ok {
		frame.UserID = v
	}
	if payload, ok := raw["payload"].(map[string]any); ok {
		frame.Payload = payload
	} else {
		frame.Payload = map[string]any{}
	}
	return frame, nil
}
