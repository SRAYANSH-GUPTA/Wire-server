package chat

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"

	"wire-server/internal/presence"
	"wire-server/pkg/db/sqlc"
	eb "wire-server/pkg/eventbus"
	"wire-server/pkg/ws"

	goredis "github.com/redis/go-redis/v9"
)

type Service struct {
	repo     *sqlc.Queries
	hub      *ws.Hub
	bus      eb.Bus
	presence *presence.Tracker
	redis    *goredis.Client
}

type SendRequest struct {
	ConversationID string
	RecipientIDs   []string
	SenderID       string
	Body           string
	MediaURL       *string
	TraceID        string
}

type ReadRequest struct {
	MessageID string
	UserID    string
	TraceID   string
}

func New(repo *sqlc.Queries, hub *ws.Hub, bus eb.Bus, tracker *presence.Tracker, redisClient *goredis.Client) *Service {
	return &Service{repo: repo, hub: hub, bus: bus, presence: tracker, redis: redisClient}
}

func (s *Service) SendMessage(ctx context.Context, req SendRequest) (sqlc.Message, error) {
	conversationID := req.ConversationID
	if conversationID == "" {
		if len(req.RecipientIDs) == 1 {
			conversationID = directConversationID(req.SenderID, req.RecipientIDs[0])
		} else {
			conversationID = newID("group")
		}
	}
	kind := "group"
	if len(req.RecipientIDs) == 1 {
		kind = "direct"
	}
	if err := s.repo.EnsureConversation(ctx, sqlc.EnsureConversationParams{ID: conversationID, Kind: kind}); err != nil {
		return sqlc.Message{}, fmt.Errorf("chat.SendMessage ensure conversation: %w", err)
	}
	msg, err := s.repo.CreateMessage(ctx, sqlc.CreateMessageParams{
		ID:             newID("msg"),
		ConversationID: conversationID,
		SenderID:       req.SenderID,
		Body:           req.Body,
		MediaURL:       req.MediaURL,
	})
	if err != nil {
		return sqlc.Message{}, fmt.Errorf("chat.SendMessage create: %w", err)
	}
	for _, recipientID := range req.RecipientIDs {
		_, _ = s.repo.AddConversationMember(ctx, sqlc.AddConversationMemberParams{
			ConversationID: conversationID,
			UserID:         recipientID,
		})
	}
	_, _ = s.repo.AddConversationMember(ctx, sqlc.AddConversationMemberParams{
		ConversationID: conversationID,
		UserID:         req.SenderID,
	})
	receipt, _ := s.repo.UpsertReceipt(ctx, sqlc.UpsertReceiptParams{
		MessageID: msg.ID,
		UserID:    req.SenderID,
		Status:    "sent",
	})
	ack := map[string]any{
		"message_id":      msg.ID,
		"conversation_id": msg.ConversationID,
		"status":          receipt.Status,
		"body":            msg.Body,
	}
	data, err := ws.MarshalFrame(ws.Frame{
		Type:    "message.sent",
		TraceID: req.TraceID,
		UserID:  req.SenderID,
		Payload: ack,
	})
	if err != nil {
		return sqlc.Message{}, fmt.Errorf("chat.SendMessage marshal: %w", err)
	}
	s.hub.Broadcast(ws.Outbound{Targets: []string{req.SenderID}, Data: data})
	targets := append([]string{}, req.RecipientIDs...)
	if len(targets) == 0 {
		members, err := s.repo.ListConversationMembers(ctx, conversationID)
		if err == nil {
			for _, member := range members {
				if member.UserID != req.SenderID {
					targets = append(targets, member.UserID)
				}
			}
		}
	}
	if len(targets) > 0 {
		evt := eb.Event{
			Type:   "msg.sent",
			Stream: "events.chat",
			ID:     msg.ID,
			Payload: []byte(strings.Join([]string{
				msg.ID,
				msg.ConversationID,
				req.SenderID,
			}, "|")),
			Metadata: map[string]string{
				"trace_id": req.TraceID,
			},
		}
		_ = s.bus.Publish(ctx, evt.Stream, evt)
		for _, target := range targets {
			s.deliver(ctx, target, msg, req.TraceID)
		}
	}
	return msg, nil
}

func (s *Service) MarkRead(ctx context.Context, req ReadRequest) error {
	_, err := s.repo.UpsertReceipt(ctx, sqlc.UpsertReceiptParams{
		MessageID: req.MessageID,
		UserID:    req.UserID,
		Status:    "read",
	})
	if err != nil {
		return fmt.Errorf("chat.MarkRead: %w", err)
	}
	_ = s.bus.Publish(ctx, "events.chat", eb.Event{
		Type:     "msg.read",
		Stream:   "events.chat",
		ID:       req.MessageID,
		Metadata: map[string]string{"user_id": req.UserID, "trace_id": req.TraceID},
	})
	return nil
}

func (s *Service) deliver(ctx context.Context, target string, msg sqlc.Message, traceID string) {
	online := false
	if s.presence != nil {
		online, _ = s.presence.IsOnline(ctx, target)
	}
	frame, err := ws.MarshalFrame(ws.Frame{
		Type:    "message.delivered",
		TraceID: traceID,
		UserID:  target,
		Payload: map[string]any{
			"message_id":      msg.ID,
			"conversation_id": msg.ConversationID,
			"sender_id":       msg.SenderID,
			"body":            msg.Body,
		},
	})
	if err != nil {
		return
	}
	if online {
		s.hub.Broadcast(ws.Outbound{Targets: []string{target}, Data: frame})
		_, _ = s.repo.UpsertReceipt(ctx, sqlc.UpsertReceiptParams{
			MessageID: msg.ID,
			UserID:    target,
			Status:    "delivered",
		})
		return
	}
	if s.redis != nil {
		_ = s.redis.LPush(ctx, inboxKey(target), frame).Err()
		_ = s.redis.Expire(ctx, inboxKey(target), 24*time.Hour).Err()
	}
}

func (s *Service) DrainPending(ctx context.Context, userID string) error {
	if s.redis == nil {
		return nil
	}
	key := inboxKey(userID)
	for {
		data, err := s.redis.RPop(ctx, key).Bytes()
		if err == goredis.Nil {
			return nil
		}
		if err != nil {
			return fmt.Errorf("chat.DrainPending: %w", err)
		}
		s.hub.Broadcast(ws.Outbound{Targets: []string{userID}, Data: data})
		frame, err := ws.UnmarshalFrame(data)
		if err == nil {
			if messageID, ok := frame.Payload["message_id"].(string); ok {
				_, _ = s.repo.UpsertReceipt(ctx, sqlc.UpsertReceiptParams{
					MessageID: messageID,
					UserID:    userID,
					Status:    "delivered",
				})
			}
		}
	}
}

func directConversationID(a, b string) string {
	users := []string{a, b}
	sort.Strings(users)
	sum := sha256.Sum256([]byte(strings.Join(users, ":")))
	return "dm_" + hex.EncodeToString(sum[:])[:24]
}

func newID(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, time.Now().UTC().UnixNano())
}

func inboxKey(userID string) string {
	return "inbox:" + userID
}
