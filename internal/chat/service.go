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
	"wire-server/pkg/eventbus"
	"wire-server/pkg/proto/chatpb"
	"wire-server/pkg/ws"

	goredis "github.com/redis/go-redis/v9"
)

type Service struct {
	repo     *sqlc.Queries
	hub      *ws.Hub
	bus      eventbus.EventBus
	presence *presence.Tracker
	redis    *goredis.Client
}

type SendRequest struct {
	ConversationID  string
	RecipientPhones []string
	SenderPhone     string
	Body            string
	MediaURL        *string
	TraceID         string
}

type ReadRequest struct {
	MessageID string
	UserPhone string
	TraceID   string
}

func New(repo *sqlc.Queries, hub *ws.Hub, bus eventbus.EventBus, tracker *presence.Tracker, redisClient *goredis.Client) *Service {
	return &Service{repo: repo, hub: hub, bus: bus, presence: tracker, redis: redisClient}
}

func (s *Service) SendMessage(ctx context.Context, req SendRequest) (sqlc.Message, error) {
	conversationID := req.ConversationID
	if conversationID == "" {
		if len(req.RecipientPhones) == 1 {
			conversationID = directConversationID(req.SenderPhone, req.RecipientPhones[0])
		} else {
			conversationID = newID("group")
		}
	}
	kind := "group"
	if len(req.RecipientPhones) == 1 {
		kind = "direct"
	}
	if err := s.repo.EnsureConversation(ctx, sqlc.EnsureConversationParams{ID: conversationID, Kind: kind}); err != nil {
		return sqlc.Message{}, fmt.Errorf("chat.SendMessage ensure conversation: %w", err)
	}
	msg, err := s.repo.InsertMessage(ctx, sqlc.InsertMessageParams{
		SenderPhone:    req.SenderPhone,
		RecipientPhone: pgtype.Text{String: req.RecipientPhones[0], Valid: len(req.RecipientPhones) == 1},
		Body:           req.Body,
		BodyEncrypted:  req.Body, // temporary
		BodyType:       "text",
		ConversationID: conversationID,
	})
	if err != nil {
		return sqlc.Message{}, fmt.Errorf("chat.SendMessage insert: %w", err)
	}
	for _, recipientPhone := range req.RecipientPhones {
		_, _ = s.repo.AddConversationMember(ctx, sqlc.AddConversationMemberParams{
			ConversationID: conversationID,
			UserPhone:      recipientPhone,
		})
	}
	_, _ = s.repo.AddConversationMember(ctx, sqlc.AddConversationMemberParams{
		ConversationID: conversationID,
		UserPhone:      req.SenderPhone,
	})
	_ = s.repo.UpsertReceipt(ctx, sqlc.UpsertReceiptParams{
		MessageID: msg.ID,
		UserPhone: req.SenderPhone,
		Status:    "sent",
	})
	ack := map[string]any{
		"message_id":      msg.ID,
		"conversation_id": msg.ConversationID,
		"status":          "sent",
		"body":            msg.Body,
	}
	data, err := ws.MarshalFrame(ws.Frame{
		Type:      "message.sent",
		TraceID:   req.TraceID,
		UserPhone: req.SenderPhone,
		Payload:   ack,
	})
	if err != nil {
		return sqlc.Message{}, fmt.Errorf("chat.SendMessage marshal: %w", err)
	}
	s.hub.Broadcast(ws.Outbound{Targets: []string{req.SenderPhone}, Data: data})
	targets := append([]string{}, req.RecipientPhones...)
	if len(targets) == 0 {
		members, err := s.repo.ListConversationMembers(ctx, conversationID)
		if err == nil {
			for _, member := range members {
				if member.UserPhone != req.SenderPhone {
					targets = append(targets, member.UserPhone)
				}
			}
		}
	}
	if len(targets) > 0 {
		evt := &chatpb.Message{
			MessageId:       msg.ID,
			SenderPhone:     req.SenderPhone,
			RecipientPhones: targets,
			Body:            msg.Body,
			Status:          "sent",
		}
		_ = s.bus.Publish(ctx, "events.chat", evt)
		for _, target := range targets {
			s.deliver(ctx, target, msg, req.TraceID)
		}
	}
	return msg, nil
}

func (s *Service) MarkRead(ctx context.Context, req ReadRequest) error {
	_ = s.repo.UpsertReceipt(ctx, sqlc.UpsertReceiptParams{
		MessageID: req.MessageID,
		UserPhone: req.UserPhone,
		Status:    "read",
	})
	evt := &chatpb.MarkStatusRequest{
		MessageId: req.MessageID,
		UserPhone: req.UserPhone,
		Status:    "read",
	}
	_ = s.bus.Publish(ctx, "events.chat", evt)
	return nil
}

func (s *Service) deliver(ctx context.Context, target string, msg sqlc.Message, traceID string) {
	online := false
	if s.presence != nil {
		online, _ = s.presence.IsOnline(ctx, target)
	}
	frame, err := ws.MarshalFrame(ws.Frame{
		Type:      "message.delivered",
		TraceID:   traceID,
		UserPhone: target,
		Payload: map[string]any{
			"message_id":      msg.ID,
			"conversation_id": msg.ConversationID,
			"sender_phone":    msg.SenderPhone,
			"body":            msg.Body,
		},
	})
	if err != nil {
		return
	}
	if online {
		s.hub.Broadcast(ws.Outbound{Targets: []string{target}, Data: frame})
		_ = s.repo.UpsertReceipt(ctx, sqlc.UpsertReceiptParams{
			MessageID: msg.ID,
			UserPhone: target,
			Status:    "delivered",
		})
		return
	}
	if s.redis != nil {
		_ = s.redis.LPush(ctx, inboxKey(target), frame).Err()
		_ = s.redis.Expire(ctx, inboxKey(target), 24*time.Hour).Err()
	}
}

func (s *Service) DrainPending(ctx context.Context, userPhone string) error {
	if s.redis == nil {
		return nil
	}
	key := inboxKey(userPhone)
	for {
		data, err := s.redis.RPop(ctx, key).Bytes()
		if err == goredis.Nil {
			return nil
		}
		if err != nil {
			return fmt.Errorf("chat.DrainPending: %w", err)
		}
		s.hub.Broadcast(ws.Outbound{Targets: []string{userPhone}, Data: data})
		frame, err := ws.UnmarshalFrame(data)
		if err == nil {
			if messageID, ok := frame.Payload["message_id"].(string); ok {
				_ = s.repo.UpsertReceipt(ctx, sqlc.UpsertReceiptParams{
					MessageID: messageID,
					UserPhone: userPhone,
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

func inboxKey(userPhone string) string {
	return "inbox:" + userPhone
}
