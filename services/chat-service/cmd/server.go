package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
	"github.com/go-playground/validator/v10"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"wire-server/pkg/db/sqlc"
	"wire-server/pkg/eventbus"
	"wire-server/pkg/proto/chatpb"
)

const (
	maxHistoryLimit   = 50
	groupWorkerCount  = 20
	offlineTTL        = 24 * time.Hour
	presenceKeyPrefix = "presence:user:"
	pubsubPrefix      = "pod:user:"
	offlinePrefix     = "offline:"
)

type chatServer struct {
	chatpb.UnimplementedChatServiceServer
	primaryQ *sqlc.Queries
	replicaQ *sqlc.Queries
	redis    *redis.Client
	bus      eventbus.EventBus
	validate *validator.Validate
	log      *zap.Logger
}

func newChatServer(
	primaryPool *pgxpool.Pool,
	replicaPool *pgxpool.Pool,
	redisClient *redis.Client,
	bus eventbus.EventBus,
	log *zap.Logger,
) *chatServer {
	return &chatServer{
		primaryQ: sqlc.New(primaryPool),
		replicaQ: sqlc.New(replicaPool),
		redis:    redisClient,
		bus:      bus,
		validate: validator.New(),
		log:      log,
	}
}

type sendMessageInput struct {
	SenderPhone     string   `validate:"required"`
	RecipientPhones []string `validate:"required_without=GroupID,max=256,dive,required"`
	GroupID         string   `validate:"omitempty,max=128"`
	Body            string   `validate:"required,max=8000"`
}

func (s *chatServer) SendMessage(ctx context.Context, req *chatpb.SendMessageRequest) (*chatpb.SendMessageResponse, error) {
	in := sendMessageInput{
		SenderPhone:     strings.TrimSpace(req.GetSenderPhone()),
		RecipientPhones: req.GetRecipientPhones(),
		GroupID:         strings.TrimSpace(req.GetGroupId()),
		Body:            req.GetBody(),
	}
	if err := s.validate.Struct(in); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "validation failed: %v", err)
	}

	recipients := dedupeRecipients(in.RecipientPhones, in.SenderPhone)
	if in.GroupID != "" {
		groupMembers, err := s.replicaQ.GetGroupMembers(ctx, in.GroupID)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "group members read failed: %v", err)
		}
		recipients = dedupeRecipients(groupMembers, in.SenderPhone)
	}
	if len(recipients) == 0 {
		return nil, status.Error(codes.InvalidArgument, "no recipients resolved")
	}

	conversationID := buildConversationID(in.SenderPhone, recipients, in.GroupID)
	kind := "direct"
	if in.GroupID != "" {
		kind = "group"
	}
	if err := s.primaryQ.EnsureConversation(ctx, sqlc.EnsureConversationParams{ID: conversationID, Kind: kind}); err != nil {
		return nil, status.Errorf(codes.Internal, "conversation ensure failed: %v", err)
	}

	arg := sqlc.InsertMessageParams{
		SenderPhone:    in.SenderPhone,
		BodyEncrypted:  in.Body,
		BodyType:       "text",
		Body:           in.Body,
		ConversationID: conversationID,
	}
	if in.GroupID != "" {
		arg.GroupID = pgtype.Text{String: in.GroupID, Valid: true}
	} else if len(recipients) > 0 {
		arg.RecipientPhone = pgtype.Text{String: recipients[0], Valid: true}
	}

	msg, err := s.primaryQ.InsertMessage(ctx, arg)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "message insert failed: %v", err)
	}

	_ = s.primaryQ.UpsertMessageStatus(ctx, sqlc.UpsertMessageStatusParams{
		MessageID: msg.ID,
		UserPhone: in.SenderPhone,
		Status:    "sent",
	})

	eventMsg := &chatpb.Message{
		MessageId:       msg.ID,
		SenderPhone:     msg.SenderPhone,
		RecipientPhones: recipients,
		Body:            msg.BodyEncrypted,
		Status:          "sent",
		CreatedAt:       timestamppb.New(msg.ServerTs.Time),
	}
	if err := s.bus.Publish(ctx, "msg.sent", eventMsg); err != nil {
		s.log.Warn("event publish msg.sent failed", zap.Error(err), zap.String("message_id", msg.ID))
	}

	if in.GroupID != "" {
		s.groupFanout(ctx, recipients, eventMsg)
	} else {
		for _, r := range recipients {
			s.deliverToRecipient(ctx, r, eventMsg)
		}
	}

	return &chatpb.SendMessageResponse{
		MessageId: msg.ID,
		ServerTs:  timestamppb.New(msg.ServerTs.Time),
	}, nil
}

func (s *chatServer) GetHistory(ctx context.Context, req *chatpb.GetHistoryRequest) (*chatpb.GetHistoryResponse, error) {
	limit := req.GetLimit()
	if limit <= 0 {
		limit = 20
	}
	if limit > maxHistoryLimit {
		limit = maxHistoryLimit
	}
	conversationID := strings.TrimSpace(req.GetConversationId())
	if conversationID == "" {
		return nil, status.Error(codes.InvalidArgument, "conversation_id is required")
	}

	// read-op: query from replica
	rows, err := s.replicaQ.GetMessageHistory(ctx, sqlc.GetMessageHistoryParams{
		ConversationID: conversationID,
		ID:             strings.TrimSpace(req.GetCursor()),
		Limit:          limit,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "history read failed: %v", err)
	}

	out := make([]*chatpb.Message, 0, len(rows))
	for _, row := range rows {
		recipients := make([]string, 0, 1)
		if row.RecipientPhone.Valid && row.RecipientPhone.String != "" {
			recipients = append(recipients, row.RecipientPhone.String)
		}
		out = append(out, &chatpb.Message{
			MessageId:       row.ID,
			SenderPhone:     row.SenderPhone,
			RecipientPhones: recipients,
			Body:            row.BodyEncrypted,
			Status:          "",
			CreatedAt:       timestamppb.New(row.ServerTs.Time),
		})
	}

	nextCursor := ""
	if len(rows) == int(limit) {
		nextCursor = rows[len(rows)-1].ID
	}
	return &chatpb.GetHistoryResponse{
		Messages:   out,
		NextCursor: nextCursor,
	}, nil
}

func (s *chatServer) MarkDelivered(ctx context.Context, req *chatpb.MarkStatusRequest) (*chatpb.MarkStatusResponse, error) {
	return s.markStatus(ctx, req, "delivered", "msg.delivered")
}

func (s *chatServer) MarkRead(ctx context.Context, req *chatpb.MarkStatusRequest) (*chatpb.MarkStatusResponse, error) {
	return s.markStatus(ctx, req, "read", "msg.read")
}

func (s *chatServer) markStatus(ctx context.Context, req *chatpb.MarkStatusRequest, statusValue, stream string) (*chatpb.MarkStatusResponse, error) {
	msgID := strings.TrimSpace(req.GetMessageId())
	userPhone := strings.TrimSpace(req.GetUserPhone())
	if msgID == "" || userPhone == "" {
		return nil, status.Error(codes.InvalidArgument, "message_id and user_phone are required")
	}

	// write-op: update status in primary
	if err := s.primaryQ.UpsertMessageStatus(ctx, sqlc.UpsertMessageStatusParams{
		MessageID: msgID,
		UserPhone: userPhone,
		Status:    statusValue,
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "status upsert failed: %v", err)
	}

	evt := &chatpb.MarkStatusRequest{
		MessageId: msgID,
		UserPhone: userPhone,
		Status:    statusValue,
	}
	if err := s.bus.Publish(ctx, stream, evt); err != nil {
		s.log.Warn("event publish failed", zap.Error(err), zap.String("stream", stream), zap.String("message_id", msgID))
	}
	return &chatpb.MarkStatusResponse{Success: true}, nil
}

func (s *chatServer) startStatusConsumers(ctx context.Context) {
	for _, stream := range []string{"msg.delivered", "msg.read"} {
		streamName := stream
		if err := s.bus.Subscribe(ctx, streamName, "chat-service-status", func(cbCtx context.Context, payload []byte) {
			s.handleStatusEvent(cbCtx, streamName, payload)
		}); err != nil {
			s.log.Warn("status consumer subscribe failed", zap.String("stream", streamName), zap.Error(err))
		}
	}
}

func (s *chatServer) handleStatusEvent(ctx context.Context, stream string, payload []byte) {
	var evt chatpb.MarkStatusRequest
	if err := proto.Unmarshal(payload, &evt); err != nil {
		s.log.Warn("status event unmarshal failed", zap.Error(err))
		return
	}
	msg, err := s.primaryQ.GetMessageByID(ctx, evt.GetMessageId())
	if err != nil {
		s.log.Warn("status event message lookup failed", zap.Error(err), zap.String("message_id", evt.GetMessageId()))
		return
	}

	tick := map[string]any{
		"type":       stream,
		"message_id": evt.GetMessageId(),
		"user_phone": evt.GetUserPhone(),
		"status":     evt.GetStatus(),
		"server_ts":  time.Now().UTC().Format(time.RFC3339Nano),
	}
	raw, _ := json.Marshal(tick)
	if err := s.redis.Publish(ctx, senderChannel(msg.SenderPhone), raw).Err(); err != nil {
		s.log.Warn("sender tick publish failed", zap.Error(err), zap.String("sender_phone", msg.SenderPhone))
	}
}

func (s *chatServer) groupFanout(ctx context.Context, recipients []string, eventMsg *chatpb.Message) {
	batches := batchRecipients(recipients, 64)
	jobs := make(chan []string)
	var wg sync.WaitGroup

	worker := func() {
		defer wg.Done()
		for batch := range jobs {
			for _, userID := range batch {
				s.deliverToRecipient(ctx, userID, eventMsg)
			}
		}
	}

	workers := groupWorkerCount
	if len(batches) < workers {
		workers = len(batches)
	}
	if workers < 1 {
		workers = 1
	}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go worker()
	}
	for _, b := range batches {
		jobs <- b
	}
	close(jobs)
	wg.Wait()
}

func (s *chatServer) deliverToRecipient(ctx context.Context, userPhone string, eventMsg *chatpb.Message) {
	payload, _ := json.Marshal(map[string]any{
		"type":            "msg.sent",
		"message_id":      eventMsg.GetMessageId(),
		"sender_phone":    eventMsg.GetSenderPhone(),
		"recipient_phone": userPhone,
		"body":            eventMsg.GetBody(),
		"server_ts":       eventMsg.GetCreatedAt().AsTime().Format(time.RFC3339Nano),
	})
	online := s.isOnline(ctx, userPhone)
	if online {
		if err := s.redis.Publish(ctx, senderChannel(userPhone), payload).Err(); err != nil {
			s.log.Warn("pubsub delivery failed", zap.Error(err), zap.String("user_phone", userPhone))
		}
		_ = s.primaryQ.UpsertMessageStatus(ctx, sqlc.UpsertMessageStatusParams{
			MessageID: eventMsg.GetMessageId(),
			UserPhone: userPhone,
			Status:    "delivered",
		})
		return
	}
	offlineKey := offlinePrefix + userPhone
	pipe := s.redis.Pipeline()
	pipe.LPush(ctx, offlineKey, payload)
	pipe.Expire(ctx, offlineKey, offlineTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		s.log.Warn("offline queue push failed", zap.Error(err), zap.String("user_phone", userPhone))
	}
}

func (s *chatServer) isOnline(ctx context.Context, userPhone string) bool {
	v, err := s.redis.Get(ctx, presenceKeyPrefix+userPhone).Result()
	if err != nil {
		return false
	}
	return strings.EqualFold(v, "online")
}

func senderChannel(userPhone string) string {
	return pubsubPrefix + userPhone
}

func dedupeRecipients(ids []string, senderPhone string) []string {
	out := make([]string, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" || id == senderPhone {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func buildConversationID(sender string, recipients []string, groupID string) string {
	if groupID != "" {
		return "group:" + groupID
	}
	if len(recipients) > 0 {
		pair := []string{sender, recipients[0]}
		sort.Strings(pair)
		return fmt.Sprintf("dm:%s:%s", pair[0], pair[1])
	}
	return "dm:" + sender
}

func batchRecipients(recipients []string, batchSize int) [][]string {
	if batchSize <= 0 {
		batchSize = 64
	}
	if len(recipients) == 0 {
		return nil
	}
	out := make([][]string, 0, (len(recipients)+batchSize-1)/batchSize)
	for i := 0; i < len(recipients); i += batchSize {
		end := i + batchSize
		if end > len(recipients) {
			end = len(recipients)
		}
		out = append(out, recipients[i:end])
	}
	return out
}
