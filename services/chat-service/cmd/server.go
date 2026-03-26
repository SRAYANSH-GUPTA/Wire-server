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
	SenderID     string   `validate:"required"`
	RecipientIDs []string `validate:"required_without=GroupID,max=256,dive,required"`
	GroupID      string   `validate:"omitempty,max=128"`
	Body         string   `validate:"required,max=8000"`
}

func (s *chatServer) SendMessage(ctx context.Context, req *chatpb.SendMessageRequest) (*chatpb.SendMessageResponse, error) {
	in := sendMessageInput{
		SenderID:     strings.TrimSpace(req.GetSenderId()),
		RecipientIDs: req.GetRecipientIds(),
		GroupID:      strings.TrimSpace(req.GetGroupId()),
		Body:         req.GetBody(),
	}
	if err := s.validate.Struct(in); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "validation failed: %v", err)
	}

	recipients := dedupeRecipients(in.RecipientIDs, in.SenderID)
	if in.GroupID != "" {
		groupMembers, err := s.replicaQ.GetGroupMembers(ctx, in.GroupID)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "group members read failed: %v", err)
		}
		recipients = dedupeRecipients(groupMembers, in.SenderID)
	}
	if len(recipients) == 0 {
		return nil, status.Error(codes.InvalidArgument, "no recipients resolved")
	}
	if in.GroupID == "" && len(recipients) > 1 {
		return nil, status.Error(codes.InvalidArgument, "group_id is required for multi-recipient fan-out")
	}

	conversationID := buildConversationID(in.SenderID, recipients, in.GroupID)
	kind := "direct"
	if in.GroupID != "" {
		kind = "group"
	}
	if err := s.primaryQ.EnsureConversation(ctx, sqlc.EnsureConversationParams{ID: conversationID, Kind: kind}); err != nil {
		return nil, status.Errorf(codes.Internal, "conversation ensure failed: %v", err)
	}
	var recipientID *string
	var groupID *string
	if in.GroupID != "" {
		groupID = &in.GroupID
	} else if len(recipients) > 0 {
		recipientID = &recipients[0]
	}

	// write-op: persist message to primary
	msg, err := s.primaryQ.InsertMessage(ctx, sqlc.InsertMessageParams{
		SenderID:       in.SenderID,
		RecipientID:    recipientID,
		GroupID:        groupID,
		BodyEncrypted:  in.Body,
		BodyType:       "text",
		Body:           in.Body,
		ConversationID: conversationID,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "message insert failed: %v", err)
	}
	_ = s.primaryQ.UpsertMessageStatus(ctx, sqlc.UpsertMessageStatusParams{
		MessageID: msg.ID,
		UserID:    in.SenderID,
		Status:    "sent",
	})

	// publish msg.sent to event bus
	eventMsg := &chatpb.Message{
		MessageId:    msg.ID,
		SenderId:     msg.SenderID,
		RecipientIds: recipients,
		Body:         msg.BodyEncrypted,
		Status:       "sent",
		CreatedAt:    timestamppb.New(msg.ServerTs),
	}
	if err := s.bus.Publish(ctx, "msg.sent", eventMsg); err != nil {
		s.log.Warn("event publish msg.sent failed", zap.Error(err), zap.String("message_id", msg.ID))
	}

	// fan-out to recipients (group uses worker pool)
	if in.GroupID != "" {
		s.groupFanout(ctx, recipients, eventMsg)
	} else {
		for _, r := range recipients {
			s.deliverToRecipient(ctx, r, eventMsg)
		}
	}

	return &chatpb.SendMessageResponse{
		MessageId: msg.ID,
		ServerTs:  timestamppb.New(msg.ServerTs),
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
		Cursor:         strings.TrimSpace(req.GetCursor()),
		Limit:          limit,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "history read failed: %v", err)
	}

	out := make([]*chatpb.Message, 0, len(rows))
	for _, row := range rows {
		recipients := make([]string, 0, 1)
		if row.RecipientID != nil && *row.RecipientID != "" {
			recipients = append(recipients, *row.RecipientID)
		}
		out = append(out, &chatpb.Message{
			MessageId:    row.ID,
			SenderId:     row.SenderID,
			RecipientIds: recipients,
			Body:         row.BodyEncrypted,
			Status:       "",
			CreatedAt:    timestamppb.New(row.ServerTs),
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
	userID := strings.TrimSpace(req.GetUserId())
	if msgID == "" || userID == "" {
		return nil, status.Error(codes.InvalidArgument, "message_id and user_id are required")
	}

	// write-op: update status in primary
	if err := s.primaryQ.UpsertMessageStatus(ctx, sqlc.UpsertMessageStatusParams{
		MessageID: msgID,
		UserID:    userID,
		Status:    statusValue,
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "status upsert failed: %v", err)
	}

	evt := &chatpb.MarkStatusRequest{
		MessageId: msgID,
		UserId:    userID,
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
		"user_id":    evt.GetUserId(),
		"status":     evt.GetStatus(),
		"server_ts":  time.Now().UTC().Format(time.RFC3339Nano),
	}
	raw, _ := json.Marshal(tick)
	if err := s.redis.Publish(ctx, senderChannel(msg.SenderID), raw).Err(); err != nil {
		s.log.Warn("sender tick publish failed", zap.Error(err), zap.String("sender_id", msg.SenderID))
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

func (s *chatServer) deliverToRecipient(ctx context.Context, userID string, eventMsg *chatpb.Message) {
	payload, _ := json.Marshal(map[string]any{
		"type":         "msg.sent",
		"message_id":   eventMsg.GetMessageId(),
		"sender_id":    eventMsg.GetSenderId(),
		"recipient_id": userID,
		"body":         eventMsg.GetBody(),
		"server_ts":    eventMsg.GetCreatedAt().AsTime().Format(time.RFC3339Nano),
	})
	online := s.isOnline(ctx, userID)
	if online {
		if err := s.redis.Publish(ctx, senderChannel(userID), payload).Err(); err != nil {
			s.log.Warn("pubsub delivery failed", zap.Error(err), zap.String("user_id", userID))
		}
		_ = s.primaryQ.UpsertMessageStatus(ctx, sqlc.UpsertMessageStatusParams{
			MessageID: eventMsg.GetMessageId(),
			UserID:    userID,
			Status:    "delivered",
		})
		return
	}
	offlineKey := offlinePrefix + userID
	pipe := s.redis.Pipeline()
	pipe.LPush(ctx, offlineKey, payload)
	pipe.Expire(ctx, offlineKey, offlineTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		s.log.Warn("offline queue push failed", zap.Error(err), zap.String("user_id", userID))
	}
}

func (s *chatServer) isOnline(ctx context.Context, userID string) bool {
	v, err := s.redis.Get(ctx, presenceKeyPrefix+userID).Result()
	if err != nil {
		return false
	}
	return strings.EqualFold(v, "online")
}

func senderChannel(userID string) string {
	return pubsubPrefix + userID
}

func dedupeRecipients(ids []string, senderID string) []string {
	out := make([]string, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" || id == senderID {
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
