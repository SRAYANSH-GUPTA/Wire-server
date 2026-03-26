package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"wire-server/pkg/db"
	"wire-server/pkg/eventbus"
	"wire-server/pkg/proto/chatpb"
)

func TestChatAPISmoke(t *testing.T) {
	dbURL := os.Getenv("CHAT_SMOKE_DB_URL")
	if dbURL == "" {
		t.Skip("CHAT_SMOKE_DB_URL not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	migrationsPath := filepath.Clean(filepath.Join(wd, "..", "..", "..", "infra", "migrations"))
	if err := db.RunMigrations(dbURL, migrationsPath); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	primary, err := db.NewPrimaryPool(ctx, dbURL)
	if err != nil {
		t.Fatalf("primary pool: %v", err)
	}
	defer primary.Close()

	replica, err := db.NewReplicaPool(ctx, dbURL)
	if err != nil {
		t.Fatalf("replica pool: %v", err)
	}
	defer replica.Close()

	redisAddr := os.Getenv("CHAT_SMOKE_REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "127.0.0.1:6379"
	}
	redisClient := goredis.NewClient(&goredis.Options{Addr: redisAddr})
	defer redisClient.Close()

	log := zap.NewNop()
	svc := newChatServer(
		primary,
		replica,
		redisClient,
		eventbus.NewEventBus(eventbus.Config{Driver: "redis", Client: redisClient, Logger: log}),
		log,
	)

	senderID := "smoke-sender"
	recipientID := "smoke-recipient"
	sendResp, err := svc.SendMessage(ctx, &chatpb.SendMessageRequest{
		SenderId:     senderID,
		RecipientIds: []string{recipientID},
		Body:         "hello from smoke test",
		TraceId:      "smoke-trace",
	})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if sendResp.GetMessageId() == "" {
		t.Fatalf("SendMessage empty message_id")
	}

	historyResp, err := svc.GetHistory(ctx, &chatpb.GetHistoryRequest{
		ConversationId: buildConversationID(senderID, []string{recipientID}, ""),
		Limit:          10,
	})
	if err != nil {
		t.Fatalf("GetHistory: %v", err)
	}
	if len(historyResp.GetMessages()) == 0 {
		t.Fatalf("GetHistory returned no messages")
	}

	if _, err := svc.MarkDelivered(ctx, &chatpb.MarkStatusRequest{
		MessageId: sendResp.GetMessageId(),
		UserId:    recipientID,
	}); err != nil {
		t.Fatalf("MarkDelivered: %v", err)
	}

	if _, err := svc.MarkRead(ctx, &chatpb.MarkStatusRequest{
		MessageId: sendResp.GetMessageId(),
		UserId:    recipientID,
	}); err != nil {
		t.Fatalf("MarkRead: %v", err)
	}
}
