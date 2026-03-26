package sqlc

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type DBTX interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type Queries struct {
	db DBTX
}

func New(db DBTX) *Queries {
	return &Queries{db: db}
}

func (q *Queries) CreateMessage(ctx context.Context, params CreateMessageParams) (Message, error) {
	const query = `
INSERT INTO messages (id, conversation_id, sender_id, body, media_url, created_at)
VALUES ($1, $2, $3, $4, $5, NOW())
RETURNING id, conversation_id, sender_id, body, media_url, created_at`
	var msg Message
	err := q.db.QueryRow(ctx, query, params.ID, params.ConversationID, params.SenderID, params.Body, params.MediaURL).Scan(
		&msg.ID, &msg.ConversationID, &msg.SenderID, &msg.Body, &msg.MediaURL, &msg.CreatedAt,
	)
	if err != nil {
		return Message{}, fmt.Errorf("sqlc.CreateMessage: %w", err)
	}
	return msg, nil
}

func (q *Queries) EnsureConversation(ctx context.Context, params EnsureConversationParams) error {
	const query = `
INSERT INTO conversations (id, kind, created_at)
VALUES ($1, $2, NOW())
ON CONFLICT (id) DO NOTHING`
	if _, err := q.db.Exec(ctx, query, params.ID, params.Kind); err != nil {
		return fmt.Errorf("sqlc.EnsureConversation: %w", err)
	}
	return nil
}

type EnsureConversationParams struct {
	ID   string
	Kind string
}

type CreateMessageParams struct {
	ID             string
	ConversationID string
	SenderID       string
	Body           string
	MediaURL       *string
}

func (q *Queries) AddConversationMember(ctx context.Context, params AddConversationMemberParams) (ConversationMember, error) {
	const query = `
INSERT INTO conversation_members (conversation_id, user_id, created_at)
VALUES ($1, $2, NOW())
ON CONFLICT (conversation_id, user_id) DO UPDATE SET created_at = conversation_members.created_at
RETURNING conversation_id, user_id, created_at`
	var member ConversationMember
	if err := q.db.QueryRow(ctx, query, params.ConversationID, params.UserID).Scan(
		&member.ConversationID, &member.UserID, &member.CreatedAt,
	); err != nil {
		return ConversationMember{}, fmt.Errorf("sqlc.AddConversationMember: %w", err)
	}
	return member, nil
}

type AddConversationMemberParams struct {
	ConversationID string
	UserID         string
}

func (q *Queries) ListConversationMembers(ctx context.Context, conversationID string) ([]ConversationMember, error) {
	const query = `
SELECT conversation_id, user_id, created_at
FROM conversation_members
WHERE conversation_id = $1
ORDER BY created_at ASC`
	rows, err := q.db.Query(ctx, query, conversationID)
	if err != nil {
		return nil, fmt.Errorf("sqlc.ListConversationMembers: %w", err)
	}
	defer rows.Close()
	members := make([]ConversationMember, 0)
	for rows.Next() {
		var member ConversationMember
		if err := rows.Scan(&member.ConversationID, &member.UserID, &member.CreatedAt); err != nil {
			return nil, fmt.Errorf("sqlc.ListConversationMembers scan: %w", err)
		}
		members = append(members, member)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlc.ListConversationMembers rows: %w", err)
	}
	return members, nil
}

func (q *Queries) UpsertReceipt(ctx context.Context, params UpsertReceiptParams) (MessageReceipt, error) {
	const query = `
INSERT INTO message_receipts (message_id, user_id, status, updated_at)
VALUES ($1, $2, $3, NOW())
ON CONFLICT (message_id, user_id)
DO UPDATE SET status = EXCLUDED.status, updated_at = NOW()
RETURNING message_id, user_id, status, updated_at`
	var receipt MessageReceipt
	if err := q.db.QueryRow(ctx, query, params.MessageID, params.UserID, params.Status).Scan(
		&receipt.MessageID, &receipt.UserID, &receipt.Status, &receipt.UpdatedAt,
	); err != nil {
		return MessageReceipt{}, fmt.Errorf("sqlc.UpsertReceipt: %w", err)
	}
	return receipt, nil
}

type UpsertReceiptParams struct {
	MessageID string
	UserID    string
	Status    string
}

func (q *Queries) UpdateUserPresence(ctx context.Context, params UpdateUserPresenceParams) error {
	const query = `
INSERT INTO user_presence (user_id, status, last_seen, updated_at)
VALUES ($1, $2, $3, NOW())
ON CONFLICT (user_id)
DO UPDATE SET status = EXCLUDED.status, last_seen = EXCLUDED.last_seen, updated_at = NOW()`
	_, err := q.db.Exec(ctx, query, params.UserID, params.Status, params.LastSeen)
	if err != nil {
		return fmt.Errorf("sqlc.UpdateUserPresence: %w", err)
	}
	return nil
}

type UpdateUserPresenceParams struct {
	UserID   string
	Status   string
	LastSeen *time.Time
}

func (q *Queries) InsertMediaObject(ctx context.Context, params InsertMediaObjectParams) (MediaObject, error) {
	const query = `
INSERT INTO media_objects (id, owner_id, bucket, object_key, content_type, size_bytes, status, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
RETURNING id, owner_id, bucket, object_key, content_type, size_bytes, status, created_at`
	var media MediaObject
	if err := q.db.QueryRow(ctx, query, params.ID, params.OwnerID, params.Bucket, params.ObjectKey, params.ContentType, params.SizeBytes, params.Status).Scan(
		&media.ID, &media.OwnerID, &media.Bucket, &media.ObjectKey, &media.ContentType, &media.SizeBytes, &media.Status, &media.CreatedAt,
	); err != nil {
		return MediaObject{}, fmt.Errorf("sqlc.InsertMediaObject: %w", err)
	}
	return media, nil
}

type InsertMediaObjectParams struct {
	ID          string
	OwnerID     string
	Bucket      string
	ObjectKey   string
	ContentType string
	SizeBytes   int64
	Status      string
}

func (q *Queries) FindUsersByPhones(ctx context.Context, phones []string) ([]User, error) {
	const query = `
SELECT id, email, phone, role, created_at, updated_at
FROM users
WHERE phone = ANY($1)`
	rows, err := q.db.Query(ctx, query, phones)
	if err != nil {
		return nil, fmt.Errorf("sqlc.FindUsersByPhones: %w", err)
	}
	defer rows.Close()
	var users []User
	for rows.Next() {
		var u User
		if err := rows.Scan(&u.ID, &u.Email, &u.Phone, &u.Role, &u.CreatedAt, &u.UpdatedAt); err != nil {
			return nil, fmt.Errorf("sqlc.FindUsersByPhones scan: %w", err)
		}
		users = append(users, u)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlc.FindUsersByPhones rows: %w", err)
	}
	return users, nil
}

type InsertMessageParams struct {
	SenderID       string
	RecipientID    *string
	GroupID        *string
	BodyEncrypted  string
	BodyType       string
	Body           string
	ConversationID string
}

func (q *Queries) InsertMessage(ctx context.Context, params InsertMessageParams) (Message, error) {
	const query = `
INSERT INTO messages (
    sender_id,
    recipient_id,
    group_id,
    body_encrypted,
    body_type,
    server_ts,
    body,
    conversation_id,
    created_at
)
VALUES (
    $1, $2, $3, $4, $5, NOW(), $6, $7, NOW()
)
RETURNING
    id,
    conversation_id,
    sender_id,
    body,
    media_url,
    created_at,
    recipient_id,
    group_id,
    body_encrypted,
    body_type,
    server_ts`
	var msg Message
	if err := q.db.QueryRow(
		ctx,
		query,
		params.SenderID,
		params.RecipientID,
		params.GroupID,
		params.BodyEncrypted,
		params.BodyType,
		params.Body,
		params.ConversationID,
	).Scan(
		&msg.ID,
		&msg.ConversationID,
		&msg.SenderID,
		&msg.Body,
		&msg.MediaURL,
		&msg.CreatedAt,
		&msg.RecipientID,
		&msg.GroupID,
		&msg.BodyEncrypted,
		&msg.BodyType,
		&msg.ServerTs,
	); err != nil {
		return Message{}, fmt.Errorf("sqlc.InsertMessage: %w", err)
	}
	return msg, nil
}

func (q *Queries) GetMessageByID(ctx context.Context, id string) (Message, error) {
	const query = `
SELECT
    id,
    conversation_id,
    sender_id,
    body,
    media_url,
    created_at,
    recipient_id,
    group_id,
    body_encrypted,
    body_type,
    server_ts
FROM messages
WHERE id = $1`
	var msg Message
	if err := q.db.QueryRow(ctx, query, id).Scan(
		&msg.ID,
		&msg.ConversationID,
		&msg.SenderID,
		&msg.Body,
		&msg.MediaURL,
		&msg.CreatedAt,
		&msg.RecipientID,
		&msg.GroupID,
		&msg.BodyEncrypted,
		&msg.BodyType,
		&msg.ServerTs,
	); err != nil {
		return Message{}, fmt.Errorf("sqlc.GetMessageByID: %w", err)
	}
	return msg, nil
}

type GetMessageHistoryParams struct {
	ConversationID string
	Cursor         string
	Limit          int32
}

func (q *Queries) GetMessageHistory(ctx context.Context, params GetMessageHistoryParams) ([]Message, error) {
	const query = `
SELECT
    id,
    conversation_id,
    sender_id,
    body,
    media_url,
    created_at,
    recipient_id,
    group_id,
    body_encrypted,
    body_type,
    server_ts
FROM messages
WHERE conversation_id = $1
  AND ($2::text = '' OR created_at < COALESCE((SELECT created_at FROM messages WHERE id = $2), NOW()))
ORDER BY created_at DESC, id DESC
LIMIT $3`
	rows, err := q.db.Query(ctx, query, params.ConversationID, params.Cursor, params.Limit)
	if err != nil {
		return nil, fmt.Errorf("sqlc.GetMessageHistory: %w", err)
	}
	defer rows.Close()
	out := make([]Message, 0)
	for rows.Next() {
		var msg Message
		if err := rows.Scan(
			&msg.ID,
			&msg.ConversationID,
			&msg.SenderID,
			&msg.Body,
			&msg.MediaURL,
			&msg.CreatedAt,
			&msg.RecipientID,
			&msg.GroupID,
			&msg.BodyEncrypted,
			&msg.BodyType,
			&msg.ServerTs,
		); err != nil {
			return nil, fmt.Errorf("sqlc.GetMessageHistory scan: %w", err)
		}
		out = append(out, msg)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlc.GetMessageHistory rows: %w", err)
	}
	return out, nil
}

type UpsertMessageStatusParams struct {
	MessageID string
	UserID    string
	Status    string
}

func (q *Queries) UpsertMessageStatus(ctx context.Context, params UpsertMessageStatusParams) error {
	const query = `
INSERT INTO message_status (message_id, user_id, status, updated_at)
VALUES ($1, $2, $3, NOW())
ON CONFLICT (message_id, user_id)
DO UPDATE SET status = EXCLUDED.status, updated_at = NOW()`
	if _, err := q.db.Exec(ctx, query, params.MessageID, params.UserID, params.Status); err != nil {
		return fmt.Errorf("sqlc.UpsertMessageStatus: %w", err)
	}
	return nil
}

func (q *Queries) GetGroupMembers(ctx context.Context, groupID string) ([]string, error) {
	const query = `
SELECT user_id
FROM group_members
WHERE group_id = $1
ORDER BY created_at ASC`
	rows, err := q.db.Query(ctx, query, groupID)
	if err != nil {
		return nil, fmt.Errorf("sqlc.GetGroupMembers: %w", err)
	}
	defer rows.Close()
	out := make([]string, 0)
	for rows.Next() {
		var userID string
		if err := rows.Scan(&userID); err != nil {
			return nil, fmt.Errorf("sqlc.GetGroupMembers scan: %w", err)
		}
		out = append(out, userID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlc.GetGroupMembers rows: %w", err)
	}
	return out, nil
}
