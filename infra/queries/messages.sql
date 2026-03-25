-- name: CreateMessage :one
INSERT INTO messages (id, conversation_id, sender_id, body, media_url, created_at)
VALUES ($1, $2, $3, $4, $5, NOW())
RETURNING id, conversation_id, sender_id, body, media_url, created_at;

-- name: AddConversationMember :one
INSERT INTO conversation_members (conversation_id, user_id, created_at)
VALUES ($1, $2, NOW())
ON CONFLICT (conversation_id, user_id) DO UPDATE SET created_at = conversation_members.created_at
RETURNING conversation_id, user_id, created_at;

-- name: ListConversationMembers :many
SELECT conversation_id, user_id, created_at
FROM conversation_members
WHERE conversation_id = $1
ORDER BY created_at ASC;

-- name: UpsertReceipt :one
INSERT INTO message_receipts (message_id, user_id, status, updated_at)
VALUES ($1, $2, $3, NOW())
ON CONFLICT (message_id, user_id)
DO UPDATE SET status = EXCLUDED.status, updated_at = NOW()
RETURNING message_id, user_id, status, updated_at;

-- name: UpdateUserPresence :exec
INSERT INTO user_presence (user_id, status, last_seen, updated_at)
VALUES ($1, $2, $3, NOW())
ON CONFLICT (user_id)
DO UPDATE SET status = EXCLUDED.status, last_seen = EXCLUDED.last_seen, updated_at = NOW();

-- name: InsertMediaObject :one
INSERT INTO media_objects (id, owner_id, bucket, object_key, content_type, size_bytes, status, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
RETURNING id, owner_id, bucket, object_key, content_type, size_bytes, status, created_at;

