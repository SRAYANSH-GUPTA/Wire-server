-- name: InsertMessage :one
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
    server_ts;

-- name: GetMessageByID :one
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
WHERE id = $1;

-- name: GetMessageHistory :many
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
LIMIT $3;

-- name: UpsertMessageStatus :exec
INSERT INTO message_status (message_id, user_id, status, updated_at)
VALUES ($1, $2, $3, NOW())
ON CONFLICT (message_id, user_id)
DO UPDATE SET status = EXCLUDED.status, updated_at = NOW();

-- name: GetGroupMembers :many
SELECT user_id
FROM group_members
WHERE group_id = $1
ORDER BY created_at ASC;

