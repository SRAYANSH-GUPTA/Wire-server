-- name: InsertMessage :one
INSERT INTO messages (
    sender_phone,
    recipient_phone,
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
    sender_phone,
    body,
    media_url,
    created_at,
    recipient_phone,
    group_id,
    body_encrypted,
    body_type,
    server_ts;

-- name: GetMessageByID :one
SELECT
    id,
    conversation_id,
    sender_phone,
    body,
    media_url,
    created_at,
    recipient_phone,
    group_id,
    body_encrypted,
    body_type,
    server_ts
FROM messages
WHERE id = $1;

-- name: GetMessageHistory :many
SELECT
    m.id,
    m.conversation_id,
    m.sender_phone,
    m.body,
    m.media_url,
    m.created_at,
    m.recipient_phone,
    m.group_id,
    m.body_encrypted,
    m.body_type,
    m.server_ts
FROM messages m
LEFT JOIN messages cursor_msg ON cursor_msg.id = $2
WHERE m.conversation_id = $1
  AND ($2::text = '' OR m.created_at < cursor_msg.created_at)
ORDER BY m.created_at DESC, m.id DESC
LIMIT $3;

-- name: UpsertMessageStatus :exec
INSERT INTO message_status (message_id, user_phone, status, updated_at)
VALUES ($1, $2, $3, NOW())
ON CONFLICT (message_id, user_phone)
DO UPDATE SET status = EXCLUDED.status, updated_at = NOW();

-- name: GetGroupMembers :many
SELECT user_phone
FROM group_members
WHERE group_id = $1
ORDER BY created_at ASC;

