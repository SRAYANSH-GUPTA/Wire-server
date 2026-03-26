ALTER TABLE messages
    ADD COLUMN IF NOT EXISTS recipient_id TEXT,
    ADD COLUMN IF NOT EXISTS group_id TEXT,
    ADD COLUMN IF NOT EXISTS body_encrypted TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS body_type TEXT NOT NULL DEFAULT 'text',
    ADD COLUMN IF NOT EXISTS server_ts TIMESTAMPTZ NOT NULL DEFAULT NOW();

ALTER TABLE messages
    ALTER COLUMN id SET DEFAULT gen_random_uuid()::text;

UPDATE messages
SET body_encrypted = body
WHERE body_encrypted = '';

CREATE INDEX IF NOT EXISTS idx_messages_conversation_cursor ON messages (conversation_id, created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_messages_group_cursor ON messages (group_id, created_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS message_status (
    message_id TEXT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    user_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('sent', 'delivered', 'read')),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (message_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_message_status_user_updated_at ON message_status (user_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS group_members (
    group_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (group_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_group_members_group ON group_members (group_id);

