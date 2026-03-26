DROP INDEX IF EXISTS idx_group_members_group;
DROP TABLE IF EXISTS group_members;

DROP INDEX IF EXISTS idx_message_status_user_updated_at;
DROP TABLE IF EXISTS message_status;

DROP INDEX IF EXISTS idx_messages_group_cursor;
DROP INDEX IF EXISTS idx_messages_conversation_cursor;

ALTER TABLE messages
    DROP COLUMN IF EXISTS recipient_id,
    DROP COLUMN IF EXISTS group_id,
    DROP COLUMN IF EXISTS body_encrypted,
    DROP COLUMN IF EXISTS body_type,
    DROP COLUMN IF EXISTS server_ts;

