-- 0006_phone_refactor.up.sql

-- 1. Update conversations table
-- Already generic enough, but let's ensure indexes are okay if we ever use phone-based matching here.

-- 2. Update conversation_members table
ALTER TABLE conversation_members RENAME COLUMN user_id TO user_phone;
ALTER TABLE conversation_members ALTER COLUMN user_phone TYPE TEXT;

-- 3. Update messages table
ALTER TABLE messages RENAME COLUMN sender_id TO sender_phone;
ALTER TABLE messages RENAME COLUMN recipient_id TO recipient_phone;
ALTER TABLE messages ALTER COLUMN sender_phone TYPE TEXT;
ALTER TABLE messages ALTER COLUMN recipient_phone TYPE TEXT;

-- 4. Update message_status table
ALTER TABLE message_status RENAME COLUMN user_id TO user_phone;
ALTER TABLE message_status ALTER COLUMN user_phone TYPE TEXT;

-- 5. Update group_members table
ALTER TABLE group_members RENAME COLUMN user_id TO user_phone;
ALTER TABLE group_members ALTER COLUMN user_phone TYPE TEXT;

-- 6. Update message_receipts table
ALTER TABLE message_receipts RENAME COLUMN user_id TO user_phone;
ALTER TABLE message_receipts ALTER COLUMN user_phone TYPE TEXT;

-- 7. Update user_presence table
ALTER TABLE user_presence RENAME COLUMN user_id TO user_phone;
ALTER TABLE user_presence ALTER COLUMN user_phone TYPE TEXT;

-- 8. Update media_objects table
ALTER TABLE media_objects RENAME COLUMN owner_id TO owner_phone;
ALTER TABLE media_objects ALTER COLUMN owner_phone TYPE TEXT;

-- 9. Update groups table
ALTER TABLE groups RENAME COLUMN created_by TO created_by_phone;
ALTER TABLE groups ALTER COLUMN created_by_phone TYPE TEXT;

-- Indexes
DROP INDEX IF EXISTS idx_messages_sender_id;
DROP INDEX IF EXISTS idx_messages_recipient_id;
CREATE INDEX IF NOT EXISTS idx_messages_sender_phone ON messages(sender_phone);
CREATE INDEX IF NOT EXISTS idx_messages_recipient_phone ON messages(recipient_phone);

DROP INDEX IF EXISTS idx_message_status_user_updated_at;
CREATE INDEX IF NOT EXISTS idx_message_status_phone_updated_at ON message_status (user_phone, updated_at DESC);

DROP INDEX IF EXISTS idx_group_members_group;
CREATE INDEX IF NOT EXISTS idx_group_members_group_phone ON group_members (group_id, user_phone);
