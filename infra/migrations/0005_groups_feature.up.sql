CREATE TABLE IF NOT EXISTS groups (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT NULL,
    avatar_url TEXT NULL,
    created_by TEXT NOT NULL,
    invite_code TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE group_members
    ADD COLUMN IF NOT EXISTS role TEXT NOT NULL DEFAULT 'member',
    ADD COLUMN IF NOT EXISTS added_by TEXT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'group_members_role_check'
    ) THEN
        ALTER TABLE group_members
            ADD CONSTRAINT group_members_role_check CHECK (role IN ('admin', 'member'));
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_groups_created_by ON groups (created_by, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_group_members_user ON group_members (user_id, created_at DESC);

