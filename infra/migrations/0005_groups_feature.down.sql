DROP INDEX IF EXISTS idx_group_members_user;
DROP INDEX IF EXISTS idx_groups_created_by;

ALTER TABLE group_members
    DROP CONSTRAINT IF EXISTS group_members_role_check;

ALTER TABLE group_members
    DROP COLUMN IF EXISTS added_by,
    DROP COLUMN IF EXISTS role;

DROP TABLE IF EXISTS groups;

