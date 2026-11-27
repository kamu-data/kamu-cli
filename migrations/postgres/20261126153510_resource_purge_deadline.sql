/*
 * A deleted resource may be purged once its deadline passes. Resources deleted
 * before deadlines were kept get the framework's default retention of 7 days:
 * a migration cannot read the node's configuration.
 */

ALTER TABLE resources ADD COLUMN purge_at TIMESTAMPTZ NULL;

UPDATE resources
SET purge_at = deleted_at + INTERVAL '7 days'
WHERE deleted_at IS NOT NULL;

CREATE INDEX idx_resources_purge_at
    ON resources (purge_at)
    WHERE purge_at IS NOT NULL;
