/*
 * A deleted resource keeps its name. Completing a deletion used to rename the
 * resource to `deleted-<id>`; the name it had before is restored from the last
 * event that set it. Stored `Deleted` events keep their `tombstone_name`, which
 * replaying them ignores.
 */

UPDATE resources
SET resource_name = COALESCE(
    (
        SELECT CASE e.event_type
            WHEN 'Created'        THEN json_extract(e.event_payload, '$.Created.headers.name')
            WHEN 'HeadersUpdated' THEN json_extract(e.event_payload, '$.HeadersUpdated.new_headers.name')
        END
        FROM resource_events e
        WHERE e.resource_id = resources.resource_id
          AND e.event_type IN ('Created', 'HeadersUpdated')
        ORDER BY e.event_id DESC
        LIMIT 1
    ),
    resource_name
)
WHERE deleted_at IS NOT NULL;
