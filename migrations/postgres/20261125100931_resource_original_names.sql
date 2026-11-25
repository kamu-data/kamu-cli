/*
 * A deleted resource keeps its name. Completing a deletion used to rename the
 * resource to `deleted-<id>`; the name it had before is restored from the last
 * event that set it. Stored `Deleted` events keep their `tombstone_name`, which
 * replaying them ignores.
 */

UPDATE resources r
SET resource_name = named.resource_name
FROM (
    SELECT DISTINCT ON (resource_id)
        resource_id,
        CASE event_type
            WHEN 'Created'        THEN event_payload #>> '{Created,headers,name}'
            WHEN 'HeadersUpdated' THEN event_payload #>> '{HeadersUpdated,new_headers,name}'
        END AS resource_name
    FROM resource_events
    WHERE event_type IN ('Created', 'HeadersUpdated')
    ORDER BY resource_id, event_id DESC
) named
WHERE r.resource_id = named.resource_id
  AND r.deleted_at IS NOT NULL
  AND named.resource_name IS NOT NULL;
