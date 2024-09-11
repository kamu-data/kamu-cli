/* ------------------------------ */

ALTER TABLE tasks
   ADD COLUMN last_event_id BIGINT
   REFERENCES task_events(event_id);

/* ------------------------------ */

ALTER TABLE flows
   ADD COLUMN last_event_id BIGINT
   REFERENCES flow_events(event_id);

/* ------------------------------ */
