/* ------------------------------ */

ALTER TABLE tasks
   ADD COLUMN last_event_id INTEGER
   REFERENCES task_events(event_id);

/* ------------------------------ */

ALTER TABLE flows
   ADD COLUMN last_event_id INTEGER
   REFERENCES flow_events(event_id);

/* ------------------------------ */
