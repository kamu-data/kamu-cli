
/* ------------------------------ */

ALTER TABLE flows
   ADD COLUMN scheduled_for_activation_at TIMESTAMPTZ;

/* ------------------------------ */
