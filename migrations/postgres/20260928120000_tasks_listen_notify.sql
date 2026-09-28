/* ------------------------------ */

-- Wake the task agent only when a task enters the queue: on creation or requeue

CREATE OR REPLACE FUNCTION notify_tasks_queued()
    RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('tasks_queued', '');
    RETURN NULL;
END $$;

CREATE TRIGGER tasks_insert_notify
    AFTER INSERT ON tasks
    FOR EACH STATEMENT EXECUTE FUNCTION notify_tasks_queued();

-- Row-level to filter by status; Postgres collapses duplicate notifications per transaction
CREATE TRIGGER tasks_requeue_notify
    AFTER UPDATE OF task_status ON tasks
    FOR EACH ROW
    WHEN (NEW.task_status = 'queued'::task_status_type AND OLD.task_status IS DISTINCT FROM NEW.task_status)
    EXECUTE FUNCTION notify_tasks_queued();

/* ------------------------------ */
