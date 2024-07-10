CREATE SEQUENCE outbox_message_id_seq AS BIGINT;

CREATE TABLE outbox_messages(
    message_id BIGINT PRIMARY KEY DEFAULT NEXTVAL('outbox_message_id_seq'),
    message_type VARCHAR(200) NOT NULL,
    producer_name VARCHAR(200) NOT NULL,
    content_json JSONB NOT NULL,
    occurred_on timestamptz NOT NULL
);

CREATE INDEX outbox_messages_producer_name_idx ON outbox_messages(producer_name);

CREATE TABLE outbox_message_consumptions(
    consumer_name VARCHAR(200) NOT NULL,
    producer_name VARCHAR(200) NOT NULL,
    last_consumed_message_id BIGINT NOT NULL,
    PRIMARY KEY (consumer_name, producer_name)
);
