CREATE TYPE account_origin AS ENUM ('cli', 'github');

CREATE TABLE accounts(
    id VARCHAR(100) NOT NULL,
    PRIMARY KEY (id),
    email VARCHAR(320) NOT NULL UNIQUE,
    account_name VARCHAR(100) NOT NULL,
    display_name VARCHAR(200) NOT NULL,
    origin account_origin NOT NULL,
    registered_at timestamptz NOT NULL
);

CREATE INDEX accounts_name_idx ON accounts (account_name);