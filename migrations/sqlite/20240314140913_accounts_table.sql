CREATE TABLE accounts 
(
    id VARCHAR(100) NOT NULL PRIMARY KEY,
    email VARCHAR(320) NOT NULL UNIQUE,
    account_name VARCHAR(100) NOT NULL,
    display_name VARCHAR(200) NOT NULL,
    origin VARCHAR(10) CHECK( origin IN ('cli', 'github') ) NOT NULL,
    registered_at timestamptz NOT NULL
);

CREATE INDEX accounts_name_idx ON accounts (account_name);