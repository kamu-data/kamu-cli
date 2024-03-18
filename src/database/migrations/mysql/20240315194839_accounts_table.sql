CREATE TABLE accounts(
    id uuid NOT NULL,
    PRIMARY KEY (id),
    email TEXT NOT NULL UNIQUE,
    account_name TEXT NOT NULL,
    display_name TEXT NOT NULL,
    origin ENUM('cli', 'github') NOT NULL,
    registered_at TIMESTAMP NOT NULL
);

CREATE INDEX accounts_name_idx ON accounts (account_name);
