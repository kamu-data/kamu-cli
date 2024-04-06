CREATE TABLE accounts(
    id VARCHAR(100) NOT NULL,
    PRIMARY KEY (id),
    email VARCHAR(320) NOT NULL UNIQUE,
    account_name VARCHAR(100) NOT NULL,
    display_name VARCHAR(200) NOT NULL,
    origin ENUM('cli', 'github') NOT NULL,
    registered_at TIMESTAMP(6) NOT NULL
);

CREATE INDEX accounts_name_idx ON accounts (account_name);
