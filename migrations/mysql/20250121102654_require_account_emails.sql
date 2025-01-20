UPDATE accounts
    SET email = CONCAT(account_name, '@example.com')
    WHERE email IS NULL;

ALTER TABLE accounts
    MODIFY email VARCHAR(320) NOT NULL;