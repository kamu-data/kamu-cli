UPDATE accounts
    SET email = account_name || '@example.com'
    WHERE email IS NULL;

ALTER TABLE accounts
    ALTER COLUMN email SET NOT NULL;
