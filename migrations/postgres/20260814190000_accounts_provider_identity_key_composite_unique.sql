/* ------------------------------ */

DROP INDEX idx_accounts_provider_identity_key;
CREATE UNIQUE INDEX idx_uniq_accounts_provider_provider_identity_key
    ON accounts (provider, provider_identity_key);

/* ------------------------------ */

