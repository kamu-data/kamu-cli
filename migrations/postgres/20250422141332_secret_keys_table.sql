CREATE TABLE dataset_secret_keys(
   id UUID PRIMARY KEY,
   dataset_id VARCHAR(100) NOT NULL REFERENCES dataset_entries(dataset_id) ON DELETE CASCADE,
   secret_nonce BYTEA NOT NULL,
   secret_key BYTEA NOT NULL
);

CREATE TABLE account_secret_keys(
   id UUID PRIMARY KEY,
   account_id VARCHAR(100) NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
   secret_nonce BYTEA NOT NULL,
   secret_key BYTEA NOT NULL
);
