CREATE TABLE dataset_secret_keys(
   dataset_id VARCHAR(100) NOT NULL REFERENCES dataset_entries(dataset_id) ON DELETE CASCADE,
   secret_nonce BLOB NOT NULL,
   secret_key BLOB NOT NULL
);

CREATE TABLE account_secret_keys(
   account_id VARCHAR(100) NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
   secret_nonce BLOB NOT NULL,
   secret_key BLOB NOT NULL
);