CREATE TABLE dataset_did_secret_keys(
   id VARCHAR(100) NOT NULL PRIMARY KEY,
   dataset_id VARCHAR(100) NOT NULL REFERENCES dataset_entries(dataset_id) ON DELETE CASCADE,
   secret_nonce BLOB NOT NULL,
   secret_key BLOB NOT NULL
);

CREATE TABLE account_did_secret_keys(
   id VARCHAR(100) NOT NULL PRIMARY KEY,
   account_id VARCHAR(100) NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
   secret_nonce BLOB NOT NULL,
   secret_key BLOB NOT NULL
);