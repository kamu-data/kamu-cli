/* ------------------------------ */

CREATE TABLE web3_auth_eip4361_nonces
(
    wallet_address VARCHAR(100) NOT NULL PRIMARY KEY,
    nonce          VARCHAR(32)  NOT NULL,
    expires_at     TEXT         NOT NULL
);

/* ------------------------------ */
