/* ------------------------------ */

WITH computed_prefix AS (SELECT id,
                                CONCAT_WS('.',
                                          SPLIT_PART(id, ':', 1),
                                          SPLIT_PART(id, ':', 2),
                                          SPLIT_PART(id, ':', 3),
                                          SPLIT_PART(id, ':', 4)
                                ) || '.' AS prefix -- did.pkh.eip155.1.
                         FROM accounts
                         WHERE provider = 'web3_wallet'
                           AND email NOT ILIKE 'did.pkh.%') -- exclude already processed accounts
UPDATE accounts a
SET email                 = p.prefix || a.email,
    account_name          = p.prefix || a.account_name,
    provider_identity_key = p.prefix || a.provider_identity_key
FROM computed_prefix p
WHERE a.id = p.id;

/* ------------------------------ */
