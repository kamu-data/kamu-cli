/* ------------------------------ */

-- Web3 addresses are checksumed, so their case must be preserved.
UPDATE dataset_entries
SET owner_name = LOWER(owner_name)
FROM accounts a
WHERE owner_id = a.id
  AND a.provider <> 'web3_wallet';

/* ------------------------------ */
