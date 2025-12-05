// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{SubsecRound, Utc};
use kamu_accounts::AccessToken;
use rand::Rng;
use uuid::Uuid;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn make_test_access_token(
    name: &str,
    token_hash_maybe: Option<[u8; 32]>,
    account_name: &str,
) -> AccessToken {
    AccessToken {
        id: Uuid::new_v4(),
        token_name: name.to_string(),
        token_hash: token_hash_maybe.unwrap_or(generate_random_bytes()),
        created_at: Utc::now().round_subsecs(6),
        revoked_at: None,
        account_id: odf::AccountID::new_seeded_ed25519(account_name.as_bytes()),
    }
}

pub(crate) fn generate_random_bytes() -> [u8; 32] {
    let mut random_bytes = [0_u8; 32];
    rand::rng().fill(&mut random_bytes);
    random_bytes
}
