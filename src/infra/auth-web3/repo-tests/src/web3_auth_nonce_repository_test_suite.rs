// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{Duration, TimeZone, Utc};
use kamu_auth_web3::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_set_and_get_nonce(catalog: &dill::Catalog) {
    let harness = Web3AuthNonceRepositoryTestSuiteHarness::new(catalog);

    let t0 = Utc.with_ymd_and_hms(2050, 1, 2, 3, 4, 5).unwrap();
    let wallet_address = EvmWalletAddress::random();

    {
        let nonce_entity = Web3AuthEip4361NonceEntity {
            wallet_address,
            nonce: Web3AuthenticationEip4361Nonce::new(),
            expires_at: t0,
        };

        pretty_assertions::assert_eq!(
            Err(GetNonceError::NotFound {
                wallet: wallet_address
            }),
            harness
                .web3_auth_nonce_repo
                .get_nonce(&wallet_address)
                .await
        );

        pretty_assertions::assert_eq!(
            Ok(()),
            harness.web3_auth_nonce_repo.set_nonce(&nonce_entity).await,
        );

        pretty_assertions::assert_eq!(
            Ok(&nonce_entity),
            harness
                .web3_auth_nonce_repo
                .get_nonce(&wallet_address)
                .await
                .as_ref()
        );
    }
    {
        let updated_nonce_entity = Web3AuthEip4361NonceEntity {
            wallet_address,
            nonce: Web3AuthenticationEip4361Nonce::new(),
            expires_at: t0 + Duration::minutes(15),
        };

        pretty_assertions::assert_eq!(
            Ok(()),
            harness
                .web3_auth_nonce_repo
                .set_nonce(&updated_nonce_entity)
                .await,
        );

        pretty_assertions::assert_eq!(
            Ok(updated_nonce_entity),
            harness
                .web3_auth_nonce_repo
                .get_nonce(&wallet_address)
                .await
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_cleanup_expired_nonces(catalog: &dill::Catalog) {
    let harness = Web3AuthNonceRepositoryTestSuiteHarness::new(catalog);

    let t0 = Utc.with_ymd_and_hms(2050, 1, 2, 12, 0, 0).unwrap();

    let nonce_1_expired = Web3AuthEip4361NonceEntity {
        wallet_address: EvmWalletAddress::random(),
        nonce: Web3AuthenticationEip4361Nonce::new(),
        expires_at: t0 - Duration::seconds(1),
    };
    let nonce_2 = Web3AuthEip4361NonceEntity {
        wallet_address: EvmWalletAddress::random(),
        nonce: Web3AuthenticationEip4361Nonce::new(),
        expires_at: t0 + Duration::minutes(15),
    };

    {
        pretty_assertions::assert_eq!(
            Ok(()),
            harness
                .web3_auth_nonce_repo
                .set_nonce(&nonce_1_expired)
                .await,
        );
        pretty_assertions::assert_eq!(
            Ok(()),
            harness.web3_auth_nonce_repo.set_nonce(&nonce_2).await,
        );

        pretty_assertions::assert_eq!(
            Ok(()),
            harness
                .web3_auth_nonce_repo
                .cleanup_expired_nonces(t0)
                .await,
        );

        pretty_assertions::assert_eq!(
            Err(GetNonceError::NotFound {
                wallet: nonce_1_expired.wallet_address
            }),
            harness
                .web3_auth_nonce_repo
                .get_nonce(&nonce_1_expired.wallet_address)
                .await
        );
        pretty_assertions::assert_eq!(
            Ok(&nonce_2),
            harness
                .web3_auth_nonce_repo
                .get_nonce(&nonce_2.wallet_address)
                .await
                .as_ref()
        );
    }
    {
        let t1 = t0 + Duration::minutes(20);

        pretty_assertions::assert_eq!(
            Ok(()),
            harness
                .web3_auth_nonce_repo
                .cleanup_expired_nonces(t1)
                .await,
        );

        pretty_assertions::assert_eq!(
            Err(GetNonceError::NotFound {
                wallet: nonce_1_expired.wallet_address
            }),
            harness
                .web3_auth_nonce_repo
                .get_nonce(&nonce_1_expired.wallet_address)
                .await
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct Web3AuthNonceRepositoryTestSuiteHarness {
    pub web3_auth_nonce_repo: Arc<dyn Web3AuthNonceRepository>,
}

impl Web3AuthNonceRepositoryTestSuiteHarness {
    pub fn new(catalog: &dill::Catalog) -> Self {
        Self {
            web3_auth_nonce_repo: catalog.get_one().unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
