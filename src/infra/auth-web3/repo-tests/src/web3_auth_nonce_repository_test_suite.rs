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
            Err(GetNonceError::NotFound(NonceNotFoundError {
                wallet: wallet_address
            })),
            harness.nonce_repo.get_nonce(&wallet_address).await
        );

        pretty_assertions::assert_eq!(Ok(()), harness.nonce_repo.set_nonce(&nonce_entity).await,);

        pretty_assertions::assert_eq!(
            Ok(&nonce_entity),
            harness.nonce_repo.get_nonce(&wallet_address).await.as_ref()
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
            harness.nonce_repo.set_nonce(&updated_nonce_entity).await,
        );

        pretty_assertions::assert_eq!(
            Ok(updated_nonce_entity),
            harness.nonce_repo.get_nonce(&wallet_address).await
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_consume_nonce(catalog: &dill::Catalog) {
    let harness = Web3AuthNonceRepositoryTestSuiteHarness::new(catalog);

    let t0 = Utc.with_ymd_and_hms(2050, 1, 2, 3, 4, 5).unwrap();
    let wallet_address = EvmWalletAddress::random();
    let nonce_entity = Web3AuthEip4361NonceEntity {
        wallet_address,
        nonce: Web3AuthenticationEip4361Nonce::new(),
        expires_at: t0,
    };

    {
        pretty_assertions::assert_eq!(Ok(()), harness.nonce_repo.set_nonce(&nonce_entity).await,);

        pretty_assertions::assert_eq!(
            Ok(&nonce_entity),
            harness.nonce_repo.get_nonce(&wallet_address).await.as_ref()
        );
    }
    {
        // Simulate a future consumption attempt (expired nonce)
        pretty_assertions::assert_eq!(
            Err(ConsumeNonceError::NotFound(NonceNotFoundError {
                wallet: wallet_address
            })),
            harness
                .nonce_repo
                .consume_nonce(&wallet_address, t0 + Duration::seconds(15))
                .await
        );

        pretty_assertions::assert_eq!(
            Ok(&nonce_entity),
            harness.nonce_repo.get_nonce(&wallet_address).await.as_ref()
        );
    }
    {
        // Consuming unexpired nonce ...
        let t_before_expiration = t0 - Duration::seconds(15);

        pretty_assertions::assert_eq!(
            Ok(()),
            harness
                .nonce_repo
                .consume_nonce(&wallet_address, t_before_expiration)
                .await
        );

        pretty_assertions::assert_eq!(
            Err(GetNonceError::NotFound(NonceNotFoundError {
                wallet: wallet_address
            })),
            harness.nonce_repo.get_nonce(&wallet_address).await
        );

        // ... and we guarantee the consumption is disposable
        pretty_assertions::assert_eq!(
            Err(ConsumeNonceError::NotFound(NonceNotFoundError {
                wallet: wallet_address
            })),
            harness
                .nonce_repo
                .consume_nonce(&wallet_address, t_before_expiration)
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
        pretty_assertions::assert_eq!(Ok(()), harness.nonce_repo.set_nonce(&nonce_1_expired).await,);
        pretty_assertions::assert_eq!(Ok(()), harness.nonce_repo.set_nonce(&nonce_2).await,);

        pretty_assertions::assert_eq!(Ok(()), harness.nonce_repo.cleanup_expired_nonces(t0).await,);

        pretty_assertions::assert_eq!(
            Err(GetNonceError::NotFound(NonceNotFoundError {
                wallet: nonce_1_expired.wallet_address
            })),
            harness
                .nonce_repo
                .get_nonce(&nonce_1_expired.wallet_address)
                .await
        );
        pretty_assertions::assert_eq!(
            Ok(&nonce_2),
            harness
                .nonce_repo
                .get_nonce(&nonce_2.wallet_address)
                .await
                .as_ref()
        );
    }
    {
        let t1 = t0 + Duration::minutes(20);

        pretty_assertions::assert_eq!(Ok(()), harness.nonce_repo.cleanup_expired_nonces(t1).await,);

        pretty_assertions::assert_eq!(
            Err(GetNonceError::NotFound(NonceNotFoundError {
                wallet: nonce_2.wallet_address
            })),
            harness.nonce_repo.get_nonce(&nonce_2.wallet_address).await
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct Web3AuthNonceRepositoryTestSuiteHarness {
    pub nonce_repo: Arc<dyn Web3AuthEip4361NonceRepository>,
}

impl Web3AuthNonceRepositoryTestSuiteHarness {
    pub fn new(catalog: &dill::Catalog) -> Self {
        Self {
            nonce_repo: catalog.get_one().unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
