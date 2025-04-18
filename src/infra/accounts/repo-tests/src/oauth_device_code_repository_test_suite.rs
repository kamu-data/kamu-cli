// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::sync::Arc;

use chrono::{DateTime, Duration, SubsecRound, TimeZone, Utc};
use kamu_accounts::*;
use kamu_accounts_services::DEVICE_CODE_EXPIRES_IN_5_MINUTES;

use crate::accounts_repository_test_utils::make_test_account;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_save_device_code(catalog: &dill::Catalog) {
    let harness = OAuthDeviceCodeRepositoryTestSuiteHarness::new(catalog);

    let new_device_token = {
        let now = Utc::now();

        DeviceTokenCreated {
            device_code: DeviceCode::new_uuid_v4(),
            created_at: now,
            expires_at: now + Duration::minutes(5),
        }
    };

    assert_matches!(
        harness
            .oauth_device_code_repo
            .save_device_code(&new_device_token)
            .await,
        Ok(_)
    );

    pretty_assertions::assert_eq!(
        Err(CreateDeviceCodeError::Duplicate(DeviceCodeDuplicateError {
            device_code: new_device_token.device_code.clone()
        })),
        harness
            .oauth_device_code_repo
            .save_device_code(&new_device_token)
            .await,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_update_device_token_with_token_params_part(catalog: &dill::Catalog) {
    let harness = OAuthDeviceCodeRepositoryTestSuiteHarness::new(catalog);

    let not_saved_token_device_code = DeviceCode::new_uuid_v4();
    let [user] = harness.create_accounts(["user"]).await;
    let device_token_params_part = DeviceTokenParamsPart {
        iat: 100,
        exp: 500,
        account_id: user.id,
    };

    pretty_assertions::assert_eq!(
        Err(UpdateDeviceCodeWithTokenParamsPartError::NotFound(
            DeviceTokenNotFoundError {
                device_code: not_saved_token_device_code.clone()
            }
        )),
        harness
            .oauth_device_code_repo
            .update_device_token_with_token_params_part(
                &not_saved_token_device_code,
                &device_token_params_part
            )
            .await,
    );

    let [device_token] = harness
        .make_saved_device_tokens_from_expired_time_points([Utc::now() + Duration::minutes(5)])
        .await;

    assert_matches!(
        harness
            .oauth_device_code_repo
            .update_device_token_with_token_params_part(
                device_token.device_code(),
                &device_token_params_part
            )
            .await,
        Ok(_)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_find_device_token_by_device_code(catalog: &dill::Catalog) {
    let harness = OAuthDeviceCodeRepositoryTestSuiteHarness::new(catalog);

    let not_saved_token_device_code = DeviceCode::new_uuid_v4();

    pretty_assertions::assert_eq!(
        Err(FindDeviceTokenByDeviceCodeError::NotFound(
            DeviceTokenNotFoundError {
                device_code: not_saved_token_device_code.clone()
            }
        )),
        harness
            .oauth_device_code_repo
            .find_device_token_by_device_code(&not_saved_token_device_code)
            .await,
    );

    let [device_token] = harness
        .make_saved_device_tokens_from_expired_time_points([Utc::now() + Duration::minutes(5)])
        .await;

    pretty_assertions::assert_eq!(
        Ok(&device_token),
        harness
            .oauth_device_code_repo
            .find_device_token_by_device_code(device_token.device_code())
            .await
            .as_ref()
    );

    let [user] = harness.create_accounts(["user"]).await;
    let [device_token_with_access_token] = harness
        .issue_access_tokens_for_device_tokens([(device_token, user)])
        .await;

    pretty_assertions::assert_eq!(
        Ok(&device_token_with_access_token),
        harness
            .oauth_device_code_repo
            .find_device_token_by_device_code(device_token_with_access_token.device_code())
            .await
            .as_ref()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_cleanup_expired_device_codes(catalog: &dill::Catalog) {
    let harness = OAuthDeviceCodeRepositoryTestSuiteHarness::new(catalog);

    let half_period = DEVICE_CODE_EXPIRES_IN_5_MINUTES / 2;

    let t0 = Utc.with_ymd_and_hms(2050, 1, 2, 12, 0, 0).unwrap();
    let t1 = t0 + half_period * 1;
    let t2 = t0 + half_period * 2;
    let t3 = t0 + half_period * 3;
    let t4 = t0 + half_period * 4;

    //   t0    t1    t2    t3    t4
    //   |     |     |     |     |
    // 0 --------------------------> t
    //   [  token_1  ]     .     .
    //         [  token_2  ]     .
    //         [  token_3  ]     .
    //               [  token_4  ]

    // Guarantee that device tokens will be exactly expired at the time of the event
    let expire_drift = Duration::seconds(5);

    let [token_1_t2, token_2_t3, token_3_t3, token_4_t4] = harness
        .make_saved_device_tokens_from_expired_time_points([
            t2 - expire_drift,
            t3 - expire_drift,
            t3 - expire_drift,
            t4 - expire_drift,
        ])
        .await;

    pretty_assertions::assert_eq!(
        [true, true, true, true],
        harness
            .are_device_tokens_exist([&token_1_t2, &token_2_t3, &token_3_t3, &token_4_t4])
            .await
    );

    // t0
    {
        harness
            .cleanup_expired_device_tokens_at_time(t0, "t0")
            .await;

        pretty_assertions::assert_eq!(
            [true, true, true, true],
            harness
                .are_device_tokens_exist([&token_1_t2, &token_2_t3, &token_3_t3, &token_4_t4])
                .await
        );
    }

    // t1
    {
        harness
            .cleanup_expired_device_tokens_at_time(t1, "t1")
            .await;

        pretty_assertions::assert_eq!(
            [true, true, true, true],
            harness
                .are_device_tokens_exist([&token_1_t2, &token_2_t3, &token_3_t3, &token_4_t4])
                .await
        );
    }

    let [user1, user2] = harness.create_accounts(["user1", "user2"]).await;
    let [token_3_t3_with_access_token, token_4_t4_with_access_token] = harness
        .issue_access_tokens_for_device_tokens([(token_3_t3, user1), (token_4_t4, user2)])
        .await;

    // t2
    {
        harness
            .cleanup_expired_device_tokens_at_time(t2, "t2")
            .await;

        pretty_assertions::assert_eq!(
            [false, true, true, true],
            harness
                .are_device_tokens_exist([
                    &token_1_t2,
                    &token_2_t3,
                    &token_3_t3_with_access_token,
                    &token_4_t4_with_access_token
                ])
                .await
        );
    }

    // t3
    {
        harness
            .cleanup_expired_device_tokens_at_time(t3, "t3")
            .await;

        pretty_assertions::assert_eq!(
            [false, false, false, true],
            harness
                .are_device_tokens_exist([
                    &token_1_t2,
                    &token_2_t3,
                    &token_3_t3_with_access_token,
                    &token_4_t4_with_access_token
                ])
                .await
        );
    }

    // t4
    {
        harness
            .cleanup_expired_device_tokens_at_time(t4, "t4")
            .await;

        pretty_assertions::assert_eq!(
            [false, false, false, false],
            harness
                .are_device_tokens_exist([
                    &token_1_t2,
                    &token_2_t3,
                    &token_3_t3_with_access_token,
                    &token_4_t4_with_access_token
                ])
                .await
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct OAuthDeviceCodeRepositoryTestSuiteHarness {
    pub oauth_device_code_repo: Arc<dyn OAuthDeviceCodeRepository>,
    pub account_repo: Arc<dyn AccountRepository>,
}

impl OAuthDeviceCodeRepositoryTestSuiteHarness {
    pub fn new(catalog: &dill::Catalog) -> Self {
        Self {
            oauth_device_code_repo: catalog.get_one().unwrap(),
            account_repo: catalog.get_one().unwrap(),
        }
    }

    pub async fn make_saved_device_tokens_from_expired_time_points<const N: usize>(
        &self,
        expired_at: [DateTime<Utc>; N],
    ) -> [DeviceToken; N] {
        use std::mem::MaybeUninit;

        let mut device_tokens: [MaybeUninit<DeviceToken>; N] = MaybeUninit::uninit_array();

        for (i, t) in expired_at.into_iter().enumerate() {
            let new_token = DeviceTokenCreated {
                device_code: DeviceCode::new_uuid_v4(),
                // Postgres stores 6 decimal places in the timestamptz type
                created_at: (t - DEVICE_CODE_EXPIRES_IN_5_MINUTES).round_subsecs(6),
                expires_at: t.round_subsecs(6),
            };

            assert_matches!(
                self.oauth_device_code_repo
                    .save_device_code(&new_token)
                    .await,
                Ok(_),
                "Tag: {i}",
            );

            device_tokens[i].write(new_token.into());
        }

        unsafe { device_tokens.map(|item| item.assume_init()) }
    }

    pub async fn are_device_tokens_exist<const N: usize>(
        &self,
        device_tokens: [&DeviceToken; N],
    ) -> [bool; N] {
        let mut report = [true; N];

        for (i, device_token) in device_tokens.iter().enumerate() {
            let exists = match self
                .oauth_device_code_repo
                .find_device_token_by_device_code(device_token.device_code())
                .await
            {
                Ok(_) => true,
                Err(FindDeviceTokenByDeviceCodeError::NotFound(_)) => false,
                e => panic!("Unexpected error: {e:?}"),
            };

            report[i] = exists;
        }

        report
    }

    pub async fn create_accounts<const N: usize>(&self, account_names: [&str; N]) -> [Account; N] {
        use std::mem::MaybeUninit;

        let mut accounts: [MaybeUninit<Account>; N] = MaybeUninit::uninit_array();

        for (i, account_name) in account_names.into_iter().enumerate() {
            let new_account = make_test_account(
                account_name,
                &format!("{account_name}@example.com"),
                kamu_adapter_oauth::PROVIDER_GITHUB,
                account_name,
            );

            assert_matches!(
                self.account_repo.create_account(&new_account).await,
                Ok(_),
                "Tag: {account_name}",
            );

            accounts[i].write(new_account);
        }

        unsafe { accounts.map(|item| item.assume_init()) }
    }

    pub async fn issue_access_tokens_for_device_tokens<const N: usize>(
        &self,
        device_token_and_account: [(DeviceToken, Account); N],
    ) -> [DeviceToken; N] {
        use std::mem::MaybeUninit;

        let mut device_tokens: [MaybeUninit<DeviceToken>; N] = MaybeUninit::uninit_array();

        for (i, (device_token, account)) in device_token_and_account.into_iter().enumerate() {
            let device_token_params_part = DeviceTokenParamsPart {
                iat: 100,
                exp: 500,
                account_id: account.id,
            };

            assert_matches!(
                self.oauth_device_code_repo
                    .update_device_token_with_token_params_part(
                        device_token.device_code(),
                        &device_token_params_part
                    )
                    .await,
                Ok(_),
                "Tag: {}",
                account.account_name
            );

            let device_token = device_token.with_token_params_part(device_token_params_part);

            device_tokens[i].write(device_token);
        }

        unsafe { device_tokens.map(|item| item.assume_init()) }
    }

    async fn cleanup_expired_device_tokens_at_time(&self, t: DateTime<Utc>, tag: &str) {
        assert_matches!(
            self.oauth_device_code_repo
                .cleanup_expired_device_codes(t)
                .await,
            Ok(_),
            "Tag: {tag}",
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
