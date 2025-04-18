// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use chrono::{DateTime, Duration, TimeZone, Utc};
use kamu_accounts::*;
use kamu_accounts_services::DEVICE_CODE_EXPIRES_IN_5_MINUTES;

use crate::accounts_repository_test_utils::make_test_account;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_save_device_code(catalog: &dill::Catalog) {
    let oauth_device_code_repo = catalog.get_one::<dyn OAuthDeviceCodeRepository>().unwrap();

    let new_device_token = {
        let now = Utc::now();

        DeviceTokenCreated {
            device_code: DeviceCode::new_uuid_v4(),
            created_at: now,
            expires_at: now + Duration::minutes(5),
        }
    };

    assert_matches!(
        oauth_device_code_repo
            .save_device_code(&new_device_token)
            .await,
        Ok(_)
    );

    assert_matches!(
        oauth_device_code_repo.save_device_code(&new_device_token).await,
        Err(CreateDeviceCodeError::Duplicate(e)) if e.device_code == new_device_token.device_code
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_update_device_token_with_token_params_part(catalog: &dill::Catalog) {
    let oauth_device_code_repo = catalog.get_one::<dyn OAuthDeviceCodeRepository>().unwrap();
    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();

    let not_saved_token_device_code = DeviceCode::new_uuid_v4();
    let account_id = {
        let account = make_test_account(
            "test",
            "test@example.com",
            kamu_adapter_oauth::PROVIDER_GITHUB,
            "test",
        );

        assert_matches!(account_repo.create_account(&account).await, Ok(_));

        account.id
    };
    let device_token_params_part = DeviceTokenParamsPart {
        iat: 100,
        exp: 500,
        account_id,
    };

    assert_matches!(
        oauth_device_code_repo
            .update_device_token_with_token_params_part(
                &not_saved_token_device_code,
                &device_token_params_part
            )
            .await,
        Err(UpdateDeviceCodeWithTokenParamsPartError::NotFound(e)) if e.device_code == not_saved_token_device_code
    );

    let new_saved_device_token = {
        let now = Utc::now();
        let new_token = DeviceTokenCreated {
            device_code: not_saved_token_device_code,
            created_at: now,
            expires_at: now + Duration::minutes(5),
        };

        assert_matches!(
            oauth_device_code_repo.save_device_code(&new_token).await,
            Ok(_)
        );

        new_token
    };

    assert_matches!(
        oauth_device_code_repo
            .update_device_token_with_token_params_part(
                &new_saved_device_token.device_code,
                &device_token_params_part
            )
            .await,
        Ok(_)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_find_device_token_by_device_code(catalog: &dill::Catalog) {
    let oauth_device_code_repo = catalog.get_one::<dyn OAuthDeviceCodeRepository>().unwrap();
    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();

    let not_saved_token_device_code = DeviceCode::new_uuid_v4();

    assert_matches!(
        oauth_device_code_repo
            .find_device_token_by_device_code(&not_saved_token_device_code)
            .await,
        Err(FindDeviceTokenByDeviceCodeError::NotFound(e)) if e.device_code == not_saved_token_device_code
    );

    let new_device_token = {
        let now = Utc::now();

        DeviceTokenCreated {
            device_code: not_saved_token_device_code,
            created_at: now,
            expires_at: now + Duration::minutes(5),
        }
    };

    assert_matches!(
        oauth_device_code_repo
            .save_device_code(&new_device_token)
            .await,
        Ok(_)
    );

    let device_token = DeviceToken::DeviceCodeCreated(new_device_token);

    pretty_assertions::assert_eq!(
        Ok(&device_token),
        oauth_device_code_repo
            .find_device_token_by_device_code(device_token.device_code())
            .await
            .as_ref()
    );

    let account_id = {
        let account = make_test_account(
            "test",
            "test@example.com",
            kamu_adapter_oauth::PROVIDER_GITHUB,
            "test",
        );

        assert_matches!(account_repo.create_account(&account).await, Ok(_));

        account.id
    };
    let device_token_params_part = DeviceTokenParamsPart {
        iat: 100,
        exp: 500,
        account_id,
    };

    assert_matches!(
        oauth_device_code_repo
            .update_device_token_with_token_params_part(
                device_token.device_code(),
                &device_token_params_part
            )
            .await,
        Ok(_)
    );

    let device_token_with_issued_token =
        device_token.with_token_params_part(device_token_params_part);

    pretty_assertions::assert_eq!(
        Ok(&device_token_with_issued_token),
        oauth_device_code_repo
            .find_device_token_by_device_code(device_token_with_issued_token.device_code())
            .await
            .as_ref()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_cleanup_expired_device_codes(catalog: &dill::Catalog) {
    let oauth_device_code_repo = catalog.get_one::<dyn OAuthDeviceCodeRepository>().unwrap();
    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();

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

    let [token_1_t2, token_2_t3, token_3_t3, token_4_t4] =
        make_saved_device_tokens_from_expired_time_points(
            oauth_device_code_repo.as_ref(),
            [
                t2 - expire_drift,
                t3 - expire_drift,
                t3 - expire_drift,
                t4 - expire_drift,
            ],
        )
        .await;

    pretty_assertions::assert_eq!(
        [true, true, true, true],
        are_tokens_exist(
            oauth_device_code_repo.as_ref(),
            [&token_1_t2, &token_2_t3, &token_3_t3, &token_4_t4]
        )
        .await
    );

    // t0
    {
        cleanup_expired_device_tokens_at_time(oauth_device_code_repo.as_ref(), t0, "t0").await;

        pretty_assertions::assert_eq!(
            [true, true, true, true],
            are_tokens_exist(
                oauth_device_code_repo.as_ref(),
                [&token_1_t2, &token_2_t3, &token_3_t3, &token_4_t4]
            )
            .await
        );
    }

    // t1
    {
        cleanup_expired_device_tokens_at_time(oauth_device_code_repo.as_ref(), t1, "t1").await;

        pretty_assertions::assert_eq!(
            [true, true, true, true],
            are_tokens_exist(
                oauth_device_code_repo.as_ref(),
                [&token_1_t2, &token_2_t3, &token_3_t3, &token_4_t4]
            )
            .await
        );
    }

    let [user1, user2] = create_accounts(account_repo.as_ref(), ["user1", "user2"]).await;
    let [token_3_t3_with_access_token, token_4_t4_with_access_token] =
        issue_access_tokens_for_device_tokens(
            oauth_device_code_repo.as_ref(),
            [(token_3_t3, user1), (token_4_t4, user2)],
        )
        .await;

    // t2
    {
        cleanup_expired_device_tokens_at_time(oauth_device_code_repo.as_ref(), t2, "t2").await;

        pretty_assertions::assert_eq!(
            [false, true, true, true],
            are_tokens_exist(
                oauth_device_code_repo.as_ref(),
                [
                    &token_1_t2,
                    &token_2_t3,
                    &token_3_t3_with_access_token,
                    &token_4_t4_with_access_token
                ]
            )
            .await
        );
    }

    // t3
    {
        cleanup_expired_device_tokens_at_time(oauth_device_code_repo.as_ref(), t3, "t3").await;

        pretty_assertions::assert_eq!(
            [false, false, false, true],
            are_tokens_exist(
                oauth_device_code_repo.as_ref(),
                [
                    &token_1_t2,
                    &token_2_t3,
                    &token_3_t3_with_access_token,
                    &token_4_t4_with_access_token
                ]
            )
            .await
        );
    }

    // t4
    {
        cleanup_expired_device_tokens_at_time(oauth_device_code_repo.as_ref(), t4, "t4").await;

        pretty_assertions::assert_eq!(
            [false, false, false, false],
            are_tokens_exist(
                oauth_device_code_repo.as_ref(),
                [
                    &token_1_t2,
                    &token_2_t3,
                    &token_3_t3_with_access_token,
                    &token_4_t4_with_access_token
                ]
            )
            .await
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn make_saved_device_tokens_from_expired_time_points<const N: usize>(
    oauth_device_code_repo: &dyn OAuthDeviceCodeRepository,
    expired_at: [DateTime<Utc>; N],
) -> [DeviceToken; N] {
    use std::mem::MaybeUninit;

    let mut device_tokens: [MaybeUninit<DeviceToken>; N] = MaybeUninit::uninit_array();

    for (i, t) in expired_at.into_iter().enumerate() {
        let new_token = DeviceTokenCreated {
            device_code: DeviceCode::new_uuid_v4(),
            created_at: t - DEVICE_CODE_EXPIRES_IN_5_MINUTES,
            expires_at: t,
        };

        assert_matches!(
            oauth_device_code_repo.save_device_code(&new_token).await,
            Ok(_),
            "Tag: {i}",
        );

        device_tokens[i].write(new_token.into());
    }

    unsafe { device_tokens.map(|item| item.assume_init()) }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn are_tokens_exist<const N: usize>(
    oauth_device_code_repo: &dyn OAuthDeviceCodeRepository,
    device_tokens: [&DeviceToken; N],
) -> [bool; N] {
    let mut report = [true; N];

    for (i, device_token) in device_tokens.iter().enumerate() {
        let exists = match oauth_device_code_repo
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn create_accounts<const N: usize>(
    account_repo: &dyn AccountRepository,
    account_names: [&str; N],
) -> [Account; N] {
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
            account_repo.create_account(&new_account).await,
            Ok(_),
            "Tag: {account_name}",
        );

        accounts[i].write(new_account);
    }

    unsafe { accounts.map(|item| item.assume_init()) }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn issue_access_tokens_for_device_tokens<const N: usize>(
    oauth_device_code_repo: &dyn OAuthDeviceCodeRepository,
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
            oauth_device_code_repo
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn cleanup_expired_device_tokens_at_time(
    oauth_device_code_repo: &dyn OAuthDeviceCodeRepository,
    t: DateTime<Utc>,
    tag: &str,
) {
    assert_matches!(
        oauth_device_code_repo.cleanup_expired_device_codes(t).await,
        Ok(_),
        "Tag: {tag}",
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
