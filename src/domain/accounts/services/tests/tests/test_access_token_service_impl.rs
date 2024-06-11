// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_accounts::{AccessTokenService, EncodeTokenError, KamuAccessToken};
use kamu_accounts_inmem::AccessTokenRepositoryInMemory;
use kamu_accounts_services::AccessTokenServiceImpl;

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_generate_and_encode_access_token() {
    let harness = AccessTokenServiceImplTestHarness::new();

    let generated_access_token = harness.access_token_svc.generate_access_token();

    let decoded_access_token = harness
        .access_token_svc
        .decode_access_token(&generated_access_token.composed_token)
        .unwrap();

    assert_eq!(generated_access_token, decoded_access_token);
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_decode_invalid_tokens() {
    let harness = AccessTokenServiceImplTestHarness::new();

    struct InvalidTokenCase {
        token_str: String,
        expected_error: EncodeTokenError,
    }
    // Test token with invalid prefix
    let test_cases = [
        InvalidTokenCase {
            token_str: "ks_1211TGDWBS05F563JTYCAQRXKWJCJ09HW1CQWDN2G4TWVMQTCBEQR2J2FC".to_string(),
            expected_error: EncodeTokenError::InvalidTokenPrefix,
        },
        InvalidTokenCase {
            token_str: "ka_1211TGDWBS05F563JTYCAQRXKWJCJ09HW1CQWDN2G4TWVMQTCBE".to_string(),
            expected_error: EncodeTokenError::InvalidTokenLength,
        },
        InvalidTokenCase {
            token_str: "ka+1211TGDWBS05F563JTYCAQRXKWJCJ09HW1CQWDN2G4TWVMQTCBEQR2J2FC".to_string(),
            expected_error: EncodeTokenError::InvalidTokenFormat,
        },
        InvalidTokenCase {
            token_str: "ka_1211TGDWBS05F563JTYCAQRXKWJCJ09HW1CQWDN2G4TWVMQTCBEQR2J2KC".to_string(),
            expected_error: EncodeTokenError::InvalidTokenChecksum,
        },
    ];

    for test_case in test_cases {
        let decoding_result = harness
            .access_token_svc
            .decode_access_token(&test_case.token_str);

        assert!(AccessTokenServiceImplTestHarness::match_errors(
            &decoding_result,
            &test_case.expected_error
        ));
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

struct AccessTokenServiceImplTestHarness {
    access_token_svc: Arc<dyn AccessTokenService>,
}

impl AccessTokenServiceImplTestHarness {
    fn new() -> Self {
        let catalog = dill::CatalogBuilder::new()
            .add::<AccessTokenRepositoryInMemory>()
            .add::<AccessTokenServiceImpl>()
            .build();

        let access_token_svc = catalog.get_one::<dyn AccessTokenService>().unwrap();

        Self { access_token_svc }
    }

    fn match_errors(
        result: &Result<KamuAccessToken, EncodeTokenError>,
        expected_error: &EncodeTokenError,
    ) -> bool {
        if let Err(result_err) = result {
            return result_err == expected_error;
        }
        false
    }
}
