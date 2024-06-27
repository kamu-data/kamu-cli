// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_accounts::{DecodeTokenError, KamuAccessToken};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_generate_and_encode_access_token() {
    let generated_access_token = KamuAccessToken::new();

    let decoded_access_token =
        KamuAccessToken::decode(&generated_access_token.composed_token).unwrap();

    assert_eq!(generated_access_token, decoded_access_token);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_decode_invalid_tokens() {
    struct InvalidTokenCase {
        token_str: String,
        expected_error: DecodeTokenError,
    }
    // Test token with invalid prefix
    let test_cases = [
        InvalidTokenCase {
            token_str: "ks_1211TGDWBS05F563JTYCAQRXKWJCJ09HW1CQWDN2G4TWVMQTCBEQR2J2FC".to_string(),
            expected_error: DecodeTokenError::InvalidTokenPrefix,
        },
        InvalidTokenCase {
            token_str: "ka_1211TGDWBS05F563JTYCAQRXKWJCJ09HW1CQWDN2G4TWVMQTCBE".to_string(),
            expected_error: DecodeTokenError::InvalidTokenLength,
        },
        InvalidTokenCase {
            token_str: "ka+1211TGDWBS05F563JTYCAQRXKWJCJ09HW1CQWDN2G4TWVMQTCBEQR2J2FC".to_string(),
            expected_error: DecodeTokenError::InvalidTokenFormat,
        },
        InvalidTokenCase {
            token_str: "ka_1211TGDWBS05F563JTYCAQRXKWJCJ09HW1CQWDN2G4TWVMQTCBEQR2J2KC".to_string(),
            expected_error: DecodeTokenError::InvalidTokenChecksum,
        },
    ];

    for test_case in test_cases {
        let decoding_result = KamuAccessToken::decode(&test_case.token_str);

        assert!(match_decode_errors(
            &decoding_result,
            &test_case.expected_error
        ));
    }
}

fn match_decode_errors(
    result: &Result<KamuAccessToken, DecodeTokenError>,
    expected_error: &DecodeTokenError,
) -> bool {
    if let Err(result_err) = result {
        return result_err == expected_error;
    }
    false
}
