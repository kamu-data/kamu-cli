// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::sync::{Arc, LazyLock};

use chrono::{TimeZone, Utc};
use kamu_accounts::{AuthenticationProvider, ProviderLoginError};
use kamu_adapter_auth_web3::{LoginRequest, Web3WalletAuthenticationProvider};
use kamu_auth_web3::{
    EvmWalletAddressConvertor,
    Web3AuthEip4361NonceEntity,
    Web3AuthEip4361NonceRepository,
    Web3AuthEip4361NonceService,
    Web3AuthenticationEip4361Nonce,
};
use kamu_auth_web3_inmem::InMemoryWeb3AuthEip4361NonceRepository;
use kamu_auth_web3_services::Web3AuthEip4361NonceServiceImpl;
use kamu_core::ServerUrlConfig;
use serde_json::json;
use time_source::{SystemTimeSource, SystemTimeSourceStub};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// To generate a new signature:
// - 1. Install MetaMask (https://metamask.io/download)
// - 2. Import account
//     - Add account
//     - Import a wallet or account / Secret Recovery Phrase:
//         - brisk lamp level dumb region beach kit moon cat egg detail morning
//         - SECURITY NOTE: This is an empty test account.
// - 3. git clone -b chore/unittest-message-generation https://github.com/s373r/siwe-quickstart
// - 4. cd siwe-quickstart/03_complete_app/frontend/
// - 5. npm install
// - 6. npm start
// - 7. Open http://localhost:9090/
// - 8. Click "Connect wallet" and select the imported account
// - 9. Click "Sign-in with Ethereum"
// - 10. Copy the signature from the console

// Imported account address
const WALLET_ADDRESS: &str = "0xC945C7Ee6360eb2375FB2ABcAEAD114AB4ac733B";
const PREDEFINED_NONCE: &str = "FROZEN1NONCE1TEST";

static MESSAGE: LazyLock<String> = LazyLock::new(|| {
    indoc::formatdoc!(
        r#"
        platform.example.com wants you to sign in with your Ethereum account:
        0xC945C7Ee6360eb2375FB2ABcAEAD114AB4ac733B

        By signing, you confirm wallet ownership and log in. No transaction or fees are involved.

        URI: http://platform.example.com/v/login
        Version: 1
        Chain ID: 1
        Nonce: FROZEN1NONCE1TEST
        Issued At: 2050-01-02T03:04:05.000Z
        Expiration Time: 2050-01-02T03:19:05.000Z"#
    )
});

// Suitable for the message above
const SIGNATURE: &str = "0x553ddc5944bd8df1a0ad3403c131877a86658c07303c793a1af88f6229dbe6540625a8cfe99a5b671e9e14cafbb947882bf9d24f91df513147a8c14078de43fe1b";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_incorrect_json() {
    let harness = Web3WalletAuthenticationProviderHarness::new();

    let incorrect_json = "{".to_string();

    assert_matches!(
        harness.provider.login(incorrect_json).await,
        Err(ProviderLoginError::InvalidCredentials(e))
            if format!("{e:?}") == "InvalidCredentialsError { source: Error(\"EOF while parsing an object\", line: 1, column: 1) }"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_incorrect_login_fields_json() {
    let harness = Web3WalletAuthenticationProviderHarness::new();

    let json_payload = {
        let correct_json_with_wrong_field = json!({
            "messag3": "foo",
            "signature": "0x1337",
        });

        serde_json::to_string(&correct_json_with_wrong_field).unwrap()
    };

    assert_matches!(
        harness.provider.login(json_payload).await,
        Err(ProviderLoginError::InvalidCredentials(e))
            if format!("{e:?}") == "InvalidCredentialsError { source: Error(\"missing field `message`\", line: 1, column: 38) }"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_message_parse_error() {
    let harness = Web3WalletAuthenticationProviderHarness::new();

    let json_payload = {
        let request = LoginRequest {
            message: format!(
                "{}\nTo get a super cool AirDrop, follow the link...",
                *MESSAGE
            ),
            signature: SIGNATURE.to_string(),
        };

        serde_json::to_string(&request).unwrap()
    };

    assert_matches!(
        harness.provider.login(json_payload).await,
        Err(ProviderLoginError::InvalidCredentials(e))
            if format!("{e:?}") == "InvalidCredentialsError { source: MessageParseError(Format(\"Unexpected Content\")) }"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_missing_expiration_time() {
    let harness = Web3WalletAuthenticationProviderHarness::new();

    let json_payload = {
        let request = LoginRequest {
            message: MESSAGE.replace("\nExpiration Time: 2050-01-02T03:19:05.000Z", ""),
            signature: SIGNATURE.to_string(),
        };

        serde_json::to_string(&request).unwrap()
    };

    harness.create_nonce().await;

    assert_matches!(
        harness.provider.login(json_payload).await,
        Err(ProviderLoginError::InvalidCredentials(e))
            if format!("{e:?}") == "InvalidCredentialsError { source: MissingMessageField(MissingMessageFieldError { field: ExpirationTime }) }"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_message_expired() {
    let harness = Web3WalletAuthenticationProviderHarness::new();

    let json_payload = {
        let request = LoginRequest {
            message: MESSAGE
                .replace(
                    "Issued At: 2050-01-02T03:04:05.000Z",
                    "Issued At: 2049-01-02T03:04:05.000Z", // -1 year
                )
                .replace(
                    "Expiration Time: 2050-01-02T03:19:05.000Z",
                    "Expiration Time: 2049-01-02T03:19:05.000Z", // -1 year
                ),
            signature: SIGNATURE.to_string(),
        };

        serde_json::to_string(&request).unwrap()
    };

    harness.create_nonce().await;

    assert_matches!(
        harness.provider.login(json_payload).await,
        Err(ProviderLoginError::InvalidCredentials(e))
            if format!("{e:?}") == "InvalidCredentialsError { source: MessageExpired }"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_missing_statement() {
    let harness = Web3WalletAuthenticationProviderHarness::new();

    let json_payload = {
        let request = LoginRequest {
            message: MESSAGE.replace(
                "By signing, you confirm wallet ownership and log in. No transaction or fees are \
                 involved.\n",
                "",
            ),
            signature: SIGNATURE.to_string(),
        };

        serde_json::to_string(&request).unwrap()
    };

    harness.create_nonce().await;

    assert_matches!(
        harness.provider.login(json_payload).await,
        Err(ProviderLoginError::InvalidCredentials(e))
            if format!("{e:?}") == "InvalidCredentialsError { source: MissingMessageField(MissingMessageFieldError { field: Statement }) }"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_unexpected_statement() {
    let harness = Web3WalletAuthenticationProviderHarness::new();

    let json_payload = {
        let request = LoginRequest {
            message: MESSAGE.replace(
                "By signing, you confirm wallet ownership and log in. No transaction or fees are \
                 involved.",
                "This is a stick-up!",
            ),
            signature: SIGNATURE.to_string(),
        };

        serde_json::to_string(&request).unwrap()
    };

    harness.create_nonce().await;

    assert_matches!(
        harness.provider.login(json_payload).await,
        Err(ProviderLoginError::InvalidCredentials(e))
            if format!("{e:?}") == "InvalidCredentialsError { source: UnexpectedMessageStatement(UnexpectedMessageStatementError { statement: \"This is a stick-up!\" }) }"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_unexpected_uri() {
    let harness = Web3WalletAuthenticationProviderHarness::new();

    let json_payload = {
        let request = LoginRequest {
            message: MESSAGE.replace(
                "http://platform.example.com/v/login",
                "http://platform.exam.pl/e.com/v/login",
            ),
            signature: SIGNATURE.to_string(),
        };

        serde_json::to_string(&request).unwrap()
    };

    harness.create_nonce().await;

    assert_matches!(
        harness.provider.login(json_payload).await,
        Err(ProviderLoginError::InvalidCredentials(e))
            if format!("{e:?}") == "InvalidCredentialsError { source: UnexpectedMessageUri(UnexpectedMessageUriError { uri: \"http://platform.exam.pl/e.com/v/login\" }) }"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_invalid_signature_format() {
    let harness = Web3WalletAuthenticationProviderHarness::new();

    let json_payload = {
        let request = LoginRequest {
            message: MESSAGE.clone(),
            signature: format!("{SIGNATURE}1337"),
        };

        serde_json::to_string(&request).unwrap()
    };

    harness.create_nonce().await;

    assert_matches!(
        harness.provider.login(json_payload).await,
        Err(ProviderLoginError::InvalidCredentials(e))
            if format!("{e:?}") == "InvalidCredentialsError { source: VerificationError(SignatureLength) }"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_signature_verification_error() {
    let harness = Web3WalletAuthenticationProviderHarness::new();

    let json_payload = {
        let request = LoginRequest {
            message: MESSAGE.clone(),
            // Correct signature, but the message is different
            signature: "0x226ba3e4213c3175202e873963cb18dd622175abb01d5faf6375016e95a9145c5b19afb98e2b5a017f1f4b838f6db0f4e4b863a542dd6c19b464b493c67c9e001c".to_string(),
        };

        serde_json::to_string(&request).unwrap()
    };

    harness.create_nonce().await;

    assert_matches!(
        harness.provider.login(json_payload).await,
        Err(ProviderLoginError::InvalidCredentials(e))
            if format!("{e:?}") == "InvalidCredentialsError { source: VerificationError(Signer) }"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_nonce_not_found_error() {
    let harness = Web3WalletAuthenticationProviderHarness::new();

    let json_payload = {
        let request = LoginRequest {
            message: MESSAGE.clone(),
            signature: SIGNATURE.to_string(),
        };

        serde_json::to_string(&request).unwrap()
    };

    assert_matches!(
        harness.provider.login(json_payload).await,
        Err(ProviderLoginError::InvalidCredentials(e))
            if format!("{e:?}") == "InvalidCredentialsError { source: NonceNotFound(NonceNotFoundError { wallet: 0xc945c7ee6360eb2375fb2abcaead114ab4ac733b }) }"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_nonce_expired() {
    let harness = Web3WalletAuthenticationProviderHarness::new();

    let json_payload = {
        let request = LoginRequest {
            message: MESSAGE.replace("Expiration Time: 2050-01-02T01:19:05.000Z", ""),
            signature: SIGNATURE.to_string(),
        };

        serde_json::to_string(&request).unwrap()
    };

    harness.create_nonce().await;
    {
        let old_now = harness.system_time_source_stub.now();
        let new_now = old_now + std::time::Duration::from_mins(20);

        harness.system_time_source_stub.set(new_now);
    }

    assert_matches!(
        harness.provider.login(json_payload).await,
        Err(ProviderLoginError::InvalidCredentials(e))
            if format!("{e:?}") == "InvalidCredentialsError { source: NonceNotFound(NonceNotFoundError { wallet: 0xc945c7ee6360eb2375fb2abcaead114ab4ac733b }) }"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_signature_verified() {
    let harness = Web3WalletAuthenticationProviderHarness::new();

    {
        let nonce_entity = Web3AuthEip4361NonceEntity {
            wallet_address: EvmWalletAddressConvertor::parse_checksummed(WALLET_ADDRESS).unwrap(),
            nonce: Web3AuthenticationEip4361Nonce::try_new(PREDEFINED_NONCE).unwrap(),
            expires_at: Utc.with_ymd_and_hms(2050, 1, 2, 3, 4, 5).unwrap()
                + std::time::Duration::from_mins(10),
        };

        harness.nonce_repo.set_nonce(&nonce_entity).await.unwrap();
    }

    let json_payload = {
        let request = LoginRequest {
            message: MESSAGE.to_string(),
            signature: SIGNATURE.to_string(),
        };

        serde_json::to_string(&request).unwrap()
    };

    assert_matches!(harness.provider.login(json_payload).await, Ok(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct Web3WalletAuthenticationProviderHarness {
    provider: Arc<dyn AuthenticationProvider>,
    nonce_service: Arc<dyn Web3AuthEip4361NonceService>,
    system_time_source_stub: Arc<SystemTimeSourceStub>,
    nonce_repo: Arc<dyn Web3AuthEip4361NonceRepository>,
}

impl Web3WalletAuthenticationProviderHarness {
    fn new() -> Self {
        let t = Utc.with_ymd_and_hms(2050, 1, 2, 3, 4, 5).unwrap();

        let catalog = {
            let mut b = dill::CatalogBuilder::new();
            b.add::<Web3WalletAuthenticationProvider>();
            b.add_value(ServerUrlConfig::new_test(None));
            b.add_value(SystemTimeSourceStub::new_set(t));
            b.bind::<dyn SystemTimeSource, SystemTimeSourceStub>();
            b.add::<Web3AuthEip4361NonceServiceImpl>();
            b.add::<InMemoryWeb3AuthEip4361NonceRepository>();
            b.build()
        };
        Self {
            provider: catalog.get_one().unwrap(),
            nonce_service: catalog.get_one().unwrap(),
            system_time_source_stub: catalog.get_one().unwrap(),
            nonce_repo: catalog.get_one().unwrap(),
        }
    }

    async fn create_nonce(&self) {
        let wallet_address = EvmWalletAddressConvertor::parse_checksummed(WALLET_ADDRESS).unwrap();

        self.nonce_service
            .create_nonce(wallet_address)
            .await
            .unwrap();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
