// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use email_utils::Email;
use internal_error::{ErrorIntoInternal, InternalError};
use kamu_accounts::{
    AccountProvider,
    AccountType,
    AuthenticationProvider,
    ProviderLoginError,
    ProviderLoginResponse,
};
use kamu_auth_web3::{
    ConsumeNonceError,
    EvmWalletAddress,
    EvmWalletAddressConvertor,
    NonceNotFoundError,
};
use kamu_core::ServerUrlConfig;
use nutype::nutype;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use time_source::SystemTimeSource;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const EIP_4361_EXPECTED_STATEMENT: &str =
    "By signing, you confirm wallet ownership and log in. No transaction or fees are involved.";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn AuthenticationProvider)]
pub struct Web3WalletAuthenticationProvider {
    server_url_config: Arc<ServerUrlConfig>,
    nonce_service: Arc<dyn kamu_auth_web3::Web3AuthEip4361NonceService>,
    time_source: Arc<dyn SystemTimeSource>,
}

impl Web3WalletAuthenticationProvider {
    // Public only for tests
    pub fn parse_siwe_message(
        &self,
        serialized_message: &str,
    ) -> Result<siwe::Message, siwe::ParseError> {
        serialized_message.parse()
    }

    // Public only for tests
    pub fn verify_eip4361_message_format(
        &self,
        message: &siwe::Message,
    ) -> Result<(), SignatureVerificationError> {
        {
            if message.expiration_time.is_none() {
                return Err(SignatureVerificationError::MissingMessageField(
                    MissedMessageField::ExpirationTime.into(),
                ));
            }

            let now = {
                let chrono_now = self.time_source.now();
                time::OffsetDateTime::from_unix_timestamp(chrono_now.timestamp()).unwrap()
            };

            if !message.valid_at(&now) {
                return Err(SignatureVerificationError::MessageExpired);
            }
        }
        {
            let Some(statement) = &message.statement else {
                return Err(SignatureVerificationError::MissingMessageField(
                    MissedMessageField::Statement.into(),
                ));
            };

            if statement != EIP_4361_EXPECTED_STATEMENT {
                return Err(SignatureVerificationError::UnexpectedMessageStatement(
                    statement.clone().into(),
                ));
            }
        }
        {
            let server_base_url = &self.server_url_config.protocols.base_url_platform;
            let mut is_uri_correct = false;
            if let Ok(mut uri) = Url::parse(message.uri.as_str()) {
                uri.set_path("");
                uri.set_query(None);
                uri.set_fragment(None);

                // We're only interested in scheme and host
                is_uri_correct = uri.as_str() == server_base_url.as_str();
            }

            if !is_uri_correct {
                return Err(SignatureVerificationError::UnexpectedMessageUri(
                    message.uri.to_string().into(),
                ));
            }
        }

        Ok(())
    }

    fn verify_eip191_message_signature(
        &self,
        message: &siwe::Message,
        signature: &str,
    ) -> Result<(), SignatureVerificationError> {
        let signature_bytes = {
            let striped_signature = signature.strip_prefix("0x").unwrap_or(signature);
            hex::decode(striped_signature.as_bytes())?
        };

        let Ok(signature_as_array) = signature_bytes[..].try_into() else {
            return Err(SignatureVerificationError::VerificationError(
                siwe::VerificationError::SignatureLength,
            ));
        };

        message.verify_eip191(signature_as_array)?;

        Ok(())
    }

    async fn handle_login(
        &self,
        login_request: &LoginRequest,
    ) -> Result<odf::metadata::DidPkh, SignatureVerificationError> {
        let message = self.parse_siwe_message(&login_request.message)?;
        let wallet_address = EvmWalletAddress::new(message.address);

        self.nonce_service.consume_nonce(&wallet_address).await?;

        self.verify_eip4361_message_format(&message)?;
        self.verify_eip191_message_signature(&message, &login_request.signature)?;

        let did_pkh = {
            let chain_id = message.chain_id;
            let checksum_wallet_address =
                ChecksumEvmWalletAddress::from(wallet_address).into_inner();

            // Only EVM-based (eip155) at the moment
            odf::metadata::DidPkh::parse_caip10_account_id(&format!(
                "eip155:{chain_id}:{checksum_wallet_address}",
            ))
            .unwrap()
        };

        Ok(did_pkh)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl AuthenticationProvider for Web3WalletAuthenticationProvider {
    fn provider_name(&self) -> &'static str {
        AccountProvider::Web3Wallet.into()
    }

    async fn login(
        &self,
        login_credentials_json: String,
    ) -> Result<ProviderLoginResponse, ProviderLoginError> {
        let request = serde_json::from_str::<LoginRequest>(login_credentials_json.as_str())
            .map_err(ProviderLoginError::invalid_credentials)?;

        let did_pkh = self.handle_login(&request).await?;
        let wallet_address = did_pkh.wallet_address().to_string();

        Ok(ProviderLoginResponse {
            account_id: did_pkh.into(),
            account_name: odf::AccountName::new_unchecked(&wallet_address),
            // TODO: Wallet-based auth: replace with none
            email: Email::parse(&format!("{wallet_address}@example.com")).unwrap(),
            display_name: wallet_address.clone(),
            account_type: AccountType::User,
            avatar_url: None,
            provider_identity_key: wallet_address,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoginRequest {
    pub message: String,
    pub signature: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[nutype(derive(AsRef))]
struct ChecksumEvmWalletAddress(String);

impl From<EvmWalletAddress> for ChecksumEvmWalletAddress {
    fn from(value: EvmWalletAddress) -> Self {
        Self::new(EvmWalletAddressConvertor::checksummed_string(&value))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum SignatureVerificationError {
    #[error("Parse error: {0}")]
    MessageParseError(#[from] siwe::ParseError),

    #[error(transparent)]
    NonceNotFound(#[from] NonceNotFoundError),

    #[error(transparent)]
    MissingMessageField(#[from] MissingMessageFieldError),

    #[error("EIP-4361 message expired")]
    MessageExpired,

    #[error(transparent)]
    UnexpectedMessageStatement(#[from] UnexpectedMessageStatementError),

    #[error(transparent)]
    UnexpectedMessageUri(#[from] UnexpectedMessageUriError),

    #[error("Invalid signature: {0}")]
    InvalidSignature(#[from] hex::FromHexError),

    #[error("Verification error: {0}")]
    VerificationError(#[from] siwe::VerificationError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl From<ConsumeNonceError> for SignatureVerificationError {
    fn from(e: ConsumeNonceError) -> Self {
        use ConsumeNonceError as E;

        match e {
            E::NotFound(e) => Self::NonceNotFound(e),
            E::Internal(_) => Self::Internal(e.int_err()),
        }
    }
}

impl From<SignatureVerificationError> for ProviderLoginError {
    fn from(e: SignatureVerificationError) -> Self {
        Self::invalid_credentials(e)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
enum MissedMessageField {
    ExpirationTime,
    Statement,
}

#[derive(Error, Debug)]
#[error("Missed field: {field:?}")]
pub struct MissingMessageFieldError {
    field: MissedMessageField,
}

impl From<MissedMessageField> for MissingMessageFieldError {
    fn from(value: MissedMessageField) -> Self {
        Self { field: value }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("Unexpected message statement: {statement}")]
pub struct UnexpectedMessageStatementError {
    statement: String,
}

impl From<String> for UnexpectedMessageStatementError {
    fn from(value: String) -> Self {
        Self { statement: value }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("Unexpected message URI: {uri}")]
pub struct UnexpectedMessageUriError {
    uri: String,
}

impl From<String> for UnexpectedMessageUriError {
    fn from(value: String) -> Self {
        Self { uri: value }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
