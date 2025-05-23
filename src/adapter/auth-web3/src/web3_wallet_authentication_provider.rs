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
    AccountType,
    AuthenticationProvider,
    ProviderLoginError,
    ProviderLoginResponse,
    PROVIDER_WEB3_WALLET,
};
use kamu_auth_web3::{ConsumeNonceError, EvmWalletAddress, NonceNotFoundError};
use kamu_core::ServerUrlConfig;
use nutype::nutype;
use serde::Deserialize;
use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const EIP_4361_EXPECTED_STATEMENT: &str =
    "By signing, you confirm wallet ownership and log in. No transaction or fees are involved.";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn AuthenticationProvider)]
pub struct Web3WalletAuthenticationProvider {
    server_url_config: Arc<ServerUrlConfig>,
    nonce_service: Arc<dyn kamu_auth_web3::Web3AuthEip4361NonceService>,
}

impl Web3WalletAuthenticationProvider {
    fn verify_eip4361_message_format(
        &self,
        message: &siwe::Message,
    ) -> Result<(), SignatureVerificationError> {
        {
            if message.expiration_time.is_none() {
                return Err(SignatureVerificationError::MissingMessageField(
                    MissedMessageField::ExpirationTime.into(),
                ));
            };

            if !message.valid_now() {
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
            let login_page_url = format!(
                "{}v/login",
                self.server_url_config.protocols.base_url_platform
            );

            if message.uri != login_page_url {
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
    ) -> Result<ChecksumEvmWalletAddress, SignatureVerificationError> {
        let message: siwe::Message = login_request.message.parse()?;
        let wallet_address = EvmWalletAddress::new(message.address);

        self.nonce_service.consume_nonce(&wallet_address).await?;

        self.verify_eip4361_message_format(&message)?;
        self.verify_eip191_message_signature(&message, &login_request.signature)?;

        Ok(ChecksumEvmWalletAddress::from(message.address))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl AuthenticationProvider for Web3WalletAuthenticationProvider {
    fn provider_name(&self) -> &'static str {
        PROVIDER_WEB3_WALLET
    }

    // TODO: Wallet-based auth: absorb into login() method
    // TODO: Wallet-based auth: return enum:
    //       - odf::AccountID; and
    //       - the brand new did:ethr:0x1337....FFFF
    fn generate_id(&self, _: &odf::AccountName) -> odf::AccountID {
        odf::AccountID::new_generated_ed25519().1
    }

    async fn login(
        &self,
        login_credentials_json: String,
    ) -> Result<ProviderLoginResponse, ProviderLoginError> {
        let request = serde_json::from_str::<LoginRequest>(login_credentials_json.as_str())
            .map_err(ProviderLoginError::invalid_credentials)?;

        let wallet_address = self.handle_login(&request).await?.into_inner();

        Ok(ProviderLoginResponse {
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

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LoginRequest {
    pub message: String,
    pub signature: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[nutype]
struct ChecksumEvmWalletAddress(String);

impl From<[u8; 20]> for ChecksumEvmWalletAddress {
    fn from(value: [u8; 20]) -> Self {
        Self::new(siwe::eip55(&value))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
enum SignatureVerificationError {
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
struct MissingMessageFieldError {
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
struct UnexpectedMessageStatementError {
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
struct UnexpectedMessageUriError {
    uri: String,
}

impl From<String> for UnexpectedMessageUriError {
    fn from(value: String) -> Self {
        Self { uri: value }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
