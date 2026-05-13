// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::common::ProofType;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait SignEip712UseCase: Send + Sync {
    async fn execute(
        &self,
        key: odf::metadata::DidOdf,
        typed_data: crypto_eip712_utils::Eip712TypedData,
        options: SignEip712UseCaseOptions,
    ) -> Result<SignEip712Response, SignEip712UseCaseError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(bon::Builder, Default)]
pub struct SignEip712UseCaseOptions {
    pub include_node_proof: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct SignEip712Response {
    pub r#type: ProofType,
    pub verification_method: odf::metadata::DidKey,
    pub signature: odf::metadata::Signature,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proof: Option<SignEip712Proof>,
}

#[derive(Debug, serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[serde(rename_all = "camelCase")]
pub struct SignEip712Proof {
    pub r#type: ProofType,
    pub verification_method: crypto_eip712_utils::Secp256k1VerifyingKey,
    pub signature: crypto_eip712_utils::Secp256k1Signature,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum SignEip712UseCaseError {
    #[error("Response signing is not configured by the node")]
    NotConfigured,

    #[error("Secret key was not found for DID '{did_str}'", did_str = did.as_did_str())]
    SecretKeyNotFound { did: odf::metadata::DidOdf },

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
