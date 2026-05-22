// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use axum::extract::{Extension, Query};
use axum::response::Json;
use database_common_macros::transactional_handler;
use http_common::{ApiError, ApiErrorResponse};
use internal_error::ResultIntoInternal;
use kamu_accounts::{DidEntity, DidSecretKeyRepository, GetDidSecretKeyError};
use odf::metadata::ed25519::Signer;

use super::sign_eip712_types::{
    Eip712TypedDataRequestBody,
    SignEip712QueryParams,
    SignEip712Response,
};
use crate::data::query_handler::ResponseSigningNotConfigured;
use crate::data::query_types::{IdentityConfig, ProofType};
use crate::data::sign::SignEip712Proof;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Sign EIP-712 typed data
///
/// This endpoint authorizes that the caller has signing permissions for a
/// specified managed key and signs the message with it.
///
/// The optional `includeNodeProof` flag is used to additionally sign the
/// resulting signature with the node's own secp256k1 key, allowing for on-chain
/// verification.
#[utoipa::path(
    post,
    path = "/sign/eip712",
    params(SignEip712QueryParams),
    request_body = Eip712TypedDataRequestBody,
    responses(
        (status = OK, body = SignEip712Response),
        (status = BAD_REQUEST, body = ApiErrorResponse),
        (status = UNAUTHORIZED, body = ApiErrorResponse),
        (status = FORBIDDEN, body = ApiErrorResponse),
    ),
    tag = "odf-sign",
    security(
        (),
        ("api_key" = [])
    )
)]
#[transactional_handler]
pub async fn sign_eip712_handler(
    Extension(catalog): Extension<dill::Catalog>,
    Query(params): Query<SignEip712QueryParams>,
    Json(request): Json<Eip712TypedDataRequestBody>,
) -> Result<Json<SignEip712Response>, ApiError> {
    use kamu_accounts::*;

    tracing::debug!(params = ?params, request = ?request, "EIP-712 sign request");

    let identity = catalog.get_one::<IdentityConfig>().ok();
    let did_secret_key_repo = catalog.get_one::<dyn DidSecretKeyRepository>().unwrap();
    let did_secret_encryption_config = catalog.get_one::<DidSecretEncryptionConfig>().unwrap();

    let Some(identity) = identity else {
        Err(ApiError::not_implemented(ResponseSigningNotConfigured))?
    };

    let Some(encryption_key) = did_secret_encryption_config.get_encryption_key() else {
        return Err(ApiError::not_implemented(ResponseSigningNotConfigured));
    };

    let Some(secret_key) = get_secret_key(&params.key, did_secret_key_repo.as_ref())
        .await
        .int_err()?
    else {
        return Err(ApiError::not_found_without_reason());
    };

    // TODO: Molecule: Phase 3: remove back to json conversion
    let request_as_json = serde_json::to_value(request).int_err()?;
    let typed_data = crypto_eip712_utils::Eip712TypedData::from_json(request_as_json)?;
    let proof_hash = typed_data.signing_hash_with_eip191_prefix()?;

    let signature = {
        // TODO: SEC: Molecule: Phase 3: auth checks:
        // - molecule account can only access its own keys and projects
        // - while others can only access their own account and their own datasets
        let ed25519_private_key = secret_key
            .get_decrypted_private_key(encryption_key)
            .int_err()?;
        // TODO: SEC: Molecule: Phase 3: PK zeroing after usage

        // TODO: Molecule: Phase 3: add verification key to response?
        // let _verification_key =
        // odf::metadata::DidKey::new_ed25519(&private_key.verifying_key());

        ed25519_private_key.sign(proof_hash.as_slice())
    };

    let proof_response_node = if params.include_node_proof {
        let signature = crypto_eip712_utils::sign_prefixed(
            &identity.secp256k1_private_key,
            proof_hash.as_slice(),
        )?;

        Some(SignEip712Proof {
            r#type: ProofType::EcdsaSecp256k1Signature2019,
            verification_method: crypto_eip712_utils::verification_key_prefixed(
                &identity.secp256k1_private_key,
            ),
            signature,
        })
    } else {
        None
    };

    Ok(Json(SignEip712Response {
        r#type: ProofType::Ed25519Signature2020,
        signature: signature.into(),
        proof: proof_response_node,
    }))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn get_secret_key(
    key: &str,
    repo: &dyn DidSecretKeyRepository,
) -> Result<Option<kamu_accounts::DidSecretKey>, GetDidSecretKeyError> {
    // 1. Try for an account
    let account = DidEntity::new_account(key);

    match repo.get_did_secret_key(&account).await {
        Ok(key) => return Ok(Some(key)),
        Err(GetDidSecretKeyError::NotFound(_)) => { /* continue */ }
        Err(e) => return Err(e),
    }

    // 2. Try for a dataset
    let dataset = DidEntity::new_dataset(key);

    match repo.get_did_secret_key(&dataset).await {
        Ok(key) => Ok(Some(key)),
        Err(GetDidSecretKeyError::NotFound(_)) => Ok(None),
        Err(e) => Err(e),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
