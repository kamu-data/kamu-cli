// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use kamu_accounts::{
    DidEntity,
    DidSecretEncryptionConfig,
    DidSecretKeyRepository,
    GetDidSecretKeyError,
};
use kamu_signing::common::ProofType;
use kamu_signing::entities::IdentityConfig;
use kamu_signing::use_cases::{
    SignEip712Proof,
    SignEip712Response,
    SignEip712UseCase,
    SignEip712UseCaseError,
    SignEip712UseCaseOptions,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn SignEip712UseCase)]
pub struct SignEip712UseCaseImpl {
    did_secret_key_repo: Arc<dyn DidSecretKeyRepository>,
    did_secret_encryption_config: Arc<DidSecretEncryptionConfig>,
    identity_config: Option<IdentityConfig>,
}

impl SignEip712UseCaseImpl {
    async fn get_secret_key(
        &self,
        key: odf::metadata::DidKey,
        repo: &dyn DidSecretKeyRepository,
    ) -> Result<kamu_accounts::DidSecretKey, SignEip712UseCaseError> {
        let key_stack = key.as_did_str().to_stack_string();

        let dataset = DidEntity::new_dataset(key_stack.as_str());

        match repo.get_did_secret_key(&dataset).await {
            Ok(key) => return Ok(key),
            Err(GetDidSecretKeyError::NotFound(_)) => { /* continue */ }
            Err(e) => return Err(e.int_err().into()),
        }

        let account = DidEntity::new_account(key_stack.as_str());

        match repo.get_did_secret_key(&account).await {
            Ok(key) => Ok(key),
            Err(GetDidSecretKeyError::NotFound(_)) => {
                Err(SignEip712UseCaseError::SecretKeyNotFound { did: key })
            }
            Err(e) => Err(e.int_err().into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl SignEip712UseCase for SignEip712UseCaseImpl {
    async fn execute(
        &self,
        key: odf::metadata::DidKey,
        typed_data: crypto_eip712_utils::Eip712TypedData,
        options: SignEip712UseCaseOptions,
    ) -> Result<SignEip712Response, SignEip712UseCaseError> {
        let Some(encryption_key) = self.did_secret_encryption_config.get_encryption_key() else {
            return Err(SignEip712UseCaseError::NotConfigured);
        };
        let Some(identity_config) = self.identity_config.as_ref() else {
            return Err(SignEip712UseCaseError::NotConfigured);
        };

        let secret_key = self
            .get_secret_key(key, self.did_secret_key_repo.as_ref())
            .await?;

        let (signature, verification_method) = {
            use odf::metadata::ed25519::Signer;

            let ed25519_private_key = secret_key
                .get_decrypted_private_key(encryption_key)
                .int_err()?;

            let request_hash = typed_data.eip712_signing_hash().int_err()?;
            let signature = ed25519_private_key.sign(request_hash.as_slice());

            (signature, ed25519_private_key.verifying_key())
        };

        Ok(SignEip712Response {
            r#type: ProofType::Ed25519Signature2020,
            verification_method,
            signature: signature.into(),
            //
            proof: if options.include_node_proof {
                let signer = &identity_config.secp256k1_private_key;

                let proof = signature.to_bytes();
                let signature = signer.sign(proof.as_slice()).int_err()?;

                Some(SignEip712Proof {
                    r#type: ProofType::EcdsaSecp256k1Signature2019,
                    verification_method: signer.verification_key(),
                    signature,
                })
            } else {
                None
            },
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
