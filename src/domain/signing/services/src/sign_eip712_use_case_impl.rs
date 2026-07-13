// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_accounts::{
    Account,
    AccountService,
    CurrentAccountSubject,
    DidEntity,
    DidSecretEncryptionConfig,
    DidSecretKey,
    DidSecretKeyRepository,
    GetDidSecretKeyError,
    LoggedAccount,
};
use kamu_accounts_services::utils::AccountAuthorizationHelper;
use kamu_datasets::DatasetActionAuthorizer;
use kamu_molecule_domain::{MOLECULE_DEV_ORG_ACCOUNT_NAME, MOLECULE_ORG_ACCOUNT_NAME};
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
    dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
    current_account_subject: Arc<CurrentAccountSubject>,
    account_service: Arc<dyn AccountService>,
    account_authorization_helper: Arc<dyn AccountAuthorizationHelper>,
    identity_config: IdentityConfig,
}

impl SignEip712UseCaseImpl {
    // Public only for tests
    pub fn has_access_by_account_name(
        subject_account_name: &str,
        target_account_name: &str,
    ) -> bool {
        // 1. We always have access only for our own account
        if subject_account_name == target_account_name {
            return true;
        }
        // TODO: HACK: SEC: subject account has permissions to target account
        //                  Currently only allowing cross-account access
        //                  for and `molecule` / `molecule.dev`.
        //
        //                  See: https://github.com/kamu-data/kamu-node/issues/233

        // 2. Molecule account can access to any own project accounts

        // NOTE: Dragons here... You'd better take a look at the tests.
        match subject_account_name {
            MOLECULE_ORG_ACCOUNT_NAME => {
                let Some(s) = target_account_name.strip_prefix(subject_account_name) else {
                    return false;
                };

                let Some(s) = s.strip_prefix(".dev") else {
                    return true;
                };

                if s.is_empty() {
                    // Edge-case: molecule.dev is not a project account
                    return false;
                }

                s.strip_prefix(".").is_none()
            }

            MOLECULE_DEV_ORG_ACCOUNT_NAME => {
                let Some(s) = target_account_name.strip_prefix(subject_account_name) else {
                    return false;
                };

                s.strip_prefix(".").is_some()
            }

            _ => false,
        }
    }

    async fn get_secret_key_for_dataset(
        &self,
        dataset_id: odf::DatasetID,
    ) -> Result<Option<DidSecretKey>, InternalError> {
        use kamu_datasets::DatasetActionAuthorizerExt;

        let has_read_access = self
            .dataset_action_authorizer
            .is_action_allowed(&dataset_id, kamu_datasets::DatasetAction::Read)
            .await?;

        if !has_read_access {
            return Ok(None);
        }

        let dataset_id = dataset_id.as_did_str().to_stack_string();
        let dataset_entity = DidEntity::new_dataset(dataset_id.as_str());

        get_did_secret_key(self.did_secret_key_repo.as_ref(), dataset_entity).await
    }

    async fn get_secret_key_for_account(
        &self,
        account_id: odf::AccountID,
    ) -> Result<Option<DidSecretKey>, InternalError> {
        use kamu_accounts::AccountServiceExt;

        let Some(account_id_did) = account_id.as_did_odf() else {
            // Only odf account ids at the moment
            return Ok(None);
        };

        let CurrentAccountSubject::Logged(logged_account) = self.current_account_subject.as_ref()
        else {
            // NOTE: Only logged can search for other accounts
            return Ok(None);
        };

        let Some(account) = self
            .account_service
            .try_get_account_by_id(&account_id)
            .await?
        else {
            return Ok(None);
        };

        let has_access = self
            .has_access_for_account(logged_account, &account)
            .await?;

        if !has_access {
            return Ok(None);
        }

        let account_id = account_id_did.as_did_str().to_stack_string();
        let account_entity = DidEntity::new_account(account_id.as_str());

        get_did_secret_key(self.did_secret_key_repo.as_ref(), account_entity).await
    }

    async fn has_access_for_account(
        &self,
        logged_account: &LoggedAccount,
        account: &Account,
    ) -> Result<bool, InternalError> {
        // Admin can access any account
        if self.account_authorization_helper.is_admin().await? {
            return Ok(true);
        }

        Ok(Self::has_access_by_account_name(
            &logged_account.account_name,
            &account.account_name,
        ))
    }

    async fn get_secret_key(
        &self,
        key: odf::metadata::DidOdf,
    ) -> Result<DidSecretKey, SignEip712UseCaseError> {
        let (dataset_key, account_key) = tokio::try_join!(
            self.get_secret_key_for_dataset(key.into()),
            self.get_secret_key_for_account(key.into()),
        )?;

        if dataset_key.is_some() && account_key.is_some() {
            return Err("Unexpected situation: both keys have been found"
                .int_err()
                .into());
        }

        if let Some(key) = dataset_key {
            return Ok(key);
        }

        if let Some(key) = account_key {
            return Ok(key);
        }

        Err(SignEip712UseCaseError::SecretKeyNotFound { did: key })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl SignEip712UseCase for SignEip712UseCaseImpl {
    async fn execute(
        &self,
        key: odf::metadata::DidOdf,
        typed_data: crypto_eip712_utils::Eip712TypedData,
        options: SignEip712UseCaseOptions,
    ) -> Result<SignEip712Response, SignEip712UseCaseError> {
        let Some(encryption_key) = self.did_secret_encryption_config.get_encryption_key() else {
            return Err(SignEip712UseCaseError::NotConfigured);
        };
        let Some(secp256k1_private_key) = &self.identity_config.secp256k1_private_key else {
            return Err(SignEip712UseCaseError::NotConfigured);
        };

        let secret_key = self.get_secret_key(key).await?;

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
                let proof = signature.to_bytes();
                let signature = secp256k1_private_key.sign(proof.as_slice()).int_err()?;

                Some(SignEip712Proof {
                    r#type: ProofType::EcdsaSecp256k1Signature2019,
                    verification_method: secp256k1_private_key.verification_key(),
                    signature,
                })
            } else {
                None
            },
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn get_did_secret_key(
    did_secret_key_repo: &dyn DidSecretKeyRepository,
    entity: DidEntity<'_>,
) -> Result<Option<DidSecretKey>, InternalError> {
    match did_secret_key_repo.get_did_secret_key(&entity).await {
        Ok(key) => Ok(Some(key)),
        Err(GetDidSecretKeyError::NotFound(_)) => Ok(None),
        Err(e) => Err(e.int_err()),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
