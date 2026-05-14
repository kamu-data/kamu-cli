// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use bon::bon;
use crypto_eip712_utils::{Eip712TypedData, Secp256k1Signer};
use database_common::NoOpDatabasePlugin;
use kamu_accounts::{
    AccountConfig,
    AccountPropertyName,
    CurrentAccountSubject,
    DidEntity,
    DidSecretEncryptionConfig,
    DidSecretKey,
    DidSecretKeyRepository,
    PredefinedAccountsConfig,
    SAMPLE_DID_SECRET_KEY_ENCRYPTION_KEY,
};
use kamu_accounts_inmem::{InMemoryAccountRepository, InMemoryDidSecretKeyRepository};
use kamu_accounts_services::{
    AccountServiceImpl,
    CreateAccountUseCaseImpl,
    PredefinedAccountsRegistrator,
    UpdateAccountUseCaseImpl,
};
use kamu_auth_rebac_inmem::InMemoryRebacRepository;
use kamu_auth_rebac_services::{
    DefaultAccountProperties,
    DefaultDatasetProperties,
    RebacServiceImpl,
};
use kamu_datasets::{AlwaysHappyDatasetActionAuthorizer, DatasetActionAuthorizer};
use kamu_datasets_services::testing::MockDatasetActionAuthorizer;
use kamu_signing::entities::IdentityConfig;
use kamu_signing::use_cases::{SignEip712UseCase, SignEip712UseCaseOptions};
use kamu_signing::utils::b256;
use kamu_signing_services::SignEip712UseCaseImpl;
use pretty_assertions::assert_eq;
use serde_json::{json, to_value};
use time_source::SystemTimeSourceDefault;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Actors (w/ owned datasets):
// - molecule / dataset1
// - molecule.project / dataset2
// - molecule.dev / dataset3
// - molecule.dev.project / dataset4
// - user / dataset5
// - admin
// - no user

const MOLECULE: &str = "molecule";
const MOLECULE_PROJECT: &str = "molecule.project";
const MOLECULE_DEV: &str = "molecule.dev";
const MOLECULE_DEV_PROJECT: &str = "molecule.dev.project";
const USER: &str = "user";
const ADMIN: &str = "admin";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_has_access_by_name_matrix() {
    let f = SignEip712UseCaseImpl::has_access_by_account_name;

    for (subject_account_name, target_account_name, expected) in [
        // molecule
        (MOLECULE, MOLECULE, true),
        (MOLECULE, MOLECULE_PROJECT, true),
        (MOLECULE, MOLECULE_DEV, false),
        (MOLECULE, MOLECULE_DEV_PROJECT, false),
        (MOLECULE, "molecule.devops", true),
        // molecule.dev
        (MOLECULE_DEV, MOLECULE_DEV, true),
        (MOLECULE_DEV, MOLECULE_DEV_PROJECT, true),
        (MOLECULE_DEV, MOLECULE, false),
        (MOLECULE_DEV, MOLECULE_PROJECT, false),
        (MOLECULE_DEV, "molecule.devops", false),
    ] {
        let actual = f(subject_account_name, target_account_name);

        assert_eq!(
            expected, actual,
            "Subject: {}, Target: {}",
            subject_account_name, target_account_name
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_not_configured() {
    let harness = SignEip712UseCaseHarness::builder()
        .current_account_subject(CurrentAccountSubject::anonymous_no_authentication_provided())
        .has_identity_config(false)
        .build()
        .await;

    assert_eq!(
        Err("Response signing is not configured by the node".to_string()),
        harness
            .execute_and_convert_to_json(
                not_found_dataset_id().into(),
                typed_data(),
                SignEip712UseCaseOptions {
                    include_node_proof: true,
                },
            )
            .await,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_execute_matrix() {
    for (subject, mock_dataset_action_authorizer, test_cases) in [
        // 1. Admin has access to all datasets / accounts
        (
            CurrentAccountSubject::new_test_with(&ADMIN),
            mock_dataset_action_authorizer()
                .expect_check_read_dataset(&molecule_dataset_id(), 1, true)
                .expect_check_read_dataset(&molecule_project_dataset_id(), 1, true)
                .expect_check_read_dataset(&molecule_dev_dataset_id(), 1, true)
                .expect_check_read_dataset(&molecule_dev_project_dataset_id(), 1, true)
                .expect_check_read_dataset(&user_dataset_id(), 1, true)
                .expect_check_read_dataset(&not_found_dataset_id(), 1, false),
            [
                // Datasets
                (
                    "1) Test 1",
                    odf::metadata::DidOdf::from(molecule_dataset_id()),
                    expected_ok_response_for_molecule_dataset_id(),
                ),
                (
                    "1) Test 2",
                    molecule_project_dataset_id().into(),
                    expected_ok_response_for_molecule_project_dataset_id(),
                ),
                (
                    "1) Test 3",
                    molecule_dev_dataset_id().into(),
                    expected_ok_response_for_molecule_dev_dataset_id(),
                ),
                (
                    "1) Test 4",
                    molecule_dev_project_dataset_id().into(),
                    expected_ok_response_for_molecule_dev_project_dataset_id(),
                ),
                (
                    "1) Test 5",
                    user_dataset_id().into(),
                    expected_ok_response_for_user_dataset_id(),
                ),
                (
                    "1) Test 6",
                    not_found_dataset_id().into(),
                    expected_not_found(not_found_dataset_id().into()),
                ),
                // Accounts
                (
                    "1) Test 7",
                    to_odf_did(&molecule_account_id()),
                    expected_ok_response_for_molecule_account_id(),
                ),
                (
                    "1) Test 8",
                    to_odf_did(&molecule_project_account_id()),
                    expected_ok_response_for_molecule_project_account_id(),
                ),
                (
                    "1) Test 9",
                    to_odf_did(&molecule_dev_account_id()),
                    expected_ok_response_for_molecule_dev_account_id(),
                ),
                (
                    "1) Test 10",
                    to_odf_did(&molecule_dev_project_account_id()),
                    expected_ok_response_for_molecule_dev_project_account_id(),
                ),
                (
                    "1) Test 11",
                    to_odf_did(&user_account_id()),
                    expected_ok_response_for_user_account_id(),
                ),
                (
                    "1) Test 12",
                    to_odf_did(&not_found_account_id()),
                    expected_not_found(to_odf_did(&not_found_account_id())),
                ),
            ],
        ),
        // 2. Molecule has access to own/project datasets / accounts
        (
            CurrentAccountSubject::new_test_with(&MOLECULE),
            mock_dataset_action_authorizer()
                .expect_check_read_dataset(&molecule_dataset_id(), 1, true)
                .expect_check_read_dataset(&molecule_project_dataset_id(), 1, true)
                .expect_check_read_dataset(&molecule_dev_dataset_id(), 1, false)
                .expect_check_read_dataset(&molecule_dev_project_dataset_id(), 1, false)
                .expect_check_read_dataset(&user_dataset_id(), 1, false)
                .expect_check_read_dataset(&not_found_dataset_id(), 1, false),
            [
                // Datasets
                (
                    "2) Test 1",
                    odf::metadata::DidOdf::from(molecule_dataset_id()),
                    expected_ok_response_for_molecule_dataset_id(),
                ),
                (
                    "2) Test 2",
                    molecule_project_dataset_id().into(),
                    expected_ok_response_for_molecule_project_dataset_id(),
                ),
                (
                    "2) Test 3",
                    molecule_dev_dataset_id().into(),
                    expected_not_found(molecule_dev_dataset_id().into()),
                ),
                (
                    "2) Test 4",
                    molecule_dev_project_dataset_id().into(),
                    expected_not_found(molecule_dev_project_dataset_id().into()),
                ),
                (
                    "2) Test 5",
                    user_dataset_id().into(),
                    expected_not_found(user_dataset_id().into()),
                ),
                (
                    "2) Test 6",
                    not_found_dataset_id().into(),
                    expected_not_found(not_found_dataset_id().into()),
                ),
                // Accounts
                (
                    "2) Test 7",
                    to_odf_did(&molecule_account_id()),
                    expected_ok_response_for_molecule_account_id(),
                ),
                (
                    "2) Test 8",
                    to_odf_did(&molecule_project_account_id()),
                    expected_ok_response_for_molecule_project_account_id(),
                ),
                (
                    "2) Test 9",
                    to_odf_did(&molecule_dev_account_id()),
                    expected_not_found(to_odf_did(&molecule_dev_account_id())),
                ),
                (
                    "2) Test 10",
                    to_odf_did(&molecule_dev_project_account_id()),
                    expected_not_found(to_odf_did(&molecule_dev_project_account_id())),
                ),
                (
                    "2) Test 11",
                    to_odf_did(&user_account_id()),
                    expected_not_found(to_odf_did(&user_account_id())),
                ),
                (
                    "2) Test 12",
                    to_odf_did(&not_found_account_id()),
                    expected_not_found(to_odf_did(&not_found_account_id())),
                ),
            ],
        ),
        // 3. Molecule.dev has access to own/project datasets / accounts
        (
            CurrentAccountSubject::new_test_with(&MOLECULE_DEV),
            mock_dataset_action_authorizer()
                .expect_check_read_dataset(&molecule_dataset_id(), 1, false)
                .expect_check_read_dataset(&molecule_project_dataset_id(), 1, false)
                .expect_check_read_dataset(&molecule_dev_dataset_id(), 1, true)
                .expect_check_read_dataset(&molecule_dev_project_dataset_id(), 1, true)
                .expect_check_read_dataset(&user_dataset_id(), 1, false)
                .expect_check_read_dataset(&not_found_dataset_id(), 1, false),
            [
                // Datasets
                (
                    "3) Test 1",
                    odf::metadata::DidOdf::from(molecule_dataset_id()),
                    expected_not_found(molecule_dataset_id().into()),
                ),
                (
                    "3) Test 2",
                    molecule_project_dataset_id().into(),
                    expected_not_found(molecule_project_dataset_id().into()),
                ),
                (
                    "3) Test 3",
                    molecule_dev_dataset_id().into(),
                    expected_ok_response_for_molecule_dev_dataset_id(),
                ),
                (
                    "3) Test 4",
                    molecule_dev_project_dataset_id().into(),
                    expected_ok_response_for_molecule_dev_project_dataset_id(),
                ),
                (
                    "3) Test 5",
                    user_dataset_id().into(),
                    expected_not_found(user_dataset_id().into()),
                ),
                (
                    "3) Test 6",
                    not_found_dataset_id().into(),
                    expected_not_found(not_found_dataset_id().into()),
                ),
                // Accounts
                (
                    "3) Test 7",
                    to_odf_did(&molecule_account_id()),
                    expected_not_found(to_odf_did(&molecule_account_id())),
                ),
                (
                    "3) Test 8",
                    to_odf_did(&molecule_project_account_id()),
                    expected_not_found(to_odf_did(&molecule_project_account_id())),
                ),
                (
                    "3) Test 9",
                    to_odf_did(&molecule_dev_account_id()),
                    expected_ok_response_for_molecule_dev_account_id(),
                ),
                (
                    "3) Test 10",
                    to_odf_did(&molecule_dev_project_account_id()),
                    expected_ok_response_for_molecule_dev_project_account_id(),
                ),
                (
                    "3) Test 11",
                    to_odf_did(&user_account_id()),
                    expected_not_found(to_odf_did(&user_account_id())),
                ),
                (
                    "3) Test 12",
                    to_odf_did(&not_found_account_id()),
                    expected_not_found(to_odf_did(&not_found_account_id())),
                ),
            ],
        ),
        // 4. User has access to their own datasets / accounts
        (
            CurrentAccountSubject::new_test_with(&USER),
            mock_dataset_action_authorizer()
                .expect_check_read_dataset(&molecule_dataset_id(), 1, false)
                .expect_check_read_dataset(&molecule_project_dataset_id(), 1, false)
                .expect_check_read_dataset(&molecule_dev_dataset_id(), 1, false)
                .expect_check_read_dataset(&molecule_dev_project_dataset_id(), 1, false)
                .expect_check_read_dataset(&user_dataset_id(), 1, true)
                .expect_check_read_dataset(&not_found_dataset_id(), 1, false),
            [
                // Datasets
                (
                    "4) Test 1",
                    odf::metadata::DidOdf::from(molecule_dataset_id()),
                    expected_not_found(molecule_dataset_id().into()),
                ),
                (
                    "4) Test 2",
                    molecule_project_dataset_id().into(),
                    expected_not_found(molecule_project_dataset_id().into()),
                ),
                (
                    "4) Test 3",
                    molecule_dev_dataset_id().into(),
                    expected_not_found(molecule_dev_dataset_id().into()),
                ),
                (
                    "4) Test 4",
                    molecule_dev_project_dataset_id().into(),
                    expected_not_found(molecule_dev_project_dataset_id().into()),
                ),
                (
                    "4) Test 5",
                    user_dataset_id().into(),
                    expected_ok_response_for_user_dataset_id(),
                ),
                (
                    "4) Test 6",
                    not_found_dataset_id().into(),
                    expected_not_found(not_found_dataset_id().into()),
                ),
                // Accounts
                (
                    "4) Test 7",
                    to_odf_did(&molecule_account_id()),
                    expected_not_found(to_odf_did(&molecule_account_id())),
                ),
                (
                    "4) Test 8",
                    to_odf_did(&molecule_project_account_id()),
                    expected_not_found(to_odf_did(&molecule_project_account_id())),
                ),
                (
                    "4) Test 9",
                    to_odf_did(&molecule_dev_account_id()),
                    expected_not_found(to_odf_did(&molecule_dev_account_id())),
                ),
                (
                    "4) Test 10",
                    to_odf_did(&molecule_dev_project_account_id()),
                    expected_not_found(to_odf_did(&molecule_dev_project_account_id())),
                ),
                (
                    "4) Test 11",
                    to_odf_did(&user_account_id()),
                    expected_ok_response_for_user_account_id(),
                ),
                (
                    "4) Test 12",
                    to_odf_did(&not_found_account_id()),
                    expected_not_found(to_odf_did(&not_found_account_id())),
                ),
            ],
        ),
        // 5. Anonymous has no access to any datasets / accounts
        (
            CurrentAccountSubject::anonymous_no_authentication_provided(),
            mock_dataset_action_authorizer()
                .expect_check_read_dataset(&molecule_dataset_id(), 1, false)
                .expect_check_read_dataset(&molecule_project_dataset_id(), 1, false)
                .expect_check_read_dataset(&molecule_dev_dataset_id(), 1, false)
                .expect_check_read_dataset(&molecule_dev_project_dataset_id(), 1, false)
                .expect_check_read_dataset(&user_dataset_id(), 1, false)
                .expect_check_read_dataset(&not_found_dataset_id(), 1, false),
            [
                // Datasets
                (
                    "5) Test 1",
                    odf::metadata::DidOdf::from(molecule_dataset_id()),
                    expected_not_found(molecule_dataset_id().into()),
                ),
                (
                    "5) Test 2",
                    molecule_project_dataset_id().into(),
                    expected_not_found(molecule_project_dataset_id().into()),
                ),
                (
                    "5) Test 3",
                    molecule_dev_dataset_id().into(),
                    expected_not_found(molecule_dev_dataset_id().into()),
                ),
                (
                    "5) Test 4",
                    molecule_dev_project_dataset_id().into(),
                    expected_not_found(molecule_dev_project_dataset_id().into()),
                ),
                (
                    "5) Test 5",
                    user_dataset_id().into(),
                    expected_not_found(user_dataset_id().into()),
                ),
                (
                    "5) Test 6",
                    not_found_dataset_id().into(),
                    expected_not_found(not_found_dataset_id().into()),
                ),
                // Accounts
                (
                    "5) Test 7",
                    to_odf_did(&molecule_account_id()),
                    expected_not_found(to_odf_did(&molecule_account_id())),
                ),
                (
                    "5) Test 8",
                    to_odf_did(&molecule_project_account_id()),
                    expected_not_found(to_odf_did(&molecule_project_account_id())),
                ),
                (
                    "5) Test 9",
                    to_odf_did(&molecule_dev_account_id()),
                    expected_not_found(to_odf_did(&molecule_dev_account_id())),
                ),
                (
                    "5) Test 10",
                    to_odf_did(&molecule_dev_project_account_id()),
                    expected_not_found(to_odf_did(&molecule_dev_project_account_id())),
                ),
                (
                    "5) Test 11",
                    to_odf_did(&user_account_id()),
                    expected_not_found(to_odf_did(&user_account_id())),
                ),
                (
                    "5) Test 12",
                    to_odf_did(&not_found_account_id()),
                    expected_not_found(to_odf_did(&not_found_account_id())),
                ),
            ],
        ),
    ] {
        let harness = SignEip712UseCaseHarness::builder()
            .current_account_subject(subject.clone())
            .maybe_mock_dataset_action_authorizer(mock_dataset_action_authorizer)
            .has_identity_config(true)
            .build()
            .await;

        for (tag, did_odf, expected_response) in test_cases {
            assert_eq!(
                expected_response,
                harness
                    .execute_and_convert_to_json(
                        did_odf,
                        typed_data(),
                        SignEip712UseCaseOptions {
                            include_node_proof: true,
                        },
                    )
                    .await,
                "Subject: {:?}, Tag: {}",
                subject,
                tag
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SignEip712UseCaseHarness {
    pub use_case: Arc<dyn SignEip712UseCase>,
}

#[bon]
impl SignEip712UseCaseHarness {
    #[builder]
    pub async fn new(
        current_account_subject: CurrentAccountSubject,
        maybe_mock_dataset_action_authorizer: Option<MockDatasetActionAuthorizer>,
        has_identity_config: bool,
    ) -> Self {
        let mut b = dill::CatalogBuilder::new();

        let mut predefined_accounts_config = PredefinedAccountsConfig::new();
        for (account_name, is_admin) in [
            (MOLECULE, false),
            (MOLECULE_PROJECT, false),
            (MOLECULE_DEV, false),
            (MOLECULE_DEV_PROJECT, false),
            (USER, false),
            (ADMIN, true),
        ] {
            let mut account =
                AccountConfig::test_config_from_name(odf::AccountName::new_unchecked(account_name));

            if is_admin {
                account.properties.push(AccountPropertyName::IsAdmin);
            }

            predefined_accounts_config.predefined.push(account);
        }

        b.add::<SystemTimeSourceDefault>()
            .add_value(predefined_accounts_config)
            .add::<PredefinedAccountsRegistrator>()
            .add::<InMemoryAccountRepository>()
            .add::<InMemoryDidSecretKeyRepository>()
            .add_value(DidSecretEncryptionConfig::sample())
            .add::<AccountServiceImpl>()
            .add::<UpdateAccountUseCaseImpl>()
            .add::<CreateAccountUseCaseImpl>()
            .add::<RebacServiceImpl>()
            .add::<InMemoryRebacRepository>()
            .add_value(DefaultAccountProperties::default())
            .add_value(DefaultDatasetProperties::default())
            .add::<SignEip712UseCaseImpl>()
            .add_builder(messaging_outbox::OutboxImmediateImpl::builder(
                messaging_outbox::ConsumerFilter::AllConsumers,
            ))
            .bind::<dyn messaging_outbox::Outbox, messaging_outbox::OutboxImmediateImpl>();

        if has_identity_config {
            // Label: "kamu-attester"
            //        privateKey = uint256(keccak256(abi.encodePacked(label)))
            let private_key =
                b256!("0x42f3bebeb03afa3f14440c6837fa653a84e76bb74d62856227a97f3ee487b601");

            let identity_config = IdentityConfig {
                ed25519_private_key: odf::metadata::PrivateKey::from_bytes(&[123; _]),
                secp256k1_private_key: Secp256k1Signer::from_bytes(&(private_key.0.into()))
                    .unwrap(),
            };

            b.add_value(identity_config);
        }

        b.add_value(current_account_subject);

        if let Some(mock_dataset_action_authorizer) = maybe_mock_dataset_action_authorizer {
            b.add_value(mock_dataset_action_authorizer)
                .bind::<dyn DatasetActionAuthorizer, MockDatasetActionAuthorizer>();
        } else {
            b.add::<AlwaysHappyDatasetActionAuthorizer>();
        }

        NoOpDatabasePlugin::init_database_components(&mut b);

        let catalog = b.build();

        init_on_startup::run_startup_jobs(&catalog).await.unwrap();

        // Initialization
        let did_secret_repo = catalog.get_one::<dyn DidSecretKeyRepository>().unwrap();

        for (did_entity, private_key_bytes) in [
            // Datasets
            (
                DidEntity::new_dataset(molecule_dataset_id().to_string()),
                [1; _],
            ),
            (
                DidEntity::new_dataset(molecule_project_dataset_id().to_string()),
                [2; _],
            ),
            (
                DidEntity::new_dataset(molecule_dev_dataset_id().to_string()),
                [3; _],
            ),
            (
                DidEntity::new_dataset(molecule_dev_project_dataset_id().to_string()),
                [4; _],
            ),
            (
                DidEntity::new_dataset(user_dataset_id().to_string()),
                [5; _],
            ),
            // Accounts
            // NOTE: for InMemoryDidSecretKeyRepository we rewrite keys with pregenerated values
            (
                DidEntity::new_account(molecule_account_id().to_string()),
                [6; _],
            ),
            (
                DidEntity::new_account(molecule_project_account_id().to_string()),
                [7; _],
            ),
            (
                DidEntity::new_account(molecule_dev_account_id().to_string()),
                [8; _],
            ),
            (
                DidEntity::new_account(molecule_dev_project_account_id().to_string()),
                [9; _],
            ),
            (
                DidEntity::new_account(user_account_id().to_string()),
                [10; _],
            ),
            (DidEntity::new_account(admin_id().to_string()), [11; _]),
        ] {
            let private_key = odf::metadata::PrivateKey::from_bytes(&private_key_bytes);
            let did_secret_key =
                DidSecretKey::try_new(&private_key, SAMPLE_DID_SECRET_KEY_ENCRYPTION_KEY).unwrap();

            did_secret_repo
                .save_did_secret_key(&did_entity, &did_secret_key)
                .await
                .unwrap();
        }

        Self {
            use_case: catalog.get_one().unwrap(),
        }
    }

    pub async fn execute_and_convert_to_json(
        &self,
        key: odf::metadata::DidOdf,
        typed_data: Eip712TypedData,
        options: SignEip712UseCaseOptions,
    ) -> Result<serde_json::Value, String> {
        self.use_case
            .execute(key, typed_data, options)
            .await
            .map(|res| to_value(res).unwrap())
            .map_err(|e| e.to_string())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Account IDs
fn molecule_account_id() -> odf::AccountID {
    odf::AccountID::new_seeded_ed25519("molecule".as_bytes())
}

fn molecule_project_account_id() -> odf::AccountID {
    odf::AccountID::new_seeded_ed25519("molecule.project".as_bytes())
}

fn molecule_dev_account_id() -> odf::AccountID {
    odf::AccountID::new_seeded_ed25519("molecule.dev".as_bytes())
}

fn molecule_dev_project_account_id() -> odf::AccountID {
    odf::AccountID::new_seeded_ed25519("molecule.dev.project".as_bytes())
}

fn user_account_id() -> odf::AccountID {
    odf::AccountID::new_seeded_ed25519("user".as_bytes())
}

fn admin_id() -> odf::AccountID {
    odf::AccountID::new_seeded_ed25519("admin".as_bytes())
}

fn not_found_account_id() -> odf::AccountID {
    odf::AccountID::new_seeded_ed25519("not_found_account_id".as_bytes())
}

fn to_odf_did(account_id: &odf::AccountID) -> odf::metadata::DidOdf {
    *account_id.as_did_odf().unwrap()
}

fn to_dataset_id(account_id: &odf::AccountID) -> odf::DatasetID {
    (*account_id.as_did_odf().unwrap()).into()
}

// Dataset IDs

fn molecule_dataset_id() -> odf::DatasetID {
    odf::DatasetID::new_seeded_ed25519("dataset1".as_bytes())
}

fn molecule_project_dataset_id() -> odf::DatasetID {
    odf::DatasetID::new_seeded_ed25519("dataset2".as_bytes())
}

fn molecule_dev_dataset_id() -> odf::DatasetID {
    odf::DatasetID::new_seeded_ed25519("dataset3".as_bytes())
}

fn molecule_dev_project_dataset_id() -> odf::DatasetID {
    odf::DatasetID::new_seeded_ed25519("dataset4".as_bytes())
}

fn user_dataset_id() -> odf::DatasetID {
    odf::DatasetID::new_seeded_ed25519("dataset5".as_bytes())
}

fn not_found_dataset_id() -> odf::DatasetID {
    odf::DatasetID::new_seeded_ed25519("not_found_dataset_id".as_bytes())
}

fn typed_data() -> Eip712TypedData {
    // Adapted from:
    // EIP-712 LinkDidRequest — Handoff for Kamu
    // https://github.com/moleculeprotocol/onchainlabs/blob/main/docs/identity/kamu-eip712-linkdidrequest-handoff.md
    Eip712TypedData::from_json(json!({
        "domain": {
            "name": "MoleculeOclDidRegistry",
            "version": "1",
            "chainId": 8453,
            "verifyingContract": "0x00000000000000000000000000000000DeaDBeeF"
        },
        "types": {
            "LinkDidRequest": [
                {
                    "name": "oclId",
                    "type": "bytes32"
                },
                {
                    "name": "provider",
                    "type": "bytes32"
                },
                {
                    "name": "subject",
                    "type": "bytes32"
                },
                {
                    "name": "did",
                    "type": "string"
                },
                {
                    "name": "requestId",
                    "type": "bytes32"
                },
                {
                    "name": "deadline",
                    "type": "uint256"
                }
            ]
        },
        "primaryType": "LinkDidRequest",
        "message": {
            "oclId": "0x0101000000000000000000000000000000000000000000000000000000000042",
            "provider": "0xd025eab60676ea26fe9a9a945c991e6d0f3f06e3a940bb5123899345a9b2a413",
            "subject": "0xd844bb55167ab332117049e2ccd3d8863d241bcc80f46302310a6d942a90e851",
            "did": "did:odf:ed25519:z6MkhaXgBZDvotAccount",
            "requestId": "0xad68a4a8b76681274554d59f863c35d7abcef09a798c99e9717a2582037764a5",
            "deadline": "1893456000",
        },
    }))
    .unwrap()
}

fn mock_dataset_action_authorizer() -> MockDatasetActionAuthorizer {
    MockDatasetActionAuthorizer::new()
        // NOTE: At the time of the request, we don't know what the DID is, so we can check it
        //       for the account as well
        .expect_check_read_dataset(&to_dataset_id(&molecule_account_id()), 1, false)
        .expect_check_read_dataset(&to_dataset_id(&molecule_project_account_id()), 1, false)
        .expect_check_read_dataset(&to_dataset_id(&molecule_dev_account_id()), 1, false)
        .expect_check_read_dataset(&to_dataset_id(&molecule_dev_project_account_id()), 1, false)
        .expect_check_read_dataset(&to_dataset_id(&user_account_id()), 1, false)
        .expect_check_read_dataset(&to_dataset_id(&not_found_account_id()), 1, false)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Responses
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Accounts

#[expect(clippy::unnecessary_wraps)]
fn expected_ok_response_for_molecule_account_id() -> Result<serde_json::Value, String> {
    Ok(json!({
        "type": "Ed25519Signature2020",
        "verificationMethod": "did:key:z6Mkon22vwz9JoNpGDxCrGZRgeNFTdRTwXYYN3fvAhA3K19x",
        "signature": "uOwr4WVV4m7knrGkxZBzTmCpB54p9PM6HCGgn82_j-VMcV2ZyJd0P017s_hwYVXHmMLevjlMcW2mpb3CW9UW3Dw",
        "proof": {
            "type": "EcdsaSecp256k1Signature2019",
            "verificationMethod": "0x03993fbdd2f7a840b78202496af7e699dc9fcd1667f16dcce73887d563f448cc31",
            "signature": "0x7babd3a1d73b49f703a638bc2adc74f8abb8f57f048517330a760e1e105a7b7e2916a6a0bc10e3eccb4fa80f399cc8a2d3fcc3e37fa0041e685bb0ff3e615b381b"
        }
    }))
}

#[expect(clippy::unnecessary_wraps)]
fn expected_ok_response_for_molecule_project_account_id() -> Result<serde_json::Value, String> {
    Ok(json!({
        "type": "Ed25519Signature2020",
        "verificationMethod": "did:key:z6MkvDqGT54cXesYGvABpF1UapVNwjCqRcafi4Px6Thv5T3Z",
        "signature": "uDvYYfeTTyCHDN7yiejdz2FBBQeugEb5XeYSAQn7Dtu4zW1N1c1c20r5PHRXXSKgnQrhZIbF3eqPQHh5JJQZBAg",
        "proof": {
            "type": "EcdsaSecp256k1Signature2019",
            "verificationMethod": "0x03993fbdd2f7a840b78202496af7e699dc9fcd1667f16dcce73887d563f448cc31",
            "signature": "0xaeeb5aa5680969eccfc1ccc71cb3eb17873eaef29dcda27d7c5e8ebdbf4a43c53ca5879367481281d50eaa766bd8ffd7510f50c2d3f4813161894862942ed8281b"
        }
    }))
}

#[expect(clippy::unnecessary_wraps)]
fn expected_ok_response_for_molecule_dev_account_id() -> Result<serde_json::Value, String> {
    Ok(json!({
        "type": "Ed25519Signature2020",
        "verificationMethod": "did:key:z6Mkfmm57fsb6VL7zVusP8zeA9SYkCKdvUhby2G7Yh8vvQ1P",
        "signature": "u-KIngmQQ2uFw0IbXLc4f-DPmBrnuAxAkhinFbQFNsRL0-V5QbHmIwq5GfNngQWToV8WkY8JuCWoSH0iU4158AQ",
        "proof": {
            "type": "EcdsaSecp256k1Signature2019",
            "verificationMethod": "0x03993fbdd2f7a840b78202496af7e699dc9fcd1667f16dcce73887d563f448cc31",
            "signature": "0x4a6821f92b37fa00ad50145610e31d5b442e6b66f4dfc64655a69aee787cdaad7e777d7aa64863c8e8cc9be94f994856f1eac927738ca526e7baa456fe549af41b"
        }
    }))
}

#[expect(clippy::unnecessary_wraps)]
fn expected_ok_response_for_molecule_dev_project_account_id() -> Result<serde_json::Value, String> {
    Ok(json!({
        "type": "Ed25519Signature2020",
        "verificationMethod": "did:key:z6MkwVDfCg9LbbY6xjH3EZk8YSFQZujV5Y4y1ZWeER9tDiN3",
        "signature": "uFTIojN2XAPslAtHYHGEhlkIp_LyK8IcJZgsPn9B2yII-VDkAHNTnYKrM7ejpwwEEZN3JL88yFAOAyJ6TDcoLAQ",
        "proof": {
            "type": "EcdsaSecp256k1Signature2019",
            "verificationMethod": "0x03993fbdd2f7a840b78202496af7e699dc9fcd1667f16dcce73887d563f448cc31",
            "signature": "0x37e23c26b4383f38225269bd3a38724952fb24b2c20f3f022540a9b62ef6c369652b00ec6a98a15902334cbc992dc5bca6c11f8e615a376e6e1d809499d07a2a1c"
        }
    }))
}

#[expect(clippy::unnecessary_wraps)]
fn expected_ok_response_for_user_account_id() -> Result<serde_json::Value, String> {
    Ok(json!({
        "type": "Ed25519Signature2020",
        "verificationMethod": "did:key:z6Mkj1MDZKcfx9AX5CeXHdysiGkRLzBbALyFuShD6wNeY1E3",
        "signature": "ubKzLjYBRr9Rg0BrAmCWN331Q_njju_SAL0Hj7CsGE6rde_T6VOlwMqBikgxNgtYoQVQ9KIA3mIAMSRoqDKZdBQ",
        "proof": {
            "type": "EcdsaSecp256k1Signature2019",
            "verificationMethod": "0x03993fbdd2f7a840b78202496af7e699dc9fcd1667f16dcce73887d563f448cc31",
            "signature": "0x07512adfb7e8d7f56c4b14f88c4fe7636767cacdb743b49fc39294accf7882be7d0cb6b693f94b25cb0fedab9cb636f3f9d80bea51411c781b476527e38a77d31c"
        }
    }))
}

// Datasets

// 1) Ok

#[expect(clippy::unnecessary_wraps)]
fn expected_ok_response_for_molecule_dataset_id() -> Result<serde_json::Value, String> {
    Ok(json!({
        "type": "Ed25519Signature2020",
        "verificationMethod": "did:key:z6Mkon3Necd6NkkyfoGoHxid2znGc59LU3K7mubaRcFbLfLX",
        "signature": "uP2Wm1caUZkB2h0eF5uWMohhOV1JBeFdpJuPJhp8JFmIZlShUwsCta13xf1r6jyRnqiwLyNYNG4ByirYTtjQGBA",
        "proof": {
            "type": "EcdsaSecp256k1Signature2019",
            "verificationMethod": "0x03993fbdd2f7a840b78202496af7e699dc9fcd1667f16dcce73887d563f448cc31",
            "signature": "0x1d16b5f09481dcb5e7a8e58d9d7585c00d930d551b6c693894ebb74319f0f04f6792c9bbbc00cf3a8bca166dc7054e9b8da615b21be89e914b8074776a79f3f01c"
        }
    }))
}

#[expect(clippy::unnecessary_wraps)]
fn expected_ok_response_for_molecule_project_dataset_id() -> Result<serde_json::Value, String> {
    Ok(json!({
        "type": "Ed25519Signature2020",
        "verificationMethod": "did:key:z6Mko9hTggMwjSTEaJaPUfE6tqcy2xvU6BnNq3e3o8qVBiyH",
        "signature": "uNMrt0vM--o3MG4Oz39lBtSVSJ9gZYAY5a-kM6IJqCEAyrHXlqkrvV4Jo4V0NwbXssrL9QkN-nWwG83kkk7mbAw",
        "proof": {
            "type": "EcdsaSecp256k1Signature2019",
            "verificationMethod": "0x03993fbdd2f7a840b78202496af7e699dc9fcd1667f16dcce73887d563f448cc31",
            "signature": "0xc5058b3334b01617293815af7fb8d8db17cba8b8140f497bc0adaf3e71d570777e7b55ac818898114ad1eeb4ad9a2afd2a653b3ab208141ce223c788ec0f9e6a1c"
        }
    }))
}

#[expect(clippy::unnecessary_wraps)]
fn expected_ok_response_for_molecule_dev_dataset_id() -> Result<serde_json::Value, String> {
    Ok(json!({
        "type": "Ed25519Signature2020",
        "verificationMethod": "did:key:z6MkvRXNYcE7MMduynWTgeKbDaT1iijDSC8pZqXZc8rHPrf2",
        "signature": "ujeZTyGt6emrpISPWgwD-bbxAD69vCVXgIvzAjKcAQwi66K4FcoYd8z7sMt6MwJf8n2jPG7cQtoo59EF5fBD6CA",
        "proof": {
            "type": "EcdsaSecp256k1Signature2019",
            "verificationMethod": "0x03993fbdd2f7a840b78202496af7e699dc9fcd1667f16dcce73887d563f448cc31",
            "signature": "0xa303ef06e8fc3cc01beddcaeab7c32a5d17c70e3b5c55f54879c5c4940e9e2f653343bc05c6a2cc4c704aace774cc805d47042667e6143335d1074bc9d5ca0c51b"
        }
    }))
}

#[expect(clippy::unnecessary_wraps)]
fn expected_ok_response_for_molecule_dev_project_dataset_id() -> Result<serde_json::Value, String> {
    Ok(json!({
        "type": "Ed25519Signature2020",
        "verificationMethod": "did:key:z6Mkt6316e2PN3mZdB6N9CrzomJYUd1s5yBZi1XYHmwT9TUP",
        "signature": "ulIf24QdmjFyuBJMopZpTZ6Ehjpk8z1i0Am8rxBP6_9S23YNsnVCnHIhtjrbIuR76mACCFQfE1gjW75pKgjaSAw",
        "proof": {
            "type": "EcdsaSecp256k1Signature2019",
            "verificationMethod": "0x03993fbdd2f7a840b78202496af7e699dc9fcd1667f16dcce73887d563f448cc31",
            "signature": "0xe47b0b9f0017e32fd0de8f3378b2252f29fec678725e8937df5d3d07801d3cc749012a21722b96b53a46f1c64dab0cad3340aa18fc93965eab056740198155331b"
        }
    }))
}

#[expect(clippy::unnecessary_wraps)]
fn expected_ok_response_for_user_dataset_id() -> Result<serde_json::Value, String> {
    Ok(json!({
        "type": "Ed25519Signature2020",
        "verificationMethod": "did:key:z6MkmtWtY63GQVBrpMyRJWEzsnxfsGkemu6CtMDwGTv4RYj2",
        "signature": "u5LoI2Nv7HkxihalcMhkNq7PKh39FR_doSNE-IpNAsVK3-Mozp5REKvdk2N19BF3l5nr3YtYABV-spDC0qdl6Cg",
        "proof": {
            "type": "EcdsaSecp256k1Signature2019",
            "verificationMethod": "0x03993fbdd2f7a840b78202496af7e699dc9fcd1667f16dcce73887d563f448cc31",
            "signature": "0x9cb3b2bfe670e16ab19670d7a5bfd5fb159a1cf3bb4d3948c8015eb1dbd87bf775fe66258964416cf9e0f74e11f926b1bedb1ab450da72f5698f0c5ea68653001b"
        }
    }))
}

// 2) Err

fn expected_not_found(did_odf: odf::metadata::DidOdf) -> Result<serde_json::Value, String> {
    Err(format!(
        "Secret key was not found for DID '{}'",
        did_odf.as_did_str()
    ))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
