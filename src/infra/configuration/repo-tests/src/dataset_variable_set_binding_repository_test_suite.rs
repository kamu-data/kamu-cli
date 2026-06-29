// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::sync::Arc;

use chrono::Utc;
use dill::Catalog;
use email_utils::Email;
use kamu_accounts::{Account, AccountRepository, AccountType};
use kamu_configuration::{
    DatasetVariableSetBindingRepository,
    ReplaceDatasetBindingsError,
    VariableSetResource,
    VariableSetSpec,
    VariableSpec,
    VariableValueSpec,
};
use kamu_datasets::{DatasetEntry, DatasetEntryRepository};
use kamu_resources::{ResourceHeaders, ResourceRepository, ResourceSnapshot, ResourceUID};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_replace_and_list_bindings(catalog: &Catalog) {
    let (dataset_id, resource_uids) = prepare_dataset(catalog).await;
    let repo = catalog
        .get_one::<dyn DatasetVariableSetBindingRepository>()
        .unwrap();

    repo.replace_bindings(&dataset_id, &resource_uids)
        .await
        .unwrap();

    let bindings = repo.list_bindings(&dataset_id).await.unwrap();
    assert_eq!(bindings.len(), 3);
    assert_eq!(bindings[0].resource_uid, resource_uids[0]);
    assert_eq!(bindings[1].resource_uid, resource_uids[1]);
    assert_eq!(bindings[2].resource_uid, resource_uids[2]);
    assert_eq!(bindings[0].binding_order, 0);
    assert_eq!(bindings[1].binding_order, 1);
    assert_eq!(bindings[2].binding_order, 2);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_replace_overwrites_previous_bindings(catalog: &Catalog) {
    let (dataset_id, resource_uids) = prepare_dataset(catalog).await;
    let repo = catalog
        .get_one::<dyn DatasetVariableSetBindingRepository>()
        .unwrap();

    repo.replace_bindings(&dataset_id, &resource_uids[..2])
        .await
        .unwrap();
    repo.replace_bindings(&dataset_id, &resource_uids[1..])
        .await
        .unwrap();

    let bindings = repo.list_bindings(&dataset_id).await.unwrap();
    assert_eq!(bindings.len(), 2);
    assert_eq!(bindings[0].resource_uid, resource_uids[1]);
    assert_eq!(bindings[1].resource_uid, resource_uids[2]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_replace_rejects_duplicates(catalog: &Catalog) {
    let (dataset_id, resource_uids) = prepare_dataset(catalog).await;
    let repo = catalog
        .get_one::<dyn DatasetVariableSetBindingRepository>()
        .unwrap();

    let result = repo
        .replace_bindings(
            &dataset_id,
            &[resource_uids[0], resource_uids[1], resource_uids[0]],
        )
        .await;

    assert_matches!(result, Err(ReplaceDatasetBindingsError::Duplicate(_)));
    assert!(repo.list_bindings(&dataset_id).await.unwrap().is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_delete_bindings_for_dataset(catalog: &Catalog) {
    let (dataset_id, resource_uids) = prepare_dataset(catalog).await;
    let repo = catalog
        .get_one::<dyn DatasetVariableSetBindingRepository>()
        .unwrap();

    repo.replace_bindings(&dataset_id, &resource_uids)
        .await
        .unwrap();

    repo.delete_bindings_for_dataset(&dataset_id).await.unwrap();

    assert!(repo.list_bindings(&dataset_id).await.unwrap().is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_list_bindings_empty_initially(catalog: &Catalog) {
    let (dataset_id, _resource_uids) = prepare_dataset(catalog).await;
    let repo = catalog
        .get_one::<dyn DatasetVariableSetBindingRepository>()
        .unwrap();

    let bindings = repo.list_bindings(&dataset_id).await.unwrap();
    assert!(bindings.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_delete_bindings_for_dataset_no_op(catalog: &Catalog) {
    let (dataset_id, _resource_uids) = prepare_dataset(catalog).await;
    let repo = catalog
        .get_one::<dyn DatasetVariableSetBindingRepository>()
        .unwrap();

    let result = repo.delete_bindings_for_dataset(&dataset_id).await;
    assert!(result.is_ok());

    assert!(repo.list_bindings(&dataset_id).await.unwrap().is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn prepare_dataset(catalog: &Catalog) -> (odf::DatasetID, Vec<ResourceUID>) {
    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    let dataset_entry_repo = catalog.get_one::<dyn DatasetEntryRepository>().unwrap();
    let resource_repo = catalog.get_one::<dyn ResourceRepository>().unwrap();

    let account = new_account(&account_repo).await;
    let dataset_entry = new_dataset_entry(&account);
    dataset_entry_repo
        .save_dataset_entry(&dataset_entry)
        .await
        .unwrap();

    let resource_uids = vec![
        ResourceUID::new(uuid::Uuid::new_v4()),
        ResourceUID::new(uuid::Uuid::new_v4()),
        ResourceUID::new(uuid::Uuid::new_v4()),
    ];

    for resource_uid in &resource_uids {
        create_resource(&resource_repo, &account, *resource_uid).await;
    }

    (dataset_entry.id, resource_uids)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn new_account(account_repo: &Arc<dyn AccountRepository>) -> Account {
    let (_, account_id) = odf::AccountID::new_generated_ed25519();
    let account_name = "configuration-binding-user";

    let account = Account {
        id: account_id,
        account_name: odf::AccountName::new_unchecked(account_name),
        email: Email::parse("configuration-binding-user@example.com").unwrap(),
        display_name: String::new(),
        account_type: AccountType::User,
        avatar_url: None,
        registered_at: Utc::now(),
        provider: "unit-test-provider".to_string(),
        provider_identity_key: account_name.to_string(),
    };

    account_repo.save_account(&account).await.unwrap();

    account
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn new_dataset_entry(owner: &Account) -> DatasetEntry {
    let (_, dataset_id) = odf::DatasetID::new_generated_ed25519();

    DatasetEntry::new(
        dataset_id,
        owner.id.clone(),
        owner.account_name.clone(),
        odf::DatasetName::new_unchecked("binding-dataset"),
        Utc::now(),
        odf::DatasetKind::Root,
    )
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn create_resource(
    resource_repo: &Arc<dyn ResourceRepository>,
    account: &Account,
    resource_uid: ResourceUID,
) {
    resource_repo
        .create_resource(&ResourceSnapshot {
            uid: resource_uid,
            kind: VariableSetResource::RESOURCE_TYPE.to_string(),
            api_version: VariableSetResource::API_VERSION.to_string(),
            headers: ResourceHeaders::simple(
                Utc::now(),
                account.id.clone(),
                resource_uid.to_string(),
            ),
            spec: serde_json::to_value(VariableSetSpec {
                variables: [(
                    "PLACEHOLDER".to_string(),
                    VariableSpec::Value(VariableValueSpec {
                        value: "placeholder".to_string(),
                    }),
                )]
                .into(),
            })
            .unwrap(),
            status: None,
            last_reconciled_at: None,
            last_event_id: None,
        })
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
