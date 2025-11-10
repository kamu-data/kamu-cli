// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::sync::Arc;

use chrono::{SubsecRound, Utc};
use dill::Catalog;
use email_utils::Email;
use kamu_accounts::{
    Account,
    AccountRepository,
    AccountType,
    DEFAULT_ACCOUNT_ID,
    DEFAULT_ACCOUNT_NAME,
};
use kamu_datasets::{DatasetBlock, DatasetEntry, DatasetEntryRepository, MetadataEventType};
use odf::metadata::testing::MetadataFactory;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn new_account_with_name(
    account_repo: &Arc<dyn AccountRepository>,
    account_name: &str,
) -> Account {
    let (_, id) = odf::AccountID::new_generated_ed25519();

    let account = Account {
        id,
        account_name: odf::AccountName::new_unchecked(account_name),
        email: Email::parse(format!("{account_name}@example.com").as_str()).unwrap(),
        display_name: String::new(),
        account_type: AccountType::User,
        avatar_url: None,
        registered_at: Default::default(),
        provider: "unit-test-provider".to_string(),
        provider_identity_key: account_name.to_string(),
    };
    let create_res = account_repo.save_account(&account).await;

    assert_matches!(create_res, Ok(_));

    account
}

pub(crate) async fn new_account(account_repo: &Arc<dyn AccountRepository>) -> Account {
    new_account_with_name(account_repo, "unit-test-user").await
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn new_dataset_entry_with(
    owner: &Account,
    dataset_name: &str,
    dataset_kind: odf::DatasetKind,
) -> DatasetEntry {
    let (_, dataset_id) = odf::DatasetID::new_generated_ed25519();
    let owner_id = owner.id.clone();
    let owner_name = owner.account_name.clone();
    let dataset_alias = odf::DatasetName::new_unchecked(dataset_name);
    let created_at = Utc::now().round_subsecs(6);

    DatasetEntry::new(
        dataset_id,
        owner_id,
        owner_name,
        dataset_alias,
        created_at,
        dataset_kind,
    )
}

pub(crate) fn new_dataset_entry(owner: &Account, dataset_kind: odf::DatasetKind) -> DatasetEntry {
    // NOTE: Dataset name mixed case here is intentional
    new_dataset_entry_with(owner, "daTAseT", dataset_kind)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn init_test_account(catalog: &Catalog) -> (odf::AccountID, odf::AccountName) {
    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    account_repo.save_account(&Account::dummy()).await.unwrap();

    (DEFAULT_ACCOUNT_ID.clone(), DEFAULT_ACCOUNT_NAME.clone())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn init_dataset_entry(
    catalog: &Catalog,
    account_id: &odf::AccountID,
    account_name: &odf::AccountName,
    dataset_id: &odf::DatasetID,
    dataset_name: &odf::DatasetName,
    dataset_kind: odf::DatasetKind,
) {
    let dataset_entry_repo = catalog.get_one::<dyn DatasetEntryRepository>().unwrap();
    dataset_entry_repo
        .save_dataset_entry(&DatasetEntry {
            id: dataset_id.clone(),
            owner_id: account_id.clone(),
            owner_name: account_name.clone(),
            name: dataset_name.clone(),
            created_at: Utc::now(),
            kind: dataset_kind,
        })
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn remove_dataset_entry(catalog: &Catalog, dataset_id: &odf::DatasetID) {
    let dataset_entry_repo = catalog.get_one::<dyn DatasetEntryRepository>().unwrap();
    dataset_entry_repo
        .delete_dataset_entry(dataset_id)
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn make_add_data_block(sequence_number: u64) -> DatasetBlock {
    let event = MetadataFactory::add_data()
        .prev_offset(if sequence_number > 1 {
            Some(sequence_number * 10 - 1)
        } else {
            None
        })
        .some_new_data_with_offset(sequence_number * 10, sequence_number * 10 + 9)
        .build();

    let block = MetadataFactory::metadata_block(event).build();

    make_block(sequence_number, &block, MetadataEventType::AddData)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn make_execute_transform_block(sequence_number: u64) -> DatasetBlock {
    let event = MetadataFactory::execute_transform()
        .empty_query_inputs_from_seeded_ids(["upstream-1", "upstream-2"])
        .prev_offset(if sequence_number > 1 {
            Some(sequence_number * 10 - 1)
        } else {
            None
        })
        .some_new_data_with_offset(sequence_number * 10, sequence_number * 10 + 9)
        .build();

    let block = MetadataFactory::metadata_block(event).build();

    make_block(sequence_number, &block, MetadataEventType::ExecuteTransform)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn make_seed_block(kind: odf::DatasetKind) -> DatasetBlock {
    let event = MetadataFactory::seed(kind).build();

    let block = MetadataFactory::metadata_block(event).build();

    make_block(0, &block, MetadataEventType::Seed)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn make_info_block(sequence_number: u64) -> DatasetBlock {
    let event = MetadataFactory::set_info()
        .description("Test dataset")
        .keyword("demo")
        .build();

    let block = MetadataFactory::metadata_block(event).build();

    make_block(sequence_number, &block, MetadataEventType::SetInfo)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn make_license_block(sequence_number: u64) -> DatasetBlock {
    let event = MetadataFactory::set_license()
        .short_name("MIT")
        .name("MIT License")
        .build();

    let block = MetadataFactory::metadata_block(event).build();

    make_block(sequence_number, &block, MetadataEventType::SetLicense)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn make_block(
    sequence_number: u64,
    block: &odf::MetadataBlock,
    kind: MetadataEventType,
) -> DatasetBlock {
    let block_hash =
        odf::Multihash::from_digest_sha3_256(format!("block-{sequence_number}").as_bytes());

    let block_payload = bytes::Bytes::from(odf::storage::serialize_metadata_block(block).unwrap());

    DatasetBlock {
        event_kind: kind,
        sequence_number,
        block_hash,
        block_payload,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
