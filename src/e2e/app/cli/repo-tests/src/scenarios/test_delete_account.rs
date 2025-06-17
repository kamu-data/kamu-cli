// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_puppet::KamuCliPuppet;
use kamu_cli_puppet::extensions::{
    AddDatasetOptions,
    DatasetListOptions,
    DeleteAccountResult,
    KamuCliPuppetExt,
};
use odf::metadata::testing::MetadataFactory;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_delete_account_with_datasets_mt(mut kamu: KamuCliPuppet) {
    // NOTE: When running through kamu-cli we automatically simulate kamu-api-server
    //       restarts and various reindexing operations.

    let alice = odf::AccountName::new_unchecked("alice");
    let bob = odf::AccountName::new_unchecked("bob");

    kamu.create_account(&alice).await;
    kamu.create_account(&bob).await;

    {
        kamu.set_account(Some(alice.clone()));

        kamu.add_dataset(
            MetadataFactory::dataset_snapshot().name("dataset1").build(),
            AddDatasetOptions::builder()
                .visibility(odf::DatasetVisibility::Public)
                .build(),
        )
        .await;
        kamu.add_dataset(
            MetadataFactory::dataset_snapshot().name("dataset2").build(),
            AddDatasetOptions::builder()
                .visibility(odf::DatasetVisibility::Public)
                .build(),
        )
        .await;
    }
    {
        kamu.set_account(Some(bob.clone()));

        kamu.add_dataset(
            MetadataFactory::dataset_snapshot().name("dataset3").build(),
            AddDatasetOptions::builder()
                .visibility(odf::DatasetVisibility::Public)
                .build(),
        )
        .await;
        kamu.add_dataset(
            MetadataFactory::dataset_snapshot().name("dataset4").build(),
            AddDatasetOptions::builder()
                .visibility(odf::DatasetVisibility::Public)
                .build(),
        )
        .await;
    }

    kamu.set_account(None);

    pretty_assertions::assert_eq!(
        [
            (
                Some(alice.clone()),
                odf::DatasetName::new_unchecked("dataset1")
            ),
            (
                Some(alice.clone()),
                odf::DatasetName::new_unchecked("dataset2")
            ),
            (
                Some(bob.clone()),
                odf::DatasetName::new_unchecked("dataset3")
            ),
            (
                Some(bob.clone()),
                odf::DatasetName::new_unchecked("dataset4")
            ),
        ],
        *kamu
            .list_datasets_ex(DatasetListOptions::builder().all_accounts(true).build())
            .await
            .into_iter()
            .map(|dataset| (dataset.owner, dataset.name))
            .collect::<Vec<_>>()
    );

    pretty_assertions::assert_matches!(
        kamu.delete_account(&alice).await,
        DeleteAccountResult::Deleted
    );

    pretty_assertions::assert_eq!(
        [
            (
                Some(bob.clone()),
                odf::DatasetName::new_unchecked("dataset3")
            ),
            (
                Some(bob.clone()),
                odf::DatasetName::new_unchecked("dataset4")
            ),
        ],
        *kamu
            .list_datasets_ex(DatasetListOptions::builder().all_accounts(true).build())
            .await
            .into_iter()
            .map(|dataset| (dataset.owner, dataset.name))
            .collect::<Vec<_>>()
    );

    pretty_assertions::assert_matches!(
        kamu.delete_account(&alice).await,
        DeleteAccountResult::NotFound
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
