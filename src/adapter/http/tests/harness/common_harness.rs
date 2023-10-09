// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;
use std::str::FromStr;
use std::{fs, io};

use kamu::domain::*;
use kamu::testing::{AddDataBuilder, MetadataFactory};
use kamu::{DatasetLayout, ObjectRepositoryLocalFS};
use opendatafabric::{AccountName, DatasetAlias, DatasetRef, MetadataEvent, Multihash};

/////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn copy_folder_recursively(src: &Path, dst: &Path) -> io::Result<()> {
    fs::create_dir_all(&dst)?;
    let copy_options = fs_extra::dir::CopyOptions::new().content_only(true);
    fs_extra::dir::copy(src, dst, &copy_options).unwrap();
    Ok(())
}

/////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn write_dataset_alias(dataset_layout: &DatasetLayout, alias: &DatasetAlias) {
    if !dataset_layout.info_dir.is_dir() {
        std::fs::create_dir_all(dataset_layout.info_dir.clone()).unwrap();
    }

    use tokio::io::AsyncWriteExt;

    let alias_path = dataset_layout.info_dir.join("alias");
    let mut alias_file = tokio::fs::File::create(alias_path).await.unwrap();
    alias_file
        .write_all(alias.to_string().as_bytes())
        .await
        .unwrap();
}

/////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn create_random_data(dataset_layout: &DatasetLayout) -> AddDataBuilder {
    let (d_hash, d_size) = create_random_file(&dataset_layout.data_dir).await;
    let (c_hash, c_size) = create_random_file(&dataset_layout.checkpoints_dir).await;
    MetadataFactory::add_data()
        .data_physical_hash(d_hash)
        .data_size(d_size as i64)
        .checkpoint_physical_hash(c_hash)
        .checkpoint_size(c_size as i64)
}

/////////////////////////////////////////////////////////////////////////////////////////

async fn create_random_file(root: &Path) -> (Multihash, usize) {
    use rand::RngCore;

    let mut data = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut data);

    std::fs::create_dir_all(root).unwrap();

    let repo = ObjectRepositoryLocalFS::<sha3::Sha3_256, 0x16>::new(root);
    let hash = repo
        .insert_bytes(&data, InsertOpts::default())
        .await
        .unwrap()
        .hash;

    (hash, data.len())
}

/////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn commit_add_data_event(
    dataset_repo: &dyn DatasetRepository,
    dataset_ref: &DatasetRef,
    dataset_layout: &DatasetLayout,
) -> CommitResult {
    dataset_repo
        .get_dataset(&dataset_ref)
        .await
        .unwrap()
        .commit_event(
            MetadataEvent::AddData(create_random_data(dataset_layout).await.build()),
            CommitOpts::default(),
        )
        .await
        .unwrap()
}

/////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn make_dataset_ref(
    account_name: &Option<AccountName>,
    dataset_name: &str,
) -> DatasetRef {
    match account_name {
        Some(account_name) => {
            DatasetRef::from_str(format!("{}/{}", account_name, dataset_name).as_str()).unwrap()
        }
        None => DatasetRef::from_str(dataset_name).unwrap(),
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
