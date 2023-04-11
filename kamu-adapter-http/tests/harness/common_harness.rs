// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{fs, io, path::Path};

use kamu::{
    domain::{InsertOpts, ObjectRepository},
    infra::{DatasetLayout, ObjectRepositoryLocalFS},
    testing::{AddDataBuilder, MetadataFactory},
};
use opendatafabric::Multihash;

/////////////////////////////////////////////////////////////////////////////////////////

pub fn copy_folder_recursively(src: &Path, dst: &Path) -> io::Result<()> {
    fs::create_dir_all(&dst)?;
    let copy_options = fs_extra::dir::CopyOptions::new().content_only(true);
    fs_extra::dir::copy(src, dst, &copy_options).unwrap();
    Ok(())
}

/////////////////////////////////////////////////////////////////////////////////////////

pub async fn create_random_data(dataset_layout: &DatasetLayout) -> AddDataBuilder {
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
