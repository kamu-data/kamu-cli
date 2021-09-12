// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu::domain::*;
use kamu::infra::*;
use kamu::testing::*;
use opendatafabric::*;

#[test]
fn test_delete_dataset() {
    let tempdir = tempfile::tempdir().unwrap();

    let workspace_layout = WorkspaceLayout::create(tempdir.path()).unwrap();
    let metadata_repo = MetadataRepositoryImpl::new(Arc::new(workspace_layout));

    let snapshots = vec![
        MetadataFactory::dataset_snapshot()
            .id("foo")
            .source(MetadataFactory::dataset_source_root().build())
            .build(),
        MetadataFactory::dataset_snapshot()
            .id("bar")
            .source(MetadataFactory::dataset_source_deriv(["foo"].iter()).build())
            .build(),
    ];

    metadata_repo.add_datasets(&mut snapshots.into_iter());

    assert!(matches!(
        metadata_repo.delete_dataset(DatasetID::try_from("foo").unwrap()),
        Err(DomainError::DanglingReference { .. })
    ));

    assert!(matches!(
        metadata_repo.get_metadata_chain(DatasetID::try_from("foo").unwrap()),
        Ok(_)
    ));

    assert!(matches!(
        metadata_repo.delete_dataset(DatasetID::try_from("bar").unwrap()),
        Ok(_)
    ));

    assert!(matches!(
        metadata_repo.get_metadata_chain(DatasetID::try_from("bar").unwrap()),
        Err(DomainError::DoesNotExist { .. })
    ));

    assert!(matches!(
        metadata_repo.delete_dataset(DatasetID::try_from("foo").unwrap()),
        Ok(_)
    ));

    assert!(matches!(
        metadata_repo.get_metadata_chain(DatasetID::try_from("foo").unwrap()),
        Err(DomainError::DoesNotExist { .. })
    ));
}
