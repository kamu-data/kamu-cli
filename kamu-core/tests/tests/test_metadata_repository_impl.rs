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

macro_rules! rl {
    ($s:expr) => {
        DatasetRefLocal::Name(DatasetName::try_from($s).unwrap())
    };
}

#[test]
fn test_delete_dataset() {
    let tempdir = tempfile::tempdir().unwrap();

    let workspace_layout = WorkspaceLayout::create(tempdir.path()).unwrap();
    let metadata_repo = MetadataRepositoryImpl::new(Arc::new(workspace_layout));

    let snapshots = vec![
        MetadataFactory::dataset_snapshot()
            .name("foo")
            .kind(DatasetKind::Root)
            .push_event(MetadataFactory::set_polling_source().build())
            .build(),
        MetadataFactory::dataset_snapshot()
            .name("bar")
            .kind(DatasetKind::Derivative)
            .push_event(MetadataFactory::set_transform(["foo"]).build())
            .build(),
    ];

    metadata_repo.add_datasets(&mut snapshots.into_iter());

    assert!(matches!(
        metadata_repo.delete_dataset(&rl!("foo")),
        Err(DomainError::DanglingReference { .. })
    ));

    assert!(matches!(
        metadata_repo.get_metadata_chain(&rl!("foo")),
        Ok(_)
    ));

    assert!(matches!(metadata_repo.delete_dataset(&rl!("bar")), Ok(_)));

    assert!(matches!(
        metadata_repo.get_metadata_chain(&rl!("bar")),
        Err(DomainError::DoesNotExist { .. })
    ));

    assert!(matches!(metadata_repo.delete_dataset(&rl!("foo")), Ok(_)));

    assert!(matches!(
        metadata_repo.get_metadata_chain(&rl!("foo")),
        Err(DomainError::DoesNotExist { .. })
    ));
}
