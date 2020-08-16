use kamu::domain::*;
use kamu::infra::*;
use kamu_test::*;

#[test]
fn test_delete_dataset() {
    let tempdir = tempfile::tempdir().unwrap();

    let workspace_layout = WorkspaceLayout::create(tempdir.path()).unwrap();
    let mut metadata_repo = MetadataRepositoryImpl::new(&workspace_layout);

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
