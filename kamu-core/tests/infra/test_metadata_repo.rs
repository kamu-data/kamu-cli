use kamu::domain::*;
use kamu::infra::*;
use kamu_test::*;

#[test]
fn test_visit_dataset_dependencies() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let mut repo = MetadataRepositoryImpl::new(&WorkspaceLayout::create(tmp_dir.path()).unwrap());

    {
        let mut visitor = TestVisitor::new();
        let res = repo.visit_dataset_dependencies(DatasetID::new_unchecked("blah"), &mut visitor);
        assert_err!(res, DomainError::DoesNotExist { .. });
    }

    repo.add_dataset(MetadataFactory::dataset_snapshot().id("foo").build())
        .unwrap();

    repo.add_dataset(
        MetadataFactory::dataset_snapshot()
            .id("bar")
            .source(MetadataFactory::dataset_source_deriv(["foo"].iter()).build())
            .build(),
    )
    .unwrap();

    {
        let mut visitor = TestVisitor::new();
        repo.visit_dataset_dependencies(DatasetID::new_unchecked("foo"), &mut visitor)
            .unwrap();
        assert_eq!(visitor.enters, 1);
        assert_eq!(visitor.exits, 1);
    }

    {
        let mut visitor = TestVisitor::new();
        repo.visit_dataset_dependencies(DatasetID::new_unchecked("bar"), &mut visitor)
            .unwrap();
        assert_eq!(visitor.enters, 2);
        assert_eq!(visitor.exits, 2);
    }
}

struct TestVisitor {
    enters: i32,
    exits: i32,
}

impl TestVisitor {
    fn new() -> Self {
        Self {
            enters: 0,
            exits: 0,
        }
    }
}

impl DatasetDependencyVisitor for TestVisitor {
    fn enter(&mut self, _dataset_id: &DatasetID, _meta_chain: &dyn MetadataChain) -> bool {
        self.enters += 1;
        true
    }

    fn exit(&mut self, _dataset_id: &DatasetID, _meta_chain: &dyn MetadataChain) {
        self.exits += 1;
    }
}
