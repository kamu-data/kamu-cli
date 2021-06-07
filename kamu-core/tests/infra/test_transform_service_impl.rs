use kamu::domain::*;
use kamu::infra::utils::docker_client::DockerClient;
use kamu::infra::*;
use kamu_test::*;
use opendatafabric::*;

use chrono::{TimeZone, Utc};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

macro_rules! map(
    { } => { ::std::collections::BTreeMap::new() };
    { $($key:expr => $value:expr),+ } => {
        {
            let mut m = ::std::collections::BTreeMap::new();
            $(
                m.insert($key, $value);
            )+
            m
        }
     };
);

fn new_root(
    metadata_repo: &Rc<RefCell<MetadataRepositoryImpl>>,
    id_str: &'static str,
) -> &'static DatasetID {
    let id = DatasetID::new_unchecked(id_str);
    let snap = MetadataFactory::dataset_snapshot()
        .id(id)
        .source(MetadataFactory::dataset_source_root().build())
        .build();

    metadata_repo.borrow_mut().add_dataset(snap).unwrap();
    id
}

fn new_deriv(
    metadata_repo: &Rc<RefCell<MetadataRepositoryImpl>>,
    id_str: &'static str,
    inputs: &[&'static DatasetID],
) -> (&'static DatasetID, DatasetSourceDerivative) {
    let id = DatasetID::new_unchecked(id_str);
    let source = MetadataFactory::dataset_source_deriv(inputs.iter()).build_inner();
    let snap = MetadataFactory::dataset_snapshot()
        .id(id)
        .source(DatasetSource::Derivative(source.clone()))
        .build();

    metadata_repo.borrow_mut().add_dataset(snap).unwrap();
    (id, source)
}

fn append_data_block(
    metadata_repo: &Rc<RefCell<MetadataRepositoryImpl>>,
    id: &DatasetID,
) -> MetadataBlock {
    let mut chain = metadata_repo.borrow_mut().get_metadata_chain(id).unwrap();
    chain.append(
        MetadataFactory::metadata_block()
            .prev(&chain.read_ref(&BlockRef::Head).unwrap())
            .output_slice(DataSlice {
                hash: Sha3_256::zero(),
                num_records: 100,
                interval: TimeInterval::singleton(Utc.ymd(2020, 1, 1).and_hms(12, 0, 0)),
            })
            .output_watermark(Utc.ymd(2020, 1, 1).and_hms(10, 0, 0))
            .build(),
    );
    chain.iter_blocks().next().unwrap()
}

#[test]
fn test_get_next_operation() {
    let tempdir = tempfile::tempdir().unwrap();
    let workspace_layout = WorkspaceLayout::create(tempdir.path()).unwrap();
    let volume_layout = VolumeLayout::new(&workspace_layout.local_volume_dir);

    let metadata_repo = Rc::new(RefCell::new(MetadataRepositoryImpl::new(&workspace_layout)));
    let transform_svc = TransformServiceImpl::new(
        metadata_repo.clone(),
        // TODO: Use a mock
        Arc::new(Mutex::new(EngineFactory::new(
            &workspace_layout,
            DockerClient::default(),
            slog::Logger::root(slog::Discard, slog::o!()),
        ))),
        &volume_layout,
        slog::Logger::root(slog::Discard, slog::o!()),
    );

    let foo = new_root(&metadata_repo, "foo");
    let foo_layout = DatasetLayout::new(&volume_layout, foo);

    let (bar, bar_source) = new_deriv(&metadata_repo, "bar", &[foo]);

    // No data - no work
    assert_eq!(transform_svc.get_next_operation(bar).unwrap(), None);

    let foo_block = append_data_block(&metadata_repo, foo);

    assert!(matches!(
        transform_svc.get_next_operation(bar).unwrap(),
        Some(ExecuteQueryRequest { source, input_slices, .. })
        if source == bar_source &&
        input_slices == map! { foo.to_owned() => InputDataSlice {
            interval: TimeInterval::unbounded_closed_right(foo_block.system_time),
            data_paths: vec![foo_layout.data_dir.join(foo_block.block_hash.to_string())],
            schema_file: foo_layout.data_dir.join(foo_block.block_hash.to_string()),
            explicit_watermarks: vec![Watermark {
                system_time: foo_block.system_time,
                event_time: Utc.ymd(2020, 1, 1).and_hms(10, 0, 0),
            }],
        }}
    ));
}
