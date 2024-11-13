// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use dill::{Catalog, CatalogBuilder};
use internal_error::InternalError;
use kamu::testing::MetadataFactory;
use kamu::{
    DatasetImpl,
    MetadataBlockRepositoryImpl,
    MetadataChainImpl,
    NamedObjectRepositoryInMemory,
    ObjectRepositoryInMemory,
    ReferenceRepositoryImpl,
    RemoteStatusServiceImpl,
};
use kamu_core::testing::MockRemoteAliasesRegistry;
use kamu_core::utils::metadata_chain_comparator::CompareChainsResult;
use kamu_core::{
    AppendOpts,
    BuildDatasetError,
    Dataset,
    DatasetFactory,
    DatasetHandleStream,
    DatasetHandlesResolution,
    DatasetRegistry,
    GetDatasetError,
    // GetDatasetUrlError,
    GetMultipleDatasetsError,
    MetadataChain,
    RemoteAliasKind,
    RemoteAliases,
    RemoteAliasesRegistry,
    RemoteStatusService,
    ResolvedDataset,
    StatusCheckError,
};
use opendatafabric::{
    AccountName,
    DatasetHandle,
    DatasetID,
    DatasetKind,
    DatasetRef,
    DatasetRefRemote,
    MetadataBlock,
    Multihash,
};
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type MetaChain = MetadataChainImpl<
    MetadataBlockRepositoryImpl<ObjectRepositoryInMemory>,
    ReferenceRepositoryImpl<NamedObjectRepositoryInMemory>,
>;

type PrevBlock = (Multihash, u64);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct FakeRemoteAliases {
    pull_aliases: Vec<DatasetRefRemote>,
    push_aliases: Vec<DatasetRefRemote>,
}

#[async_trait::async_trait]
impl RemoteAliases for FakeRemoteAliases {
    fn get_by_kind<'a>(
        &'a self,
        kind: RemoteAliasKind,
    ) -> Box<dyn Iterator<Item = &'a DatasetRefRemote> + 'a> {
        let aliases = match kind {
            RemoteAliasKind::Pull => &self.pull_aliases,
            RemoteAliasKind::Push => &self.push_aliases,
        };
        Box::new(aliases.iter())
    }

    fn contains(&self, _remote_ref: &DatasetRefRemote, _kind: RemoteAliasKind) -> bool {
        todo!()
    }

    fn is_empty(&self, _kind: RemoteAliasKind) -> bool {
        todo!()
    }

    async fn add(
        &mut self,
        _remote_ref: &DatasetRefRemote,
        _kind: RemoteAliasKind,
    ) -> Result<bool, InternalError> {
        todo!()
    }

    async fn delete(
        &mut self,
        _remote_ref: &DatasetRefRemote,
        _kind: RemoteAliasKind,
    ) -> Result<bool, InternalError> {
        todo!()
    }

    async fn clear(&mut self, _kind: RemoteAliasKind) -> Result<usize, InternalError> {
        todo!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct FakeDatasetFactory {
    datasets: HashMap<String, Arc<dyn Dataset>>,
}

#[async_trait]
impl DatasetFactory for FakeDatasetFactory {
    async fn get_dataset(
        &self,
        url: &Url,
        _create_if_not_exists: bool,
    ) -> Result<Arc<dyn Dataset>, BuildDatasetError> {
        Ok(self.datasets.get(&url.to_string()).unwrap().clone())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct FakeDatasetRegistry {
    datasets: HashMap<String, Arc<dyn Dataset>>,
}

#[async_trait]
impl DatasetRegistry for FakeDatasetRegistry {
    fn all_dataset_handles(&self) -> DatasetHandleStream<'_> {
        todo!()
    }

    fn all_dataset_handles_by_owner(&self, _owner_name: &AccountName) -> DatasetHandleStream<'_> {
        todo!()
    }

    async fn resolve_dataset_handle_by_ref(
        &self,
        _dataset_ref: &DatasetRef,
    ) -> Result<DatasetHandle, GetDatasetError> {
        todo!()
    }

    async fn resolve_multiple_dataset_handles_by_ids(
        &self,
        _dataset_ids: Vec<DatasetID>,
    ) -> Result<DatasetHandlesResolution, GetMultipleDatasetsError> {
        todo!()
    }

    fn get_dataset_by_handle(&self, dataset_handle: &DatasetHandle) -> ResolvedDataset {
        let key = &dataset_handle.alias.dataset_name.to_string();
        ResolvedDataset::new(
            self.datasets.get(key).unwrap().clone(),
            dataset_handle.clone(),
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn append_block(chain: &MetaChain, block: MetadataBlock) -> Multihash {
    chain.append(block, AppendOpts::default()).await.unwrap()
}

fn data_block(prev_block: &PrevBlock, offset: u64) -> MetadataBlock {
    MetadataFactory::metadata_block(
        MetadataFactory::add_data()
            .some_new_data()
            .new_offset_interval(offset, offset + 9)
            .build(),
    )
    .prev(&prev_block.0, prev_block.1)
    .build()
}

fn root_block() -> MetadataBlock {
    MetadataFactory::metadata_block(MetadataFactory::seed(DatasetKind::Root).build()).build()
}

fn schema_block(root_hash: &Multihash) -> MetadataBlock {
    MetadataFactory::metadata_block(MetadataFactory::set_data_schema().build())
        .prev(root_hash, 0)
        .build()
}

fn chain() -> MetaChain {
    MetadataChainImpl::new(
        MetadataBlockRepositoryImpl::new(ObjectRepositoryInMemory::new()),
        ReferenceRepositoryImpl::new(NamedObjectRepositoryInMemory::new()),
    )
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn dataset(chain: MetaChain) -> Arc<dyn Dataset> {
    Arc::new(DatasetImpl::new(
        chain,
        ObjectRepositoryInMemory::new(),
        ObjectRepositoryInMemory::new(),
        NamedObjectRepositoryInMemory::new(),
        Url::from_str("file://test").unwrap(),
    ))
}

async fn datasets() -> (
    Arc<dyn Dataset>,
    Arc<dyn Dataset>,
    Arc<dyn Dataset>,
    Arc<dyn Dataset>,
    Arc<dyn Dataset>,
) {
    let local_chain = chain();
    let one_data_block_chain = chain();
    let two_data_block_chain = chain();
    let three_data_block_chain = chain();
    let diverged_chain = chain();

    let block = root_block();
    let last_hash = append_block(&local_chain, block.clone()).await;
    append_block(&diverged_chain, block.clone()).await;
    append_block(&one_data_block_chain, block.clone()).await;
    append_block(&two_data_block_chain, block.clone()).await;
    append_block(&three_data_block_chain, block).await;

    let block = schema_block(&last_hash);
    let last_hash = append_block(&local_chain, block.clone()).await;
    append_block(&diverged_chain, block.clone()).await;
    append_block(&one_data_block_chain, block.clone()).await;
    append_block(&two_data_block_chain, block.clone()).await;
    append_block(&three_data_block_chain, block).await;

    let block = data_block(&(last_hash, 1), 0);
    let last_hash = append_block(&local_chain, block.clone()).await;
    append_block(&diverged_chain, block.clone()).await;
    append_block(&one_data_block_chain, block.clone()).await;
    append_block(&two_data_block_chain, block.clone()).await;
    append_block(&three_data_block_chain, block).await;

    let block = data_block(&(last_hash.clone(), 2), 10);
    let diverge_block = data_block(&(last_hash, 2), 10);
    append_block(&diverged_chain, diverge_block).await;
    let last_hash = append_block(&local_chain, block.clone()).await;
    append_block(&two_data_block_chain, block.clone()).await;
    append_block(&three_data_block_chain, block).await;

    let block = data_block(&(last_hash, 3), 20);
    append_block(&three_data_block_chain, block).await;

    (
        dataset(local_chain),
        dataset(diverged_chain),
        dataset(one_data_block_chain),
        dataset(two_data_block_chain),
        dataset(three_data_block_chain),
    )
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn catalog(
    fake_ds_registry: FakeDatasetRegistry,
    fake_ds_factory: FakeDatasetFactory,
    mock_remote_alias_reg: MockRemoteAliasesRegistry,
) -> Catalog {
    let mut b = CatalogBuilder::new();
    b.add_value(fake_ds_registry);
    b.bind::<dyn DatasetRegistry, FakeDatasetRegistry>();

    b.add_value(fake_ds_factory);
    b.bind::<dyn DatasetFactory, FakeDatasetFactory>();

    b.add_value(mock_remote_alias_reg);
    b.bind::<dyn RemoteAliasesRegistry, MockRemoteAliasesRegistry>();

    b.build()
}

fn service(
    fake_ds_registry: FakeDatasetRegistry,
    fake_ds_factory: FakeDatasetFactory,
    mock_remote_alias_reg: MockRemoteAliasesRegistry,
) -> Box<dyn RemoteStatusService> {
    let catalog = catalog(fake_ds_registry, fake_ds_factory, mock_remote_alias_reg);
    Box::new(RemoteStatusServiceImpl::new(
        catalog.get_one().unwrap(),
        catalog.get_one().unwrap(),
        catalog.get_one().unwrap(),
    ))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_check_remotes_status() {
    let mut mock_remote_alias_reg = MockRemoteAliasesRegistry::new();

    let (local_ds, diverged_ds, one_data_block_ds, two_data_blocks_ds, three_data_blocks_ds) =
        datasets().await;

    let fake_ds_registry = FakeDatasetRegistry {
        datasets: HashMap::from([("local".to_string(), local_ds)]),
    };

    let diverged_url = "https://example.com/diverged";
    let one_data_block_url = "https://example.com/one_data_block";
    let two_data_blocks_url = "https://example.com/two_data_blocks";
    let three_data_blocks_url = "https://example.com/three_data_blocks";
    let unknown_ds_url = "https://example.com/unknown";

    let fake_ds_fact = FakeDatasetFactory {
        datasets: HashMap::from([
            (diverged_url.to_string(), diverged_ds),
            (one_data_block_url.to_string(), one_data_block_ds),
            (two_data_blocks_url.to_string(), two_data_blocks_ds),
            (three_data_blocks_url.to_string(), three_data_blocks_ds),
            (unknown_ds_url.to_string(), dataset(chain())),
        ]),
    };

    mock_remote_alias_reg
        .expect_get_remote_aliases()
        .returning(move |_| {
            Ok(Box::new(FakeRemoteAliases {
                pull_aliases: vec![],
                push_aliases: vec![
                    DatasetRefRemote::Url(Arc::new(Url::from_str(diverged_url).unwrap())),
                    DatasetRefRemote::Url(Arc::new(Url::from_str(one_data_block_url).unwrap())),
                    DatasetRefRemote::Url(Arc::new(Url::from_str(two_data_blocks_url).unwrap())),
                    DatasetRefRemote::Url(Arc::new(Url::from_str(three_data_blocks_url).unwrap())),
                    DatasetRefRemote::Url(Arc::new(Url::from_str(unknown_ds_url).unwrap())),
                ],
            }))
        });

    let service = service(fake_ds_registry, fake_ds_fact, mock_remote_alias_reg);

    let (_key, local_ds_id) = DatasetID::new_generated_ed25519();
    let local_ds_handle = DatasetHandle {
        id: local_ds_id,
        alias: "local".try_into().unwrap(),
    };

    let result = service
        .check_remotes_status(&local_ds_handle)
        .await
        .unwrap();

    assert_eq!(result.statuses.len(), 5);

    let map: HashMap<String, Result<CompareChainsResult, StatusCheckError>> = result
        .statuses
        .into_iter()
        .map(|s| (s.remote.to_string(), s.check_result))
        .collect();

    assert_matches!(
        map.get(diverged_url),
        Some(Ok(CompareChainsResult::Divergence { .. }))
    );

    assert_matches!(
        map.get(one_data_block_url),
        Some(Ok(CompareChainsResult::LhsAhead { .. }))
    );

    assert_matches!(
        map.get(two_data_blocks_url),
        Some(Ok(CompareChainsResult::Equal))
    );

    assert_matches!(
        map.get(three_data_blocks_url),
        Some(Ok(CompareChainsResult::LhsBehind { .. }))
    );

    assert_matches!(map.get(unknown_ds_url), Some(Err(_)));
}
