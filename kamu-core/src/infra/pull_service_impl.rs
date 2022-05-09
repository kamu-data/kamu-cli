// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::domain::*;
use opendatafabric::*;

use chrono::prelude::*;
use dill::*;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

pub struct PullServiceImpl {
    dataset_reg: Arc<dyn DatasetRegistry>,
    remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
    ingest_svc: Arc<dyn IngestService>,
    transform_svc: Arc<dyn TransformService>,
    sync_svc: Arc<dyn SyncService>,
}

#[component(pub)]
impl PullServiceImpl {
    pub fn new(
        dataset_reg: Arc<dyn DatasetRegistry>,
        remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
        ingest_svc: Arc<dyn IngestService>,
        transform_svc: Arc<dyn TransformService>,
        sync_svc: Arc<dyn SyncService>,
    ) -> Self {
        Self {
            dataset_reg,
            remote_alias_reg,
            ingest_svc,
            transform_svc,
            sync_svc,
        }
    }

    // This function descends down the dependency tree of datasets (starting with provided references)
    // assigning depth index to every dataset in the graph(s).
    // Datasets that share the same depth level are independent and can be pulled in parallel.
    fn collect_pull_graph<'i>(
        &self,
        starting_dataset_refs: impl Iterator<Item = &'i DatasetRefAny>,
    ) -> Result<Vec<PullInfo>, (DatasetRefAny, PullError)> {
        let mut visited = HashMap::new();
        for dr in starting_dataset_refs {
            self.collect_pull_graph_depth_first(dr, &mut visited)?;
        }

        let mut ordered = Vec::with_capacity(visited.len());
        ordered.extend(visited.into_values());
        ordered.sort();
        Ok(ordered)
    }

    fn collect_pull_graph_depth_first(
        &self,
        dataset_ref: &DatasetRefAny,
        visited: &mut HashMap<DatasetName, PullInfo>,
    ) -> Result<i32, (DatasetRefAny, PullError)> {
        let (dataset_id, local_name, remote_ref) = match dataset_ref {
            DatasetRefAny::ID(id) => {
                match self.dataset_reg.resolve_dataset_ref(&id.as_local_ref()) {
                    Ok(local_hdl) => {
                        let remote_name = self
                            .resolve_local_to_remote_alias(&local_hdl.as_local_ref())
                            .map_err(|e| (dataset_ref.clone(), e))?;
                        (Some(id.clone()), local_hdl.name, remote_name)
                    }
                    Err(_) => {
                        // TODO: REMOTE ID
                        unimplemented!("Pulling via remote ID is not yet supported")
                    }
                }
            }
            DatasetRefAny::Name(_) | DatasetRefAny::Handle(_) => {
                let local_hdl = self
                    .dataset_reg
                    .resolve_dataset_ref(&dataset_ref.as_local_ref().unwrap())
                    .map_err(|e| (dataset_ref.clone(), e.into()))?;

                let remote_name = self
                    .resolve_local_to_remote_alias(&local_hdl.as_local_ref())
                    .map_err(|e| (dataset_ref.clone(), e))?;

                (Some(local_hdl.id), local_hdl.name, remote_name)
            }
            DatasetRefAny::RemoteName(name)
            | DatasetRefAny::RemoteHandle(RemoteDatasetHandle { name, .. }) => {
                let local_name = self.resolve_remote_name_to_local(&name);
                (None, local_name, Some(name.as_remote_ref()))
            }
            DatasetRefAny::Url(_) => unimplemented!("Pulling via URL is not yet supported"),
        };

        // Already visited?
        if let Some(pi) = visited.get(&local_name) {
            return Ok(pi.depth);
        }

        // Pulling from repository has depth 0
        if let Some(remote_ref) = remote_ref {
            visited.insert(
                local_name.clone(),
                PullInfo {
                    depth: 0,
                    dataset_id,
                    local_name,
                    remote_ref: Some(remote_ref),
                },
            );
            return Ok(0);
        }

        // Pulling an existing local root or derivative dataset
        let summary = self
            .dataset_reg
            .get_summary(&local_name.as_local_ref())
            .unwrap();

        // TODO: EVO: Should be accounting for historical dependencies, not only current ones?
        let mut max_dep_depth = -1;

        for dep in &summary.dependencies {
            let depth = self
                .collect_pull_graph_depth_first(&dep.id.as_ref().unwrap().as_any_ref(), visited)?;
            max_dep_depth = std::cmp::max(max_dep_depth, depth);
        }

        visited.insert(
            local_name.clone(),
            PullInfo {
                depth: max_dep_depth + 1,
                dataset_id,
                local_name,
                remote_ref: None,
            },
        );
        Ok(max_dep_depth + 1)
    }

    /// Given a remote name determines the name of a (new or existing) dataset
    /// it should be synced into
    fn resolve_remote_name_to_local(&self, remote_name: &RemoteDatasetName) -> DatasetName {
        // Do a quick check when remote and local names match
        if let Ok(local_hdl) = self
            .dataset_reg
            .resolve_dataset_ref(&remote_name.dataset().as_local_ref())
        {
            if let Ok(aliases) = self
                .remote_alias_reg
                .get_remote_aliases(&local_hdl.as_local_ref())
            {
                if aliases.contains(&remote_name.as_remote_ref(), RemoteAliasKind::Pull) {
                    return local_hdl.name;
                }
            }
        }

        // No luck - now have to search through aliases
        // TODO: Avoid iterating all datasets for every remote reference
        for local_hdl in self.dataset_reg.get_all_datasets() {
            let aliases = self
                .remote_alias_reg
                .get_remote_aliases(&local_hdl.as_local_ref())
                .unwrap();

            if aliases.contains(&remote_name.as_remote_ref(), RemoteAliasKind::Pull) {
                return local_hdl.name;
            }
        }

        // This must be a new dataset - we will use dataset name from the remote reference
        // TODO: Handle conflicts early?
        remote_name.dataset().clone()
    }

    /// Given a local dataset tries to inver where to pull data from using remote aliases
    fn resolve_local_to_remote_alias(
        &self,
        local_ref: &DatasetRefLocal,
    ) -> Result<Option<DatasetRefRemote>, PullError> {
        let mut pull_aliases: Vec<_> = self
            .remote_alias_reg
            .get_remote_aliases(local_ref)?
            .get_by_kind(RemoteAliasKind::Pull)
            .map(|r| r.clone())
            .collect();

        if pull_aliases.is_empty() {
            Ok(None)
        } else {
            if pull_aliases.len() == 1 {
                Ok(Some(pull_aliases.remove(0)))
            } else {
                Err(PullError::AmbiguousSource)
            }
        }
    }

    fn slice<'a>(&self, to_slice: &'a [PullInfo]) -> (i32, bool, &'a [PullInfo], &'a [PullInfo]) {
        let first = &to_slice[0];
        let count = to_slice
            .iter()
            .take_while(|pi| {
                pi.depth == first.depth && pi.remote_ref.is_some() == first.remote_ref.is_some()
            })
            .count();
        (
            first.depth,
            first.remote_ref.is_some(),
            &to_slice[..count],
            &to_slice[count..],
        )
    }

    fn result_into<R: Into<PullResult>, E: Into<PullError>>(
        res: Result<R, E>,
    ) -> Result<PullResult, PullError> {
        match res {
            Ok(r) => Ok(r.into()),
            Err(e) => Err(e.into()),
        }
    }
}

#[async_trait::async_trait(?Send)]
impl PullService for PullServiceImpl {
    async fn pull_multi(
        &self,
        dataset_refs: &mut dyn Iterator<Item = DatasetRefAny>,
        options: PullOptions,
        ingest_listener: Option<Arc<dyn IngestMultiListener>>,
        transform_listener: Option<Arc<dyn TransformMultiListener>>,
        sync_listener: Option<Arc<dyn SyncMultiListener>>,
    ) -> Vec<(DatasetRefAny, Result<PullResult, PullError>)> {
        // Starting refs can contain:
        // - Local datasets (Root and Derivative)
        // - Datasets that have a remote pull alias (thus should be synced)
        // - Remote datasets that are not present locally and should be synced
        // - Remote datasets that already exist locally (thus explicitly specifying an alias to pull from)
        let starting_dataset_refs: Vec<_> = if !options.all {
            dataset_refs.collect()
        } else {
            self.dataset_reg
                .get_all_datasets()
                .map(|hdl| hdl.into())
                .collect()
        };

        info!(starting_dataset_refs = ?starting_dataset_refs, "Performing pull_multi");

        let mut pull_plan = match self.collect_pull_graph(starting_dataset_refs.iter()) {
            Ok(plan) => plan,
            Err((dr, err)) => return vec![(dr, Err(err))],
        };

        if !(options.recursive || options.all) {
            // Leave only datasets explicitly mentioned (preserving the depth order)
            pull_plan.retain(|pi| starting_dataset_refs.iter().any(|sr| pi.referenced_by(sr)));
        }

        let mut results = Vec::with_capacity(pull_plan.len());

        let mut rest = &pull_plan[..];
        while !rest.is_empty() {
            let (depth, is_remote, batch, tail) = self.slice(rest);
            rest = tail;

            let results_level: Vec<(DatasetRefAny, _)> = if depth == 0 && !is_remote {
                self.ingest_svc
                    .ingest_multi(
                        &mut batch.iter().map(|pi| pi.local_name.as_local_ref()),
                        options.ingest_options.clone(),
                        ingest_listener.clone(),
                    )
                    .await
                    .into_iter()
                    .map(|(dr, res)| (dr.into(), Self::result_into(res)))
                    .collect()
            } else if depth == 0 && is_remote {
                let sync_results = self
                    .sync_svc
                    .sync_multi(
                        &mut batch.iter().map(|pi| {
                            (
                                pi.remote_ref.as_ref().unwrap().into(),
                                pi.local_name.as_any_ref(),
                            )
                        }),
                        options.sync_options.clone(),
                        sync_listener.clone(),
                    )
                    .await;

                // Associate newly-synced datasets with remotes
                if options.create_remote_aliases {
                    for res in &sync_results {
                        if let Ok(SyncResult::Updated { old_head: None, .. }) = res.result {
                            if let Some(remote_ref) = res.src.as_remote_ref() {
                                self.remote_alias_reg
                                    .get_remote_aliases(&res.dst.as_local_ref().unwrap())
                                    .unwrap()
                                    .add(&remote_ref, RemoteAliasKind::Pull)
                                    .unwrap();
                            }
                        }
                    }
                }

                sync_results
                    .into_iter()
                    .map(|res| (res.dst, Self::result_into(res.result)))
                    .collect()
            } else {
                self.transform_svc
                    .transform_multi(
                        &mut batch.iter().map(|pi| pi.local_name.as_local_ref()),
                        transform_listener.clone(),
                    )
                    .await
                    .into_iter()
                    .map(|(dr, res)| (dr.into(), Self::result_into(res)))
                    .collect()
            };

            let errors = results_level.iter().any(|(_, r)| r.is_err());
            results.extend(results_level);
            if errors {
                break;
            }
        }

        results
    }

    async fn sync_from(
        &self,
        remote_ref: &DatasetRefRemote,
        local_name: &DatasetName,
        options: PullOptions,
        listener: Option<Arc<dyn SyncListener>>,
    ) -> Result<PullResult, PullError> {
        let res = self
            .sync_svc
            .sync(
                &remote_ref.into(),
                &local_name.into(),
                options.sync_options,
                listener,
            )
            .await;

        if res.is_ok() && options.create_remote_aliases {
            self.remote_alias_reg
                .get_remote_aliases(&local_name.as_local_ref())?
                .add(remote_ref, RemoteAliasKind::Pull)?;
        }

        Self::result_into(res)
    }

    async fn ingest_from(
        &self,
        dataset_ref: &DatasetRefLocal,
        fetch: FetchStep,
        options: PullOptions,
        listener: Option<Arc<dyn IngestListener>>,
    ) -> Result<PullResult, PullError> {
        if !self
            .remote_alias_reg
            .get_remote_aliases(dataset_ref)?
            .is_empty(RemoteAliasKind::Pull)
        {
            // TODO: Consider extracting into an error type
            panic!("Attempting to ingest data into a remote dataset");
        }

        let res = self
            .ingest_svc
            .ingest_from(dataset_ref, fetch, options.ingest_options, listener)
            .await;

        Self::result_into(res)
    }

    async fn set_watermark(
        &self,
        dataset_ref: &DatasetRefLocal,
        watermark: DateTime<Utc>,
    ) -> Result<PullResult, PullError> {
        if !self
            .remote_alias_reg
            .get_remote_aliases(dataset_ref)?
            .is_empty(RemoteAliasKind::Pull)
        {
            // TODO: Consider extracting into a watermark-specific error type
            panic!("Attempting to set watermark on a remote dataset");
        }

        let mut chain = self.dataset_reg.get_metadata_chain(dataset_ref)?;

        if let Some(last_watermark) = chain
            .iter_blocks()
            .filter_map(|(_, b)| b.into_data_stream_block())
            .find_map(|b| b.event.output_watermark)
        {
            if last_watermark >= watermark {
                return Ok(PullResult::UpToDate);
            }
        }

        let old_head = chain.read_ref(&BlockRef::Head);

        let new_block = MetadataBlock {
            system_time: Utc::now(),
            prev_block_hash: old_head.clone(),
            event: MetadataEvent::SetWatermark(SetWatermark {
                output_watermark: watermark,
            }),
        };

        let new_head = chain.append(new_block);
        Ok(PullResult::Updated {
            old_head,
            new_head,
            num_blocks: 1,
        })
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
struct PullInfo {
    depth: i32,
    dataset_id: Option<DatasetID>,
    local_name: DatasetName,
    remote_ref: Option<DatasetRefRemote>,
}

impl PullInfo {
    fn referenced_by(&self, r: &DatasetRefAny) -> bool {
        match r {
            DatasetRefAny::ID(id) => Some(id) == self.dataset_id.as_ref(),
            DatasetRefAny::Name(name) | DatasetRefAny::Handle(DatasetHandle { name, .. }) => {
                *name == self.local_name
            }
            DatasetRefAny::RemoteName(name)
            | DatasetRefAny::RemoteHandle(RemoteDatasetHandle { name, .. }) => {
                match &self.remote_ref {
                    Some(DatasetRefRemote::RemoteName(sname))
                    | Some(DatasetRefRemote::RemoteHandle(RemoteDatasetHandle {
                        name: sname,
                        ..
                    })) => sname == name,
                    _ => false,
                }
            }
            DatasetRefAny::Url(_) => unimplemented!(),
        }
    }
}

impl PartialOrd for PullInfo {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PullInfo {
    fn cmp(&self, other: &Self) -> Ordering {
        let depth_ord = self.depth.cmp(&other.depth);
        if depth_ord != Ordering::Equal {
            return depth_ord;
        }

        if self.remote_ref.is_some() != other.remote_ref.is_some() {
            return if self.remote_ref.is_some() {
                Ordering::Less
            } else {
                Ordering::Greater
            };
        }

        self.local_name.cmp(&other.local_name)
    }
}
