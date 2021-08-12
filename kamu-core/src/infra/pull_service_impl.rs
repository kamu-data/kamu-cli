use crate::domain::*;
use opendatafabric::*;

use chrono::prelude::*;
use dill::*;
use slog::{info, Logger};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

pub struct PullServiceImpl {
    metadata_repo: Arc<dyn MetadataRepository>,
    ingest_svc: Arc<dyn IngestService>,
    transform_svc: Arc<dyn TransformService>,
    sync_svc: Arc<dyn SyncService>,
    logger: Logger,
}

#[component(pub)]
impl PullServiceImpl {
    pub fn new(
        metadata_repo: Arc<dyn MetadataRepository>,
        ingest_svc: Arc<dyn IngestService>,
        transform_svc: Arc<dyn TransformService>,
        sync_svc: Arc<dyn SyncService>,
        logger: Logger,
    ) -> Self {
        Self {
            metadata_repo,
            ingest_svc,
            transform_svc,
            sync_svc,
            logger,
        }
    }

    // This function descends down the dependency tree of datasets (starting with provided references)
    // assigning depth index to every dataset in the graph(s).
    // Datasets that share the same depth level are independent and can be pulled in parallel.
    fn collect_pull_graph<'i>(
        &self,
        starting_dataset_refs: impl Iterator<Item = &'i DatasetRef>,
    ) -> Result<Vec<PullInfo>, (DatasetRefBuf, PullError)> {
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
        dataset_ref: &DatasetRef,
        visited: &mut HashMap<DatasetRefBuf, PullInfo>,
    ) -> Result<i32, (DatasetRefBuf, PullError)> {
        // Already visited? (by original ref)
        if let Some(pi) = visited.get(dataset_ref) {
            return Ok(pi.depth);
        }

        let is_local = dataset_ref.is_local();

        let local_id = if is_local {
            dataset_ref.local_id().to_owned()
        } else {
            // This can be either an alias of existing dataset or a completely new dataset
            self.resolve_remote_ref_to_local(dataset_ref)
        };

        // Already visited? (by local ID)
        if !is_local {
            if let Some(pi) = visited.get(local_id.as_dataset_ref()) {
                return Ok(pi.depth);
            }
            visited.insert(
                local_id.to_owned().into(),
                PullInfo {
                    depth: 0,
                    local_id: local_id.to_owned(),
                    remote_ref: Some(dataset_ref.to_owned()),
                    original_ref: dataset_ref.to_owned(),
                },
            );
            return Ok(0);
        }

        // Check config for pull aliases (a sign of a remote dataset)
        let mut pull_aliases: Vec<_> = self
            .metadata_repo
            .get_remote_aliases(&local_id)
            .map_err(|e| (dataset_ref.to_owned(), e.into()))?
            .get_by_kind(RemoteAliasKind::Pull)
            .map(|r| r.to_owned())
            .collect();

        if !pull_aliases.is_empty() {
            // Considering a remote dataset, so not resolving dependencies
            let remote_ref = if pull_aliases.len() == 1 {
                pull_aliases.remove(0)
            } else {
                return Err((dataset_ref.to_owned(), PullError::AmbiguousSource));
            };
            visited.insert(
                local_id.to_owned().into(),
                PullInfo {
                    depth: 0,
                    local_id: local_id.to_owned(),
                    remote_ref: Some(remote_ref.to_owned()),
                    original_ref: dataset_ref.to_owned(),
                },
            );
            Ok(0)
        } else {
            // This is either local root or derivative dataset
            let summary = self.metadata_repo.get_summary(&local_id).unwrap();

            // TODO: Should be accounting for historical dependencies, not only current ones?
            let mut max_dep_depth = -1;

            for dep in &summary.dependencies {
                let depth = self.collect_pull_graph_depth_first(dep.as_dataset_ref(), visited)?;
                max_dep_depth = std::cmp::max(max_dep_depth, depth);
            }

            visited.insert(
                local_id.to_owned().into(),
                PullInfo {
                    depth: max_dep_depth + 1,
                    local_id: local_id.to_owned(),
                    remote_ref: None,
                    original_ref: dataset_ref.to_owned(),
                },
            );
            Ok(max_dep_depth + 1)
        }
    }

    // Given a remote reference determines the name of a (new or existing) dataset
    // it should be synced into
    fn resolve_remote_ref_to_local(&self, remote_ref: &DatasetRef) -> DatasetIDBuf {
        // Do a quick check when remote and local names match
        if let Ok(aliases) = self.metadata_repo.get_remote_aliases(remote_ref.local_id()) {
            if aliases.contains(remote_ref, RemoteAliasKind::Pull) {
                return remote_ref.local_id().to_owned();
            }
        }

        // No luck - now have to search through aliases
        // TODO: Avoid iterating all datasets for every remote reference
        for local_id in self.metadata_repo.get_all_datasets() {
            let aliases = self.metadata_repo.get_remote_aliases(&local_id).unwrap();

            if aliases.contains(remote_ref, RemoteAliasKind::Pull) {
                return local_id;
            }
        }

        // This must be a new dataset
        remote_ref.local_id().to_owned()
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

impl PullService for PullServiceImpl {
    fn pull_multi(
        &self,
        dataset_refs: &mut dyn Iterator<Item = &DatasetRef>,
        options: PullOptions,
        ingest_listener: Option<Arc<dyn IngestMultiListener>>,
        transform_listener: Option<Arc<dyn TransformMultiListener>>,
        sync_listener: Option<Arc<dyn SyncMultiListener>>,
    ) -> Vec<(DatasetRefBuf, Result<PullResult, PullError>)> {
        // Starting refs can contain:
        // - DatasetIDs of local datasets (Root and Derivative)
        // - DatasetIDs of datasets that have a remote pull alias (thus should be synced)
        // - DatasetRefs of remote datasets that are not present locally and should be synced
        // - DatasetRefs of remote datasets that already exist locally (thus explicitly specifying an alias to pull from)
        let starting_dataset_refs: std::collections::HashSet<DatasetRefBuf> = if !options.all {
            dataset_refs.map(|r| r.to_owned()).collect()
        } else {
            self.metadata_repo
                .get_all_datasets()
                .map(|id| id.into())
                .collect()
        };

        info!(self.logger, "Performing pull_multi"; "starting_dataset_refs" => ?starting_dataset_refs);

        let mut pull_plan =
            match self.collect_pull_graph(starting_dataset_refs.iter().map(|r| r.as_ref())) {
                Ok(plan) => plan,
                Err((id, err)) => return vec![(id, Err(err))],
            };

        if !(options.recursive || options.all) {
            pull_plan.retain(|pi| starting_dataset_refs.contains(&pi.original_ref));
        }

        let mut results = Vec::with_capacity(pull_plan.len());

        let mut rest = &pull_plan[..];
        while !rest.is_empty() {
            let (depth, is_remote, batch, tail) = self.slice(rest);
            rest = tail;

            let results_level: Vec<(DatasetRefBuf, _)> = if depth == 0 && !is_remote {
                self.ingest_svc
                    .ingest_multi(
                        &mut batch.iter().map(|pi| pi.local_id.as_ref()),
                        options.ingest_options.clone(),
                        ingest_listener.clone(),
                    )
                    .into_iter()
                    .map(|(id, res)| (id.into(), Self::result_into(res)))
                    .collect()
            } else if depth == 0 && is_remote {
                self.sync_svc
                    .sync_from_multi(
                        &mut batch.iter().map(|pi| {
                            (
                                pi.remote_ref.as_ref().unwrap().as_ref(),
                                pi.local_id.as_ref(),
                            )
                        }),
                        options.sync_options.clone(),
                        sync_listener.clone(),
                    )
                    .into_iter()
                    .map(|((remote_ref, local_id), res)| {
                        // Associate newly-synced datasets with remotes
                        if options.create_remote_aliases {
                            if let Ok(SyncResult::Updated { old_head: None, .. }) = res {
                                self.metadata_repo
                                    .get_remote_aliases(&local_id)
                                    .unwrap()
                                    .add(remote_ref, RemoteAliasKind::Pull)
                                    .unwrap();
                            }
                        }
                        (local_id.into(), Self::result_into(res))
                    })
                    .collect()
            } else {
                self.transform_svc
                    .transform_multi(
                        &mut batch.iter().map(|pi| pi.local_id.as_ref()),
                        transform_listener.clone(),
                    )
                    .into_iter()
                    .map(|(id, res)| (id.into(), Self::result_into(res)))
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

    fn sync_from(
        &self,
        remote_ref: &DatasetRef,
        local_id: &DatasetID,
        options: PullOptions,
        listener: Option<Arc<dyn SyncListener>>,
    ) -> Result<PullResult, PullError> {
        let res = self
            .sync_svc
            .sync_from(remote_ref, local_id, options.sync_options, listener);

        if res.is_ok() && options.create_remote_aliases {
            self.metadata_repo
                .get_remote_aliases(local_id)?
                .add(remote_ref.to_owned(), RemoteAliasKind::Pull)?;
        }

        Self::result_into(res)
    }

    fn ingest_from(
        &self,
        dataset_id: &DatasetID,
        fetch: FetchStep,
        options: PullOptions,
        listener: Option<Arc<dyn IngestListener>>,
    ) -> Result<PullResult, PullError> {
        if !self
            .metadata_repo
            .get_remote_aliases(dataset_id)?
            .is_empty(RemoteAliasKind::Pull)
        {
            // TODO: Consider extracting into a watermark-specific error type
            panic!("Attempting to ingest data into remote dataset");
        }

        let res = self
            .ingest_svc
            .ingest_from(dataset_id, fetch, options.ingest_options, listener);

        Self::result_into(res)
    }

    fn set_watermark(
        &self,
        dataset_id: &DatasetID,
        watermark: DateTime<Utc>,
    ) -> Result<PullResult, PullError> {
        if !self
            .metadata_repo
            .get_remote_aliases(dataset_id)?
            .is_empty(RemoteAliasKind::Pull)
        {
            // TODO: Consider extracting into a watermark-specific error type
            panic!("Attempting to set watermark on a remote dataset");
        }

        let mut chain = self.metadata_repo.get_metadata_chain(dataset_id)?;

        if let Some(last_watermark) = chain
            .iter_blocks()
            .filter_map(|b| b.output_watermark)
            .next()
        {
            if last_watermark >= watermark {
                return Ok(PullResult::UpToDate);
            }
        }

        let old_head = chain.read_ref(&BlockRef::Head);

        let new_block = MetadataBlock {
            block_hash: Sha3_256::zero(),
            prev_block_hash: old_head,
            system_time: Utc::now(),
            output_slice: None,
            output_watermark: Some(watermark),
            input_slices: None,
            source: None,
            vocab: None,
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
    original_ref: DatasetRefBuf,
    local_id: DatasetIDBuf,
    remote_ref: Option<DatasetRefBuf>,
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

        self.local_id.cmp(&other.local_id)
    }
}
