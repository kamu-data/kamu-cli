use crate::domain::*;
use opendatafabric::*;

use dill::*;
use slog::Logger;
use std::sync::{Arc, Mutex};

pub struct PushServiceImpl {
    metadata_repo: Arc<dyn MetadataRepository>,
    sync_svc: Arc<dyn SyncService>,
    _logger: Logger,
}

#[component(pub)]
impl PushServiceImpl {
    pub fn new(
        metadata_repo: Arc<dyn MetadataRepository>,
        sync_svc: Arc<dyn SyncService>,
        logger: Logger,
    ) -> Self {
        Self {
            metadata_repo,
            sync_svc,
            _logger: logger,
        }
    }

    fn collect_plan(
        &self,
        dataset_refs: &mut dyn Iterator<Item = &DatasetRef>,
    ) -> Result<Vec<PushInfo>, (DatasetRefBuf, PushError)> {
        let mut plan = Vec::new();
        for r in dataset_refs {
            let item = self
                .collect_plan_item(r)
                .map_err(|e| (r.to_owned(), e.into()))?;
            plan.push(item);
        }
        Ok(plan)
    }

    fn collect_plan_item(&self, dataset_ref: &DatasetRef) -> Result<PushInfo, PushError> {
        // A reference can be:
        // - Local ID (should be resolved to a remote alias)
        // - Remote alias (should be used to push the local dataset associated with it)
        if dataset_ref.is_local() {
            let mut push_aliases: Vec<_> = self
                .metadata_repo
                .get_remote_aliases(dataset_ref.local_id())?
                .get_by_kind(RemoteAliasKind::Push)
                .map(|r| r.to_owned())
                .collect();

            match push_aliases.len() {
                0 => Err(PushError::NoTarget),
                1 => Ok(PushInfo {
                    original_ref: dataset_ref.to_owned(),
                    local_id: Some(dataset_ref.local_id().to_owned()),
                    remote_ref: Some(push_aliases.remove(0).to_owned()),
                }),
                // TODO: Support disambiguation
                _ => Err(PushError::AmbiguousTarget),
            }
        } else {
            // TODO: avoid traversing all datasets for every alias
            for dataset_id in self.metadata_repo.get_all_datasets() {
                if self
                    .metadata_repo
                    .get_remote_aliases(&dataset_id)?
                    .contains(dataset_ref, RemoteAliasKind::Push)
                {
                    return Ok(PushInfo {
                        original_ref: dataset_ref.to_owned(),
                        local_id: Some(dataset_id),
                        remote_ref: Some(dataset_ref.to_owned()),
                    });
                }
            }
            Err(PushError::NoTarget)
        }
    }

    fn convert_sync_result(res: Result<SyncResult, SyncError>) -> Result<PushResult, PushError> {
        match res {
            Ok(sr) => match sr {
                SyncResult::UpToDate => Ok(PushResult::UpToDate),
                SyncResult::Updated {
                    old_head,
                    new_head,
                    num_blocks,
                } => Ok(PushResult::Updated {
                    old_head,
                    new_head,
                    num_blocks,
                }),
            },
            Err(se) => Err(se.into()),
        }
    }
}

impl PushService for PushServiceImpl {
    fn push_multi(
        &self,
        dataset_refs: &mut dyn Iterator<Item = &DatasetRef>,
        options: PushOptions,
        sync_listener: Option<Arc<Mutex<dyn SyncMultiListener>>>,
    ) -> Vec<(PushInfo, Result<PushResult, PushError>)> {
        if options.recursive {
            unimplemented!("Recursive push is not yet supported")
        }
        if options.all {
            unimplemented!("Pushing all datasets is not yet supported")
        }

        let sync_plan = match self.collect_plan(dataset_refs) {
            Ok(plan) => plan,
            Err((id, err)) => {
                return vec![(
                    PushInfo {
                        original_ref: id,
                        local_id: None,
                        remote_ref: None,
                    },
                    Err(err),
                )]
            }
        };

        let sync_results = self.sync_svc.sync_to_multi(
            &mut sync_plan.iter().map(|pi| {
                (
                    pi.local_id.as_ref().unwrap().as_ref(),
                    pi.remote_ref.as_ref().unwrap().as_ref(),
                )
            }),
            options.sync_options,
            sync_listener,
        );

        assert_eq!(sync_plan.len(), sync_results.len());

        std::iter::zip(sync_plan, sync_results)
            .map(|(pi, (_, res))| (pi, Self::convert_sync_result(res)))
            .collect()
    }
}
