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

use dill::*;
use std::sync::Arc;

pub struct PushServiceImpl {
    dataset_reg: Arc<dyn DatasetRegistry>,
    remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
    sync_svc: Arc<dyn SyncService>,
}

#[component(pub)]
impl PushServiceImpl {
    pub fn new(
        dataset_reg: Arc<dyn DatasetRegistry>,
        remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
        sync_svc: Arc<dyn SyncService>,
    ) -> Self {
        Self {
            dataset_reg,
            remote_alias_reg,
            sync_svc,
        }
    }

    fn collect_plan(
        &self,
        dataset_refs: &mut dyn Iterator<Item = DatasetRefAny>,
    ) -> Result<Vec<PushInfo>, (DatasetRefAny, PushError)> {
        let mut plan = Vec::new();
        for r in dataset_refs {
            let item = self
                .collect_plan_item(&r)
                .map_err(|e| (r.to_owned(), e.into()))?;
            plan.push(item);
        }
        Ok(plan)
    }

    fn collect_plan_item(&self, dataset_ref: &DatasetRefAny) -> Result<PushInfo, PushError> {
        // A reference can be:
        // - Local name or an ID (should be resolved to a remote alias)
        // - Remote alias (should be used to push the local dataset associated with it)
        if let Some(local_ref) = dataset_ref.as_local_ref() {
            let local_handle = self.dataset_reg.resolve_dataset_ref(&local_ref)?;

            let remote_aliases = self
                .remote_alias_reg
                .get_remote_aliases(&local_handle.as_local_ref())?;

            let mut push_aliases: Vec<_> =
                remote_aliases.get_by_kind(RemoteAliasKind::Push).collect();

            match push_aliases.len() {
                0 => Err(PushError::NoTarget),
                1 => Ok(PushInfo {
                    original_ref: dataset_ref.clone(),
                    local_handle: Some(local_handle.clone()),
                    remote_handle: Some(RemoteDatasetHandle::new(
                        local_handle.id.clone(),
                        push_aliases.remove(0).clone(),
                    )),
                }),
                // TODO: Support disambiguation
                _ => Err(PushError::AmbiguousTarget),
            }
        } else {
            let remote_name = match dataset_ref.as_remote_ref().unwrap() {
                DatasetRefRemote::ID(_) => unreachable!(),
                DatasetRefRemote::RemoteName(name) => name,
                DatasetRefRemote::RemoteHandle(hdl) => hdl.name,
            };

            // TODO: avoid traversing all datasets for every alias
            for dataset_handle in self.dataset_reg.get_all_datasets() {
                if self
                    .remote_alias_reg
                    .get_remote_aliases(&dataset_handle.as_local_ref())?
                    .contains(&remote_name, RemoteAliasKind::Push)
                {
                    return Ok(PushInfo {
                        original_ref: dataset_ref.to_owned(),
                        local_handle: Some(dataset_handle.clone()),
                        remote_handle: Some(RemoteDatasetHandle {
                            id: dataset_handle.id.clone(),
                            name: remote_name,
                        }),
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
        dataset_refs: &mut dyn Iterator<Item = DatasetRefAny>,
        options: PushOptions,
        sync_listener: Option<Arc<dyn SyncMultiListener>>,
    ) -> Vec<(PushInfo, Result<PushResult, PushError>)> {
        if options.recursive {
            unimplemented!("Recursive push is not yet supported")
        }
        if options.all {
            unimplemented!("Pushing all datasets is not yet supported")
        }

        let sync_plan = match self.collect_plan(dataset_refs) {
            Ok(plan) => plan,
            Err((dr, err)) => {
                return vec![(
                    PushInfo {
                        original_ref: dr,
                        local_handle: None,
                        remote_handle: None,
                    },
                    Err(err),
                )]
            }
        };

        let sync_results = self.sync_svc.sync_to_multi(
            &mut sync_plan.iter().map(|pi| {
                (
                    pi.local_handle.as_ref().unwrap().into(),
                    pi.remote_handle.as_ref().unwrap().name.clone(),
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
