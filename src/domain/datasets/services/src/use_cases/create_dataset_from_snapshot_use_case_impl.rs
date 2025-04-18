// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, HashSet, LinkedList};
use std::sync::Arc;

use futures::TryStreamExt as _;
use internal_error::ResultIntoInternal;
use kamu_accounts::CurrentAccountSubject;
use kamu_core::{DatasetRegistry, DatasetRegistryExt as _, DidGenerator, ResolvedDataset};
use kamu_datasets::{
    CreateDatasetError,
    CreateDatasetFromSnapshotError,
    CreateDatasetFromSnapshotUseCase,
    CreateDatasetPlan,
    CreateDatasetResult,
    CreateDatasetUseCaseOptions,
    CreateDatasetsFromSnapshotsPlanningError,
    CreateDatasetsFromSnapshotsPlanningResult,
    CreateDatasetsPlan,
    CyclicDependencyError,
    NameCollisionError,
};
use odf::dataset::{DatasetHandleResolverWithPredefined, MetadataChainExt as _};
use odf::metadata::AsTypedBlock as _;
use time_source::SystemTimeSource;

use crate::utils::CreateDatasetUseCaseHelper;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn CreateDatasetFromSnapshotUseCase)]
pub struct CreateDatasetFromSnapshotUseCaseImpl {
    current_account_subject: Arc<CurrentAccountSubject>,
    system_time_source: Arc<dyn SystemTimeSource>,
    did_generator: Arc<dyn DidGenerator>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    create_helper: Arc<CreateDatasetUseCaseHelper>,
}

impl CreateDatasetFromSnapshotUseCaseImpl {
    #[allow(clippy::linkedlist)]
    fn sort_snapshots_in_dependency_order(
        &self,
        mut snapshots: LinkedList<odf::DatasetSnapshot>,
    ) -> Result<Vec<odf::DatasetSnapshot>, CyclicDependencyError> {
        let mut ordered = Vec::with_capacity(snapshots.len());
        let mut pending: HashSet<odf::DatasetRef> =
            snapshots.iter().map(|s| s.name.clone().into()).collect();
        let mut added: HashSet<odf::DatasetAlias> = HashSet::new();

        let mut iterations_since_update = 0;

        // TODO: SEC: cycle detection
        while !snapshots.is_empty() {
            let snapshot = snapshots.pop_front().unwrap();

            use odf::metadata::EnumWithVariants;
            let transform = snapshot
                .metadata
                .iter()
                .find_map(|e| e.as_variant::<odf::metadata::SetTransform>());

            let has_pending_deps = if let Some(transform) = transform {
                transform.inputs.iter().any(|input| {
                    pending.contains(&input.dataset_ref)
                        && snapshot.name.as_local_ref() != input.dataset_ref // Check for circular dependency
                })
            } else {
                false
            };

            if !has_pending_deps {
                pending.remove(&snapshot.name.clone().into());
                added.insert(snapshot.name.clone());
                ordered.push(snapshot);
                iterations_since_update = 0;
            } else {
                snapshots.push_back(snapshot);
                iterations_since_update += 1;

                // At least one item should be ordered per cycle over all remaining snapshots
                if iterations_since_update > snapshots.len() {
                    return Err(CyclicDependencyError);
                }
            }
        }

        Ok(ordered)
    }

    fn new_metadata_chain_in_memory() -> impl odf::dataset::MetadataChain {
        odf::dataset::MetadataChainImpl::new(
            odf::storage::MetadataBlockRepositoryImpl::new(
                odf::storage::inmem::ObjectRepositoryInMemory::new(),
            ),
            odf::dataset::MetadataChainReferenceRepositoryImpl::new(
                odf::storage::ReferenceRepositoryImpl::new(
                    odf::storage::inmem::NamedObjectRepositoryInMemory::new(),
                ),
            ),
        )
    }

    async fn prepare_plan(
        &self,
        mut snapshot: odf::DatasetSnapshot,
        options: CreateDatasetUseCaseOptions,
        pending_datasets: &BTreeMap<odf::DatasetAlias, odf::DatasetHandle>,
        system_time: chrono::DateTime<chrono::Utc>,
    ) -> Result<CreateDatasetPlan, CreateDatasetFromSnapshotError> {
        // There must be a logged-in user
        let (logged_account_id, logged_account_name) = match self.current_account_subject.as_ref() {
            CurrentAccountSubject::Anonymous(_) => {
                panic!("Anonymous account cannot create dataset");
            }
            CurrentAccountSubject::Logged(l) => (l.account_id.clone(), &l.account_name),
        };

        // Adjust alias for current tenancy configuration
        let canonical_alias = self
            .create_helper
            .canonical_dataset_alias(&snapshot.name, logged_account_name);

        // Verify that we can create a dataset with this alias
        self.create_helper
            .validate_canonical_dataset_alias_account_name(&canonical_alias, logged_account_name)?;

        // TODO: PERF: Avoid cloning the map
        let resolver = DatasetHandleResolverWithPredefined::new(
            Some(self.dataset_registry.clone()),
            pending_datasets.clone(),
        );

        odf::dataset::normalize_and_validate_dataset_snapshot(&resolver, &mut snapshot).await?;

        let kind = snapshot.kind;
        let mut events = snapshot.metadata;

        // Early check for name collision
        match self
            .dataset_registry
            .try_resolve_dataset_handle_by_ref(&canonical_alias.as_local_ref())
            .await?
        {
            None => (),
            Some(_) => {
                return Err(CreateDatasetFromSnapshotError::NameCollision(
                    NameCollisionError {
                        alias: canonical_alias.into_inner(),
                    },
                ))
            }
        };

        // Name collision may also happen between elements of the plan
        if pending_datasets.contains_key(&canonical_alias) {
            return Err(CreateDatasetFromSnapshotError::NameCollision(
                NameCollisionError {
                    alias: canonical_alias.into_inner(),
                },
            ));
        }

        let chain = Self::new_metadata_chain_in_memory();

        // Generate a new key pair and derive dataset ID from it
        let (id, key) = self.did_generator.generate_dataset_id();

        events.insert(
            0,
            odf::metadata::Seed {
                dataset_id: id.clone(),
                dataset_kind: kind,
            }
            .into(),
        );

        let new_head = chain
            .append_events(
                events,
                system_time,
                odf::dataset::AppendOpts {
                    ..Default::default()
                },
            )
            .await?;

        // TODOOOOOOOOOOOOOOOOOOOOOOOOOOO :: Error type
        let mut blocks: Vec<_> = chain
            .iter_blocks()
            .map_ok(|(_hash, block)| block)
            .try_collect()
            .await
            .int_err()?;

        // Flip the order to chronological
        blocks.reverse();

        Ok(CreateDatasetPlan {
            owner: logged_account_id,
            id,
            alias: canonical_alias.into_inner(),
            key,
            kind,
            blocks,
            new_head,
            options,
        })
    }

    async fn apply_plan(
        &self,
        mut plan: CreateDatasetPlan,
    ) -> Result<CreateDatasetResult, CreateDatasetError> {
        // Extract seed block
        let seed_block = plan
            .blocks
            .remove(0)
            .into_typed::<odf::metadata::Seed>()
            .expect("First block mush be Seed");

        // Create dataset entry
        self.create_helper
            .create_dataset_entry(
                &seed_block.event.dataset_id,
                &plan.owner,
                &plan.alias,
                plan.kind,
            )
            .await?;

        // Make storage level dataset (no HEAD yet)
        let store_result = self
            .create_helper
            .store_dataset(&plan.alias, seed_block)
            .await?;

        // Append blocks
        if !plan.blocks.is_empty() {
            store_result
                .dataset
                .as_metadata_chain()
                .append_blocks(
                    plan.blocks,
                    odf::dataset::AppendOpts {
                        update_ref: None,
                        ..Default::default()
                    },
                )
                .await
                .int_err()?;
        }

        // Notify interested parties the dataset was created
        self.create_helper
            .notify_dataset_created(
                &plan.id,
                &plan.alias.dataset_name,
                &plan.owner,
                plan.options.dataset_visibility,
            )
            .await?;

        // Set initial dataset HEAD
        self.create_helper
            .set_created_head(
                ResolvedDataset::from_stored(&store_result, &plan.alias),
                &plan.new_head,
            )
            .await?;

        Ok(CreateDatasetResult {
            head: plan.new_head,
            dataset: store_result.dataset,
            dataset_handle: odf::DatasetHandle::new(plan.id, plan.alias, plan.kind),
        })
    }
}

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl CreateDatasetFromSnapshotUseCase for CreateDatasetFromSnapshotUseCaseImpl {
    #[tracing::instrument(level = "info", name = CreateDatasetFromSnapshotUseCaseImpl_prepare, skip_all)]
    async fn prepare(
        &self,
        snapshots: Vec<odf::DatasetSnapshot>,
        options: CreateDatasetUseCaseOptions,
    ) -> Result<CreateDatasetsFromSnapshotsPlanningResult, CreateDatasetsFromSnapshotsPlanningError>
    {
        let system_time = self.system_time_source.now();

        let snapshots = self.sort_snapshots_in_dependency_order(snapshots.into_iter().collect())?;
        let mut steps = Vec::new();
        let mut errors = Vec::new();

        // Accumulates identities of datasets for dependency resolution within a plan
        let mut pending_datasets = BTreeMap::new();

        for snapshot in snapshots {
            match self
                .prepare_plan(snapshot.clone(), options, &pending_datasets, system_time)
                .await
            {
                Ok(plan) => {
                    pending_datasets.insert(
                        plan.alias.clone(),
                        odf::DatasetHandle::new(plan.id.clone(), plan.alias.clone(), plan.kind),
                    );
                    steps.push(plan);
                }
                Err(e) => errors.push((snapshot, e)),
            }
        }

        Ok(CreateDatasetsFromSnapshotsPlanningResult {
            plan: CreateDatasetsPlan { steps },
            errors,
        })
    }

    #[tracing::instrument(level = "info", name = CreateDatasetFromSnapshotUseCaseImpl_apply, skip_all)]
    async fn apply(
        &self,
        plan: CreateDatasetsPlan,
    ) -> Result<Vec<CreateDatasetResult>, CreateDatasetFromSnapshotError> {
        let mut results = Vec::new();

        for sub_plan in plan.steps {
            let res = self.apply_plan(sub_plan).await?;
            results.push(res);
        }

        Ok(results)
    }

    // TODO: Remove
    #[tracing::instrument(level = "info", name = CreateDatasetFromSnapshotUseCaseImpl_execute, skip_all)]
    async fn execute(
        &self,
        snapshot: odf::DatasetSnapshot,
        options: CreateDatasetUseCaseOptions,
    ) -> Result<CreateDatasetResult, CreateDatasetFromSnapshotError> {
        let system_time = self.system_time_source.now();

        let plan = self
            .prepare_plan(snapshot, options, &BTreeMap::new(), system_time)
            .await?;

        let res = self.apply_plan(plan).await?;

        Ok(res)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
