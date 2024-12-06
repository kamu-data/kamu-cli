// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use database_common::{EntityListing, EntityPageStreamer};
use dill::*;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_accounts::{AccountRepository, GetAccountByIdError};
use kamu_auth_rebac::RebacService;
use kamu_datasets::{DatasetEntriesResolution, DatasetEntryRepository, GetDatasetEntryError};
use opendatafabric as odf;
use thiserror::Error;
use tokio::sync::RwLock;

use crate::{DatasetResource, UserActor};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type EntityId = String;

#[derive(Debug, Default)]
struct State {
    user_actor_cache_map: HashMap<EntityId, UserActor>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Private Datasets: add Service trait
pub struct OsoResourceServiceInMem {
    state: RwLock<State>,
    dataset_entry_repo: Arc<dyn DatasetEntryRepository>,
    rebac_service: Arc<dyn RebacService>,
    account_repo: Arc<dyn AccountRepository>,
}

// TODO: Private Datasets: Add ReBAC-specific messages to update properties at
//       runtime
#[component(pub)]
#[scope(Singleton)]
impl OsoResourceServiceInMem {
    pub fn new(
        dataset_entry_repo: Arc<dyn DatasetEntryRepository>,
        rebac_service: Arc<dyn RebacService>,
        account_repo: Arc<dyn AccountRepository>,
    ) -> Self {
        Self {
            state: RwLock::new(State::default()),
            dataset_entry_repo,
            rebac_service,
            account_repo,
        }
    }

    // TODO: Private Datasets: concrete the error (absorb option to NotFound error?)
    pub async fn user_actor(
        &self,
        maybe_account_id: Option<&odf::AccountID>,
    ) -> Result<Option<UserActor>, InternalError> {
        let Some(account_id) = maybe_account_id else {
            return Ok(Some(UserActor::anonymous()));
        };

        // First, an attempt to get from the cache
        {
            let readable_state = self.state.read().await;

            let account_id_stack = account_id.as_did_str().to_stack_string();
            let maybe_cached_user_actor = readable_state
                .user_actor_cache_map
                .get(account_id_stack.as_str())
                .cloned();

            if maybe_cached_user_actor.is_some() {
                return Ok(maybe_cached_user_actor);
            }
        }

        // The second attempt is from the database
        let user_actor = {
            let account = match self.account_repo.get_account_by_id(account_id).await {
                Ok(found_account) => found_account,
                Err(get_err) => {
                    return match get_err {
                        GetAccountByIdError::NotFound(_) => Ok(None),
                        e => Err(e.int_err()),
                    }
                }
            };

            let account_properties = self
                .rebac_service
                .get_account_properties(&account.id)
                .await
                .int_err()?;

            UserActor::logged(&account.id, account_properties.is_admin)
        };

        // Lastly, caching
        let mut writable_state = self.state.write().await;

        writable_state
            .user_actor_cache_map
            .insert(user_actor.account_id.clone(), user_actor.clone());

        Ok(Some(user_actor))
    }

    // TODO: Private Datasets: concrete the error (absorb option to NotFound error?)
    pub async fn dataset_resource(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<Option<DatasetResource>, InternalError> {
        let dataset_entry = match self.dataset_entry_repo.get_dataset_entry(dataset_id).await {
            Ok(found_dataset_entry) => found_dataset_entry,
            Err(get_err) => {
                return match get_err {
                    GetDatasetEntryError::NotFound(_) => Ok(None),
                    e => Err(e.int_err()),
                }
            }
        };
        let dataset_properties = self
            .rebac_service
            .get_dataset_properties(&dataset_entry.id)
            .await
            .int_err()?;

        let dataset_resource = DatasetResource::new(
            &dataset_entry.owner_id,
            dataset_properties.allows_public_read,
        );

        Ok(Some(dataset_resource))
    }

    pub async fn get_multiple_dataset_resources(
        &self,
        dataset_ids: &[odf::DatasetID],
    ) -> Result<DatasetResourcesResolution, GetMultipleDatasetResourcesError> {
        let DatasetEntriesResolution {
            resolved_entries,
            unresolved_entries,
        } = self
            .dataset_entry_repo
            .get_multiple_dataset_entries(dataset_ids)
            .await
            .int_err()?;

        let dataset_resources_stream = EntityPageStreamer::default().into_stream(
            || async { Ok(Arc::new(resolved_entries)) },
            |dataset_entries, pagination| {
                let dataset_entries_page = dataset_entries
                    .iter()
                    .skip(pagination.offset)
                    .take(pagination.safe_limit(dataset_entries.len()))
                    .collect::<Vec<_>>();

                let dataset_id_owner_id_mapping =
                    dataset_entries_page
                        .iter()
                        .fold(HashMap::new(), |mut acc, dataset_entry| {
                            acc.insert(dataset_entry.id.clone(), dataset_entry.owner_id.clone());
                            acc
                        });
                let dataset_ids = dataset_entries_page
                    .iter()
                    .map(|dataset_entry| dataset_entry.id.clone())
                    .collect::<Vec<_>>();

                async move {
                    let dataset_properties_map = self
                        .rebac_service
                        .get_dataset_properties_by_ids(&dataset_ids)
                        .await
                        .int_err()?;

                    let mut dataset_resources = Vec::with_capacity(dataset_properties_map.len());

                    for (dataset_id, dataset_properties) in dataset_properties_map {
                        let owner_id =
                            dataset_id_owner_id_mapping
                                .get(&dataset_id)
                                .ok_or_else(|| {
                                    format!("Unexpectedly, owner_id not found: {dataset_id}")
                                        .int_err()
                                })?;

                        let dataset_resource =
                            DatasetResource::new(owner_id, dataset_properties.allows_public_read);

                        dataset_resources.push((dataset_id, dataset_resource));
                    }

                    Ok(EntityListing {
                        list: dataset_resources,
                        total_count: dataset_entries.len(),
                    })
                }
            },
        );

        use futures::TryStreamExt;

        Ok(DatasetResourcesResolution {
            resolved_resources: dataset_resources_stream.try_collect().await?,
            unresolved_resources: unresolved_entries,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetResourcesResolution {
    pub resolved_resources: Vec<(odf::DatasetID, DatasetResource)>,
    pub unresolved_resources: Vec<odf::DatasetID>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetMultipleDatasetResourcesError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
