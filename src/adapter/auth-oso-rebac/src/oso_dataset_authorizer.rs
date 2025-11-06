// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_accounts::CurrentAccountSubject;
use kamu_core::auth::*;
use rayon::prelude::*;
use tokio::try_join;

use crate::dataset_resource::*;
use crate::user_actor::*;
use crate::{KamuAuthOso, OsoResourceServiceImpl};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn DatasetActionAuthorizer)]
pub struct OsoDatasetAuthorizer {
    kamu_auth_oso: Arc<KamuAuthOso>,
    current_account_subject: Arc<CurrentAccountSubject>,
    oso_resource_service: Arc<OsoResourceServiceImpl>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl OsoDatasetAuthorizer {
    async fn user_actor(&self) -> Result<UserActor, InternalError> {
        let maybe_account_id = self.current_account_subject.get_maybe_logged_account_id();

        let user_actor = self
            .oso_resource_service
            .user_actor(maybe_account_id)
            .await
            .int_err()?;

        Ok(user_actor)
    }

    async fn dataset_resource(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<DatasetResource, InternalError> {
        let dataset_resource = self
            .oso_resource_service
            .dataset_resource(dataset_id)
            .await
            .int_err()?;

        Ok(dataset_resource)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetActionAuthorizer for OsoDatasetAuthorizer {
    #[tracing::instrument(level = "debug", skip_all, fields(%dataset_id, ?action))]
    async fn check_action_allowed(
        &self,
        dataset_id: &odf::DatasetID,
        action: DatasetAction,
    ) -> Result<(), DatasetActionUnauthorizedError> {
        let (user_actor, dataset_resource) =
            try_join!(self.user_actor(), self.dataset_resource(dataset_id))?;

        match self
            .kamu_auth_oso
            .is_allowed(user_actor, action.to_string(), dataset_resource)
        {
            Ok(allowed) if allowed => Ok(()),
            Ok(_not_allowed) => Err(DatasetActionUnauthorizedError::not_enough_permissions(
                dataset_id.as_local_ref(),
                action,
            )),
            Err(e) => Err(DatasetActionUnauthorizedError::Internal(e.int_err())),
        }
    }

    #[tracing::instrument(level = "debug", skip_all, fields(%dataset_id))]
    async fn get_allowed_actions(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<HashSet<DatasetAction>, InternalError> {
        let (user_actor, dataset_resource) =
            try_join!(self.user_actor(), self.dataset_resource(dataset_id))?;

        self.kamu_auth_oso
            .get_allowed_actions(user_actor, dataset_resource)
            .int_err()
    }

    #[tracing::instrument(level = "debug", skip_all, fields(datasets_count=%dataset_handles.len(), %action))]
    async fn filter_datasets_allowing(
        &self,
        dataset_handles: Vec<odf::DatasetHandle>,
        action: DatasetAction,
    ) -> Result<Vec<odf::DatasetHandle>, InternalError> {
        let user_actor = self.user_actor().await?;

        let dataset_ids = dataset_handles
            .iter()
            .map(|hdl| Cow::Borrowed(&hdl.id))
            .collect::<Vec<_>>();
        let dataset_resources_resolution = self
            .oso_resource_service
            .get_multiple_dataset_resources(&dataset_ids)
            .await
            .int_err()?;
        let dataset_handle_id_mapping =
            dataset_handles
                .into_iter()
                .fold(HashMap::new(), |mut acc, hdl| {
                    acc.insert(hdl.id.clone(), hdl);
                    acc
                });

        let results: Result<Vec<_>, InternalError> = dataset_resources_resolution
            .resolved_resources
            .into_par_iter()
            .map(|(dataset_id, dataset_resource)| {
                let is_allowed = self
                    .kamu_auth_oso
                    .is_allowed(user_actor.clone(), action, dataset_resource)
                    .int_err()?;

                if is_allowed {
                    let dataset_handle = dataset_handle_id_mapping
                        .get(&dataset_id)
                        .ok_or_else(|| {
                            format!("Unexpectedly, dataset_handle not was found: {dataset_id}")
                                .int_err()
                        })?
                        .clone();
                    Ok(Some(dataset_handle))
                } else {
                    Ok(None)
                }
            })
            .collect();

        let matched_dataset_handles = results?.into_iter().flatten().collect();
        Ok(matched_dataset_handles)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(datasets_count = %dataset_handles.len(), %action))]
    async fn classify_dataset_handles_by_allowance(
        &self,
        dataset_handles: Vec<odf::DatasetHandle>,
        action: DatasetAction,
    ) -> Result<ClassifyByAllowanceResponse, InternalError> {
        let user_actor = self.user_actor().await?;

        let dataset_ids = dataset_handles
            .iter()
            .map(|hdl| Cow::Borrowed(&hdl.id))
            .collect::<Vec<_>>();
        let dataset_resources_resolution = self
            .oso_resource_service
            .get_multiple_dataset_resources(&dataset_ids)
            .await
            .int_err()?;
        let mut authorized_handles =
            Vec::with_capacity(dataset_resources_resolution.resolved_resources.len());
        let mut unauthorized_handles_with_errors =
            Vec::with_capacity(dataset_resources_resolution.unresolved_resources.len());

        let dataset_handle_id_mapping =
            dataset_handles
                .into_iter()
                .fold(HashMap::new(), |mut acc, hdl| {
                    acc.insert(hdl.id.clone(), hdl);
                    acc
                });

        type HandleResult = Result<
            (
                odf::DatasetHandle,
                Option<ClassifyByAllowanceDatasetActionUnauthorizedError>,
            ),
            InternalError,
        >;

        let results: Result<Vec<_>, InternalError> = dataset_resources_resolution
            .resolved_resources
            .into_par_iter()
            .map(|(dataset_id, dataset_resource)| -> HandleResult {
                let dataset_handle = dataset_handle_id_mapping
                    .get(&dataset_id)
                    .ok_or_else(|| {
                        format!("Unexpectedly, dataset_handle not was found: {dataset_id}")
                            .int_err()
                    })?
                    .clone();

                let is_allowed = self
                    .kamu_auth_oso
                    .is_allowed(user_actor.clone(), action, dataset_resource)
                    .int_err()?;

                if is_allowed {
                    Ok((dataset_handle, None))
                } else {
                    let dataset_ref = dataset_handle.as_local_ref();
                    Ok((
                        dataset_handle,
                        Some(
                            ClassifyByAllowanceDatasetActionUnauthorizedError::not_enough_permissions(
                                dataset_ref,
                                action,
                            ),
                        ),
                    ))
                }
            })
            .collect();

        for (dataset_handle, maybe_error) in results? {
            if let Some(error) = maybe_error {
                unauthorized_handles_with_errors.push((dataset_handle, error));
            } else {
                authorized_handles.push(dataset_handle);
            }
        }

        for unresolved_dataset_id in dataset_resources_resolution.unresolved_resources {
            let dataset_handle = dataset_handle_id_mapping
                .get(&unresolved_dataset_id)
                .ok_or_else(|| {
                    format!("Unexpectedly, dataset_handle not was found: {unresolved_dataset_id}")
                        .int_err()
                })?
                .clone();
            let dataset_ref = dataset_handle.as_local_ref();

            unauthorized_handles_with_errors.push((
                dataset_handle,
                odf::DatasetNotFoundError::new(dataset_ref).into(),
            ));
        }

        Ok(ClassifyByAllowanceResponse {
            authorized_handles,
            unauthorized_handles_with_errors,
        })
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?dataset_ids, %action))]
    async fn classify_dataset_ids_by_allowance<'a>(
        &'a self,
        dataset_ids: &[Cow<'a, odf::DatasetID>],
        action: DatasetAction,
    ) -> Result<ClassifyByAllowanceIdsResponse, InternalError> {
        let user_actor = self.user_actor().await?;

        let dataset_resources_resolution = self
            .oso_resource_service
            .get_multiple_dataset_resources(dataset_ids)
            .await
            .int_err()?;
        let mut authorized_ids =
            Vec::with_capacity(dataset_resources_resolution.resolved_resources.len());
        let mut unauthorized_ids_with_errors =
            Vec::with_capacity(dataset_resources_resolution.unresolved_resources.len());

        type IdResult = Result<
            (
                odf::DatasetID,
                Option<ClassifyByAllowanceDatasetActionUnauthorizedError>,
            ),
            InternalError,
        >;

        let results: Result<Vec<_>, InternalError> = dataset_resources_resolution
            .resolved_resources
            .into_par_iter()
            .map(|(dataset_id, dataset_resource)| -> IdResult {
                let is_allowed = self
                    .kamu_auth_oso
                    .is_allowed(user_actor.clone(), action, dataset_resource)
                    .int_err()?;

                if is_allowed {
                    Ok((dataset_id, None))
                } else {
                    let dataset_ref = dataset_id.as_local_ref();
                    Ok((
                        dataset_id,
                        Some(
                            ClassifyByAllowanceDatasetActionUnauthorizedError::not_enough_permissions(
                                dataset_ref,
                                action,
                            ),
                        ),
                    ))
                }
            })
            .collect();

        for (dataset_id, maybe_error) in results? {
            if let Some(error) = maybe_error {
                unauthorized_ids_with_errors.push((dataset_id, error));
            } else {
                authorized_ids.push(dataset_id);
            }
        }

        for unresolved_dataset_id in dataset_resources_resolution.unresolved_resources {
            let dataset_ref = unresolved_dataset_id.as_local_ref();
            unauthorized_ids_with_errors.push((
                unresolved_dataset_id,
                odf::DatasetNotFoundError::new(dataset_ref).into(),
            ));
        }

        Ok(ClassifyByAllowanceIdsResponse {
            authorized_ids,
            unauthorized_ids_with_errors,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
