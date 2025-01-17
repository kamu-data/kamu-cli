// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use dill::*;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_accounts::CurrentAccountSubject;
use kamu_core::auth::*;
use tokio::try_join;

use crate::dataset_resource::*;
use crate::user_actor::*;
use crate::{KamuAuthOso, OsoResourceServiceImpl};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct OsoDatasetAuthorizer {
    kamu_auth_oso: Arc<KamuAuthOso>,
    current_account_subject: Arc<CurrentAccountSubject>,
    oso_resource_service: Arc<OsoResourceServiceImpl>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn DatasetActionAuthorizer)]
impl OsoDatasetAuthorizer {
    pub fn new(
        kamu_auth_oso: Arc<KamuAuthOso>,
        current_account_subject: Arc<CurrentAccountSubject>,
        oso_resource_service: Arc<OsoResourceServiceImpl>,
    ) -> Self {
        Self {
            kamu_auth_oso,
            current_account_subject,
            oso_resource_service,
        }
    }

    async fn user_actor(&self) -> Result<UserActor, InternalError> {
        let maybe_account_id = self.get_maybe_logged_account_id();

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

    fn get_maybe_logged_account_id(&self) -> Option<&odf::AccountID> {
        match self.current_account_subject.as_ref() {
            CurrentAccountSubject::Anonymous(_) => None,
            CurrentAccountSubject::Logged(logged_account) => Some(&logged_account.account_id),
        }
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
            Ok(_not_allowed) => Err(DatasetActionUnauthorizedError::Access(
                odf::AccessError::Forbidden(
                    DatasetActionNotEnoughPermissionsError {
                        action,
                        dataset_ref: dataset_id.as_local_ref(),
                    }
                    .into(),
                ),
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

    #[tracing::instrument(level = "debug", skip_all, fields(dataset_handles=?dataset_handles, action=%action))]
    async fn filter_datasets_allowing(
        &self,
        dataset_handles: Vec<odf::DatasetHandle>,
        action: DatasetAction,
    ) -> Result<Vec<odf::DatasetHandle>, InternalError> {
        let user_actor = self.user_actor().await?;
        let mut matched_dataset_handles = Vec::with_capacity(dataset_handles.len());

        let dataset_ids = dataset_handles
            .iter()
            .map(|hdl| hdl.id.clone())
            .collect::<Vec<_>>();
        let dataset_resources_resolution = self
            .oso_resource_service
            .get_multiple_dataset_resources(&dataset_ids)
            .await
            .int_err()?;
        let mut dataset_handle_id_mapping =
            dataset_handles
                .into_iter()
                .fold(HashMap::new(), |mut acc, hdl| {
                    acc.insert(hdl.id.clone(), hdl);
                    acc
                });

        for (dataset_id, dataset_resource) in dataset_resources_resolution.resolved_resources {
            let is_allowed = self
                .kamu_auth_oso
                .is_allowed(user_actor.clone(), action, dataset_resource)
                .int_err()?;

            if is_allowed {
                let dataset_handle = dataset_handle_id_mapping
                    // Thus we obtain the value without cloning
                    .remove(&dataset_id)
                    .ok_or_else(|| {
                        format!("Unexpectedly, dataset_handle was found: {dataset_id}").int_err()
                    })?;

                matched_dataset_handles.push(dataset_handle);
            }
        }

        Ok(matched_dataset_handles)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(dataset_handles=?dataset_handles, action=%action))]
    async fn classify_dataset_handles_by_allowance(
        &self,
        dataset_handles: Vec<odf::DatasetHandle>,
        action: DatasetAction,
    ) -> Result<ClassifyByAllowanceResponse, InternalError> {
        let user_actor = self.user_actor().await?;
        let mut matched_dataset_handles = Vec::with_capacity(dataset_handles.len());
        let mut unmatched_results = Vec::new();

        let dataset_ids = dataset_handles
            .iter()
            .map(|hdl| hdl.id.clone())
            .collect::<Vec<_>>();
        let dataset_resources_resolution = self
            .oso_resource_service
            .get_multiple_dataset_resources(&dataset_ids)
            .await
            .int_err()?;
        let mut dataset_handle_id_mapping =
            dataset_handles
                .into_iter()
                .fold(HashMap::new(), |mut acc, hdl| {
                    acc.insert(hdl.id.clone(), hdl);
                    acc
                });

        for (dataset_id, dataset_resource) in dataset_resources_resolution.resolved_resources {
            let dataset_handle = dataset_handle_id_mapping
                // Thus we obtain the value without cloning
                .remove(&dataset_id)
                .ok_or_else(|| {
                    format!("Unexpectedly, dataset_handle was found: {dataset_id}").int_err()
                })?;

            let is_allowed = self
                .kamu_auth_oso
                .is_allowed(user_actor.clone(), action, dataset_resource)
                .int_err()?;

            if is_allowed {
                matched_dataset_handles.push(dataset_handle);
            } else {
                let dataset_ref = dataset_handle.as_local_ref();
                unmatched_results.push((
                    dataset_handle,
                    DatasetActionUnauthorizedError::Access(odf::AccessError::Forbidden(
                        DatasetActionNotEnoughPermissionsError {
                            action,
                            dataset_ref,
                        }
                        .into(),
                    )),
                ));
            }
        }

        Ok(ClassifyByAllowanceResponse {
            authorized_handles: matched_dataset_handles,
            unauthorized_handles_with_errors: unmatched_results,
        })
    }

    async fn classify_dataset_ids_by_allowance(
        &self,
        dataset_ids: Vec<odf::DatasetID>,
        action: DatasetAction,
    ) -> Result<ClassifyByAllowanceIdsResponse, InternalError> {
        let user_actor = self.user_actor().await?;
        let mut authorized_ids = Vec::with_capacity(dataset_ids.len());
        let mut unauthorized_ids_with_errors = Vec::new();

        let dataset_resources_resolution = self
            .oso_resource_service
            .get_multiple_dataset_resources(&dataset_ids)
            .await
            .int_err()?;

        for (dataset_id, dataset_resource) in dataset_resources_resolution.resolved_resources {
            let is_allowed = self
                .kamu_auth_oso
                .is_allowed(user_actor.clone(), action, dataset_resource)
                .int_err()?;

            if is_allowed {
                authorized_ids.push(dataset_id);
            } else {
                let dataset_ref = dataset_id.as_local_ref();
                unauthorized_ids_with_errors.push((
                    dataset_id,
                    DatasetActionUnauthorizedError::Access(odf::AccessError::Forbidden(
                        DatasetActionNotEnoughPermissionsError {
                            action,
                            dataset_ref,
                        }
                        .into(),
                    )),
                ));
            }
        }

        Ok(ClassifyByAllowanceIdsResponse {
            authorized_ids,
            unauthorized_ids_with_errors,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
