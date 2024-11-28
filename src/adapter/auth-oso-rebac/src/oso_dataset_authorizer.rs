// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::sync::Arc;

use dill::*;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_accounts::CurrentAccountSubject;
use kamu_core::auth::*;
use kamu_core::AccessError;
use opendatafabric as odf;
use oso::Oso;

use crate::dataset_resource::*;
use crate::user_actor::*;
use crate::{KamuAuthOso, OsoResourceServiceInMem};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct OsoDatasetAuthorizer {
    oso: Arc<Oso>,
    current_account_subject: Arc<CurrentAccountSubject>,
    oso_resource_service: Arc<OsoResourceServiceInMem>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn DatasetActionAuthorizer)]
impl OsoDatasetAuthorizer {
    #[expect(clippy::needless_pass_by_value)]
    pub fn new(
        kamu_auth_oso: Arc<KamuAuthOso>,
        current_account_subject: Arc<CurrentAccountSubject>,
        oso_resource_holder: Arc<OsoResourceServiceInMem>,
    ) -> Self {
        Self {
            oso: kamu_auth_oso.oso.clone(),
            current_account_subject,
            oso_resource_service: oso_resource_holder,
        }
    }

    async fn user_dataset_pair(
        &self,
        dataset_handle: &odf::DatasetHandle,
    ) -> Result<(UserActor, DatasetResource), InternalError> {
        let dataset_id = &dataset_handle.id;
        let maybe_account_id = self.get_maybe_logged_account_id();

        let (maybe_user_actor, maybe_dataset_resource) = self
            .oso_resource_service
            .user_dataset_pair(dataset_id, maybe_account_id)
            .await;

        let user_actor = maybe_user_actor
            .ok_or_else(|| format!("UserActor not found: {maybe_account_id:?}").int_err())?;
        let dataset_resource = maybe_dataset_resource
            .ok_or_else(|| format!("DatasetResource not found: {dataset_id}").int_err())?;

        Ok((user_actor, dataset_resource))
    }

    async fn user_actor(&self) -> Result<UserActor, InternalError> {
        let maybe_account_id = self.get_maybe_logged_account_id();

        let maybe_user_actor = self.oso_resource_service.user_actor(maybe_account_id).await;

        let user_actor = maybe_user_actor
            .ok_or_else(|| format!("UserActor not found: {maybe_account_id:?}").int_err())?;

        Ok(user_actor)
    }

    async fn dataset_resource(
        &self,
        dataset_handle: &odf::DatasetHandle,
    ) -> Result<DatasetResource, InternalError> {
        let dataset_id = &dataset_handle.id;

        let maybe_dataset_resource = self.oso_resource_service.dataset_resource(dataset_id).await;

        let dataset_resource = maybe_dataset_resource
            .ok_or_else(|| format!("DatasetResource not found: {dataset_id}").int_err())?;

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
    #[tracing::instrument(level = "debug", skip_all, fields(%dataset_handle, ?action))]
    async fn check_action_allowed(
        &self,
        dataset_handle: &odf::DatasetHandle,
        action: DatasetAction,
    ) -> Result<(), DatasetActionUnauthorizedError> {
        let (user_actor, dataset_resource) = self.user_dataset_pair(dataset_handle).await?;

        match self
            .oso
            .is_allowed(user_actor, action.to_string(), dataset_resource)
        {
            Ok(allowed) if allowed => Ok(()),
            Ok(_not_allowed) => Err(DatasetActionUnauthorizedError::Access(
                AccessError::Forbidden(
                    DatasetActionNotEnoughPermissionsError {
                        action,
                        dataset_ref: dataset_handle.as_local_ref(),
                    }
                    .into(),
                ),
            )),
            Err(e) => Err(DatasetActionUnauthorizedError::Internal(e.int_err())),
        }
    }

    #[tracing::instrument(level = "debug", skip_all, fields(%dataset_handle))]
    async fn get_allowed_actions(
        &self,
        dataset_handle: &odf::DatasetHandle,
    ) -> Result<HashSet<DatasetAction>, InternalError> {
        let (user_actor, dataset_resource) = self.user_dataset_pair(dataset_handle).await?;

        self.oso
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

        for hdl in dataset_handles {
            let dataset_resource = self.dataset_resource(&hdl).await?;

            let is_allowed = self
                .oso
                .is_allowed(user_actor.clone(), action, dataset_resource)
                .int_err()?;

            if is_allowed {
                matched_dataset_handles.push(hdl);
            }
        }

        Ok(matched_dataset_handles)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(dataset_handles=?dataset_handles, action=%action))]
    async fn classify_datasets_by_allowance(
        &self,
        dataset_handles: Vec<odf::DatasetHandle>,
        action: DatasetAction,
    ) -> Result<ClassifyByAllowanceResponse, InternalError> {
        let user_actor = self.user_actor().await?;
        let mut matched_dataset_handles = Vec::with_capacity(dataset_handles.len());
        let mut unmatched_results = Vec::new();

        for hdl in dataset_handles {
            let dataset_resource = self.dataset_resource(&hdl).await?;

            let is_allowed = self
                .oso
                .is_allowed(user_actor.clone(), action, dataset_resource)
                .int_err()?;

            if is_allowed {
                matched_dataset_handles.push(hdl);
            } else {
                let dataset_ref = hdl.as_local_ref();
                unmatched_results.push((
                    hdl,
                    DatasetActionUnauthorizedError::Access(AccessError::Forbidden(
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
