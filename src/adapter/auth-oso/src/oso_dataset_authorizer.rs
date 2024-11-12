// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;

use dill::*;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_accounts::{CurrentAccountSubject, DEFAULT_ACCOUNT_NAME_STR};
use kamu_core::auth::*;
use kamu_core::AccessError;
use opendatafabric::DatasetHandle;
use oso::Oso;

use crate::dataset_resource::*;
use crate::user_actor::*;
use crate::KamuAuthOso;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct OsoDatasetAuthorizer {
    oso: Arc<Oso>,
    current_account_subject: Arc<CurrentAccountSubject>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn DatasetActionAuthorizer)]
impl OsoDatasetAuthorizer {
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(
        kamu_auth_oso: Arc<KamuAuthOso>,
        current_account_subject: Arc<CurrentAccountSubject>,
    ) -> Self {
        Self {
            oso: kamu_auth_oso.oso.clone(),
            current_account_subject,
        }
    }

    fn actor(&self) -> UserActor {
        match self.current_account_subject.as_ref() {
            CurrentAccountSubject::Anonymous(_) => UserActor::new("", true, false),
            CurrentAccountSubject::Logged(l) => {
                UserActor::new(l.account_name.as_str(), false, l.is_admin)
            }
        }
    }

    fn dataset_resource(&self, dataset_handle: &DatasetHandle) -> DatasetResource {
        let dataset_alias = &dataset_handle.alias;
        let creator = dataset_alias
            .account_name
            .as_ref()
            .map_or(DEFAULT_ACCOUNT_NAME_STR, |a| a.as_str());

        // TODO: for now let's treat all datasets as public
        // TODO: explicit read/write permissions
        DatasetResource::new(creator, true)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetActionAuthorizer for OsoDatasetAuthorizer {
    #[tracing::instrument(level = "debug", skip_all, fields(%dataset_handle, ?action))]
    async fn check_action_allowed(
        &self,
        dataset_handle: &DatasetHandle,
        action: DatasetAction,
    ) -> Result<(), DatasetActionUnauthorizedError> {
        let actor = self.actor();
        let dataset_resource = self.dataset_resource(dataset_handle);

        match self
            .oso
            .is_allowed(actor, action.to_string(), dataset_resource)
        {
            Ok(r) => {
                if r {
                    Ok(())
                } else {
                    Err(DatasetActionUnauthorizedError::Access(
                        AccessError::Forbidden(
                            DatasetActionNotEnoughPermissionsError {
                                action,
                                dataset_ref: dataset_handle.as_local_ref(),
                            }
                            .into(),
                        ),
                    ))
                }
            }
            Err(e) => Err(DatasetActionUnauthorizedError::Internal(e.int_err())),
        }
    }

    #[tracing::instrument(level = "debug", skip_all, fields(%dataset_handle))]
    async fn get_allowed_actions(&self, dataset_handle: &DatasetHandle) -> HashSet<DatasetAction> {
        let actor = self.actor();
        let dataset_resource = self.dataset_resource(dataset_handle);

        let allowed_action_names: HashSet<String> = self
            .oso
            .get_allowed_actions(actor, dataset_resource)
            .unwrap();

        let mut allowed_actions = HashSet::new();
        for action_name in allowed_action_names {
            let action = DatasetAction::from_str(action_name.as_str()).unwrap();
            allowed_actions.insert(action);
        }

        allowed_actions
    }

    #[tracing::instrument(level = "debug", skip_all, fields(dataset_handles=?dataset_handles, action=%action))]
    async fn filter_datasets_allowing(
        &self,
        dataset_handles: Vec<DatasetHandle>,
        action: DatasetAction,
    ) -> Result<Vec<DatasetHandle>, InternalError> {
        let mut matched_dataset_handles = Vec::new();
        for hdl in dataset_handles {
            let is_allowed = self
                .oso
                .is_allowed(
                    self.actor(),
                    action.to_string(),
                    self.dataset_resource(&hdl),
                )
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
        dataset_handles: Vec<DatasetHandle>,
        action: DatasetAction,
    ) -> Result<ClassifyByAllowanceResponse, InternalError> {
        let mut matched_dataset_handles = Vec::with_capacity(dataset_handles.len());
        let mut unmatched_results = Vec::new();

        for hdl in dataset_handles {
            let is_allowed = self
                .oso
                .is_allowed(
                    self.actor(),
                    action.to_string(),
                    self.dataset_resource(&hdl),
                )
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
