// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::component;
use kamu_core::auth::*;
use kamu_core::{AccessError, CurrentAccountSubject, ErrorIntoInternal};
use opendatafabric::{AccountName, DatasetHandle};
use oso::Oso;

use crate::dataset_resource::*;
use crate::user_actor::*;

///////////////////////////////////////////////////////////////////////////////

pub struct OsoDatasetAuthorizer {
    oso: Arc<Oso>,
    current_account_subject: Arc<CurrentAccountSubject>,
}

///////////////////////////////////////////////////////////////////////////////

#[component(pub)]
impl OsoDatasetAuthorizer {
    pub fn new(oso: Arc<Oso>, current_account_subject: Arc<CurrentAccountSubject>) -> Self {
        Self {
            oso,
            current_account_subject,
        }
    }

    fn actor(&self, account_name: &AccountName) -> UserActor {
        UserActor::new(account_name.as_str())
    }

    fn dataset_resource(&self, dataset_handle: &DatasetHandle) -> DatasetResource {
        let dataset_alias = &dataset_handle.alias;
        let creator = dataset_alias
            .account_name
            .as_ref()
            .map(|a| a.as_str())
            .unwrap_or(self.current_account_subject.account_name.as_str());

        // TODO: for now let's treat all datasets as public
        // TODO: explicit read/write permissions
        DatasetResource::new(creator, true)
    }
}

///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetActionAuthorizer for OsoDatasetAuthorizer {
    async fn check_action_allowed(
        &self,
        dataset_handle: &DatasetHandle,
        account_name: &AccountName,
        action: DatasetAction,
    ) -> Result<(), DatasetActionUnauthorizedError> {
        let actor = self.actor(&account_name);
        let dataset_resource = self.dataset_resource(dataset_handle);

        match self
            .oso
            .is_allowed(actor, action.to_string(), dataset_resource)
        {
            Ok(r) => {
                if r {
                    Ok(())
                } else {
                    let permission_error = DatasetActionNotEnoughPermissionsError {
                        account_name: account_name.clone(),
                        action,
                        dataset_ref: dataset_handle.as_local_ref(),
                    };
                    if action == DatasetAction::Write {
                        // Try with Read permissions to improve error messages
                        if let Ok(()) = self
                            .check_action_allowed(dataset_handle, account_name, DatasetAction::Read)
                            .await
                        {
                            return Err(DatasetActionUnauthorizedError::Access(
                                AccessError::ReadOnly(Some(permission_error.into())),
                            ));
                        }
                    }
                    Err(DatasetActionUnauthorizedError::Access(
                        AccessError::Forbidden(permission_error.into()),
                    ))
                }
            }
            Err(e) => Err(DatasetActionUnauthorizedError::Internal(e.int_err())),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////
