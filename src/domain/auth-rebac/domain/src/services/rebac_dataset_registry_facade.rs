// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use internal_error::{ErrorIntoInternal, InternalError};
use kamu_datasets::{DatasetAction, DatasetActionNotEnoughPermissionsError, ResolvedDataset};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait RebacDatasetRegistryFacade: Send + Sync {
    async fn resolve_dataset_handle_by_ref(
        &self,
        dataset_ref: &odf::DatasetRef,
        action: DatasetAction,
    ) -> Result<odf::DatasetHandle, RebacDatasetRefUnresolvedError>;

    async fn resolve_dataset_by_ref(
        &self,
        dataset_ref: &odf::DatasetRef,
        action: DatasetAction,
    ) -> Result<ResolvedDataset, RebacDatasetRefUnresolvedError>;

    async fn resolve_dataset_by_handle(
        &self,
        dataset_handle: &odf::DatasetHandle,
        action: DatasetAction,
    ) -> Result<ResolvedDataset, RebacDatasetIdUnresolvedError>;

    async fn classify_dataset_refs_by_allowance(
        &self,
        dataset_refs: &[&odf::DatasetRef],
        action: DatasetAction,
    ) -> Result<ClassifyDatasetRefsByAllowanceResponse, InternalError>;

    async fn classify_dataset_refs_by_access(
        &self,
        dataset_refs: &[&odf::DatasetRef],
        action: DatasetAction,
    ) -> Result<ClassifyDatasetRefsByAccessResponse, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct ClassifyDatasetRefsByAllowanceResponse {
    pub accessible_resolved_refs: Vec<(odf::DatasetRef, odf::DatasetHandle)>,
    pub inaccessible_refs: Vec<(odf::DatasetRef, RebacDatasetRefUnresolvedError)>,
}

impl ClassifyDatasetRefsByAllowanceResponse {
    pub fn into_inaccessible_input_datasets_message(
        self,
        dataset_ref_alias_map: &HashMap<&odf::DatasetRef, &String>,
    ) -> String {
        use itertools::Itertools;

        let inaccessible_dataset_refs_it = self
            .inaccessible_refs
            .into_iter()
            .map(|(dataset_ref, _)| dataset_ref);
        let joined_inaccessible_datasets = inaccessible_dataset_refs_it
            .map(|dataset_ref| {
                if let Some(alias) = dataset_ref_alias_map.get(&dataset_ref) {
                    format!("'{alias}'")
                } else {
                    format!("'{dataset_ref}'")
                }
            })
            .join(", ");

        format!("Some input dataset(s) are inaccessible: {joined_inaccessible_datasets}")
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct ClassifyDatasetRefsByAccessResponse {
    pub not_found: Vec<(odf::DatasetRef, odf::DatasetRefUnresolvedError)>,
    pub forbidden: Vec<(odf::DatasetRef, RebacDatasetRefUnresolvedError)>,
    pub insufficient: Vec<(odf::DatasetRef, odf::DatasetHandle)>,
    pub allowed: Vec<(odf::DatasetRef, odf::DatasetHandle)>,
}

impl ClassifyDatasetRefsByAccessResponse {
    pub fn try_get_user_error_report(&self, skip_missing: bool) -> Option<String> {
        let have_missing = !(self.not_found.is_empty() && self.forbidden.is_empty());
        let have_missing_to_report = have_missing && !skip_missing;

        if self.insufficient.is_empty() && !have_missing_to_report {
            return None;
        }

        let unresolved = if skip_missing {
            String::new()
        } else {
            // NOTE: We are preparing a report for the user, so the forbidden items are not
            //       found ones
            let not_found_iter = self.not_found.iter().map(|(r, _)| r);
            let forbidden_iter = self.forbidden.iter().map(|(r, _)| r);

            itertools::join(not_found_iter.chain(forbidden_iter), ",")
        };

        let report = indoc::formatdoc!(
            r#"
            Dataset access error:
            - insufficient access level: [{}]
            - unresolved: [{}]
            "#,
            itertools::join(
                self.insufficient
                    .iter()
                    .map(|(_, h)| format!("{}", h.alias)),
                ","
            ),
            unresolved
        );

        Some(report)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum RebacDatasetRefUnresolvedError {
    #[error(transparent)]
    NotFound(#[from] odf::DatasetNotFoundError),

    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        odf::AccessError,
    ),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

impl RebacDatasetRefUnresolvedError {
    pub fn not_enough_permissions(dataset_ref: odf::DatasetRef, action: DatasetAction) -> Self {
        Self::Access(odf::AccessError::Unauthorized(
            DatasetActionNotEnoughPermissionsError {
                action,
                dataset_ref,
            }
            .into(),
        ))
    }
}

impl From<odf::dataset::DatasetRefUnresolvedError> for RebacDatasetRefUnresolvedError {
    fn from(e: odf::dataset::DatasetRefUnresolvedError) -> Self {
        use odf::dataset::DatasetRefUnresolvedError as E;

        match e {
            E::NotFound(e) => Self::NotFound(e),
            e @ E::Internal(_) => Self::Internal(e.int_err()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum RebacDatasetIdUnresolvedError {
    #[error(transparent)]
    NotFound(#[from] odf::DatasetNotFoundError),

    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        odf::AccessError,
    ),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
