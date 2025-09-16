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
use kamu_core::{ResolvedDataset, auth};
use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait RebacDatasetRegistryFacade: Send + Sync {
    async fn resolve_dataset_handle_by_ref(
        &self,
        dataset_ref: &odf::DatasetRef,
        action: auth::DatasetAction,
    ) -> Result<odf::DatasetHandle, RebacDatasetRefUnresolvedError>;

    async fn resolve_dataset_by_ref(
        &self,
        dataset_ref: &odf::DatasetRef,
        action: auth::DatasetAction,
    ) -> Result<ResolvedDataset, RebacDatasetRefUnresolvedError>;

    async fn resolve_dataset_by_handle(
        &self,
        dataset_handle: &odf::DatasetHandle,
        action: auth::DatasetAction,
    ) -> Result<ResolvedDataset, RebacDatasetIdUnresolvedError>;

    async fn classify_dataset_refs_by_allowance(
        &self,
        dataset_refs: Vec<odf::DatasetRef>,
        action: auth::DatasetAction,
    ) -> Result<ClassifyDatasetRefsByAllowanceResponse, InternalError>;

    // TODO: tests
    async fn classify_dataset_refs_by_access(
        &self,
        dataset_refs: &[&odf::DatasetRef],
        action: auth::DatasetAction,
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
    pub forbidden: Vec<(odf::DatasetRef, RebacDatasetRefUnresolvedError)>,
    pub limited: Vec<(odf::DatasetRef, odf::DatasetHandle)>,
    pub allowed: Vec<(odf::DatasetRef, odf::DatasetHandle)>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
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
    pub fn not_enough_permissions(
        dataset_ref: odf::DatasetRef,
        action: auth::DatasetAction,
    ) -> Self {
        Self::Access(odf::AccessError::Unauthorized(
            auth::DatasetActionNotEnoughPermissionsError {
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

#[derive(Error, Debug)]
pub enum RebacDatasetIdUnresolvedError {
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
