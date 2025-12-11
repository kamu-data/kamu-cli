// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_auth_rebac::{RebacDatasetRefUnresolvedError, RebacDatasetRegistryFacade};
use kamu_core::{GetDataOptions, PushIngestDataUseCase, QueryError, QueryService, auth};
use kamu_datasets::{
    CreateDatasetFromSnapshotUseCase,
    ReadCheckedDataset,
    ResolvedDataset,
    WriteCheckedDataset,
};
use odf::utils::data::DataFrameExt;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
pub struct MoleculeDatasetAccessorFactory {
    create_dataset_from_snapshot_uc: Arc<dyn CreateDatasetFromSnapshotUseCase>,
    push_ingest_uc: Arc<dyn PushIngestDataUseCase>,
    rebac_dataset_registry_facade: Arc<dyn RebacDatasetRegistryFacade>,
    query_service: Arc<dyn QueryService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MoleculeDatasetAccessorFactory {
    pub async fn read_accessor(
        &self,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<MoleculeDatasetReadAccessor, RebacDatasetRefUnresolvedError> {
        let resolved_dataset = self
            .rebac_dataset_registry_facade
            .resolve_dataset_by_ref(dataset_ref, auth::DatasetAction::Read)
            .await?;

        Ok(MoleculeDatasetReadAccessor {
            query_service: self.query_service.clone(),
            dataset: ReadCheckedDataset::from_owned(resolved_dataset),
        })
    }

    pub async fn write_accessor(
        &self,
        dataset_ref: &odf::DatasetRef,
        create_if_not_exist: bool,
        snapshot_getter: impl FnOnce() -> odf::DatasetSnapshot,
    ) -> Result<MoleculeDatasetWriteAccessor, RebacDatasetRefUnresolvedError> {
        let resolved_dataset = match self
            .rebac_dataset_registry_facade
            .resolve_dataset_by_ref(dataset_ref, auth::DatasetAction::Write)
            .await
        {
            Ok(ds) => Ok(ds),
            Err(RebacDatasetRefUnresolvedError::NotFound(_)) if create_if_not_exist => {
                let snapshot = snapshot_getter();

                let create_res = self
                    .create_dataset_from_snapshot_uc
                    .execute(snapshot, Default::default())
                    .await
                    .int_err()?;

                Ok(ResolvedDataset::new(
                    create_res.dataset,
                    create_res.dataset_handle,
                ))
            }
            Err(e) => Err(e),
        }?;

        Ok(MoleculeDatasetWriteAccessor {
            push_ingest_uc: self.push_ingest_uc.clone(),
            query_service: self.query_service.clone(),
            dataset: WriteCheckedDataset::from_owned(resolved_dataset),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeDatasetWriteAccessor {
    push_ingest_uc: Arc<dyn PushIngestDataUseCase>,
    query_service: Arc<dyn QueryService>,
    dataset: WriteCheckedDataset<'static>,
}

impl MoleculeDatasetWriteAccessor {
    #[expect(dead_code)]
    pub(crate) fn get_write_checked_dataset(&self) -> &WriteCheckedDataset<'_> {
        &self.dataset
    }

    #[expect(dead_code)]
    pub(crate) fn as_read_accessor(&self) -> MoleculeDatasetReadAccessor {
        MoleculeDatasetReadAccessor {
            query_service: Arc::clone(&self.query_service),
            dataset: self.dataset.clone().into(),
        }
    }

    pub(crate) async fn push_ndjson_data(
        &self,
        buffer: bytes::Bytes,
        source_event_time: Option<DateTime<Utc>>,
    ) -> Result<(), InternalError> {
        self.push_ingest_uc
            .execute(
                (*self.dataset).clone(),
                kamu_core::DataSource::Buffer(buffer),
                kamu_core::PushIngestDataUseCaseOptions {
                    source_name: None,
                    source_event_time,
                    is_ingest_from_upload: false,
                    media_type: Some(file_utils::MediaType::NDJSON.to_owned()),
                    expected_head: None,
                },
                None,
            )
            .await
            .int_err()?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeDatasetReadAccessor {
    query_service: Arc<dyn QueryService>,
    dataset: ReadCheckedDataset<'static>,
}

impl MoleculeDatasetReadAccessor {
    #[expect(dead_code)]
    pub(crate) fn get_read_checked_dataset(&self) -> &ReadCheckedDataset<'_> {
        &self.dataset
    }

    pub(crate) async fn try_get_data_frame(&self) -> Result<Option<DataFrameExt>, QueryError> {
        match self
            .query_service
            .get_data((*self.dataset).clone(), GetDataOptions::default())
            .await
        {
            Ok(res) => Ok(res.df),
            Err(QueryError::Access(e)) => Err(e.into()),
            Err(e) => Err(e.int_err().into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
