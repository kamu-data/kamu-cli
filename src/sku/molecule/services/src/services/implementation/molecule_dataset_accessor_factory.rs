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
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use kamu_auth_rebac::{RebacDatasetRefUnresolvedError, RebacDatasetRegistryFacade};
use kamu_core::{
    GetDataOptions,
    PushIngestDataError,
    PushIngestDataUseCase,
    PushIngestError,
    PushIngestResult,
    QueryError,
    QueryService,
};
use kamu_datasets::{
    CreateDatasetFromSnapshotUseCase,
    DatasetAction,
    ReadCheckedDataset,
    ResolvedDataset,
    WriteCheckedDataset,
};
use odf::utils::data::DataFrameExt;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: nothing Molecule-specific here, but pretty hard for now to put it
// somewhere else,  as types from multiple crates are involved
#[dill::component]
pub struct MoleculeDatasetAccessorFactory {
    create_dataset_from_snapshot_uc: Arc<dyn CreateDatasetFromSnapshotUseCase>,
    push_ingest_uc: Arc<dyn PushIngestDataUseCase>,
    rebac_dataset_registry_facade: Arc<dyn RebacDatasetRegistryFacade>,
    query_service: Arc<dyn QueryService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MoleculeDatasetAccessorFactory {
    pub async fn reader(
        &self,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<MoleculeDatasetReader, RebacDatasetRefUnresolvedError> {
        let resolved_dataset = self
            .rebac_dataset_registry_facade
            .resolve_dataset_by_ref(dataset_ref, DatasetAction::Read)
            .await?;

        Ok(MoleculeDatasetReader {
            query_service: self.query_service.clone(),
            dataset: ReadCheckedDataset::from_owned(resolved_dataset),
        })
    }

    pub async fn writer(
        &self,
        dataset_ref: &odf::DatasetRef,
        create_if_not_exist: bool,
        snapshot_getter: impl FnOnce() -> odf::DatasetSnapshot,
    ) -> Result<MoleculeDatasetWriter, RebacDatasetRefUnresolvedError> {
        let resolved_dataset = match self
            .rebac_dataset_registry_facade
            .resolve_dataset_by_ref(dataset_ref, DatasetAction::Write)
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

        Ok(MoleculeDatasetWriter {
            push_ingest_uc: self.push_ingest_uc.clone(),
            query_service: self.query_service.clone(),
            dataset: WriteCheckedDataset::from_owned(resolved_dataset),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeDatasetWriter {
    push_ingest_uc: Arc<dyn PushIngestDataUseCase>,
    query_service: Arc<dyn QueryService>,
    dataset: WriteCheckedDataset<'static>,
}

impl MoleculeDatasetWriter {
    pub(crate) fn get_write_checked_dataset(&self) -> &WriteCheckedDataset<'_> {
        &self.dataset
    }

    pub(crate) fn as_reader(&self) -> MoleculeDatasetReader {
        MoleculeDatasetReader {
            query_service: Arc::clone(&self.query_service),
            dataset: self.dataset.clone().into(),
        }
    }

    pub(crate) async fn push_ndjson_data(
        &self,
        buffer: bytes::Bytes,
        source_event_time: Option<DateTime<Utc>>,
    ) -> Result<PushIngestResult, PushIngestError> {
        let res = self
            .push_ingest_uc
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
            .map_err(|e| match e {
                PushIngestDataError::Execution(e) => e,
                e => PushIngestError::Internal(e.int_err()),
            })?;

        Ok(res)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeDatasetReader {
    query_service: Arc<dyn QueryService>,
    dataset: ReadCheckedDataset<'static>,
}

impl MoleculeDatasetReader {
    #[expect(dead_code)]
    pub(crate) fn read_checked_dataset(&self) -> &ReadCheckedDataset<'_> {
        &self.dataset
    }

    pub(crate) async fn raw_ledger_data_frame(&self) -> Result<Option<DataFrameExt>, QueryError> {
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

    pub(crate) async fn changelog_projection_data_frame_by(
        &self,
        primary_key: &str,
    ) -> Result<Option<DataFrameExt>, QueryError> {
        let maybe_df = self.raw_ledger_data_frame().await?;

        let df = if let Some(df) = maybe_df {
            Some(
                odf::utils::data::changelog::project(
                    df,
                    &[primary_key.to_string()],
                    &odf::metadata::DatasetVocabulary::default(),
                )
                .int_err()?,
            )
        } else {
            None
        };

        Ok(df)
    }

    pub(crate) async fn changelog_projection_entry_by(
        &self,
        primary_key: &str,
        filter_key: &str,
        filter_value: &str,
    ) -> Result<Option<serde_json::Value>, QueryError> {
        use datafusion::prelude::*;

        let df_opt = self.changelog_projection_data_frame_by(primary_key).await?;

        let Some(df) = df_opt else {
            return Ok(None);
        };

        let df = df.filter(col(filter_key).eq(lit(filter_value))).int_err()?;

        let records = df.collect_json_aos().await.int_err()?;
        assert!(records.len() <= 1);

        let maybe_the_record = records.into_iter().next();
        Ok(maybe_the_record)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
