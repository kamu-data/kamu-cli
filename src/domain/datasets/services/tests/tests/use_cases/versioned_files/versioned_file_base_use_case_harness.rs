// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::Cursor;
use std::sync::Arc;

use kamu_datasets::{
    ContentArgs,
    DatasetSnapshots,
    FileVersion,
    ResolvedDataset,
    UpdateVersionFileResult,
    UpdateVersionFileUseCaseError,
    UpdateVersionedFileUseCase,
    VersionedFileEntry,
    WriteCheckedDataset,
};
use kamu_datasets_services::UpdateVersionedFileUseCaseImpl;
use tokio::io::BufReader;

use crate::tests::use_cases::push_source_dataset_base_use_case_harness::PushSourceDatasetBaseUseCaseHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Versioned file datasets populated through [`UpdateVersionedFileUseCase`]
#[oop::extend(
    PushSourceDatasetBaseUseCaseHarness,
    push_source_dataset_base_use_case_harness
)]
pub struct VersionedFileBaseUseCaseHarness {
    push_source_dataset_base_use_case_harness: PushSourceDatasetBaseUseCaseHarness,
    catalog: dill::Catalog,
    update_use_case: Arc<dyn UpdateVersionedFileUseCase>,
}

impl VersionedFileBaseUseCaseHarness {
    pub fn new() -> Self {
        let push_source_dataset_base_use_case_harness = PushSourceDatasetBaseUseCaseHarness::new();

        let catalog =
            dill::CatalogBuilder::new_chained(push_source_dataset_base_use_case_harness.catalog())
                .add::<UpdateVersionedFileUseCaseImpl>()
                .build();

        Self {
            push_source_dataset_base_use_case_harness,
            update_use_case: catalog.get_one().unwrap(),
            catalog,
        }
    }

    pub fn catalog(&self) -> &dill::Catalog {
        &self.catalog
    }

    pub async fn create_versioned_file(
        &self,
        extra_columns: Vec<odf::schema::DataField>,
    ) -> ResolvedDataset {
        let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("file"));
        self.create_dataset(
            DatasetSnapshots::versioned_file(alias, extra_columns, Vec::new()).unwrap(),
        )
        .await
    }

    pub fn content_hash(content: &[u8]) -> odf::Multihash {
        odf::Multihash::from_digest_sha3_256(content)
    }

    /// Uploads a new version with the specified content
    pub async fn upload_res(
        &self,
        file: &ResolvedDataset,
        content: &[u8],
        expected_head: Option<odf::Multihash>,
    ) -> Result<UpdateVersionFileResult, UpdateVersionFileUseCaseError> {
        let content_args = ContentArgs {
            content_stream: Some(Box::new(BufReader::new(Cursor::new(content.to_vec())))),
            content_hash: Self::content_hash(content),
            content_length: content.len(),
            content_type: None,
        };

        self.update_use_case
            .execute(
                WriteCheckedDataset::from_ref(file),
                None,
                Some(content_args),
                expected_head,
                None,
            )
            .await
    }

    pub async fn upload(&self, file: &ResolvedDataset, content: &[u8]) -> UpdateVersionFileResult {
        self.upload_res(file, content, None).await.unwrap()
    }

    /// Creates a new version that keeps the content of the latest one and replaces its
    /// extra data
    pub async fn update_extra_data_res(
        &self,
        file: &ResolvedDataset,
        extra_data: serde_json::Value,
    ) -> Result<UpdateVersionFileResult, UpdateVersionFileUseCaseError> {
        self.update_use_case
            .execute(
                WriteCheckedDataset::from_ref(file),
                None,
                None,
                None,
                Some(PushSourceDatasetBaseUseCaseHarness::extra_data(extra_data)),
            )
            .await
    }

    pub async fn update_extra_data(
        &self,
        file: &ResolvedDataset,
        extra_data: serde_json::Value,
    ) -> UpdateVersionFileResult {
        self.update_extra_data_res(file, extra_data).await.unwrap()
    }

    /// The parts of an entry tests compare: `(version, content hash, extra data)`
    pub fn summary(entry: &VersionedFileEntry) -> (FileVersion, odf::Multihash, serde_json::Value) {
        (
            entry.version,
            entry.content_hash.clone(),
            serde_json::Value::Object(entry.extra_data.as_map().clone()),
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
