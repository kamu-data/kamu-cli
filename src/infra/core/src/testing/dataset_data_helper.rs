// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::prelude::*;
use kamu_core::*;
use opendatafabric::*;

pub struct DatasetDataHelper {
    dataset: Arc<dyn Dataset>,
    ctx: SessionContext,
}

impl DatasetDataHelper {
    pub fn new(dataset: Arc<dyn Dataset>) -> Self {
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        Self { dataset, ctx }
    }

    pub fn new_with_context(dataset: Arc<dyn Dataset>, ctx: SessionContext) -> Self {
        Self { dataset, ctx }
    }

    pub async fn get_last_block_typed<T: VariantOf<MetadataEvent>>(&self) -> MetadataBlockTyped<T> {
        let hash = self
            .dataset
            .as_metadata_chain()
            .resolve_ref(&BlockRef::Head)
            .await
            .unwrap();
        let block = self
            .dataset
            .as_metadata_chain()
            .get_block(&hash)
            .await
            .unwrap();
        block
            .into_typed::<T>()
            .expect("Last block is not a data block")
    }

    pub async fn get_last_data_block(&self) -> MetadataBlockDataStream {
        let hash = self
            .dataset
            .as_metadata_chain()
            .resolve_ref(&BlockRef::Head)
            .await
            .unwrap();
        let block = self
            .dataset
            .as_metadata_chain()
            .get_block(&hash)
            .await
            .unwrap();
        block
            .into_data_stream_block()
            .expect("Last block is not a data block")
    }

    pub async fn get_last_data_file(&self) -> PathBuf {
        let block = self.get_last_data_block().await;
        kamu_data_utils::data::local_url::into_local_path(
            self.dataset
                .as_data_repo()
                .get_internal_url(&block.event.new_data.unwrap().physical_hash)
                .await,
        )
        .unwrap()
    }

    pub async fn get_last_data(&self) -> DataFrame {
        let part_file = self.get_last_data_file().await;
        self.ctx
            .read_parquet(
                part_file.to_string_lossy().as_ref(),
                ParquetReadOptions {
                    file_extension: part_file
                        .extension()
                        .and_then(|s| s.to_str())
                        .unwrap_or_default(),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
    }

    pub async fn get_last_set_data_schema_block(&self) -> MetadataBlockTyped<SetDataSchema> {
        self.dataset
            .as_metadata_chain()
            .accept_one(SearchSetDataSchemaVisitor::create())
            .await
            .unwrap()
            .into_block()
            .unwrap()
    }

    pub async fn assert_last_data_schema_eq(&self, expected: &str) {
        let df = self.get_last_data().await;
        kamu_data_utils::testing::assert_schema_eq(df.schema(), expected);
    }

    pub async fn assert_last_data_records_eq(&self, expected: &str) {
        let df = self.get_last_data().await;
        kamu_data_utils::testing::assert_data_eq(df, expected).await;
    }

    pub async fn assert_last_data_eq(&self, schema: &str, records: &str) {
        let df = self.get_last_data().await;
        kamu_data_utils::testing::assert_schema_eq(df.schema(), schema);
        kamu_data_utils::testing::assert_data_eq(df, records).await;
    }
}
