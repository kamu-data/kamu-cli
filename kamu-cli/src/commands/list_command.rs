// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::{CLIError, Command};
use crate::output::*;
use chrono::{DateTime, Utc};
use chrono_humanize::HumanTime;
use futures::TryStreamExt;
use kamu::domain::*;
use opendatafabric::*;
use std::sync::Arc;

pub struct ListCommand {
    local_repo: Arc<dyn DatasetRepository>,
    remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
    output_config: Arc<OutputConfig>,
    detail_level: u8,
}

impl ListCommand {
    pub fn new(
        local_repo: Arc<dyn DatasetRepository>,
        remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
        output_config: Arc<OutputConfig>,
        detail_level: u8,
    ) -> Self {
        Self {
            local_repo,
            remote_alias_reg,
            output_config,
            detail_level,
        }
    }

    fn humanize_relative_date(last_pulled: DateTime<Utc>) -> String {
        format!("{}", HumanTime::from(last_pulled - Utc::now()))
    }

    fn humanize_data_size(size: u64) -> String {
        if size == 0 {
            return "-".to_owned();
        }
        use humansize::{format_size, BINARY};
        format_size(size, BINARY)
    }

    fn humanize_quantity(num: u64) -> String {
        use num_format::{Locale, ToFormattedString};
        if num == 0 {
            return "-".to_owned();
        }
        num.to_formatted_string(&Locale::en)
    }

    async fn get_kind(
        &self,
        handle: &DatasetHandle,
        summary: &DatasetSummary,
    ) -> Result<String, CLIError> {
        let is_remote = self
            .remote_alias_reg
            .get_remote_aliases(&handle.as_local_ref())
            .await?
            .get_by_kind(RemoteAliasKind::Pull)
            .next()
            .is_some();
        Ok(if is_remote {
            format!("Remote({:?})", summary.kind)
        } else {
            format!("{:?}", summary.kind)
        })
    }
}

#[async_trait::async_trait(?Send)]
impl Command for ListCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        use datafusion::arrow::{
            array::{StringArray, TimestampMicrosecondArray, UInt64Array},
            datatypes::{DataType, Field, Schema, TimeUnit},
            record_batch::RecordBatch,
        };

        let records_format = RecordsFormat::new()
            .with_default_column_format(ColumnFormat::default().with_null_value("-"))
            .with_column_formats(if self.detail_level == 0 {
                vec![
                    ColumnFormat::new().with_style_spec("l"),
                    ColumnFormat::new().with_style_spec("c"),
                    ColumnFormat::new()
                        .with_style_spec("c")
                        .with_value_fmt(Self::humanize_relative_date),
                    ColumnFormat::new()
                        .with_style_spec("r")
                        .with_value_fmt(Self::humanize_quantity),
                    ColumnFormat::new()
                        .with_style_spec("r")
                        .with_value_fmt(Self::humanize_data_size),
                ]
            } else {
                vec![
                    ColumnFormat::new().with_style_spec("l"),
                    ColumnFormat::new().with_style_spec("l"),
                    ColumnFormat::new().with_style_spec("c"),
                    ColumnFormat::new().with_style_spec("l"),
                    ColumnFormat::new()
                        .with_style_spec("c")
                        .with_value_fmt(Self::humanize_relative_date),
                    ColumnFormat::new()
                        .with_style_spec("r")
                        .with_value_fmt(Self::humanize_quantity),
                    ColumnFormat::new()
                        .with_style_spec("r")
                        .with_value_fmt(Self::humanize_quantity),
                    ColumnFormat::new()
                        .with_style_spec("r")
                        .with_value_fmt(Self::humanize_data_size),
                    ColumnFormat::new()
                        .with_style_spec("c")
                        .with_value_fmt(Self::humanize_relative_date),
                ]
            });

        let mut writer = self.output_config.get_records_writer(records_format);

        let tz = Some(Arc::from("+00:00"));
        let schema = Arc::new(Schema::new(if self.detail_level == 0 {
            vec![
                Field::new("Name", DataType::Utf8, false),
                Field::new("Kind", DataType::Utf8, false),
                Field::new(
                    "Pulled",
                    DataType::Timestamp(TimeUnit::Microsecond, tz.clone()),
                    true,
                ),
                Field::new("Records", DataType::UInt64, false),
                Field::new("Size", DataType::UInt64, false),
            ]
        } else {
            vec![
                Field::new("ID", DataType::Utf8, false),
                Field::new("Name", DataType::Utf8, false),
                Field::new("Kind", DataType::Utf8, false),
                Field::new("Head", DataType::Utf8, false),
                Field::new(
                    "Pulled",
                    DataType::Timestamp(TimeUnit::Microsecond, tz.clone()),
                    true,
                ),
                Field::new("Records", DataType::UInt64, false),
                Field::new("Blocks", DataType::UInt64, false),
                Field::new("Size", DataType::UInt64, false),
                Field::new(
                    "Watermark",
                    DataType::Timestamp(TimeUnit::Microsecond, tz.clone()),
                    true,
                ),
            ]
        }));

        let mut id: Vec<String> = Vec::new();
        let mut name: Vec<String> = Vec::new();
        let mut kind: Vec<String> = Vec::new();
        let mut head: Vec<String> = Vec::new();
        let mut pulled: Vec<Option<i64>> = Vec::new();
        let mut records: Vec<u64> = Vec::new();
        let mut blocks: Vec<u64> = Vec::new();
        let mut size: Vec<u64> = Vec::new();
        let mut watermark: Vec<Option<i64>> = Vec::new();

        let mut datasets: Vec<_> = self.local_repo.get_all_datasets().try_collect().await?;
        datasets.sort_by(|a, b| a.alias.cmp(&b.alias));

        for hdl in datasets.iter() {
            let dataset = self.local_repo.get_dataset(&hdl.as_local_ref()).await?;
            let current_head = dataset.as_metadata_chain().get_ref(&BlockRef::Head).await?;
            let summary = dataset.get_summary(GetSummaryOpts::default()).await?;

            name.push(hdl.alias.to_string());
            kind.push(self.get_kind(hdl, &summary).await?);
            pulled.push(summary.last_pulled.map(|t| t.timestamp_micros()));
            records.push(summary.num_records);
            size.push(summary.data_size);

            if self.detail_level > 0 {
                // TODO: Should be precomputed
                let mut num_blocks = 0;
                let mut last_watermark: Option<DateTime<Utc>> = None;
                let mut blocks_stream = dataset.as_metadata_chain().iter_blocks();
                while let Some((_, b)) = blocks_stream.try_next().await? {
                    if num_blocks == 0 {
                        num_blocks = b.sequence_number as u64 + 1;
                    }
                    if let Some(b) = b.as_data_stream_block() {
                        last_watermark = b.event.output_watermark.cloned();
                        break;
                    }
                }

                id.push(hdl.id.to_did_string());
                head.push(current_head.to_multibase_string());
                blocks.push(num_blocks);
                watermark.push(last_watermark.map(|t| t.timestamp_micros()));
            }
        }

        let records = RecordBatch::try_new(
            schema,
            if self.detail_level == 0 {
                vec![
                    Arc::new(StringArray::from(name)),
                    Arc::new(StringArray::from(kind)),
                    Arc::new(TimestampMicrosecondArray::from(pulled).with_timezone_utc()),
                    Arc::new(UInt64Array::from(records)),
                    Arc::new(UInt64Array::from(size)),
                ]
            } else {
                vec![
                    Arc::new(StringArray::from(id)),
                    Arc::new(StringArray::from(name)),
                    Arc::new(StringArray::from(kind)),
                    Arc::new(StringArray::from(head)),
                    Arc::new(TimestampMicrosecondArray::from(pulled).with_timezone_utc()),
                    Arc::new(UInt64Array::from(records)),
                    Arc::new(UInt64Array::from(blocks)),
                    Arc::new(UInt64Array::from(size)),
                    Arc::new(TimestampMicrosecondArray::from(watermark).with_timezone_utc()),
                ]
            },
        )
        .unwrap();

        writer.write_batches(&[records])?;
        writer.finish()?;

        Ok(())
    }
}
