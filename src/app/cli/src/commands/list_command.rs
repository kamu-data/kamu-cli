// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use chrono_humanize::HumanTime;
use futures::TryStreamExt;
use kamu::domain::*;
use opendatafabric::*;

use super::{CLIError, Command};
use crate::output::*;
use crate::{accounts, NotInMultiTenantWorkspace};

pub struct ListCommand {
    dataset_repo: Arc<dyn DatasetRepository>,
    remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
    current_account: accounts::CurrentAccountIndication,
    related_account: accounts::RelatedAccountIndication,
    output_config: Arc<OutputConfig>,
    detail_level: u8,
}

impl ListCommand {
    pub fn new(
        dataset_repo: Arc<dyn DatasetRepository>,
        remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
        current_account: accounts::CurrentAccountIndication,
        related_account: accounts::RelatedAccountIndication,
        output_config: Arc<OutputConfig>,
        detail_level: u8,
    ) -> Self {
        Self {
            dataset_repo,
            remote_alias_reg,
            current_account,
            related_account,
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

    fn column_formats(&self, show_owners: bool) -> Vec<ColumnFormat> {
        let mut cols: Vec<ColumnFormat> = Vec::new();
        if self.detail_level > 0 {
            cols.push(ColumnFormat::new().with_style_spec("l")); // id
        }
        if show_owners {
            cols.push(ColumnFormat::new().with_style_spec("l")); // owner
        }
        cols.push(ColumnFormat::new().with_style_spec("l")); // name
        cols.push(ColumnFormat::new().with_style_spec("c")); // kind
        if self.detail_level > 0 {
            cols.push(ColumnFormat::new().with_style_spec("l")); // head
        }
        cols.push(
            // pulled
            ColumnFormat::new()
                .with_style_spec("c")
                .with_value_fmt_t(Self::humanize_relative_date),
        );
        cols.push(
            // records
            ColumnFormat::new()
                .with_style_spec("r")
                .with_value_fmt_t(Self::humanize_quantity),
        );
        if self.detail_level > 0 {
            cols.push(
                // blocks
                ColumnFormat::new()
                    .with_style_spec("r")
                    .with_value_fmt_t(Self::humanize_quantity),
            );
        }
        cols.push(
            // size
            ColumnFormat::new()
                .with_style_spec("r")
                .with_value_fmt_t(Self::humanize_data_size),
        );
        if self.detail_level > 0 {
            cols.push(
                // watermark
                ColumnFormat::new()
                    .with_style_spec("c")
                    .with_value_fmt_t(Self::humanize_relative_date),
            );
        }
        cols
    }

    fn schema_fields(&self, show_owners: bool) -> Vec<datafusion::arrow::datatypes::Field> {
        use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};

        let mut fields: Vec<Field> = Vec::new();
        let tz = Some(Arc::from("+00:00"));

        if self.detail_level > 0 {
            fields.push(Field::new("ID", DataType::Utf8, false));
        }
        if show_owners {
            fields.push(Field::new("Owner", DataType::Utf8, false));
        }
        fields.push(Field::new("Name", DataType::Utf8, false));
        fields.push(Field::new("Kind", DataType::Utf8, false));
        if self.detail_level > 0 {
            fields.push(Field::new("Head", DataType::Utf8, false));
        }
        fields.push(Field::new(
            "Pulled",
            DataType::Timestamp(TimeUnit::Microsecond, tz.clone()),
            true,
        ));
        fields.push(Field::new("Records", DataType::UInt64, false));
        if self.detail_level > 0 {
            fields.push(Field::new("Blocks", DataType::UInt64, false));
        }
        fields.push(Field::new("Size", DataType::UInt64, false));
        if self.detail_level > 0 {
            fields.push(Field::new(
                "Watermark",
                DataType::Timestamp(TimeUnit::Microsecond, tz.clone()),
                true,
            ));
        }
        fields
    }

    fn stream_datasets(&self) -> DatasetHandleStream {
        if self.dataset_repo.is_multi_tenant() {
            match &self.related_account.target_account {
                accounts::TargetAccountSelection::Current => self
                    .dataset_repo
                    .get_datasets_by_owner(&self.current_account.account_name),
                accounts::TargetAccountSelection::Specific {
                    account_name: user_name,
                } => self
                    .dataset_repo
                    .get_datasets_by_owner(&AccountName::from_str(user_name.as_str()).unwrap()),
                accounts::TargetAccountSelection::AllUsers => self.dataset_repo.get_all_datasets(),
            }
        } else {
            self.dataset_repo.get_all_datasets()
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for ListCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        use datafusion::arrow::array::{
            ArrayRef,
            StringArray,
            TimestampMicrosecondArray,
            UInt64Array,
        };
        use datafusion::arrow::datatypes::Schema;
        use datafusion::arrow::record_batch::RecordBatch;

        let show_owners = if self.dataset_repo.is_multi_tenant() {
            self.current_account.is_explicit() || self.related_account.is_explicit()
        } else if self.related_account.is_explicit() {
            return Err(CLIError::usage_error_from(NotInMultiTenantWorkspace));
        } else {
            false
        };

        let records_format = RecordsFormat::new()
            .with_default_column_format(ColumnFormat::default().with_null_value("-"))
            .with_column_formats(self.column_formats(show_owners));

        let mut writer = self.output_config.get_records_writer(records_format);

        let schema = Arc::new(Schema::new(self.schema_fields(show_owners)));

        let mut id: Vec<String> = Vec::new();
        let mut name: Vec<String> = Vec::new();
        let mut owner: Vec<String> = Vec::new();
        let mut kind: Vec<String> = Vec::new();
        let mut head: Vec<String> = Vec::new();
        let mut pulled: Vec<Option<i64>> = Vec::new();
        let mut records: Vec<u64> = Vec::new();
        let mut blocks: Vec<u64> = Vec::new();
        let mut size: Vec<u64> = Vec::new();
        let mut watermark: Vec<Option<i64>> = Vec::new();

        let mut datasets: Vec<_> = self.stream_datasets().try_collect().await?;
        datasets.sort_by(|a, b| a.alias.cmp(&b.alias));

        for hdl in &datasets {
            let dataset = self.dataset_repo.get_dataset(&hdl.as_local_ref()).await?;
            let current_head = dataset
                .as_metadata_chain()
                .resolve_ref(&BlockRef::Head)
                .await?;
            let summary = dataset.get_summary(GetSummaryOpts::default()).await?;

            name.push(hdl.alias.dataset_name.to_string());

            if show_owners {
                owner.push(String::from(match &hdl.alias.account_name {
                    Some(name) => name.as_str(),
                    None => self.current_account.account_name.as_str(),
                }));
            }

            kind.push(self.get_kind(hdl, &summary).await?);
            pulled.push(summary.last_pulled.map(|t| t.timestamp_micros()));
            records.push(summary.num_records);
            size.push(summary.data_size);

            if self.detail_level > 0 {
                let num_blocks = dataset
                    .as_metadata_chain()
                    .get_block(&current_head)
                    .await
                    .int_err()?
                    .sequence_number
                    + 1;
                let last_watermark = dataset
                    .as_metadata_chain()
                    .last_data_block()
                    .await
                    .int_err()?
                    .into_event()
                    .and_then(|event| event.new_watermark.map(|t| t.timestamp_micros()));

                id.push(hdl.id.as_did_str().to_string());
                head.push(current_head.as_multibase().to_string());
                blocks.push(num_blocks);
                watermark.push(last_watermark);
            }
        }

        let mut columns: Vec<ArrayRef> = Vec::new();

        if self.detail_level > 0 {
            columns.push(Arc::new(StringArray::from(id)));
        }
        if show_owners {
            columns.push(Arc::new(StringArray::from(owner)));
        }
        columns.push(Arc::new(StringArray::from(name)));
        columns.push(Arc::new(StringArray::from(kind)));
        if self.detail_level > 0 {
            columns.push(Arc::new(StringArray::from(head)));
        }
        columns.push(Arc::new(
            TimestampMicrosecondArray::from(pulled).with_timezone_utc(),
        ));
        columns.push(Arc::new(UInt64Array::from(records)));
        if self.detail_level > 0 {
            columns.push(Arc::new(UInt64Array::from(blocks)));
        }
        columns.push(Arc::new(UInt64Array::from(size)));
        if self.detail_level > 0 {
            columns.push(Arc::new(
                TimestampMicrosecondArray::from(watermark).with_timezone_utc(),
            ));
        }

        let records = RecordBatch::try_new(schema, columns).unwrap();

        writer.write_batch(&records)?;
        writer.finish()?;

        Ok(())
    }
}
