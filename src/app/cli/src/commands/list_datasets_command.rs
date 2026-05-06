// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;
use std::str::FromStr;
use std::sync::Arc;

use database_common::PaginationOpts;
use internal_error::{InternalError, ResultIntoInternal};
use kamu::domain::*;
use kamu_datasets::{DatasetRegistry, ResolvedDataset};

use super::{CLIError, Command, common};
use crate::output::*;
use crate::{NotInMultiTenantWorkspace, accounts};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ListDatasetsCommand {
    tenancy_config: TenancyConfig,
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_statistics_service: Arc<dyn kamu_datasets::DatasetStatisticsService>,
    remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
    rebac_service: Arc<dyn kamu_auth_rebac::RebacService>,

    current_account: accounts::CurrentAccountIndication,
    related_account: accounts::RelatedAccountIndication,
    output_config: Arc<OutputConfig>,
    detail_level: u8,
    max_results: Option<NonZeroUsize>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ListDatasetsCommand {
    pub fn new(
        tenancy_config: TenancyConfig,
        dataset_registry: Arc<dyn DatasetRegistry>,
        dataset_statistics_service: Arc<dyn kamu_datasets::DatasetStatisticsService>,
        remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
        rebac_service: Arc<dyn kamu_auth_rebac::RebacService>,
        current_account: accounts::CurrentAccountIndication,
        related_account: accounts::RelatedAccountIndication,
        output_config: Arc<OutputConfig>,
        detail_level: u8,
        max_results: Option<NonZeroUsize>,
    ) -> Self {
        Self {
            tenancy_config,
            dataset_registry,
            dataset_statistics_service,
            remote_alias_reg,
            rebac_service,
            current_account,
            related_account,
            output_config,
            detail_level,
            max_results,
        }
    }

    async fn get_kind(&self, target: &ResolvedDataset) -> Result<String, CLIError> {
        let is_remote = self
            .remote_alias_reg
            .get_remote_aliases(target.get_handle())
            .await?
            .get_by_kind(RemoteAliasKind::Pull)
            .next()
            .is_some();
        Ok(if is_remote {
            format!("Remote({:?})", target.get_kind())
        } else {
            format!("{:?}", target.get_kind())
        })
    }

    fn column_formats(&self, show_owners: bool, show_visibility: bool) -> Vec<ColumnFormat> {
        let mut cols: Vec<ColumnFormat> = Vec::new();
        if self.detail_level > 0 {
            cols.push(ColumnFormat::new().with_style_spec("l")); // id
        }
        if show_owners {
            cols.push(ColumnFormat::new().with_style_spec("l")); // owner
        }
        cols.push(ColumnFormat::new().with_style_spec("l")); // name
        if show_visibility {
            cols.push(ColumnFormat::new().with_style_spec("l")); // visibility
        }
        cols.push(ColumnFormat::new().with_style_spec("c")); // kind
        if self.detail_level > 0 {
            cols.push(ColumnFormat::new().with_style_spec("l")); // head
        }
        cols.push(
            // pulled
            ColumnFormat::new()
                .with_style_spec("c")
                .with_value_fmt_t(common::humanize_relative_date),
        );
        cols.push(
            // records
            ColumnFormat::new()
                .with_style_spec("r")
                .with_value_fmt_t(common::humanize_quantity),
        );
        if self.detail_level > 0 {
            cols.push(
                // blocks
                ColumnFormat::new()
                    .with_style_spec("r")
                    .with_value_fmt_t(common::humanize_quantity),
            );
        }
        cols.push(
            // size
            ColumnFormat::new()
                .with_style_spec("r")
                .with_value_fmt_t(common::humanize_data_size),
        );
        if self.detail_level > 0 {
            cols.push(
                // watermark
                ColumnFormat::new()
                    .with_style_spec("c")
                    .with_value_fmt_t(common::humanize_relative_date),
            );
        }
        cols
    }

    fn schema_fields(
        &self,
        show_owners: bool,
        show_visibility: bool,
    ) -> Vec<datafusion::arrow::datatypes::Field> {
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
        if show_visibility {
            fields.push(Field::new("Visibility", DataType::Utf8, false));
        }
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

    async fn list_datasets(
        &self,
        pagination: Option<PaginationOpts>,
    ) -> Result<Vec<odf::DatasetHandle>, InternalError> {
        // Decide wheter listing all datasets of filtering by owner
        let maybe_owner_name = match self.tenancy_config {
            TenancyConfig::MultiTenant => match &self.related_account.target_account {
                accounts::TargetAccountSelection::Current => {
                    Some(self.current_account.account_name.clone())
                }
                accounts::TargetAccountSelection::Specific { account_name } => {
                    Some(odf::AccountName::from_str(account_name.as_str()).unwrap())
                }
                accounts::TargetAccountSelection::AllUsers => None,
            },
            TenancyConfig::SingleTenant => None,
        };

        use futures::TryStreamExt;

        // Owner-aware path
        let dataset_handles = if let Some(owner_name) = maybe_owner_name {
            if let Some(pagination) = pagination {
                self.dataset_registry
                    .all_dataset_handles_by_owner_name_paged(&owner_name, pagination)
                    .await?
            } else {
                self.dataset_registry
                    .all_dataset_handles_by_owner_name(&owner_name)
                    .try_collect()
                    .await?
            }
        } else {
            // All datasets path
            if let Some(pagination) = pagination {
                self.dataset_registry
                    .all_dataset_handles_paged(pagination)
                    .await?
            } else {
                self.dataset_registry
                    .all_dataset_handles()
                    .try_collect()
                    .await?
            }
        };

        Ok(dataset_handles)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait(?Send)]
impl Command for ListDatasetsCommand {
    async fn validate_args(&self) -> Result<(), CLIError> {
        if self.tenancy_config == TenancyConfig::SingleTenant && self.related_account.is_explicit()
        {
            return Err(CLIError::usage_error_from(NotInMultiTenantWorkspace));
        }

        Ok(())
    }

    async fn run(&self) -> Result<(), CLIError> {
        use datafusion::arrow::array::{
            ArrayRef,
            StringArray,
            TimestampMicrosecondArray,
            UInt64Array,
        };
        use datafusion::arrow::datatypes::Schema;
        use datafusion::arrow::record_batch::RecordBatch;
        use odf::dataset::MetadataChainExt;

        let show_owners = self.current_account.is_explicit() || self.related_account.is_explicit();
        let show_visibility = self.tenancy_config == TenancyConfig::MultiTenant;

        // ToDo use Output writer trait
        let records_format = RecordsFormat::new()
            .with_default_column_format(ColumnFormat::default().with_null_value("-"))
            .with_column_formats(self.column_formats(show_owners, show_visibility));

        let schema = Arc::new(Schema::new(
            self.schema_fields(show_owners, show_visibility),
        ));

        let mut writer = self
            .output_config
            .get_records_writer(&schema, records_format);

        let mut id: Vec<String> = Vec::new();
        let mut name: Vec<String> = Vec::new();
        let mut owner: Vec<String> = Vec::new();
        let mut visibility: Vec<String> = Vec::new();
        let mut kind: Vec<String> = Vec::new();
        let mut head: Vec<String> = Vec::new();
        let mut pulled: Vec<Option<i64>> = Vec::new();
        let mut records: Vec<u64> = Vec::new();
        let mut blocks: Vec<u64> = Vec::new();
        let mut size: Vec<u64> = Vec::new();
        let mut watermark: Vec<Option<i64>> = Vec::new();

        let pagination = self
            .max_results
            .map(|max_results| PaginationOpts::from_max_results(max_results.get()));

        let mut datasets: Vec<_> = self.list_datasets(pagination).await?;
        datasets.sort_by(|a, b| a.alias.cmp(&b.alias));

        let maybe_rebac_dataset_properties_map = if show_visibility {
            // Safety: The map guarantees the pair presence
            let dataset_ids = datasets.iter().map(|h| h.id.clone()).collect::<Vec<_>>();
            let map = self
                .rebac_service
                .get_dataset_properties_by_ids(&dataset_ids)
                .await
                .int_err()?;
            Some(map)
        } else {
            None
        };

        for hdl in &datasets {
            let target = self.dataset_registry.get_dataset_by_handle(hdl).await;

            let current_head = target
                .as_metadata_chain()
                .resolve_ref(&odf::BlockRef::Head)
                .await?;

            let statistics = self
                .dataset_statistics_service
                .get_statistics(&hdl.id, &odf::BlockRef::Head)
                .await?;

            name.push(hdl.alias.dataset_name.to_string());

            if show_owners {
                owner.push(String::from(match &hdl.alias.account_name {
                    Some(name) => name.as_str(),
                    None => self.current_account.account_name.as_str(),
                }));
            }

            if let Some(rebac_dataset_properties_map) = &maybe_rebac_dataset_properties_map {
                let dataset_properties = rebac_dataset_properties_map.get(&hdl.id).unwrap();
                let value = if dataset_properties.allows_public_read {
                    odf::DatasetVisibility::Public
                } else {
                    odf::DatasetVisibility::Private
                };
                visibility.push(value.to_string());
            }

            kind.push(self.get_kind(&target).await?);
            pulled.push(statistics.last_pulled.map(|t| t.timestamp_micros()));
            records.push(statistics.num_records);
            size.push(statistics.data_size_bytes);

            if self.detail_level > 0 {
                let num_blocks = target
                    .as_metadata_chain()
                    .get_block(&current_head)
                    .await
                    .int_err()?
                    .sequence_number
                    + 1;
                let last_watermark = target
                    .as_metadata_chain()
                    .last_data_block()
                    .await
                    .int_err()?
                    .into_event()
                    .and_then(|event| event.new_watermark.map(|t| t.timestamp_micros()));

                id.push(hdl.id.to_string());
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
        if show_visibility {
            columns.push(Arc::new(StringArray::from(visibility)));
        }
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
