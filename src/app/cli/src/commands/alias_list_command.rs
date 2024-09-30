// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use futures::TryStreamExt;
use internal_error::ResultIntoInternal;
use kamu::domain::*;
use opendatafabric::*;

use super::{CLIError, Command};
use crate::output::*;

pub struct AliasListCommand {
    dataset_repo: Arc<dyn DatasetRepository>,
    remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
    output_config: Arc<OutputConfig>,
    dataset_ref: Option<DatasetRef>,
}

impl AliasListCommand {
    pub fn new(
        dataset_repo: Arc<dyn DatasetRepository>,
        remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
        output_config: Arc<OutputConfig>,
        dataset_ref: Option<DatasetRef>,
    ) -> Self {
        Self {
            dataset_repo,
            remote_alias_reg,
            output_config,
            dataset_ref,
        }
    }

    fn records_format(&self) -> RecordsFormat {
        RecordsFormat::new()
            .with_default_column_format(ColumnFormat::default())
            .with_column_formats(vec![
                ColumnFormat::new().with_style_spec("l"),
                ColumnFormat::new().with_style_spec("c"),
                ColumnFormat::new().with_style_spec("l"),
            ])
    }

    fn schema(&self) -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("Dataset", DataType::Utf8, false),
            Field::new("Kind", DataType::Utf8, false),
            Field::new("Alias", DataType::Utf8, false),
        ]))
    }

    async fn records(
        &self,
        schema: Arc<Schema>,
        datasets: &Vec<DatasetHandle>,
    ) -> Result<RecordBatch, CLIError> {
        let mut col_dataset = Vec::new();
        let mut col_kind = Vec::new();
        let mut col_alias = Vec::new();

        for ds in datasets {
            let aliases = self
                .remote_alias_reg
                .get_remote_aliases(&ds.as_local_ref())
                .await?;
            let mut pull_aliases: Vec<_> = aliases
                .get_by_kind(RemoteAliasKind::Pull)
                .map(ToString::to_string)
                .collect();
            let mut push_aliases: Vec<_> = aliases
                .get_by_kind(RemoteAliasKind::Push)
                .map(ToString::to_string)
                .collect();

            pull_aliases.sort();
            push_aliases.sort();

            for alias in pull_aliases {
                col_dataset.push(ds.alias.to_string());
                col_kind.push("Pull");
                col_alias.push(alias);
            }

            for alias in push_aliases {
                col_dataset.push(ds.alias.to_string());
                col_kind.push("Push");
                col_alias.push(alias);
            }
        }

        let records = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(col_dataset)),
                Arc::new(StringArray::from(col_kind)),
                Arc::new(StringArray::from(col_alias)),
            ],
        )
        .int_err()?;

        Ok(records)
    }
}

#[async_trait::async_trait(?Send)]
impl Command for AliasListCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        let mut datasets: Vec<_> = if let Some(dataset_ref) = &self.dataset_ref {
            let hdl = self.dataset_repo.resolve_dataset_ref(dataset_ref).await?;
            vec![hdl]
        } else {
            self.dataset_repo.get_all_datasets().try_collect().await?
        };

        datasets.sort_by(|a, b| a.alias.cmp(&b.alias));

        let schema = self.schema();
        let records = self.records(schema.clone(), &datasets).await?;

        let mut writer = self
            .output_config
            .get_records_writer(&schema, self.records_format());

        writer.write_batch(&records)?;
        writer.finish()?;

        Ok(())
    }
}
