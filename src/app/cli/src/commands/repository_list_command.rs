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
use internal_error::{InternalError, ResultIntoInternal};
use kamu::domain::*;

use super::{CLIError, Command};
use crate::output::*;

#[dill::component]
#[dill::interface(dyn Command)]
pub struct RepositoryListCommand {
    remote_repo_reg: Arc<dyn RemoteRepositoryRegistry>,
    output_config: Arc<OutputConfig>,
}

impl RepositoryListCommand {
    fn record_batch(&self) -> Result<RecordBatch, InternalError> {
        let mut col_name = Vec::new();
        let mut col_url = Vec::new();

        let mut repos: Vec<_> = self.remote_repo_reg.get_all_repositories().collect();
        repos.sort();
        for name in &repos {
            let repo = self.remote_repo_reg.get_repository(name).int_err()?;
            col_name.push(name.as_str());
            col_url.push(repo.url.to_string());
        }

        self.records(vec![
            Arc::new(StringArray::from(col_name)),
            Arc::new(StringArray::from(col_url)),
        ])
    }
}

#[async_trait::async_trait(?Send)]
impl Command for RepositoryListCommand {
    async fn run(&self) -> Result<(), CLIError> {
        let mut writer = self
            .output_config
            .get_records_writer(&self.schema(), self.records_format());

        writer.write_batch(&self.record_batch()?)?;
        writer.finish()?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait(?Send)]
impl OutputWriter for RepositoryListCommand {
    fn records_format(&self) -> RecordsFormat {
        RecordsFormat::new()
            .with_default_column_format(ColumnFormat::default())
            .with_column_formats(vec![
                ColumnFormat::new().with_style_spec("l"),
                ColumnFormat::new().with_style_spec("l"),
            ])
    }

    fn schema(&self) -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("Name", DataType::Utf8, false),
            Field::new("Url", DataType::Utf8, false),
        ]))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
