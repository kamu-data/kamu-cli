// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::DateTime;
use kamu::domain::*;

use super::{CLIError, Command};

#[dill::component]
#[dill::interface(dyn Command)]
pub struct SetWatermarkCommand {
    dataset_registry: Arc<dyn DatasetRegistry>,
    set_watermark_use_case: Arc<dyn SetWatermarkUseCase>,
    tenancy_config: TenancyConfig,

    #[dill::component(explicit)]
    refs: Vec<odf::DatasetRefAnyPattern>,

    #[dill::component(explicit)]
    all: bool,

    #[dill::component(explicit)]
    recursive: bool,

    #[dill::component(explicit)]
    watermark: String,
}

#[async_trait::async_trait(?Send)]
impl Command for SetWatermarkCommand {
    async fn run(&self) -> Result<(), CLIError> {
        if self.refs.len() != 1 {
            return Err(CLIError::usage_error(
                "Only one dataset can be provided when setting a watermark",
            ));
        }
        if self.recursive || self.all {
            return Err(CLIError::usage_error(
                "Can't use --all or --recursive flags when setting a watermark",
            ));
        }
        if self.refs[0].is_pattern() {
            return Err(CLIError::usage_error(
                "Cannot use a pattern when setting a watermark",
            ));
        }

        let watermark = DateTime::parse_from_rfc3339(&self.watermark).map_err(|_| {
            CLIError::usage_error(format!(
                "Invalid timestamp {} should follow RFC3339 format, e.g. 2020-01-01T12:00:00Z",
                self.watermark
            ))
        })?;

        let dataset_ref = self.refs[0]
            .as_dataset_ref_any()
            .unwrap()
            .as_local_ref(|_| self.tenancy_config == TenancyConfig::SingleTenant)
            .map_err(|_| CLIError::usage_error("Expected a local dataset reference"))?;

        let dataset_handle = self
            .dataset_registry
            .resolve_dataset_handle_by_ref(&dataset_ref)
            .await
            .map_err(CLIError::critical)?;

        match self
            .set_watermark_use_case
            .execute(&dataset_handle, watermark.into())
            .await
        {
            Ok(SetWatermarkResult::UpToDate) => {
                eprintln!("{}", console::style("Watermark was up-to-date").yellow());
                Ok(())
            }
            Ok(SetWatermarkResult::Updated { new_head, .. }) => {
                eprintln!(
                    "{}",
                    console::style(format!(
                        "Committed new block {}",
                        new_head.as_multibase().short()
                    ))
                    .green()
                );
                Ok(())
            }
            Err(
                e @ (SetWatermarkError::Planning(
                    SetWatermarkPlanningError::IsRemote | SetWatermarkPlanningError::IsDerivative,
                )
                | SetWatermarkError::Access(_)),
            ) => Err(CLIError::failure(e)),
            Err(
                e @ (SetWatermarkError::Execution(SetWatermarkExecutionError::Internal(_))
                | SetWatermarkError::Planning(SetWatermarkPlanningError::Internal(_))
                | SetWatermarkError::Internal(_)),
            ) => Err(CLIError::critical(e)),
        }
    }
}
