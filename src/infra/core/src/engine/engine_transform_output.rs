// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;

use file_utils::OwnedFile;
use internal_error::ResultIntoInternal;
use kamu_core::engine::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Files an engine was asked to write for one transform
pub struct EngineTransformOutput {
    pub data_path: PathBuf,
    pub checkpoint_path: PathBuf,
}

impl EngineTransformOutput {
    /// Checks what the engine wrote against its response and takes ownership
    /// of the files. Breaches of the engine contract are
    /// [`EngineError::ContractError`].
    pub fn into_response(
        self,
        engine_response: odf::metadata::TransformResponseSuccess,
    ) -> Result<TransformResponseExt, EngineError> {
        let Self {
            data_path,
            checkpoint_path,
        } = self;

        let (new_data, output_schema) = if data_path.exists() {
            if data_path.is_symlink() || !data_path.is_file() {
                return Err(EngineError::contract_error(
                    "Engine wrote data not as a plain file",
                    Vec::new(),
                ));
            }

            // Read output schema
            let data_metadata =
                datafusion::parquet::arrow::arrow_reader::ArrowReaderMetadata::load(
                    &std::fs::File::open(&data_path).int_err()?,
                    Default::default(),
                )
                .int_err()?;
            let output_schema = data_metadata.schema().clone();

            // The offsets the engine claims become the dataset's record offsets
            let num_rows =
                u64::try_from(data_metadata.metadata().file_metadata().num_rows()).int_err()?;
            let claimed_interval = &engine_response.new_offset_interval;
            let claimed_rows = claimed_interval.as_ref().map_or(Some(0), |interval| {
                interval
                    .end
                    .checked_sub(interval.start)
                    .and_then(|span| span.checked_add(1))
            });
            if claimed_rows != Some(num_rows) {
                return Err(EngineError::contract_error(
                    &format!(
                        "Engine wrote {num_rows} records but claimed offsets {claimed_interval:?}"
                    ),
                    Vec::new(),
                ));
            }

            let new_data = OwnedFile::new(data_path);

            // Clean up the data file if it's empty - it was produced only to provide us the
            // schema
            let new_data = if engine_response.new_offset_interval.is_none() {
                None
            } else {
                Some(new_data)
            };

            (new_data, Some(output_schema))
        } else {
            if engine_response.new_offset_interval.is_some() {
                return Err(EngineError::contract_error(
                    "Engine did not write a response data file",
                    Vec::new(),
                ));
            }

            tracing::warn!(
                "Engine didn't write a data file when output slice is empty - in future this will \
                 be considered an error"
            );

            (None, None)
        };

        let new_checkpoint = if checkpoint_path.exists() {
            if checkpoint_path.is_symlink() || !checkpoint_path.is_file() {
                return Err(EngineError::contract_error(
                    "Engine wrote checkpoint not as a plain file",
                    Vec::new(),
                ));
            }
            Some(OwnedFile::new(checkpoint_path))
        } else {
            None
        };

        Ok(TransformResponseExt {
            new_offset_interval: engine_response.new_offset_interval,
            new_watermark: engine_response.new_watermark,
            output_schema,
            new_checkpoint,
            new_data,
            run_dir: None,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
