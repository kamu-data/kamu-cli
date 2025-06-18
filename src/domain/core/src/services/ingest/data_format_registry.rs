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

use datafusion::prelude::SessionContext;
use file_utils::{MediaType, MediaTypeRef};

use super::{ReadError, Reader, UnsupportedMediaTypeError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DataFormatRegistry: Send + Sync {
    fn list_formats(&self) -> Vec<DataFormatDesc>;

    fn format_by_file_extension(&self, ext: &str) -> Option<DataFormatDesc>;

    fn format_of(&self, conf: &odf::metadata::ReadStep) -> DataFormatDesc;

    // TODO: Avoid `async` poisoning by datafusion
    // TODO: Avoid passing `temp_path` here
    async fn get_reader(
        &self,
        ctx: SessionContext,
        conf: odf::metadata::ReadStep,
        temp_path: PathBuf,
    ) -> Result<Arc<dyn Reader>, ReadError>;

    /// Attempts to provide the most compatible reader configuration based on
    /// base configuration of the source and the provided media type of the
    /// actual data
    fn get_compatible_read_config(
        &self,
        base_conf: odf::metadata::ReadStep,
        actual_media_type: &MediaType,
    ) -> Result<odf::metadata::ReadStep, UnsupportedMediaTypeError>;

    fn get_best_effort_config(
        &self,
        schema: Option<Vec<String>>,
        media_type: &MediaType,
    ) -> Result<odf::metadata::ReadStep, UnsupportedMediaTypeError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy)]
pub struct DataFormatDesc {
    pub short_name: &'static str,
    pub media_type: MediaTypeRef<'static>,
    pub file_extensions: &'static [&'static str],
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
