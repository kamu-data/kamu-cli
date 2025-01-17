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
use opendatafabric as odf;

use super::{ReadError, Reader, UnsupportedMediaTypeError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DataFormatRegistry: Send + Sync {
    fn list_formats(&self) -> Vec<DataFormatDesc>;

    fn format_by_file_extension(&self, ext: &str) -> Option<DataFormatDesc>;

    fn format_of(&self, conf: &odf::ReadStep) -> DataFormatDesc;

    // TODO: Avoid `async` poisoning by datafusion
    // TODO: Avoid passing `temp_path` here
    async fn get_reader(
        &self,
        ctx: SessionContext,
        conf: odf::ReadStep,
        temp_path: PathBuf,
    ) -> Result<Arc<dyn Reader>, ReadError>;

    /// Attempts to provide the most compatible reader configuration based on
    /// base configuration of the source and the provided media type of the
    /// actual data
    fn get_compatible_read_config(
        &self,
        base_conf: odf::ReadStep,
        actual_media_type: &MediaType,
    ) -> Result<odf::ReadStep, UnsupportedMediaTypeError>;

    fn get_best_effort_config(
        &self,
        schema: Option<Vec<String>>,
        media_type: &MediaType,
    ) -> Result<odf::ReadStep, UnsupportedMediaTypeError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy)]
pub struct DataFormatDesc {
    pub short_name: &'static str,
    pub media_type: MediaTypeRef<'static>,
    pub file_extensions: &'static [&'static str],
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Consider a crate
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct MediaType(pub String);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MediaTypeRef<'a>(pub &'a str);

impl MediaType {
    // We use IANA types where we can, otherwise see comments:
    // https://www.iana.org/assignments/media-types/media-types.xhtml
    pub const CSV: MediaTypeRef<'static> = MediaTypeRef("text/csv");
    pub const JSON: MediaTypeRef<'static> = MediaTypeRef("application/json");
    /// Unofficial but used by several software projects
    pub const NDJSON: MediaTypeRef<'static> = MediaTypeRef("application/x-ndjson");
    pub const GEOJSON: MediaTypeRef<'static> = MediaTypeRef("application/geo+json");
    /// No standard found
    pub const NDGEOJSON: MediaTypeRef<'static> = MediaTypeRef("application/x-ndgeojson");
    /// See: <https://issues.apache.org/jira/browse/PARQUET-1889>
    pub const PARQUET: MediaTypeRef<'static> = MediaTypeRef("application/vnd.apache.parquet");
    /// Multiple in use
    /// See: <https://www.iana.org/assignments/media-types/application/vnd.shp>
    /// See: <https://en.wikipedia.org/wiki/Shapefile>
    pub const ESRI_SHAPEFILE: MediaTypeRef<'static> = MediaTypeRef("application/vnd.shp");
}

impl MediaTypeRef<'_> {
    pub fn to_owned(&self) -> MediaType {
        MediaType(self.0.to_string())
    }
}

impl std::fmt::Display for MediaType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::fmt::Display for MediaTypeRef<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<'a> std::cmp::PartialEq<MediaTypeRef<'a>> for MediaType {
    fn eq(&self, other: &MediaTypeRef<'a>) -> bool {
        self.0 == other.0
    }
}

impl std::cmp::PartialEq<MediaType> for MediaTypeRef<'_> {
    fn eq(&self, other: &MediaType) -> bool {
        self.0 == other.0
    }
}
