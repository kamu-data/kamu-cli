// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

impl From<&str> for MediaType {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<String> for MediaType {
    fn from(value: String) -> Self {
        Self(value)
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

impl<'a> PartialEq<MediaTypeRef<'a>> for MediaType {
    fn eq(&self, other: &MediaTypeRef<'a>) -> bool {
        self.0 == other.0
    }
}

impl PartialEq<MediaType> for MediaTypeRef<'_> {
    fn eq(&self, other: &MediaType) -> bool {
        self.0 == other.0
    }
}
