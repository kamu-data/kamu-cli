// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Debug;
use std::str::FromStr;

use kamu::domain::ExportFormat;
use strum::IntoEnumIterator;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn dataset_ref_pattern(s: &str) -> Result<odf::DatasetRefPattern, String> {
    match odf::DatasetRefPattern::from_str(s) {
        Ok(dataset_ref_pattern) => Ok(dataset_ref_pattern),
        Err(_) => Err(
            "Local reference should be in form: `did:odf:...`, `my.dataset.id`, or a wildcard \
             pattern `my.dataset.%`"
                .to_string(),
        ),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn dataset_ref_pattern_any(s: &str) -> Result<odf::DatasetRefAnyPattern, String> {
    match odf::DatasetRefAnyPattern::from_str(s) {
        Ok(dataset_ref_pattern) => Ok(dataset_ref_pattern),
        Err(_) => Err("Dataset reference should be in form: `my.dataset.id` or \
                       `repository/account/dataset-id` or `did:odf:...` or `scheme://some-url` \
                       or a wildcard pattern: `my.dataset.%` or \
                       `repository/account/remote.dataset.%`"
            .to_string()),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn dataset_name(s: &str) -> Result<odf::DatasetName, String> {
    match odf::DatasetName::try_from(s) {
        Ok(v) => Ok(v),
        Err(_) => Err(
            "Dataset name can only contain alphanumerics, dashes, and dots, e.g. `my.dataset-id`"
                .to_string(),
        ),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn dataset_ref(s: &str) -> Result<odf::DatasetRef, String> {
    match odf::DatasetRef::try_from(s) {
        Ok(v) => Ok(v),
        Err(_) => {
            Err("Local reference should be in form: `did:odf:...` or `my.dataset.id`".to_string())
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn dataset_ref_remote(s: &str) -> Result<odf::DatasetRefRemote, String> {
    match odf::DatasetRefRemote::try_from(s) {
        Ok(v) => Ok(v),
        Err(_) => Err("Remote reference should be in form: `did:odf:...` or \
                       `repository/account/dataset-id` or `scheme://some-url`"
            .to_string()),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn dataset_push_target(s: &str) -> Result<odf::DatasetPushTarget, String> {
    match odf::DatasetPushTarget::from_str(s) {
        Ok(push_dataset_ref) => Ok(push_dataset_ref),
        Err(_) => Err(
            "Remote reference should be in form: `repository/account/dataset-name` or \
             `scheme://some-url` or repository reference can only contain alphanumerics, dashes, \
             and dots"
                .to_string(),
        ),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn repo_name(s: &str) -> Result<odf::RepoName, String> {
    match odf::RepoName::try_from(s) {
        Ok(v) => Ok(v),
        Err(_) => Err("RepositoryID can only contain alphanumerics, dashes, and dots".to_string()),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn multihash(s: &str) -> Result<odf::Multihash, String> {
    match odf::Multihash::from_multibase(s) {
        Ok(v) => Ok(v),
        Err(_) => Err("Block hash must be a valid multihash string".to_string()),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn log_filter(s: &str) -> Result<String, String> {
    let items: Vec<_> = s.split(',').collect();
    for item in items {
        match item {
            "source" | "watermark" | "data" => Ok(()),
            _ => Err("Filter should be a comma-separated list of values like: \
                      source,data,watermark"
                .to_string()),
        }?;
    }
    Ok(s.to_string())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn export_format(format: &str) -> Result<ExportFormat, String> {
    match format {
        "parquet" => Ok(ExportFormat::Parquet),
        "ndjson" => Ok(ExportFormat::NdJson),
        "csv" => Ok(ExportFormat::Csv),
        _ => {
            let supported_formats = ExportFormat::iter()
                .map(|f| format!("'{f}'"))
                .collect::<Vec<_>>()
                .join(", ");
            Err(format!("Supported formats: {supported_formats}"))
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy)]
pub struct DateTimeRfc3339(chrono::DateTime<chrono::Utc>);

impl From<DateTimeRfc3339> for chrono::DateTime<chrono::Utc> {
    fn from(value: DateTimeRfc3339) -> Self {
        value.0
    }
}

impl std::str::FromStr for DateTimeRfc3339 {
    type Err = chrono::ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let dt = chrono::DateTime::parse_from_rfc3339(s)?;
        Ok(Self(dt.into()))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct UrlHttps(url::Url);

impl From<UrlHttps> for url::Url {
    fn from(value: UrlHttps) -> Self {
        value.0
    }
}

impl std::str::FromStr for UrlHttps {
    type Err = url::ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match Url::parse(s) {
            Ok(url) => Ok(Self(url)),
            Err(url::ParseError::RelativeUrlWithoutBase) => {
                // try attaching a default schema
                Url::parse(&format!("https://{s}")).map(Self)
            }
            Err(e) => Err(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum DatasetVisibility {
    Private,
    Public,
}

impl From<DatasetVisibility> for odf::DatasetVisibility {
    fn from(value: DatasetVisibility) -> Self {
        match value {
            DatasetVisibility::Private => odf::DatasetVisibility::Private,
            DatasetVisibility::Public => odf::DatasetVisibility::Public,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
