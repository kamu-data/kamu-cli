// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use opendatafabric::{
    DatasetName,
    DatasetNamePattern,
    DatasetRef,
    DatasetRefAny,
    DatasetRefPattern,
    DatasetRefRemote,
    Multihash,
    RepoName,
};
use thiserror::Error;
use url::Url;

#[derive(Clone)]
pub struct DatasetPatternValidationRes {
    pub pattern: DatasetNamePattern,
    pub is_dataset_ref: bool,
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum ValidationDatasetRefError {
    #[error(transparent)]
    Failed(#[from] ValidationError),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

#[derive(Error, Debug)]
#[error("{message}")]
pub struct ValidationError {
    pub message: String,
}

///////////////////////////////////////////////////////////////////////////////

pub fn value_parse_dataset_ref_pattern_local(
    s: &str,
) -> Result<DatasetRefPattern, ValidationDatasetRefError> {
    let dataset_pattern = match DatasetNamePattern::try_from(s) {
        Ok(dp) => dp,
        Err(_) => {
            return Err(ValidationDatasetRefError::Failed(ValidationError {
                message: "Local reference pattern be in `my.dataset.%`".to_string(),
            }));
        }
    };

    let dataset_ref_patter = DatasetRefPattern {
        pattern: dataset_pattern,
        ..Default::default()
    };
    if !dataset_ref_patter.has_wildcards() {
        match DatasetRef::try_from(s) {
            Ok(_dsr) => _dsr,
            Err(_) => {
                return Err(ValidationDatasetRefError::Failed(ValidationError {
                    message: "Local reference should be in form: `did:odf:...` or `my.dataset.id`"
                        .to_string(),
                }))
            }
        };
    }
    Ok(dataset_ref_patter)
}

pub fn value_parse_dataset_name(s: &str) -> Result<DatasetName, String> {
    match DatasetName::try_from(s) {
        Ok(v) => Ok(v),
        Err(_) => Err(
            "Dataset name can only contain alphanumerics, dashes, and dots, e.g. `my.dataset-id`"
                .to_string(),
        ),
    }
}

pub fn value_parse_dataset_ref_local(s: &str) -> Result<DatasetRef, String> {
    match DatasetRef::try_from(s) {
        Ok(v) => Ok(v),
        Err(_) => {
            Err("Local reference should be in form: `did:odf:...` or `my.dataset.id`".to_string())
        }
    }
}

pub fn value_parse_dataset_ref_remote(s: &str) -> Result<DatasetRefRemote, String> {
    match DatasetRefRemote::try_from(s) {
        Ok(v) => Ok(v),
        Err(_) => Err("Remote reference should be in form: `did:odf:...` or \
                       `repository/account/dataset-id` or `scheme://some-url`"
            .to_string()),
    }
}

pub fn value_parse_dataset_ref_any(s: &str) -> Result<DatasetRefAny, String> {
    match DatasetRefAny::try_from(s) {
        Ok(v) => Ok(v),
        Err(_) => Err("Dataset reference should be in form: `my.dataset.id` or \
                       `repository/account/dataset-id` or `did:odf:...` or `scheme://some-url`"
            .to_string()),
    }
}

pub fn value_parse_repo_name(s: &str) -> Result<RepoName, String> {
    match RepoName::try_from(s) {
        Ok(v) => Ok(v),
        Err(_) => Err("RepositoryID can only contain alphanumerics, dashes, and dots".to_string()),
    }
}

pub fn value_parse_multihash(s: &str) -> Result<Multihash, String> {
    match Multihash::from_multibase(s) {
        Ok(v) => Ok(v),
        Err(_) => Err("Block hash must be a valid multihash string".to_string()),
    }
}

pub fn validate_log_filter(s: &str) -> Result<String, String> {
    let items: Vec<_> = s.split(',').collect();
    for item in items {
        match item {
            "source" => Ok(()),
            "watermark" => Ok(()),
            "data" => Ok(()),
            _ => Err("Filter should be a comma-separated list of values like: \
                      source,data,watermark"
                .to_string()),
        }?;
    }
    Ok(s.to_string())
}

pub fn value_parse_url(url_str: &str) -> Result<Url, String> {
    let parse_result = Url::parse(url_str);
    match parse_result {
        Ok(url) => Ok(url),
        Err(e) => {
            // try attaching a default schema
            if let url::ParseError::RelativeUrlWithoutBase = e {
                let url_with_default_schema = format!("https://{url_str}");
                let url = Url::parse(&url_with_default_schema).map_err(|e| e.to_string())?;
                Ok(url)
            } else {
                Err(e.to_string())
            }
        }
    }
}
