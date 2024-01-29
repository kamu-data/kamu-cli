// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;

use opendatafabric::{
    DatasetName,
    DatasetRef,
    DatasetRefAny,
    DatasetRefPattern,
    DatasetRefRemote,
    Multihash,
    RepoName,
};
use url::Url;

pub(crate) fn value_parse_dataset_ref_pattern_local(s: &str) -> Result<DatasetRefPattern, String> {
    match DatasetRefPattern::from_str(s) {
        Ok(drp) => Ok(drp),
        Err(_) => Err(
            "Local reference should be in form: `did:odf:...`, `my.dataset.id`, or a wildcard \
             pattern `my.dataset.%`"
                .to_string(),
        ),
    }
}

pub(crate) fn value_parse_dataset_name(s: &str) -> Result<DatasetName, String> {
    match DatasetName::try_from(s) {
        Ok(v) => Ok(v),
        Err(_) => Err(
            "Dataset name can only contain alphanumerics, dashes, and dots, e.g. `my.dataset-id`"
                .to_string(),
        ),
    }
}

pub(crate) fn value_parse_dataset_ref_local(s: &str) -> Result<DatasetRef, String> {
    match DatasetRef::try_from(s) {
        Ok(v) => Ok(v),
        Err(_) => {
            Err("Local reference should be in form: `did:odf:...` or `my.dataset.id`".to_string())
        }
    }
}

pub(crate) fn value_parse_dataset_ref_remote(s: &str) -> Result<DatasetRefRemote, String> {
    match DatasetRefRemote::try_from(s) {
        Ok(v) => Ok(v),
        Err(_) => Err("Remote reference should be in form: `did:odf:...` or \
                       `repository/account/dataset-id` or `scheme://some-url`"
            .to_string()),
    }
}

pub(crate) fn value_parse_dataset_ref_any(s: &str) -> Result<DatasetRefAny, String> {
    match DatasetRefAny::try_from(s) {
        Ok(v) => Ok(v),
        Err(_) => Err("Dataset reference should be in form: `my.dataset.id` or \
                       `repository/account/dataset-id` or `did:odf:...` or `scheme://some-url`"
            .to_string()),
    }
}

pub(crate) fn value_parse_repo_name(s: &str) -> Result<RepoName, String> {
    match RepoName::try_from(s) {
        Ok(v) => Ok(v),
        Err(_) => Err("RepositoryID can only contain alphanumerics, dashes, and dots".to_string()),
    }
}

pub(crate) fn value_parse_multihash(s: &str) -> Result<Multihash, String> {
    match Multihash::from_multibase(s) {
        Ok(v) => Ok(v),
        Err(_) => Err("Block hash must be a valid multihash string".to_string()),
    }
}

pub(crate) fn validate_log_filter(s: &str) -> Result<String, String> {
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

pub(crate) fn value_parse_url(url_str: &str) -> Result<Url, String> {
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
