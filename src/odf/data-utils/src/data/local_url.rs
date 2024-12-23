// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;

use thiserror::Error;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Clone, PartialEq, Eq, Debug)]
#[error("Specified url is not a local path: {url}")]
pub struct NonLocalPathError {
    pub url: Url,
}

pub fn into_local_path(url: Url) -> Result<PathBuf, NonLocalPathError> {
    url.to_file_path().map_err(|_| NonLocalPathError { url })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
