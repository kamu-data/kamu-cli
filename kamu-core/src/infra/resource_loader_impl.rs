// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::domain::*;
use opendatafabric::serde::yaml::*;
use opendatafabric::*;

use dill::component;
use std::path::Path;
use std::time::Duration;
use url::Url;

#[component]
pub struct ResourceLoaderImpl {}

impl ResourceLoaderImpl {
    pub fn new() -> Self {
        Self {}
    }

    pub fn load_dataset_snapshot_from_http(
        &self,
        url: &Url,
    ) -> Result<DatasetSnapshot, ResourceError> {
        let mut h = curl::easy::Easy::new();
        h.url(url.as_str())?;
        h.get(true)?;
        h.connect_timeout(Duration::from_secs(30))?;

        let mut buffer = Vec::new();

        {
            let mut transfer = h.transfer();

            transfer.write_function(|data| {
                buffer.extend_from_slice(data);
                Ok(data.len())
            })?;

            transfer.perform()?;
        }

        match h.response_code()? {
            200 => {
                let snapshot = YamlDatasetSnapshotDeserializer
                    .read_manifest(&buffer)
                    .map_err(|e| ResourceError::serde(e))?;
                Ok(snapshot)
            }
            404 => Err(ResourceError::not_found(url.as_str().to_owned(), None)),
            _ => Err(ResourceError::unreachable(url.as_str().to_owned(), None)),
        }
    }
}

impl ResourceLoader for ResourceLoaderImpl {
    fn load_dataset_snapshot_from_path(
        &self,
        path: &Path,
    ) -> Result<DatasetSnapshot, ResourceError> {
        let buffer = std::fs::read(path).map_err(|e| ResourceError::internal(e))?;
        let snapshot = YamlDatasetSnapshotDeserializer
            .read_manifest(&buffer)
            .map_err(|e| ResourceError::serde(e))?;
        Ok(snapshot)
    }

    fn load_dataset_snapshot_from_url(&self, url: &Url) -> Result<DatasetSnapshot, ResourceError> {
        match url.scheme() {
            "file" => {
                self.load_dataset_snapshot_from_path(&url.to_file_path().expect("Invalid file URL"))
            }
            "http" | "https" => self.load_dataset_snapshot_from_http(url),
            _ => unimplemented!("Unsupported scheme {}", url.scheme()),
        }
    }

    fn load_dataset_snapshot_from_ref(&self, sref: &str) -> Result<DatasetSnapshot, ResourceError> {
        let path = Path::new(sref);
        if path.exists() {
            self.load_dataset_snapshot_from_path(path)
        } else if let Ok(url) = Url::parse(sref) {
            self.load_dataset_snapshot_from_url(&url)
        } else {
            self.load_dataset_snapshot_from_path(path)
        }
    }
}

impl std::convert::From<curl::Error> for ResourceError {
    fn from(e: curl::Error) -> Self {
        Self::internal(e)
    }
}
