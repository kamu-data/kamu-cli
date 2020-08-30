use crate::domain::*;
use crate::infra::serde::yaml::*;

use std::path::Path;
use std::time::Duration;
use url::Url;

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
                let manifest: Manifest<DatasetSnapshot> =
                    serde_yaml::from_slice(&buffer).map_err(|e| ResourceError::serde(e))?;
                assert_eq!(manifest.kind, "DatasetSnapshot");
                Ok(manifest.content)
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
        let file = std::fs::File::open(path).map_err(|e| ResourceError::internal(e))?;
        let manifest: Manifest<DatasetSnapshot> =
            serde_yaml::from_reader(file).map_err(|e| ResourceError::serde(e))?;
        assert_eq!(manifest.kind, "DatasetSnapshot");
        Ok(manifest.content)
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
