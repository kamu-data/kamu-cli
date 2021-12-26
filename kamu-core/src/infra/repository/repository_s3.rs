// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::domain::*;
use opendatafabric::serde::flatbuffers::*;
use opendatafabric::*;

use bytes::BytesMut;
use futures::TryStreamExt;
use rusoto_core::{Region, RusotoError};
use rusoto_s3::*;
use std::cell::RefCell;
use std::convert::TryFrom;
use std::path::{Path, PathBuf};
use tokio::io::AsyncReadExt;
use tokio_util::codec::{BytesCodec, FramedRead};
use tracing::{info, info_span};

pub struct RepositoryS3 {
    s3: S3Client,
    bucket: String,
    runtime: RefCell<tokio::runtime::Runtime>,
}

impl RepositoryS3 {
    pub fn new(endpoint: Option<String>, bucket: String) -> Self {
        let region = match endpoint {
            None => Region::default(),
            Some(endpoint) => Region::Custom {
                name: "custom".to_owned(),
                endpoint: endpoint,
            },
        };
        Self {
            s3: S3Client::new(region),
            bucket: bucket,
            runtime: RefCell::new(tokio::runtime::Runtime::new().unwrap()),
        }
    }

    async fn read_ref_async(
        &self,
        dataset_ref: &RemoteDatasetName,
    ) -> Result<Option<Multihash>, RepositoryError> {
        if let Some(body) = self
            .read_object_buf(format!("{}/meta/refs/head", dataset_ref.dataset()))
            .await?
        {
            let s = std::str::from_utf8(&body).expect("Non-utf8 string in ref");
            Ok(Some(
                Multihash::from_multibase_str(s.trim())
                    .map_err(|e| RepositoryError::protocol(e.into()))?,
            ))
        } else {
            Ok(None)
        }
    }

    // TODO: Locking
    async fn write_async(
        &self,
        dataset_ref: &RemoteDatasetName,
        expected_head: &Option<Multihash>,
        new_head: &Multihash,
        blocks: &mut dyn Iterator<Item = (Multihash, Vec<u8>)>,
        data_files: &mut dyn Iterator<Item = &Path>,
        checkpoint_dir: &Path,
    ) -> Result<(), RepositoryError> {
        if self.read_ref_async(dataset_ref).await? != *expected_head {
            return Err(RepositoryError::UpdatedConcurrently);
        }

        for data_file in data_files {
            self.upload_object_file(
                format!(
                    "{}/data/{}",
                    dataset_ref.dataset(),
                    data_file.file_name().unwrap().to_str().unwrap()
                ),
                data_file,
                IfExists::Skip, // TODO: Should raise error
            )
            .await?;
        }

        for (hash, data) in blocks {
            self.upload_object_buf(
                format!("{}/meta/blocks/{}", dataset_ref.dataset(), hash.to_string()),
                data,
                IfExists::Fail,
            )
            .await?;
        }

        // TODO: This is really bad but we need to
        // establish proper checkpoint naming and rotation first
        self.delete_objects(format!("{}/checkpoint/", dataset_ref.dataset()))
            .await?;

        if checkpoint_dir.exists() {
            self.upload_objects_dir(
                checkpoint_dir,
                format!("{}/checkpoint/", dataset_ref.dataset()),
                IfExists::Overwrite,
            )
            .await?;
        }

        let new_head_hash = new_head.to_string();
        self.upload_object_buf(
            format!("{}/meta/refs/head", dataset_ref.dataset()),
            Vec::from(new_head_hash.as_bytes()),
            IfExists::Overwrite,
        )
        .await?;

        Ok(())
    }

    async fn read_async(
        &self,
        dataset_ref: &RemoteDatasetName,
        expected_head: &Multihash,
        last_seen_block: &Option<Multihash>,
        tmp_dir: &Path,
    ) -> Result<RepositoryReadResult, RepositoryError> {
        let mut result = RepositoryReadResult {
            blocks: Vec::new(),
            data_files: Vec::new(),
            checkpoint_dir: tmp_dir.join("checkpoint"),
        };

        if self.read_ref_async(dataset_ref).await? != Some(expected_head.clone()) {
            return Err(RepositoryError::UpdatedConcurrently);
        }

        // Sync blocks
        let mut current_hash = Some(expected_head.clone());
        while current_hash.is_some() && current_hash != *last_seen_block {
            let block_hash = current_hash.unwrap();

            let buf = self
                .read_object_buf(format!(
                    "{}/meta/blocks/{}",
                    dataset_ref.dataset(),
                    block_hash
                ))
                .await?
                .ok_or_else(|| {
                    RepositoryError::corrupted(format!("Block {} is missing", block_hash))
                })?;

            // TODO: Avoid double-parsing
            let block = FlatbuffersMetadataBlockDeserializer
                .read_manifest(&buf)
                .map_err(|e| {
                    RepositoryError::corrupted_from(
                        "Cannot deserialize metadata block".to_owned(),
                        e,
                    )
                })?;

            result.blocks.push((block_hash, buf));
            current_hash = block.prev_block_hash;
        }

        if current_hash != *last_seen_block {
            return Err(RepositoryError::Diverged {
                remote_head: expected_head.clone(),
                local_head: last_seen_block.clone().unwrap(),
            });
        }

        // Sync data
        result.data_files = self
            .read_objects_to_dir(
                format!("{}/data/", dataset_ref.dataset()),
                &tmp_dir.join("data"),
            )
            .await?;

        // Sync checkpoints
        self.read_objects_to_dir(
            format!("{}/checkpoint/", dataset_ref.dataset()),
            &result.checkpoint_dir,
        )
        .await?;

        Ok(result)
    }

    async fn read_object_buf(&self, key: String) -> Result<Option<Vec<u8>>, RepositoryError> {
        let span = info_span!(
            "Reading object",
            key = key.as_str(),
            bucket = self.bucket.as_str(),
        );
        let _span_guard = span.enter();

        let resp = match self
            .s3
            .get_object(GetObjectRequest {
                bucket: self.bucket.clone(),
                key: key,
                ..GetObjectRequest::default()
            })
            .await
        {
            Ok(resp) => resp,
            Err(RusotoError::Service(GetObjectError::NoSuchKey(_))) => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        let mut stream = resp.body.expect("Response with no body").into_async_read();
        let mut body = Vec::new();
        stream.read_to_end(&mut body).await?;

        Ok(Some(body))
    }

    async fn read_object_to_file(&self, key: String, path: &Path) -> Result<(), RepositoryError> {
        let span = info_span!(
            "Reading object to file",
            key = key.as_str(),
            bucket = self.bucket.as_str(),
            file = ?path,
        );
        let _span_guard = span.enter();

        let mut file = tokio::fs::File::create(path).await?;

        let mut resp = self
            .s3
            .get_object(GetObjectRequest {
                bucket: self.bucket.clone(),
                key: key,
                ..GetObjectRequest::default()
            })
            .await?;

        let body = resp.body.take().expect("Response with no body");
        let mut body = body.into_async_read();

        tokio::io::copy(&mut body, &mut file).await?;
        Ok(())
    }

    async fn read_objects_to_dir(
        &self,
        key_prefix: String,
        out_dir: &Path,
    ) -> Result<Vec<PathBuf>, RepositoryError> {
        let span = info_span!(
            "Reading objects into directory",
            key_prefix = key_prefix.as_str(),
            bucket = self.bucket.as_str(),
            directory = ?out_dir,
        );
        let _span_guard = span.enter();

        let mut files = Vec::new();
        std::fs::create_dir_all(out_dir)?;

        let list_objects = self
            .s3
            .list_objects_v2(ListObjectsV2Request {
                bucket: self.bucket.clone(),
                prefix: Some(key_prefix.clone()),
                ..ListObjectsV2Request::default()
            })
            .await?;

        // TODO: Support iteration
        assert!(
            !list_objects.is_truncated.unwrap_or(false),
            "Cannot handle truncated response"
        );

        if list_objects.key_count.unwrap_or_default() > 0 {
            for obj in list_objects.contents.expect("Response with no body") {
                let key = obj.key.unwrap();
                let key_suffix = key.strip_prefix(&key_prefix).unwrap();
                let path = out_dir.join(key_suffix);

                std::fs::create_dir_all(path.parent().unwrap())?;
                self.read_object_to_file(key, &path).await?;
                files.push(path);
            }
        }

        Ok(files)
    }

    async fn delete_objects(&self, key_prefix: String) -> Result<(), RepositoryError> {
        let span = info_span!(
            "Deleting objects",
            key_prefix = key_prefix.as_str(),
            bucket = self.bucket.as_str(),
        );
        let _span_guard = span.enter();

        let list_objects = self
            .s3
            .list_objects_v2(ListObjectsV2Request {
                bucket: self.bucket.clone(),
                prefix: Some(key_prefix),
                ..ListObjectsV2Request::default()
            })
            .await?;

        let object_ids: Vec<_> = list_objects
            .contents
            .unwrap_or_default()
            .into_iter()
            .map(|obj| ObjectIdentifier {
                key: obj.key.unwrap(),
                ..ObjectIdentifier::default()
            })
            .collect();

        if object_ids.len() != 0 {
            self.s3
                .delete_objects(DeleteObjectsRequest {
                    bucket: self.bucket.clone(),
                    delete: Delete {
                        objects: object_ids,
                        ..Delete::default()
                    },
                    ..DeleteObjectsRequest::default()
                })
                .await?;
        }

        Ok(())
    }

    async fn upload_object_file(
        &self,
        key: String,
        file_path: &Path,
        if_exists: IfExists,
    ) -> Result<(), RepositoryError> {
        let span = info_span!(
            "Uploading object from file",
            key = key.as_str(),
            bucket = self.bucket.as_str(),
            file = ?file_path,
        );
        let _span_guard = span.enter();

        if if_exists != IfExists::Overwrite {
            if self.check_object_exists(key.clone()).await? {
                info!(
                    key = key.as_str(),
                    bucket = self.bucket.as_str(),
                    "Object already exists"
                );

                return if if_exists == IfExists::Skip {
                    Ok(())
                } else {
                    Err(RepositoryError::corrupted(format!(
                        "Key {} already exists",
                        key
                    )))
                };
            }
        }

        let file = tokio::fs::File::open(file_path).await?;
        let meta = file.metadata().await?;

        let stream = FramedRead::new(file, BytesCodec::new()).map_ok(BytesMut::freeze);

        self.s3
            .put_object(PutObjectRequest {
                bucket: self.bucket.clone(),
                key: key,
                body: Some(StreamingBody::new(stream)),
                content_length: Some(meta.len() as i64),
                ..PutObjectRequest::default()
            })
            .await?;

        Ok(())
    }

    async fn upload_object_buf(
        &self,
        key: String,
        buf: Vec<u8>,
        if_exists: IfExists,
    ) -> Result<(), RepositoryError> {
        let span = info_span!(
            "Uploading object data",
            key = key.as_str(),
            bucket = self.bucket.as_str(),
        );
        let _span_guard = span.enter();

        if if_exists != IfExists::Overwrite {
            if self.check_object_exists(key.clone()).await? {
                info!(
                    key = key.as_str(),
                    bucket = self.bucket.as_str(),
                    "Object already exists"
                );

                return if if_exists == IfExists::Skip {
                    Ok(())
                } else {
                    Err(RepositoryError::corrupted(format!(
                        "Key {} already exists",
                        key
                    )))
                };
            }
        }

        let len = buf.len();
        self.s3
            .put_object(PutObjectRequest {
                bucket: self.bucket.clone(),
                key: key,
                body: Some(buf.into()),
                content_length: Some(len as i64),
                ..PutObjectRequest::default()
            })
            .await?;

        Ok(())
    }

    async fn upload_objects_dir(
        &self,
        dir: &Path,
        key_prefix: String,
        if_exists: IfExists,
    ) -> Result<(), RepositoryError> {
        let span = info_span!(
            "Uploading objects from directory",
            key_prefix = key_prefix.as_str(),
            bucket = self.bucket.as_str(),
            dir = ?dir,
        );
        let _span_guard = span.enter();

        for entry in walkdir::WalkDir::new(dir) {
            let entry = entry.unwrap();
            let entry_path = entry.path();
            if entry_path.is_file() {
                let path_prefix = entry_path.strip_prefix(dir).unwrap();
                let prefixed_key = format!("{}{}", &key_prefix, path_prefix.to_str().unwrap());
                self.upload_object_file(prefixed_key, &entry_path, if_exists)
                    .await?;
            }
        }
        Ok(())
    }

    async fn check_object_exists(&self, key: String) -> Result<bool, RepositoryError> {
        match self
            .s3
            .head_object(HeadObjectRequest {
                bucket: self.bucket.clone(),
                key: key,
                ..HeadObjectRequest::default()
            })
            .await
        {
            Ok(_) => {
                return Ok(true);
            }
            // TODO: This error type doesn't work
            // See: https://github.com/rusoto/rusoto/issues/716
            Err(RusotoError::Service(HeadObjectError::NoSuchKey(_))) => Ok(false),
            Err(_) => Ok(false), // return Err(e.into()),
        }
    }

    async fn delete_async(&self, dataset_ref: &RemoteDatasetName) -> Result<(), RepositoryError> {
        self.delete_objects(format!("{}/", dataset_ref.dataset()))
            .await?;
        Ok(())
    }

    async fn search_async(
        &self,
        query: Option<&str>,
    ) -> Result<RepositorySearchResult, RepositoryError> {
        let query = query.unwrap_or_default();

        let list_objects_resp = self
            .s3
            .list_objects_v2(ListObjectsV2Request {
                bucket: self.bucket.clone(),
                delimiter: Some("/".to_owned()),
                ..ListObjectsV2Request::default()
            })
            .await?;

        // TODO: Support iteration
        assert!(
            !list_objects_resp.is_truncated.unwrap_or(false),
            "Cannot handle truncated response"
        );

        // TODO: Find a way to avoid this
        let repo_name = RepositoryName::try_from("undefined").unwrap();

        let mut datasets = Vec::new();

        for prefix in list_objects_resp.common_prefixes.unwrap_or_default() {
            let mut prefix = prefix.prefix.unwrap();
            while prefix.ends_with('/') {
                prefix.pop();
            }

            let name = DatasetName::try_from(prefix).map_err(|e| {
                RepositoryError::corrupted_from(
                    format!(
                        "Repository contains directory {} which is not a valid DatasetName",
                        e.value
                    ),
                    e,
                )
            })?;

            if query.is_empty() || name.contains(query) {
                datasets.push(RemoteDatasetName::new(&repo_name, None, &name));
            }
        }

        Ok(RepositorySearchResult { datasets })
    }
}

impl RepositoryClient for RepositoryS3 {
    fn read_ref(
        &self,
        dataset_ref: &RemoteDatasetName,
    ) -> Result<Option<Multihash>, RepositoryError> {
        self.runtime
            .borrow_mut()
            .block_on(self.read_ref_async(dataset_ref))
    }

    // TODO: Locking
    fn write(
        &mut self,
        dataset_ref: &RemoteDatasetName,
        expected_head: &Option<Multihash>,
        new_head: &Multihash,
        blocks: &mut dyn Iterator<Item = (Multihash, Vec<u8>)>,
        data_files: &mut dyn Iterator<Item = &Path>,
        checkpoint_dir: &Path,
    ) -> Result<(), RepositoryError> {
        self.runtime.borrow_mut().block_on(self.write_async(
            dataset_ref,
            expected_head,
            new_head,
            blocks,
            data_files,
            checkpoint_dir,
        ))
    }

    fn read(
        &self,
        dataset_ref: &RemoteDatasetName,
        expected_head: &Multihash,
        last_seen_block: &Option<Multihash>,
        tmp_dir: &Path,
    ) -> Result<RepositoryReadResult, RepositoryError> {
        self.runtime.borrow_mut().block_on(self.read_async(
            dataset_ref,
            expected_head,
            last_seen_block,
            tmp_dir,
        ))
    }

    fn delete(&self, dataset_ref: &RemoteDatasetName) -> Result<(), RepositoryError> {
        self.runtime
            .borrow_mut()
            .block_on(self.delete_async(dataset_ref))
    }

    fn search(&self, query: Option<&str>) -> Result<RepositorySearchResult, RepositoryError> {
        self.runtime.borrow_mut().block_on(self.search_async(query))
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum IfExists {
    Skip,
    Overwrite,
    Fail,
}

impl<E: 'static + std::error::Error + Send + Sync> From<RusotoError<E>> for RepositoryError {
    fn from(e: RusotoError<E>) -> Self {
        match e {
            RusotoError::Credentials(_) => RepositoryError::credentials(Box::new(e)),
            _ => RepositoryError::protocol(Box::new(e)),
        }
    }
}
