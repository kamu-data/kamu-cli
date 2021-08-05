use crate::domain::*;
use opendatafabric::serde::yaml::*;
use opendatafabric::{DatasetRef, Sha3_256};

use bytes::BytesMut;
use futures::TryStreamExt;
use rusoto_core::{Region, RusotoError};
use rusoto_s3::*;
use slog::{info, Logger};
use std::cell::RefCell;
use std::path::{Path, PathBuf};
use tokio::io::AsyncReadExt;
use tokio_util::codec::{BytesCodec, FramedRead};

pub struct RemoteS3 {
    s3: S3Client,
    bucket: String,
    runtime: RefCell<tokio::runtime::Runtime>,
    logger: Logger,
}

impl RemoteS3 {
    pub fn new(endpoint: Option<String>, bucket: String, logger: Logger) -> Self {
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
            logger: logger,
        }
    }

    async fn read_ref_async(
        &self,
        dataset_ref: &DatasetRef,
    ) -> Result<Option<Sha3_256>, RemoteError> {
        if let Some(body) = self
            .read_object_buf(format!("{}/meta/refs/head", dataset_ref.local_id()))
            .await?
        {
            Ok(Some(
                Sha3_256::from_str(std::str::from_utf8(&body).unwrap())
                    .map_err(|e| RemoteError::protocol(e.into()))?,
            ))
        } else {
            Ok(None)
        }
    }

    // TODO: Locking
    async fn write_async(
        &self,
        dataset_ref: &DatasetRef,
        expected_head: Option<Sha3_256>,
        new_head: Sha3_256,
        blocks: &mut dyn Iterator<Item = (Sha3_256, Vec<u8>)>,
        data_files: &mut dyn Iterator<Item = &Path>,
        checkpoint_dir: &Path,
    ) -> Result<(), RemoteError> {
        if self.read_ref_async(dataset_ref).await? != expected_head {
            return Err(RemoteError::UpdatedConcurrently);
        }

        for data_file in data_files {
            self.upload_object_file(
                format!(
                    "{}/data/{}",
                    dataset_ref.local_id(),
                    data_file.file_name().unwrap().to_str().unwrap()
                ),
                data_file,
                IfExists::Skip, // TODO: Should raise error
            )
            .await?;
        }

        for (hash, data) in blocks {
            self.upload_object_buf(
                format!(
                    "{}/meta/blocks/{}",
                    dataset_ref.local_id(),
                    hash.to_string()
                ),
                data,
                IfExists::Fail,
            )
            .await?;
        }

        // TODO: This is really bad but we need to
        // establish proper checkpoint naming and rotation first
        self.delete_objects(format!("{}/checkpoint/", dataset_ref.local_id()))
            .await?;

        if checkpoint_dir.exists() {
            self.upload_objects_dir(
                checkpoint_dir,
                format!("{}/checkpoint/", dataset_ref.local_id()),
                IfExists::Overwrite,
            )
            .await?;
        }

        let new_head_hash = new_head.to_string();
        self.upload_object_buf(
            format!("{}/meta/refs/head", dataset_ref.local_id()),
            Vec::from(new_head_hash.as_bytes()),
            IfExists::Overwrite,
        )
        .await?;

        Ok(())
    }

    async fn read_async(
        &self,
        dataset_ref: &DatasetRef,
        expected_head: Sha3_256,
        last_seen_block: Option<Sha3_256>,
        tmp_dir: &Path,
    ) -> Result<RemoteReadResult, RemoteError> {
        let mut result = RemoteReadResult {
            blocks: Vec::new(),
            data_files: Vec::new(),
            checkpoint_dir: tmp_dir.join("checkpoint"),
        };

        if self.read_ref_async(dataset_ref).await? != Some(expected_head) {
            return Err(RemoteError::UpdatedConcurrently);
        }

        // Sync blocks
        let mut current_hash = Some(expected_head);
        while current_hash.is_some() && current_hash != last_seen_block {
            let buf = self
                .read_object_buf(format!(
                    "{}/meta/blocks/{}",
                    dataset_ref.local_id(),
                    current_hash.unwrap()
                ))
                .await?
                .ok_or_else(|| {
                    RemoteError::corrupted(
                        format!("Block {} is missing", current_hash.unwrap()),
                        None,
                    )
                })?;

            // TODO: Avoid double-parsing
            let block = YamlMetadataBlockDeserializer
                .read_manifest(&buf)
                .map_err(|e| {
                    RemoteError::corrupted(
                        "Cannot deserialize metadata block".to_owned(),
                        Some(e.into()),
                    )
                })?;

            current_hash = block.prev_block_hash;
            result.blocks.push(buf);
        }

        if current_hash != last_seen_block {
            return Err(RemoteError::Diverged {
                remote_head: expected_head,
                local_head: last_seen_block.unwrap(),
            });
        }

        // Sync data
        result.data_files = self
            .read_objects_to_dir(
                format!("{}/data/", dataset_ref.local_id()),
                &tmp_dir.join("data"),
            )
            .await?;

        // Sync checkpoints
        self.read_objects_to_dir(
            format!("{}/checkpoint/", dataset_ref.local_id()),
            &result.checkpoint_dir,
        )
        .await?;

        Ok(result)
    }

    async fn read_object_buf(&self, key: String) -> Result<Option<Vec<u8>>, RemoteError> {
        info!(
            self.logger,
            "Reading object";
            "key" => &key,
            "bucket" => &self.bucket,
        );

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

    async fn read_object_to_file(&self, key: String, path: &Path) -> Result<(), RemoteError> {
        info!(
            self.logger,
            "Reading object to file";
            "key" => &key,
            "bucket" => &self.bucket,
            "file" => ?path,
        );

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
    ) -> Result<Vec<PathBuf>, RemoteError> {
        info!(
            self.logger,
            "Reading objects into directory";
            "key_prefix" => &key_prefix,
            "bucket" => &self.bucket,
            "directory" => ?out_dir,
        );

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

    async fn delete_objects(&self, key_prefix: String) -> Result<(), RemoteError> {
        info!(
            self.logger,
            "Deleting objects";
            "key_prefix" => &key_prefix,
            "bucket" => &self.bucket,
        );

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
    ) -> Result<(), RemoteError> {
        info!(
            self.logger,
            "Uploading object from file";
            "key" => &key,
            "bucket" => &self.bucket,
            "file" => ?file_path,
        );

        if if_exists != IfExists::Overwrite {
            if self.check_object_exists(key.clone()).await? {
                info!(
                    self.logger,
                    "Object already exists";
                    "key" => &key,
                    "bucket" => &self.bucket,
                );

                return if if_exists == IfExists::Skip {
                    Ok(())
                } else {
                    Err(RemoteError::corrupted(
                        format!("Key {} already exists", key),
                        None,
                    ))
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
    ) -> Result<(), RemoteError> {
        info!(
            self.logger,
            "Uploading object data";
            "key" => &key,
            "bucket" => &self.bucket,
        );

        if if_exists != IfExists::Overwrite {
            if self.check_object_exists(key.clone()).await? {
                info!(
                    self.logger,
                    "Object already exists";
                    "key" => &key,
                    "bucket" => &self.bucket,
                );

                return if if_exists == IfExists::Skip {
                    Ok(())
                } else {
                    Err(RemoteError::corrupted(
                        format!("Key {} already exists", key),
                        None,
                    ))
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
    ) -> Result<(), RemoteError> {
        info!(
            self.logger,
            "Uploading objects from directory";
            "key_prefix" => &key_prefix,
            "bucket" => &self.bucket,
            "dir" => ?dir,
        );

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

    async fn check_object_exists(&self, key: String) -> Result<bool, RemoteError> {
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
}

impl RemoteClient for RemoteS3 {
    fn read_ref(&self, dataset_ref: &DatasetRef) -> Result<Option<Sha3_256>, RemoteError> {
        self.runtime
            .borrow_mut()
            .block_on(self.read_ref_async(dataset_ref))
    }

    // TODO: Locking
    fn write(
        &mut self,
        dataset_ref: &DatasetRef,
        expected_head: Option<Sha3_256>,
        new_head: Sha3_256,
        blocks: &mut dyn Iterator<Item = (Sha3_256, Vec<u8>)>,
        data_files: &mut dyn Iterator<Item = &Path>,
        checkpoint_dir: &Path,
    ) -> Result<(), RemoteError> {
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
        dataset_ref: &DatasetRef,
        expected_head: Sha3_256,
        last_seen_block: Option<Sha3_256>,
        tmp_dir: &Path,
    ) -> Result<RemoteReadResult, RemoteError> {
        self.runtime.borrow_mut().block_on(self.read_async(
            dataset_ref,
            expected_head,
            last_seen_block,
            tmp_dir,
        ))
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum IfExists {
    Skip,
    Overwrite,
    Fail,
}

impl<E: 'static + std::error::Error + Send + Sync> From<RusotoError<E>> for RemoteError {
    fn from(e: RusotoError<E>) -> Self {
        match e {
            RusotoError::Credentials(_) => RemoteError::credentials(Box::new(e)),
            _ => RemoteError::protocol(Box::new(e)),
        }
    }
}
