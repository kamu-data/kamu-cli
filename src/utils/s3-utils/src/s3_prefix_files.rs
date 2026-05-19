// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::{Path, PathBuf};

use file_utils::list_files_recursively;
use internal_error::{InternalError, ResultIntoInternal};

use crate::S3Context;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct S3PrefixFiles;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl S3PrefixFiles {
    fn directory_prefix(prefix: &str) -> String {
        let prefix = prefix.trim_end_matches('/');
        if prefix.is_empty() {
            String::new()
        } else {
            format!("{prefix}/")
        }
    }

    fn relative_key_path(dir_prefix: &str, key: &str) -> PathBuf {
        PathBuf::from(key.strip_prefix(dir_prefix).unwrap())
    }

    pub async fn read_object_as_string(
        s3_context: &S3Context,
        key: impl Into<String>,
    ) -> Result<String, InternalError> {
        let object = s3_context.get_object(key.into()).await.int_err()?;
        let bytes = object.body.collect().await.int_err()?.into_bytes();

        String::from_utf8(bytes.to_vec()).int_err()
    }

    pub async fn list_prefix_relative_keys(
        s3_context: &S3Context,
        prefix: &str,
    ) -> Result<Vec<PathBuf>, InternalError> {
        let dir_prefix = Self::directory_prefix(prefix);

        let mut files = s3_context
            .bucket_list_object_keys(&dir_prefix)
            .await?
            .into_iter()
            .map(|key| Self::relative_key_path(&dir_prefix, &key))
            .collect::<Vec<_>>();

        files.sort();
        Ok(files)
    }

    pub async fn upload_dir_to_prefix(
        s3_context: &S3Context,
        local_dir: &Path,
        prefix: &str,
    ) -> Result<(), InternalError> {
        let dir_prefix = Self::directory_prefix(prefix);

        for local_path in list_files_recursively(local_dir).int_err()? {
            let rel_path = local_path.strip_prefix(local_dir).unwrap();
            let key = format!("{dir_prefix}{}", rel_path.to_string_lossy());
            let data = std::fs::read(local_path).int_err()?;
            s3_context.put_object(key, &data).await.int_err()?;
        }

        Ok(())
    }

    pub async fn download_prefix_to_dir(
        s3_context: &S3Context,
        prefix: &str,
        local_dir: &Path,
    ) -> Result<(), InternalError> {
        let dir_prefix = Self::directory_prefix(prefix);

        for key in s3_context.bucket_list_object_keys(&dir_prefix).await? {
            let dst_path = local_dir.join(Self::relative_key_path(&dir_prefix, &key));

            if let Some(parent_dir) = dst_path.parent() {
                std::fs::create_dir_all(parent_dir).int_err()?;
            }

            let object = s3_context.get_object(key).await.int_err()?;
            let data = object.body.collect().await.int_err()?.into_bytes();
            std::fs::write(dst_path, data).int_err()?;
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
