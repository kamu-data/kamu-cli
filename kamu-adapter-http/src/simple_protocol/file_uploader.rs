// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;

use kamu::domain::{InternalError, ResultIntoInternal};
use tokio::io::AsyncWriteExt;
use tokio_stream::StreamExt;

/////////////////////////////////////////////////////////////////////////////////

pub async fn upload_file_from_axum_to_disk(
    upload_file_path: &Path,
    file_body_stream: &mut axum::extract::BodyStream,
) -> Result<(), InternalError> {
    let mut file = tokio::fs::File::create(upload_file_path.clone())
        .await
        .int_err()?;

    while let Some(chunk) = file_body_stream.next().await {
        let chunk_bytes = chunk.int_err()?;
        file.write_all(&chunk_bytes).await.int_err()?
    }

    file.flush().await.int_err()
}

/////////////////////////////////////////////////////////////////////////////////
