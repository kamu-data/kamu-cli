// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait EmbeddingsProvider: Send + Sync {
    async fn provide_content_embeddings(
        &self,
        content: Vec<String>,
    ) -> Result<Vec<Vec<f32>>, InternalError>;

    async fn provide_prompt_embeddings(
        &self,
        prompt: String,
    ) -> Result<Option<Vec<f32>>, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
