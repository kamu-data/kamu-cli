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

/// Chunkers are repsonsible for splitting large text into optimal sets of
/// tokens to generate embeddings from.
#[async_trait::async_trait]
pub trait EmbeddingsChunker: Send + Sync {
    /// Given sections of a single document splits them into chinks of optimal
    /// size which will be encoded into embedding vectors
    async fn chunk(&self, content: Vec<String>) -> Result<Vec<String>, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
