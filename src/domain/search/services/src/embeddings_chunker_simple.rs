// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::*;
use kamu_search::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Default)]
pub struct EmbeddingsChunkerConfigSimple {
    // Whether to chunk separately major dataset sections like name, schema, readme, or to combine
    // them all into one chunk
    pub split_sections: bool,

    // Whether to split section content by paragraph
    pub split_paragraphs: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn EmbeddingsChunker)]
pub struct EmbeddingsChunkerSimple {
    config: Arc<EmbeddingsChunkerConfigSimple>,
}

#[async_trait::async_trait]
impl EmbeddingsChunker for EmbeddingsChunkerSimple {
    async fn chunk(&self, content: Vec<String>) -> Result<Vec<String>, InternalError> {
        let content = if self.config.split_sections {
            content
        } else {
            vec![content.join("\n\n")]
        };

        if !self.config.split_paragraphs {
            return Ok(content);
        }

        Ok(content
            .iter()
            .flat_map(|s| s.split("\n\n").map(std::string::ToString::to_string))
            .collect())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
