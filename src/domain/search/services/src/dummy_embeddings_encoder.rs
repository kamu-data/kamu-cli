// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use kamu_search::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn EmbeddingsEncoder)]
pub struct DummyEmbeddingsEncoder {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl EmbeddingsEncoder for DummyEmbeddingsEncoder {
    fn model_key(&self) -> EmbeddingModelKey {
        EmbeddingModelKey {
            provider: "dummy-provider",
            name: "dummy-model".to_string(),
            revision: None,
        }
    }

    fn dimensions(&self) -> usize {
        0
    }

    async fn encode(&self, _input: Vec<String>) -> Result<Vec<Vec<f32>>, InternalError> {
        Ok(vec![])
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
