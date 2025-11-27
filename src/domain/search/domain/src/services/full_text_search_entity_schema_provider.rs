// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::{FullTextSearchEntitySchema, FullTextSearchRepository};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FullTextSearchEntitySchemaProvider: Send + Sync {
    fn provider_name(&self) -> &'static str;

    fn provide_schemas(&self) -> &[FullTextSearchEntitySchema];

    async fn run_schema_initial_indexing(
        &self,
        repo: &dyn FullTextSearchRepository,
        schema: &FullTextSearchEntitySchema,
    ) -> Result<usize, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
