// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, SchemaProvider};

use super::KamuSchema;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Replace with "remote catalog" pattern
// See: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/remote_catalog.rs#L78
pub(crate) struct KamuCatalog {
    schema: Arc<KamuSchema>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl KamuCatalog {
    pub fn new(schema: Arc<KamuSchema>) -> Self {
        Self { schema }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl CatalogProvider for KamuCatalog {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        vec!["kamu".to_owned()]
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if name == "kamu" {
            Some(self.schema.clone())
        } else {
            None
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl std::fmt::Debug for KamuCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KamuCatalog").finish_non_exhaustive()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
