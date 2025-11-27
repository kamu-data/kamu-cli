// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use datafusion::catalog::SchemaProvider;
use datafusion::config::TableOptions;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::prelude::{SessionConfig, SessionContext};
use internal_error::InternalError;
use kamu_core::*;
use kamu_datasets::DatasetRegistry;

use super::KamuTable;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Needs to be Clone only for non-async hacks below
pub(crate) struct KamuSchema {
    // Note: Never include `SessionContext` or `SessionState` into this structure.
    // Be mindful that this will cause a circular reference and thus prevent context
    // from ever being released.
    session_config: Arc<SessionConfig>,
    table_options: Arc<TableOptions>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    options: QueryOptions,
    tables: Mutex<HashMap<String, Arc<KamuTable>>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl KamuSchema {
    pub async fn prepare(
        session_context: &SessionContext,
        dataset_registry: Arc<dyn DatasetRegistry>,
        options: QueryOptions,
    ) -> Result<Self, InternalError> {
        let schema = Self {
            session_config: Arc::new(session_context.copied_config()),
            table_options: Arc::new(session_context.copied_table_options()),
            dataset_registry,
            options,
            tables: Mutex::new(HashMap::new()),
        };

        schema.init_schema_cache().await?;

        Ok(schema)
    }

    #[tracing::instrument(level = "info", name = "KamuSchema::init_schema_cache" skip_all)]
    async fn init_schema_cache(&self) -> Result<(), InternalError> {
        let mut tables = HashMap::new();

        let input_datasets = self
            .options
            .input_datasets
            .as_ref()
            .expect("QueryService should resolve all inputs");

        for (id, opts) in input_datasets {
            let hdl = opts
                .hints
                .handle
                .clone()
                .expect("QueryService should pre-resolve handles");

            assert_eq!(*id, hdl.id);

            // Try reusing pre-resolved dataset if available
            let source_dataset = if let Some(source_dataset) = &opts.hints.source_dataset {
                source_dataset.clone()
            } else {
                self.dataset_registry.get_dataset_by_handle(&hdl).await
            };

            tables.insert(
                opts.alias.clone(),
                Arc::new(KamuTable::new(
                    self.session_config.clone(),
                    self.table_options.clone(),
                    source_dataset,
                    opts.block_hash.clone(),
                    opts.hints.clone(),
                )),
            );
        }

        {
            let mut guard = self.tables.lock().unwrap();
            *guard = tables;
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl SchemaProvider for KamuSchema {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let guard = self.tables.lock().unwrap();
        guard.keys().cloned().collect()
    }

    fn table_exist(&self, name: &str) -> bool {
        let guard = self.tables.lock().unwrap();
        guard.contains_key(name)
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let table = {
            let guard = self.tables.lock().unwrap();
            guard.get(name).cloned()
        };

        if let Some(table) = table {
            // HACK: We pre-initialize the schema here because `TableProvider::schema()` is
            // not async
            table
                .get_table_schema()
                .await
                .map_err(|e| DataFusionError::External(e.into()))?;

            Ok(Some(table))
        } else {
            Ok(None)
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl std::fmt::Debug for KamuSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KamuSchema").finish_non_exhaustive()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
