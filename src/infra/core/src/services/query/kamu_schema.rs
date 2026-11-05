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
use datafusion::common::{exec_err, plan_err};
use datafusion::config::TableOptions;
use datafusion::datasource::{TableProvider, ViewTable};
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
    /// Views and tables created in this session, as its [`SqlPolicy`] allows.
    /// They never share a name with a dataset
    session_tables: Mutex<HashMap<String, Arc<dyn TableProvider>>>,
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
            session_tables: Mutex::new(HashMap::new()),
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
    fn table_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.tables.lock().unwrap().keys().cloned().collect();
        names.extend(self.session_tables.lock().unwrap().keys().cloned());
        names
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.lock().unwrap().contains_key(name)
            || self.session_tables.lock().unwrap().contains_key(name)
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let policy = SqlPolicy::of(&self.session_config);
        if table.is::<ViewTable>() {
            if !policy.allow_session_views {
                return plan_err!("Views are not allowed in this session");
            }
        } else if !policy.allow_writes {
            return plan_err!("Tables cannot be created in this session");
        }

        if self.tables.lock().unwrap().contains_key(&name) {
            return exec_err!("Table '{name}' already exists");
        }

        let mut session_tables = self.session_tables.lock().unwrap();
        if session_tables.contains_key(&name) {
            return exec_err!("Table '{name}' already exists");
        }
        session_tables.insert(name, table);
        Ok(None)
    }

    fn deregister_table(
        &self,
        name: &str,
    ) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        if self.tables.lock().unwrap().contains_key(name) {
            return plan_err!("Dataset '{name}' cannot be replaced or dropped");
        }
        Ok(self.session_tables.lock().unwrap().remove(name))
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
            Ok(self.session_tables.lock().unwrap().get(name).cloned())
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
