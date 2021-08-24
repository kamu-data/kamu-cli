use datafusion::error::DataFusionError;
use datafusion::prelude::DataFrame;
use opendatafabric::DatasetIDBuf;
use std::sync::Arc;
use thiserror::Error;

pub trait QueryService: Send + Sync {
    fn sql_statement(
        &self,
        statement: &str,
        options: QueryOptions,
    ) -> Result<Arc<dyn DataFrame>, QueryError>;
}

#[derive(Debug, Clone, Default)]
pub struct QueryOptions {
    pub datasets: Vec<DatasetQueryOptions>,
}

#[derive(Debug, Clone)]
pub struct DatasetQueryOptions {
    pub dataset_id: DatasetIDBuf,
    /// Number of records that output requires (starting from latest entries)
    /// Setting this value allows to limit the number of part files examined.
    pub limit: Option<u64>,
}

#[derive(Debug, Error)]
pub enum QueryError {
    #[error("{0}")]
    DataFusionError(#[from] DataFusionError),
    #[error("{0}")]
    Unimplemented(String),
}

impl QueryError {
    pub fn unimplemented(message: impl Into<String>) -> Self {
        Self::Unimplemented(message.into())
    }
}
