use std::sync::Arc;

use super::{CLIError, Command};
use crate::output::*;
use kamu::domain::{DatasetQueryOptions, MetadataRepository, QueryOptions, QueryService};

use opendatafabric::DatasetIDBuf;

pub struct TailCommand {
    query_svc: Arc<dyn QueryService>,
    metadata_repo: Arc<dyn MetadataRepository>,
    dataset_id: DatasetIDBuf,
    num_records: u64,
    output_cfg: Arc<OutputConfig>,
}

impl TailCommand {
    pub fn new(
        query_svc: Arc<dyn QueryService>,
        metadata_repo: Arc<dyn MetadataRepository>,
        dataset_id: DatasetIDBuf,
        num_records: u64,
        output_cfg: Arc<OutputConfig>,
    ) -> Self {
        Self {
            query_svc,
            metadata_repo,
            dataset_id,
            num_records,
            output_cfg,
        }
    }
}

impl Command for TailCommand {
    fn run(&mut self) -> Result<(), CLIError> {
        let vocab = self
            .metadata_repo
            .get_metadata_chain(&self.dataset_id)?
            .iter_blocks()
            .filter_map(|b| b.vocab)
            .next()
            .unwrap_or_default();

        let query = format!(
            r#"SELECT * FROM "{dataset}" ORDER BY {event_time_col} DESC LIMIT {num_records}"#,
            dataset = self.dataset_id,
            event_time_col = vocab.event_time_column.unwrap_or("event_time".to_owned()),
            num_records = self.num_records
        );

        let df = self
            .query_svc
            .sql_statement(
                &query,
                QueryOptions {
                    datasets: vec![DatasetQueryOptions {
                        dataset_id: self.dataset_id.clone(),
                        limit: Some(self.num_records),
                    }],
                },
            )
            .map_err(|e| CLIError::failure(e))?;

        let runtime = tokio::runtime::Runtime::new().unwrap();
        let record_batches = runtime
            .block_on(df.collect())
            .map_err(|e| CLIError::failure(e))?;

        let mut writer = self.output_cfg.get_writer();
        writer.write_batches(&record_batches)?;
        writer.finish()?;
        Ok(())
    }
}
