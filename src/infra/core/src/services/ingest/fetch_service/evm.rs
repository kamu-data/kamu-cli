// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use futures::TryStreamExt;
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use kamu_core::*;
use kamu_datasets::DatasetEnvVar;

use super::*;
use crate::PollingSourceState;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl FetchService {
    // TODO: FIXME: This implementation is overly complex due to DataFusion's poor
    // support of streaming / unbounded sources.
    //
    // In future datafusion-ethers should implement queries as unbounded source and
    // `DataFrame::execute_stream()` should not only yield data batches, but also
    // provide access to the state of the underlying stream for us to know how
    // far in the block range we have scanned. Since currently we can't access
    // the state of streaming - we can't interrupt the stream to resume later.
    // The implementation therefore uses DataFusion only to analyze SQL and
    // convert the WHERE clause into a filter, and then calls ETH RPC directly
    // to scan through block ranges.
    //
    // TODO: Account for re-orgs
    pub(crate) async fn fetch_ethereum_logs(
        &self,
        fetch: &odf::metadata::FetchStepEthereumLogs,
        prev_source_state: Option<&PollingSourceState>,
        target_path: &Path,
        dataset_env_vars: &HashMap<String, DatasetEnvVar>,
        listener: &Arc<dyn FetchProgressListener>,
    ) -> Result<FetchResult, PollingIngestError> {
        use alloy::providers::{Provider, ProviderBuilder};
        use datafusion::prelude::*;
        use datafusion_ethers::convert::*;
        use datafusion_ethers::stream::*;

        use crate::PollingSourceState;

        // Alloy does not support newlines in log signatures, but it's nice for
        // formatting
        let signature = fetch.signature.as_ref().map(|s| s.replace('\n', " "));

        let mut coder: Box<dyn Transcoder + Send> = if let Some(sig) = &signature {
            Box::new(EthRawAndDecodedLogsToArrow::new_from_signature(sig).int_err()?)
        } else {
            Box::new(EthRawLogsToArrow::new())
        };

        // Get last state
        let resume_from_state = match prev_source_state {
            None => None,
            Some(PollingSourceState::ETag(s)) => {
                let Some((num, _hash)) = s.split_once('@') else {
                    panic!("Malformed ETag: {s}");
                };
                Some(StreamState {
                    last_seen_block: num.parse().unwrap(),
                })
            }
            _ => panic!("EthereumLogs should only use ETag state"),
        };

        // Setup node RPC client
        let node_url = if let Some(url) = &fetch.node_url {
            self.template_url(url, dataset_env_vars)?
        } else if let Some(ep) = self
            .eth_source_config
            .get_endpoint_by_chain_id(fetch.chain_id.unwrap())
        {
            ep.node_url.clone()
        } else {
            Err(EthereumRpcError::new(format!(
                "Ethereum node RPC URL is not provided in the source manifest and no default node \
                 configured for chain ID {}",
                fetch.chain_id.unwrap()
            ))
            .int_err())?
        };

        let rpc_client = ProviderBuilder::new()
            .connect(node_url.as_str())
            .await
            .int_err()?
            .erased();

        let chain_id = rpc_client.get_chain_id().await.int_err()?;
        tracing::info!(%node_url, %chain_id, "Connected to ETH node");
        if let Some(expected_chain_id) = fetch.chain_id
            && expected_chain_id != chain_id
        {
            Err(EthereumRpcError::new(format!(
                "Expected to connect to chain ID {expected_chain_id} but got {chain_id} instead"
            ))
            .int_err())?;
        }

        // Setup Datafusion context
        let mut cfg = SessionConfig::new()
            .with_target_partitions(1)
            .with_coalesce_batches(false);

        // Forcing cese-sensitive identifiers in case-insensitive language seems to
        // be a lesser evil than following DataFusion's default behavior of forcing
        // identifiers to lowercase instead of case-insensitive matching.
        //
        // See: https://github.com/apache/datafusion/issues/7460
        // TODO: Consider externalizing this config (e.g. by allowing custom engine
        // options in transform DTOs)
        cfg.options_mut().sql_parser.enable_ident_normalization = false;

        let mut ctx = SessionContext::new_with_config(cfg);
        datafusion_ethers::udf::register_all(&mut ctx).unwrap();
        ctx.register_catalog(
            "eth",
            Arc::new(datafusion_ethers::provider::EthCatalog::new(
                rpc_client.clone(),
            )),
        );

        // Prepare the query according to filters
        let sql = if let Some(filter) = &fetch.filter {
            let filter = if let Some(sig) = &signature {
                format!("({filter}) and topic0 = eth_event_selector('{sig}')")
            } else {
                filter.clone()
            };
            format!("select * from eth.eth.logs where {filter}")
        } else {
            "select * from eth.eth.logs".to_string()
        };

        // Extract the filter out of the plan
        let df = ctx.sql(&sql).await.int_err()?;
        let plan = df.create_physical_plan().await.int_err()?;

        let plan_str = datafusion::physical_plan::get_plan_string(&plan).join("\n");
        tracing::info!(sql, plan = plan_str, "Original plan");

        let Some(filter) = plan
            .as_any()
            .downcast_ref::<datafusion_ethers::provider::EthGetLogs>()
            .map(|i| i.filter().clone())
        else {
            Err(EthereumRpcError::new(format!(
                "Filter is too complex and includes conditions that cannot be pushed down into \
                 RPC request. Please move such expressions into the postprocessing stage. \
                 Physical plan:\n{plan_str}"
            ))
            .int_err())?
        };

        // Resolve filter into a numeric block range
        let block_range_all = datafusion_ethers::stream::RawLogsStream::filter_to_block_range(
            &rpc_client,
            &filter.block_option,
        )
        .await
        .int_err()?;

        // Block interval that will be considered during this iteration
        let block_range_unprocessed = (
            resume_from_state
                .as_ref()
                .map_or(block_range_all.0, |s| s.last_seen_block + 1),
            block_range_all.1,
        );

        let mut state = resume_from_state.clone();

        let stream = datafusion_ethers::stream::RawLogsStream::paginate(
            rpc_client.clone(),
            filter,
            StreamOptions {
                block_stride: self.eth_source_config.get_logs_block_stride,
            },
            resume_from_state.clone(),
        );
        futures::pin_mut!(stream);

        while let Some(batch) = stream.try_next().await.int_err()? {
            let blocks_processed = batch.state.last_seen_block + 1 - block_range_unprocessed.0;

            // TODO: Design a better listener API that can reflect scanned input range,
            // number of records, and data volume transferred
            listener.on_progress(&FetchProgress {
                fetched_bytes: blocks_processed,
                total_bytes: TotalBytes::Exact(
                    block_range_unprocessed.1 + 1 - block_range_unprocessed.0,
                ),
            });

            if !batch.logs.is_empty() {
                coder.append(&batch.logs).int_err()?;
            }

            state = Some(batch.state);

            if coder.len() as u64 >= self.source_config.target_records_per_slice {
                tracing::info!(
                    target_records_per_slice = self.source_config.target_records_per_slice,
                    num_logs = coder.len(),
                    "Interrupting the stream after reaching the target batch size",
                );
                break;
            }
            if blocks_processed >= self.eth_source_config.commit_after_blocks_scanned {
                tracing::info!(
                    commit_after_blocks_scanned =
                        self.eth_source_config.commit_after_blocks_scanned,
                    blocks_processed,
                    "Interrupting the stream to commit progress",
                );
                break;
            }
        }

        // Have we made any progress?
        if resume_from_state == state {
            return Ok(FetchResult::UpToDate);
        }

        let state = state.unwrap();

        // Record scanning state
        // TODO: Reorg tolerance
        let last_seen_block = rpc_client
            .get_block(state.last_seen_block.into())
            .await
            .int_err()?
            .unwrap();

        tracing::info!(
            blocks_scanned = state.last_seen_block - block_range_unprocessed.0 + 1,
            block_range_scanned = ?(block_range_unprocessed.0, state.last_seen_block),
            block_range_ramaining = ?(state.last_seen_block + 1, block_range_unprocessed.1),
            num_logs = coder.len(),
            "Finished block scan cycle",
        );

        // Did we exhaust the source? (not accounting for new transactions)
        let has_more = state.last_seen_block < block_range_unprocessed.1;

        // Write data, if any, to parquet file
        if !coder.is_empty() {
            let batch = coder.finish();
            {
                let mut writer = datafusion::parquet::arrow::ArrowWriter::try_new(
                    std::fs::File::create_new(target_path).int_err()?,
                    batch.schema(),
                    None,
                )
                .int_err()?;
                writer.write(&batch).int_err()?;
                writer.finish().int_err()?;
            }
        }

        Ok(FetchResult::Updated(FetchResultUpdated {
            source_state: Some(PollingSourceState::ETag(format!(
                "{}@{:x}",
                last_seen_block.header.number, last_seen_block.header.hash,
            ))),
            source_event_time: None,
            has_more,
            zero_copy_path: None,
        }))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
#[error("Ethereum RPC error: {message}")]
struct EthereumRpcError {
    pub message: String,
}

impl EthereumRpcError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
