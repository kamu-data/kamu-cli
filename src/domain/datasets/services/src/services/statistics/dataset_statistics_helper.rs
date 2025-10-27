// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{InternalError, ResultIntoInternal};
use kamu_datasets::DatasetStatistics;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub(crate) struct DatasetStatisticsIncrement {
    pub(crate) statistics: DatasetStatistics,
    pub(crate) seen_seed: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn compute_dataset_statistics_increment(
    metadata_chain: &dyn odf::MetadataChain,
    head: &odf::Multihash,
    maybe_last_seen_block: Option<&odf::Multihash>,
) -> Result<DatasetStatisticsIncrement, InternalError> {
    use odf::dataset::MetadataChainExt;
    let mut block_stream = metadata_chain.iter_blocks_interval(
        head.into(),
        maybe_last_seen_block.map(Into::into),
        true,
    );

    let mut statistics = DatasetStatistics::default();
    let mut seen_seed = false;

    use tokio_stream::StreamExt;
    while let Some((_, block)) = block_stream.try_next().await.int_err()? {
        match block.event {
            odf::MetadataEvent::Seed(_) => {
                seen_seed = true;
            }
            odf::MetadataEvent::AddData(add_data) => {
                statistics.last_pulled.get_or_insert(block.system_time);

                if let Some(output_data) = add_data.new_data {
                    let iv = output_data.offset_interval;
                    statistics.num_records += iv.end - iv.start + 1;

                    statistics.data_size += output_data.size;
                }

                if let Some(checkpoint) = add_data.new_checkpoint {
                    statistics.checkpoints_size += checkpoint.size;
                }
            }
            odf::MetadataEvent::ExecuteTransform(execute_transform) => {
                statistics.last_pulled.get_or_insert(block.system_time);

                if let Some(output_data) = execute_transform.new_data {
                    let iv = output_data.offset_interval;
                    statistics.num_records += iv.end - iv.start + 1;

                    statistics.data_size += output_data.size;
                }

                if let Some(checkpoint) = execute_transform.new_checkpoint {
                    statistics.checkpoints_size += checkpoint.size;
                }
            }
            odf::MetadataEvent::SetDataSchema(_)
            | odf::MetadataEvent::SetAttachments(_)
            | odf::MetadataEvent::SetInfo(_)
            | odf::MetadataEvent::SetLicense(_)
            | odf::MetadataEvent::SetVocab(_)
            | odf::MetadataEvent::SetTransform(_)
            | odf::MetadataEvent::SetPollingSource(_)
            | odf::MetadataEvent::DisablePollingSource(_)
            | odf::MetadataEvent::AddPushSource(_)
            | odf::MetadataEvent::DisablePushSource(_) => (),
        }
    }

    Ok(DatasetStatisticsIncrement {
        statistics,
        seen_seed,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
