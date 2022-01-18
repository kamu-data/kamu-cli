// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::scalars::*;

use async_graphql::*;
use chrono::prelude::*;
use opendatafabric as odf;

////////////////////////////////////////////////////////////////////////////////////////
// MetadataBlock
////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub(crate) struct MetadataBlock {
    pub system_time: DateTime<Utc>,
    pub block_hash: Multihash,
    pub prev_block_hash: Option<Multihash>,
    pub event: MetadataEvent,
}

impl MetadataBlock {
    pub fn new(hash: odf::Multihash, block: odf::MetadataBlock) -> Self {
        Self {
            block_hash: hash.into(),
            prev_block_hash: block.prev_block_hash.map(|v| v.into()),
            system_time: block.system_time,
            event: block.event.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////
// MetadataEvent
////////////////////////////////////////////////////////////////////////////////////////

// TODO: eventName field is unnecessary, but we can't have an empty interface
#[derive(Interface, Debug, Clone)]
#[graphql(field(name = "_dummy", type = "&str"))]
pub(crate) enum MetadataEvent {
    AddData(MetadataEventAddData),
    ExecuteQuery(MetadataEventExecuteQuery),
    Seed(MetadataEventSeed),
    SetPollingSource(MetadataEventSetPollingSource),
    SetTransform(MetadataEventSetTransform),
    SetVocab(MetadataEventSetVocab),
    SetWatermark(MetadataEventSetWatermark),
    Unsupported(MetadataEventUnsupported),
}

impl From<odf::MetadataEvent> for MetadataEvent {
    fn from(v: odf::MetadataEvent) -> Self {
        match v {
            odf::MetadataEvent::AddData(e) => MetadataEvent::AddData(MetadataEventAddData(e)),
            odf::MetadataEvent::ExecuteQuery(e) => {
                MetadataEvent::ExecuteQuery(MetadataEventExecuteQuery(e))
            }
            odf::MetadataEvent::Seed(e) => MetadataEvent::Seed(MetadataEventSeed(e)),
            odf::MetadataEvent::SetPollingSource(e) => {
                MetadataEvent::SetPollingSource(MetadataEventSetPollingSource(e))
            }
            odf::MetadataEvent::SetTransform(e) => {
                MetadataEvent::SetTransform(MetadataEventSetTransform(e))
            }
            odf::MetadataEvent::SetVocab(e) => MetadataEvent::SetVocab(MetadataEventSetVocab(e)),
            odf::MetadataEvent::SetWatermark(e) => {
                MetadataEvent::SetWatermark(MetadataEventSetWatermark(e))
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////
// MetadataEvents
////////////////////////////////////////////////////////////////////////////////////////

// TODO: Generate these types?

#[derive(Debug, Clone)]
pub(crate) struct MetadataEventAddData(odf::AddData);

#[Object]
impl MetadataEventAddData {
    async fn _dummy(&self) -> &str {
        ""
    }

    async fn output_data(&self) -> DataSliceMetadata {
        self.0.output_data.clone().into()
    }

    async fn output_watermark(&self) -> Option<DateTime<Utc>> {
        self.0.output_watermark
    }
}

////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub(crate) struct MetadataEventExecuteQuery(odf::ExecuteQuery);

#[Object]
impl MetadataEventExecuteQuery {
    async fn _dummy(&self) -> &str {
        ""
    }

    async fn input_slices(&self) -> Vec<InputSliceMetadata> {
        self.0
            .input_slices
            .iter()
            .cloned()
            .map(|v| v.into())
            .collect()
    }

    async fn output_data(&self) -> Option<DataSliceMetadata> {
        self.0.output_data.clone().map(|v| v.into())
    }

    async fn output_watermark(&self) -> Option<DateTime<Utc>> {
        self.0.output_watermark
    }
}

////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub(crate) struct MetadataEventSeed(odf::Seed);

#[Object]
impl MetadataEventSeed {
    async fn _dummy(&self) -> &str {
        ""
    }

    async fn dataset_id(&self) -> DatasetID {
        self.0.dataset_id.clone().into()
    }

    async fn dataset_kind(&self) -> DatasetKind {
        self.0.dataset_kind.into()
    }
}

////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub(crate) struct MetadataEventSetPollingSource(odf::SetPollingSource);

#[Object]
impl MetadataEventSetPollingSource {
    async fn _dummy(&self) -> &str {
        ""
    }
}

////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub(crate) struct MetadataEventSetTransform(odf::SetTransform);

#[Object]
impl MetadataEventSetTransform {
    async fn _dummy(&self) -> &str {
        ""
    }
}

////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub(crate) struct MetadataEventSetVocab(odf::SetVocab);

#[Object]
impl MetadataEventSetVocab {
    async fn _dummy(&self) -> &str {
        ""
    }
}

////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub(crate) struct MetadataEventSetWatermark(odf::SetWatermark);

#[Object]
impl MetadataEventSetWatermark {
    async fn _dummy(&self) -> &str {
        ""
    }

    async fn output_watermark(&self) -> DateTime<Utc> {
        self.0.output_watermark
    }
}

////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub(crate) struct MetadataEventUnsupported;

#[Object]
impl MetadataEventUnsupported {
    async fn _dummy(&self) -> &str {
        ""
    }
}

////////////////////////////////////////////////////////////////////////////////////////
// Fragments
////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Clone, Debug)]
pub(crate) struct DataSliceMetadata {
    pub logical_hash: Multihash,
    pub physical_hash: Multihash,
    pub interval: OffsetInterval,
}

impl From<odf::DataSlice> for DataSliceMetadata {
    fn from(v: odf::DataSlice) -> Self {
        Self {
            logical_hash: v.logical_hash.into(),
            physical_hash: v.physical_hash.into(),
            interval: v.interval.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Clone, Debug)]
pub(crate) struct InputSliceMetadata {
    pub dataset_id: DatasetID,
    pub block_interval: Option<BlockInterval>,
    pub data_interval: Option<OffsetInterval>,
}

impl From<odf::InputSlice> for InputSliceMetadata {
    fn from(v: odf::InputSlice) -> Self {
        Self {
            dataset_id: v.dataset_id.into(),
            block_interval: v.block_interval.map(|v| v.into()),
            data_interval: v.data_interval.map(|v| v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Clone, Debug)]
pub(crate) struct OffsetInterval {
    pub start: i64,
    pub end: i64,
}

impl From<odf::OffsetInterval> for OffsetInterval {
    fn from(v: odf::OffsetInterval) -> Self {
        Self {
            start: v.start,
            end: v.end,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Clone, Debug)]
pub(crate) struct BlockInterval {
    pub start: Multihash,
    pub end: Multihash,
}

impl From<odf::BlockInterval> for BlockInterval {
    fn from(v: odf::BlockInterval) -> Self {
        Self {
            start: v.start.into(),
            end: v.end.into(),
        }
    }
}
