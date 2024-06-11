// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

///////////////////////////////////////////////////////////////////////////////
// WARNING: This file is auto-generated from Open Data Fabric Schemas
// See: http://opendatafabric.org/
///////////////////////////////////////////////////////////////////////////////

#![allow(clippy::all)]
#![allow(clippy::pedantic)]

use std::path::Path;

use chrono::{DateTime, Utc};

use crate::dtos;
use crate::dtos::{CompressionFormat, DatasetKind, MqttQos, SourceOrdering};
use crate::formats::*;
use crate::identity::*;

////////////////////////////////////////////////////////////////////////////////
// AddData
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#adddata-schema
////////////////////////////////////////////////////////////////////////////////

pub trait AddData {
    fn prev_checkpoint(&self) -> Option<&Multihash>;
    fn prev_offset(&self) -> Option<u64>;
    fn new_data(&self) -> Option<&dyn DataSlice>;
    fn new_checkpoint(&self) -> Option<&dyn Checkpoint>;
    fn new_watermark(&self) -> Option<DateTime<Utc>>;
    fn new_source_state(&self) -> Option<&dyn SourceState>;
}

impl AddData for dtos::AddData {
    fn prev_checkpoint(&self) -> Option<&Multihash> {
        self.prev_checkpoint.as_ref().map(|v| -> &Multihash { v })
    }
    fn prev_offset(&self) -> Option<u64> {
        self.prev_offset.as_ref().map(|v| -> u64 { *v })
    }
    fn new_data(&self) -> Option<&dyn DataSlice> {
        self.new_data.as_ref().map(|v| -> &dyn DataSlice { v })
    }
    fn new_checkpoint(&self) -> Option<&dyn Checkpoint> {
        self.new_checkpoint
            .as_ref()
            .map(|v| -> &dyn Checkpoint { v })
    }
    fn new_watermark(&self) -> Option<DateTime<Utc>> {
        self.new_watermark.as_ref().map(|v| -> DateTime<Utc> { *v })
    }
    fn new_source_state(&self) -> Option<&dyn SourceState> {
        self.new_source_state
            .as_ref()
            .map(|v| -> &dyn SourceState { v })
    }
}

impl Into<dtos::AddData> for &dyn AddData {
    fn into(self) -> dtos::AddData {
        dtos::AddData {
            prev_checkpoint: self.prev_checkpoint().map(|v| v.clone()),
            prev_offset: self.prev_offset().map(|v| v),
            new_data: self.new_data().map(|v| v.into()),
            new_checkpoint: self.new_checkpoint().map(|v| v.into()),
            new_watermark: self.new_watermark().map(|v| v),
            new_source_state: self.new_source_state().map(|v| v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// AddPushSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#addpushsource-schema
////////////////////////////////////////////////////////////////////////////////

pub trait AddPushSource {
    fn source_name(&self) -> &str;
    fn read(&self) -> ReadStep;
    fn preprocess(&self) -> Option<Transform>;
    fn merge(&self) -> MergeStrategy;
}

impl AddPushSource for dtos::AddPushSource {
    fn source_name(&self) -> &str {
        self.source_name.as_ref()
    }
    fn read(&self) -> ReadStep {
        (&self.read).into()
    }
    fn preprocess(&self) -> Option<Transform> {
        self.preprocess.as_ref().map(|v| -> Transform { v.into() })
    }
    fn merge(&self) -> MergeStrategy {
        (&self.merge).into()
    }
}

impl Into<dtos::AddPushSource> for &dyn AddPushSource {
    fn into(self) -> dtos::AddPushSource {
        dtos::AddPushSource {
            source_name: self.source_name().to_owned(),
            read: self.read().into(),
            preprocess: self.preprocess().map(|v| v.into()),
            merge: self.merge().into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// AttachmentEmbedded
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#attachmentembedded-schema
////////////////////////////////////////////////////////////////////////////////

pub trait AttachmentEmbedded {
    fn path(&self) -> &str;
    fn content(&self) -> &str;
}

impl AttachmentEmbedded for dtos::AttachmentEmbedded {
    fn path(&self) -> &str {
        self.path.as_ref()
    }
    fn content(&self) -> &str {
        self.content.as_ref()
    }
}

impl Into<dtos::AttachmentEmbedded> for &dyn AttachmentEmbedded {
    fn into(self) -> dtos::AttachmentEmbedded {
        dtos::AttachmentEmbedded {
            path: self.path().to_owned(),
            content: self.content().to_owned(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Attachments
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#attachments-schema
////////////////////////////////////////////////////////////////////////////////

pub enum Attachments<'a> {
    Embedded(&'a dyn AttachmentsEmbedded),
}

impl<'a> From<&'a dtos::Attachments> for Attachments<'a> {
    fn from(other: &'a dtos::Attachments) -> Self {
        match other {
            dtos::Attachments::Embedded(v) => Attachments::Embedded(v),
        }
    }
}

impl Into<dtos::Attachments> for Attachments<'_> {
    fn into(self) -> dtos::Attachments {
        match self {
            Attachments::Embedded(v) => dtos::Attachments::Embedded(v.into()),
        }
    }
}

pub trait AttachmentsEmbedded {
    fn items(&self) -> Box<dyn Iterator<Item = &dyn AttachmentEmbedded> + '_>;
}

impl AttachmentsEmbedded for dtos::AttachmentsEmbedded {
    fn items(&self) -> Box<dyn Iterator<Item = &dyn AttachmentEmbedded> + '_> {
        Box::new(self.items.iter().map(|i| -> &dyn AttachmentEmbedded { i }))
    }
}

impl Into<dtos::AttachmentsEmbedded> for &dyn AttachmentsEmbedded {
    fn into(self) -> dtos::AttachmentsEmbedded {
        dtos::AttachmentsEmbedded {
            items: self.items().map(|i| i.into()).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Checkpoint
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#checkpoint-schema
////////////////////////////////////////////////////////////////////////////////

pub trait Checkpoint {
    fn physical_hash(&self) -> &Multihash;
    fn size(&self) -> u64;
}

impl Checkpoint for dtos::Checkpoint {
    fn physical_hash(&self) -> &Multihash {
        &self.physical_hash
    }
    fn size(&self) -> u64 {
        self.size
    }
}

impl Into<dtos::Checkpoint> for &dyn Checkpoint {
    fn into(self) -> dtos::Checkpoint {
        dtos::Checkpoint {
            physical_hash: self.physical_hash().clone(),
            size: self.size(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// DataSlice
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#dataslice-schema
////////////////////////////////////////////////////////////////////////////////

pub trait DataSlice {
    fn logical_hash(&self) -> &Multihash;
    fn physical_hash(&self) -> &Multihash;
    fn offset_interval(&self) -> &dyn OffsetInterval;
    fn size(&self) -> u64;
}

impl DataSlice for dtos::DataSlice {
    fn logical_hash(&self) -> &Multihash {
        &self.logical_hash
    }
    fn physical_hash(&self) -> &Multihash {
        &self.physical_hash
    }
    fn offset_interval(&self) -> &dyn OffsetInterval {
        &self.offset_interval
    }
    fn size(&self) -> u64 {
        self.size
    }
}

impl Into<dtos::DataSlice> for &dyn DataSlice {
    fn into(self) -> dtos::DataSlice {
        dtos::DataSlice {
            logical_hash: self.logical_hash().clone(),
            physical_hash: self.physical_hash().clone(),
            offset_interval: self.offset_interval().into(),
            size: self.size(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// DatasetKind
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetkind-schema
////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////
// DatasetSnapshot
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetsnapshot-schema
////////////////////////////////////////////////////////////////////////////////

pub trait DatasetSnapshot {
    fn name(&self) -> &DatasetAlias;
    fn kind(&self) -> DatasetKind;
    fn metadata(&self) -> Box<dyn Iterator<Item = MetadataEvent> + '_>;
}

impl DatasetSnapshot for dtos::DatasetSnapshot {
    fn name(&self) -> &DatasetAlias {
        &self.name
    }
    fn kind(&self) -> DatasetKind {
        self.kind
    }
    fn metadata(&self) -> Box<dyn Iterator<Item = MetadataEvent> + '_> {
        Box::new(self.metadata.iter().map(|i| -> MetadataEvent { i.into() }))
    }
}

impl Into<dtos::DatasetSnapshot> for &dyn DatasetSnapshot {
    fn into(self) -> dtos::DatasetSnapshot {
        dtos::DatasetSnapshot {
            name: self.name().to_owned(),
            kind: self.kind().into(),
            metadata: self.metadata().map(|i| i.into()).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// DatasetVocabulary
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetvocabulary-schema
////////////////////////////////////////////////////////////////////////////////

pub trait DatasetVocabulary {
    fn offset_column(&self) -> &str;
    fn operation_type_column(&self) -> &str;
    fn system_time_column(&self) -> &str;
    fn event_time_column(&self) -> &str;
}

impl DatasetVocabulary for dtos::DatasetVocabulary {
    fn offset_column(&self) -> &str {
        self.offset_column.as_ref()
    }
    fn operation_type_column(&self) -> &str {
        self.operation_type_column.as_ref()
    }
    fn system_time_column(&self) -> &str {
        self.system_time_column.as_ref()
    }
    fn event_time_column(&self) -> &str {
        self.event_time_column.as_ref()
    }
}

impl Into<dtos::DatasetVocabulary> for &dyn DatasetVocabulary {
    fn into(self) -> dtos::DatasetVocabulary {
        dtos::DatasetVocabulary {
            offset_column: self.offset_column().to_owned(),
            operation_type_column: self.operation_type_column().to_owned(),
            system_time_column: self.system_time_column().to_owned(),
            event_time_column: self.event_time_column().to_owned(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// DisablePollingSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#disablepollingsource-schema
////////////////////////////////////////////////////////////////////////////////

pub trait DisablePollingSource {}

impl DisablePollingSource for dtos::DisablePollingSource {}

impl Into<dtos::DisablePollingSource> for &dyn DisablePollingSource {
    fn into(self) -> dtos::DisablePollingSource {
        dtos::DisablePollingSource {}
    }
}

////////////////////////////////////////////////////////////////////////////////
// DisablePushSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#disablepushsource-schema
////////////////////////////////////////////////////////////////////////////////

pub trait DisablePushSource {
    fn source_name(&self) -> &str;
}

impl DisablePushSource for dtos::DisablePushSource {
    fn source_name(&self) -> &str {
        self.source_name.as_ref()
    }
}

impl Into<dtos::DisablePushSource> for &dyn DisablePushSource {
    fn into(self) -> dtos::DisablePushSource {
        dtos::DisablePushSource {
            source_name: self.source_name().to_owned(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// EnvVar
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#envvar-schema
////////////////////////////////////////////////////////////////////////////////

pub trait EnvVar {
    fn name(&self) -> &str;
    fn value(&self) -> Option<&str>;
}

impl EnvVar for dtos::EnvVar {
    fn name(&self) -> &str {
        self.name.as_ref()
    }
    fn value(&self) -> Option<&str> {
        self.value.as_ref().map(|v| -> &str { v.as_ref() })
    }
}

impl Into<dtos::EnvVar> for &dyn EnvVar {
    fn into(self) -> dtos::EnvVar {
        dtos::EnvVar {
            name: self.name().to_owned(),
            value: self.value().map(|v| v.to_owned()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// EventTimeSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#eventtimesource-schema
////////////////////////////////////////////////////////////////////////////////

pub enum EventTimeSource<'a> {
    FromMetadata(&'a dyn EventTimeSourceFromMetadata),
    FromPath(&'a dyn EventTimeSourceFromPath),
    FromSystemTime(&'a dyn EventTimeSourceFromSystemTime),
}

impl<'a> From<&'a dtos::EventTimeSource> for EventTimeSource<'a> {
    fn from(other: &'a dtos::EventTimeSource) -> Self {
        match other {
            dtos::EventTimeSource::FromMetadata(v) => EventTimeSource::FromMetadata(v),
            dtos::EventTimeSource::FromPath(v) => EventTimeSource::FromPath(v),
            dtos::EventTimeSource::FromSystemTime(v) => EventTimeSource::FromSystemTime(v),
        }
    }
}

impl Into<dtos::EventTimeSource> for EventTimeSource<'_> {
    fn into(self) -> dtos::EventTimeSource {
        match self {
            EventTimeSource::FromMetadata(v) => dtos::EventTimeSource::FromMetadata(v.into()),
            EventTimeSource::FromPath(v) => dtos::EventTimeSource::FromPath(v.into()),
            EventTimeSource::FromSystemTime(v) => dtos::EventTimeSource::FromSystemTime(v.into()),
        }
    }
}

pub trait EventTimeSourceFromMetadata {}

pub trait EventTimeSourceFromPath {
    fn pattern(&self) -> &str;
    fn timestamp_format(&self) -> Option<&str>;
}

pub trait EventTimeSourceFromSystemTime {}

impl EventTimeSourceFromMetadata for dtos::EventTimeSourceFromMetadata {}

impl EventTimeSourceFromPath for dtos::EventTimeSourceFromPath {
    fn pattern(&self) -> &str {
        self.pattern.as_ref()
    }
    fn timestamp_format(&self) -> Option<&str> {
        self.timestamp_format
            .as_ref()
            .map(|v| -> &str { v.as_ref() })
    }
}

impl EventTimeSourceFromSystemTime for dtos::EventTimeSourceFromSystemTime {}

impl Into<dtos::EventTimeSourceFromMetadata> for &dyn EventTimeSourceFromMetadata {
    fn into(self) -> dtos::EventTimeSourceFromMetadata {
        dtos::EventTimeSourceFromMetadata {}
    }
}

impl Into<dtos::EventTimeSourceFromPath> for &dyn EventTimeSourceFromPath {
    fn into(self) -> dtos::EventTimeSourceFromPath {
        dtos::EventTimeSourceFromPath {
            pattern: self.pattern().to_owned(),
            timestamp_format: self.timestamp_format().map(|v| v.to_owned()),
        }
    }
}

impl Into<dtos::EventTimeSourceFromSystemTime> for &dyn EventTimeSourceFromSystemTime {
    fn into(self) -> dtos::EventTimeSourceFromSystemTime {
        dtos::EventTimeSourceFromSystemTime {}
    }
}

////////////////////////////////////////////////////////////////////////////////
// ExecuteTransform
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#executetransform-schema
////////////////////////////////////////////////////////////////////////////////

pub trait ExecuteTransform {
    fn query_inputs(&self) -> Box<dyn Iterator<Item = &dyn ExecuteTransformInput> + '_>;
    fn prev_checkpoint(&self) -> Option<&Multihash>;
    fn prev_offset(&self) -> Option<u64>;
    fn new_data(&self) -> Option<&dyn DataSlice>;
    fn new_checkpoint(&self) -> Option<&dyn Checkpoint>;
    fn new_watermark(&self) -> Option<DateTime<Utc>>;
}

impl ExecuteTransform for dtos::ExecuteTransform {
    fn query_inputs(&self) -> Box<dyn Iterator<Item = &dyn ExecuteTransformInput> + '_> {
        Box::new(
            self.query_inputs
                .iter()
                .map(|i| -> &dyn ExecuteTransformInput { i }),
        )
    }
    fn prev_checkpoint(&self) -> Option<&Multihash> {
        self.prev_checkpoint.as_ref().map(|v| -> &Multihash { v })
    }
    fn prev_offset(&self) -> Option<u64> {
        self.prev_offset.as_ref().map(|v| -> u64 { *v })
    }
    fn new_data(&self) -> Option<&dyn DataSlice> {
        self.new_data.as_ref().map(|v| -> &dyn DataSlice { v })
    }
    fn new_checkpoint(&self) -> Option<&dyn Checkpoint> {
        self.new_checkpoint
            .as_ref()
            .map(|v| -> &dyn Checkpoint { v })
    }
    fn new_watermark(&self) -> Option<DateTime<Utc>> {
        self.new_watermark.as_ref().map(|v| -> DateTime<Utc> { *v })
    }
}

impl Into<dtos::ExecuteTransform> for &dyn ExecuteTransform {
    fn into(self) -> dtos::ExecuteTransform {
        dtos::ExecuteTransform {
            query_inputs: self.query_inputs().map(|i| i.into()).collect(),
            prev_checkpoint: self.prev_checkpoint().map(|v| v.clone()),
            prev_offset: self.prev_offset().map(|v| v),
            new_data: self.new_data().map(|v| v.into()),
            new_checkpoint: self.new_checkpoint().map(|v| v.into()),
            new_watermark: self.new_watermark().map(|v| v),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// ExecuteTransformInput
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#executetransforminput-schema
////////////////////////////////////////////////////////////////////////////////

pub trait ExecuteTransformInput {
    fn dataset_id(&self) -> &DatasetID;
    fn prev_block_hash(&self) -> Option<&Multihash>;
    fn new_block_hash(&self) -> Option<&Multihash>;
    fn prev_offset(&self) -> Option<u64>;
    fn new_offset(&self) -> Option<u64>;
}

impl ExecuteTransformInput for dtos::ExecuteTransformInput {
    fn dataset_id(&self) -> &DatasetID {
        &self.dataset_id
    }
    fn prev_block_hash(&self) -> Option<&Multihash> {
        self.prev_block_hash.as_ref().map(|v| -> &Multihash { v })
    }
    fn new_block_hash(&self) -> Option<&Multihash> {
        self.new_block_hash.as_ref().map(|v| -> &Multihash { v })
    }
    fn prev_offset(&self) -> Option<u64> {
        self.prev_offset.as_ref().map(|v| -> u64 { *v })
    }
    fn new_offset(&self) -> Option<u64> {
        self.new_offset.as_ref().map(|v| -> u64 { *v })
    }
}

impl Into<dtos::ExecuteTransformInput> for &dyn ExecuteTransformInput {
    fn into(self) -> dtos::ExecuteTransformInput {
        dtos::ExecuteTransformInput {
            dataset_id: self.dataset_id().clone(),
            prev_block_hash: self.prev_block_hash().map(|v| v.clone()),
            new_block_hash: self.new_block_hash().map(|v| v.clone()),
            prev_offset: self.prev_offset().map(|v| v),
            new_offset: self.new_offset().map(|v| v),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// FetchStep
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#fetchstep-schema
////////////////////////////////////////////////////////////////////////////////

pub enum FetchStep<'a> {
    Url(&'a dyn FetchStepUrl),
    FilesGlob(&'a dyn FetchStepFilesGlob),
    Container(&'a dyn FetchStepContainer),
    Mqtt(&'a dyn FetchStepMqtt),
    EthereumLogs(&'a dyn FetchStepEthereumLogs),
}

impl<'a> From<&'a dtos::FetchStep> for FetchStep<'a> {
    fn from(other: &'a dtos::FetchStep) -> Self {
        match other {
            dtos::FetchStep::Url(v) => FetchStep::Url(v),
            dtos::FetchStep::FilesGlob(v) => FetchStep::FilesGlob(v),
            dtos::FetchStep::Container(v) => FetchStep::Container(v),
            dtos::FetchStep::Mqtt(v) => FetchStep::Mqtt(v),
            dtos::FetchStep::EthereumLogs(v) => FetchStep::EthereumLogs(v),
        }
    }
}

impl Into<dtos::FetchStep> for FetchStep<'_> {
    fn into(self) -> dtos::FetchStep {
        match self {
            FetchStep::Url(v) => dtos::FetchStep::Url(v.into()),
            FetchStep::FilesGlob(v) => dtos::FetchStep::FilesGlob(v.into()),
            FetchStep::Container(v) => dtos::FetchStep::Container(v.into()),
            FetchStep::Mqtt(v) => dtos::FetchStep::Mqtt(v.into()),
            FetchStep::EthereumLogs(v) => dtos::FetchStep::EthereumLogs(v.into()),
        }
    }
}

pub trait FetchStepUrl {
    fn url(&self) -> &str;
    fn event_time(&self) -> Option<EventTimeSource>;
    fn cache(&self) -> Option<SourceCaching>;
    fn headers(&self) -> Option<Box<dyn Iterator<Item = &dyn RequestHeader> + '_>>;
}

pub trait FetchStepFilesGlob {
    fn path(&self) -> &str;
    fn event_time(&self) -> Option<EventTimeSource>;
    fn cache(&self) -> Option<SourceCaching>;
    fn order(&self) -> Option<SourceOrdering>;
}

pub trait FetchStepContainer {
    fn image(&self) -> &str;
    fn command(&self) -> Option<Box<dyn Iterator<Item = &str> + '_>>;
    fn args(&self) -> Option<Box<dyn Iterator<Item = &str> + '_>>;
    fn env(&self) -> Option<Box<dyn Iterator<Item = &dyn EnvVar> + '_>>;
}

pub trait FetchStepMqtt {
    fn host(&self) -> &str;
    fn port(&self) -> i32;
    fn username(&self) -> Option<&str>;
    fn password(&self) -> Option<&str>;
    fn topics(&self) -> Box<dyn Iterator<Item = &dyn MqttTopicSubscription> + '_>;
}

pub trait FetchStepEthereumLogs {
    fn chain_id(&self) -> Option<u64>;
    fn node_url(&self) -> Option<&str>;
    fn filter(&self) -> Option<&str>;
    fn signature(&self) -> Option<&str>;
}

impl FetchStepUrl for dtos::FetchStepUrl {
    fn url(&self) -> &str {
        self.url.as_ref()
    }
    fn event_time(&self) -> Option<EventTimeSource> {
        self.event_time
            .as_ref()
            .map(|v| -> EventTimeSource { v.into() })
    }
    fn cache(&self) -> Option<SourceCaching> {
        self.cache.as_ref().map(|v| -> SourceCaching { v.into() })
    }
    fn headers(&self) -> Option<Box<dyn Iterator<Item = &dyn RequestHeader> + '_>> {
        self.headers
            .as_ref()
            .map(|v| -> Box<dyn Iterator<Item = &dyn RequestHeader> + '_> {
                Box::new(v.iter().map(|i| -> &dyn RequestHeader { i }))
            })
    }
}

impl FetchStepFilesGlob for dtos::FetchStepFilesGlob {
    fn path(&self) -> &str {
        self.path.as_ref()
    }
    fn event_time(&self) -> Option<EventTimeSource> {
        self.event_time
            .as_ref()
            .map(|v| -> EventTimeSource { v.into() })
    }
    fn cache(&self) -> Option<SourceCaching> {
        self.cache.as_ref().map(|v| -> SourceCaching { v.into() })
    }
    fn order(&self) -> Option<SourceOrdering> {
        self.order.as_ref().map(|v| -> SourceOrdering { *v })
    }
}

impl FetchStepContainer for dtos::FetchStepContainer {
    fn image(&self) -> &str {
        self.image.as_ref()
    }
    fn command(&self) -> Option<Box<dyn Iterator<Item = &str> + '_>> {
        self.command
            .as_ref()
            .map(|v| -> Box<dyn Iterator<Item = &str> + '_> {
                Box::new(v.iter().map(|i| -> &str { i.as_ref() }))
            })
    }
    fn args(&self) -> Option<Box<dyn Iterator<Item = &str> + '_>> {
        self.args
            .as_ref()
            .map(|v| -> Box<dyn Iterator<Item = &str> + '_> {
                Box::new(v.iter().map(|i| -> &str { i.as_ref() }))
            })
    }
    fn env(&self) -> Option<Box<dyn Iterator<Item = &dyn EnvVar> + '_>> {
        self.env
            .as_ref()
            .map(|v| -> Box<dyn Iterator<Item = &dyn EnvVar> + '_> {
                Box::new(v.iter().map(|i| -> &dyn EnvVar { i }))
            })
    }
}

impl FetchStepMqtt for dtos::FetchStepMqtt {
    fn host(&self) -> &str {
        self.host.as_ref()
    }
    fn port(&self) -> i32 {
        self.port
    }
    fn username(&self) -> Option<&str> {
        self.username.as_ref().map(|v| -> &str { v.as_ref() })
    }
    fn password(&self) -> Option<&str> {
        self.password.as_ref().map(|v| -> &str { v.as_ref() })
    }
    fn topics(&self) -> Box<dyn Iterator<Item = &dyn MqttTopicSubscription> + '_> {
        Box::new(
            self.topics
                .iter()
                .map(|i| -> &dyn MqttTopicSubscription { i }),
        )
    }
}

impl FetchStepEthereumLogs for dtos::FetchStepEthereumLogs {
    fn chain_id(&self) -> Option<u64> {
        self.chain_id.as_ref().map(|v| -> u64 { *v })
    }
    fn node_url(&self) -> Option<&str> {
        self.node_url.as_ref().map(|v| -> &str { v.as_ref() })
    }
    fn filter(&self) -> Option<&str> {
        self.filter.as_ref().map(|v| -> &str { v.as_ref() })
    }
    fn signature(&self) -> Option<&str> {
        self.signature.as_ref().map(|v| -> &str { v.as_ref() })
    }
}

impl Into<dtos::FetchStepUrl> for &dyn FetchStepUrl {
    fn into(self) -> dtos::FetchStepUrl {
        dtos::FetchStepUrl {
            url: self.url().to_owned(),
            event_time: self.event_time().map(|v| v.into()),
            cache: self.cache().map(|v| v.into()),
            headers: self.headers().map(|v| v.map(|i| i.into()).collect()),
        }
    }
}

impl Into<dtos::FetchStepFilesGlob> for &dyn FetchStepFilesGlob {
    fn into(self) -> dtos::FetchStepFilesGlob {
        dtos::FetchStepFilesGlob {
            path: self.path().to_owned(),
            event_time: self.event_time().map(|v| v.into()),
            cache: self.cache().map(|v| v.into()),
            order: self.order().map(|v| v.into()),
        }
    }
}

impl Into<dtos::FetchStepContainer> for &dyn FetchStepContainer {
    fn into(self) -> dtos::FetchStepContainer {
        dtos::FetchStepContainer {
            image: self.image().to_owned(),
            command: self.command().map(|v| v.map(|i| i.to_owned()).collect()),
            args: self.args().map(|v| v.map(|i| i.to_owned()).collect()),
            env: self.env().map(|v| v.map(|i| i.into()).collect()),
        }
    }
}

impl Into<dtos::FetchStepMqtt> for &dyn FetchStepMqtt {
    fn into(self) -> dtos::FetchStepMqtt {
        dtos::FetchStepMqtt {
            host: self.host().to_owned(),
            port: self.port(),
            username: self.username().map(|v| v.to_owned()),
            password: self.password().map(|v| v.to_owned()),
            topics: self.topics().map(|i| i.into()).collect(),
        }
    }
}

impl Into<dtos::FetchStepEthereumLogs> for &dyn FetchStepEthereumLogs {
    fn into(self) -> dtos::FetchStepEthereumLogs {
        dtos::FetchStepEthereumLogs {
            chain_id: self.chain_id().map(|v| v),
            node_url: self.node_url().map(|v| v.to_owned()),
            filter: self.filter().map(|v| v.to_owned()),
            signature: self.signature().map(|v| v.to_owned()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// MergeStrategy
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mergestrategy-schema
////////////////////////////////////////////////////////////////////////////////

pub enum MergeStrategy<'a> {
    Append(&'a dyn MergeStrategyAppend),
    Ledger(&'a dyn MergeStrategyLedger),
    Snapshot(&'a dyn MergeStrategySnapshot),
}

impl<'a> From<&'a dtos::MergeStrategy> for MergeStrategy<'a> {
    fn from(other: &'a dtos::MergeStrategy) -> Self {
        match other {
            dtos::MergeStrategy::Append(v) => MergeStrategy::Append(v),
            dtos::MergeStrategy::Ledger(v) => MergeStrategy::Ledger(v),
            dtos::MergeStrategy::Snapshot(v) => MergeStrategy::Snapshot(v),
        }
    }
}

impl Into<dtos::MergeStrategy> for MergeStrategy<'_> {
    fn into(self) -> dtos::MergeStrategy {
        match self {
            MergeStrategy::Append(v) => dtos::MergeStrategy::Append(v.into()),
            MergeStrategy::Ledger(v) => dtos::MergeStrategy::Ledger(v.into()),
            MergeStrategy::Snapshot(v) => dtos::MergeStrategy::Snapshot(v.into()),
        }
    }
}

pub trait MergeStrategyAppend {}

pub trait MergeStrategyLedger {
    fn primary_key(&self) -> Box<dyn Iterator<Item = &str> + '_>;
}

pub trait MergeStrategySnapshot {
    fn primary_key(&self) -> Box<dyn Iterator<Item = &str> + '_>;
    fn compare_columns(&self) -> Option<Box<dyn Iterator<Item = &str> + '_>>;
}

impl MergeStrategyAppend for dtos::MergeStrategyAppend {}

impl MergeStrategyLedger for dtos::MergeStrategyLedger {
    fn primary_key(&self) -> Box<dyn Iterator<Item = &str> + '_> {
        Box::new(self.primary_key.iter().map(|i| -> &str { i.as_ref() }))
    }
}

impl MergeStrategySnapshot for dtos::MergeStrategySnapshot {
    fn primary_key(&self) -> Box<dyn Iterator<Item = &str> + '_> {
        Box::new(self.primary_key.iter().map(|i| -> &str { i.as_ref() }))
    }
    fn compare_columns(&self) -> Option<Box<dyn Iterator<Item = &str> + '_>> {
        self.compare_columns
            .as_ref()
            .map(|v| -> Box<dyn Iterator<Item = &str> + '_> {
                Box::new(v.iter().map(|i| -> &str { i.as_ref() }))
            })
    }
}

impl Into<dtos::MergeStrategyAppend> for &dyn MergeStrategyAppend {
    fn into(self) -> dtos::MergeStrategyAppend {
        dtos::MergeStrategyAppend {}
    }
}

impl Into<dtos::MergeStrategyLedger> for &dyn MergeStrategyLedger {
    fn into(self) -> dtos::MergeStrategyLedger {
        dtos::MergeStrategyLedger {
            primary_key: self.primary_key().map(|i| i.to_owned()).collect(),
        }
    }
}

impl Into<dtos::MergeStrategySnapshot> for &dyn MergeStrategySnapshot {
    fn into(self) -> dtos::MergeStrategySnapshot {
        dtos::MergeStrategySnapshot {
            primary_key: self.primary_key().map(|i| i.to_owned()).collect(),
            compare_columns: self
                .compare_columns()
                .map(|v| v.map(|i| i.to_owned()).collect()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// MetadataBlock
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#metadatablock-schema
////////////////////////////////////////////////////////////////////////////////

pub trait MetadataBlock {
    fn system_time(&self) -> DateTime<Utc>;
    fn prev_block_hash(&self) -> Option<&Multihash>;
    fn sequence_number(&self) -> u64;
    fn event(&self) -> MetadataEvent;
}

impl MetadataBlock for dtos::MetadataBlock {
    fn system_time(&self) -> DateTime<Utc> {
        self.system_time
    }
    fn prev_block_hash(&self) -> Option<&Multihash> {
        self.prev_block_hash.as_ref().map(|v| -> &Multihash { v })
    }
    fn sequence_number(&self) -> u64 {
        self.sequence_number
    }
    fn event(&self) -> MetadataEvent {
        (&self.event).into()
    }
}

impl Into<dtos::MetadataBlock> for &dyn MetadataBlock {
    fn into(self) -> dtos::MetadataBlock {
        dtos::MetadataBlock {
            system_time: self.system_time(),
            prev_block_hash: self.prev_block_hash().map(|v| v.clone()),
            sequence_number: self.sequence_number(),
            event: self.event().into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// MetadataEvent
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#metadataevent-schema
////////////////////////////////////////////////////////////////////////////////

pub enum MetadataEvent<'a> {
    AddData(&'a dyn AddData),
    ExecuteTransform(&'a dyn ExecuteTransform),
    Seed(&'a dyn Seed),
    SetPollingSource(&'a dyn SetPollingSource),
    SetTransform(&'a dyn SetTransform),
    SetVocab(&'a dyn SetVocab),
    SetAttachments(&'a dyn SetAttachments),
    SetInfo(&'a dyn SetInfo),
    SetLicense(&'a dyn SetLicense),
    SetDataSchema(&'a dyn SetDataSchema),
    AddPushSource(&'a dyn AddPushSource),
    DisablePushSource(&'a dyn DisablePushSource),
    DisablePollingSource(&'a dyn DisablePollingSource),
}

impl<'a> From<&'a dtos::MetadataEvent> for MetadataEvent<'a> {
    fn from(other: &'a dtos::MetadataEvent) -> Self {
        match other {
            dtos::MetadataEvent::AddData(v) => MetadataEvent::AddData(v),
            dtos::MetadataEvent::ExecuteTransform(v) => MetadataEvent::ExecuteTransform(v),
            dtos::MetadataEvent::Seed(v) => MetadataEvent::Seed(v),
            dtos::MetadataEvent::SetPollingSource(v) => MetadataEvent::SetPollingSource(v),
            dtos::MetadataEvent::SetTransform(v) => MetadataEvent::SetTransform(v),
            dtos::MetadataEvent::SetVocab(v) => MetadataEvent::SetVocab(v),
            dtos::MetadataEvent::SetAttachments(v) => MetadataEvent::SetAttachments(v),
            dtos::MetadataEvent::SetInfo(v) => MetadataEvent::SetInfo(v),
            dtos::MetadataEvent::SetLicense(v) => MetadataEvent::SetLicense(v),
            dtos::MetadataEvent::SetDataSchema(v) => MetadataEvent::SetDataSchema(v),
            dtos::MetadataEvent::AddPushSource(v) => MetadataEvent::AddPushSource(v),
            dtos::MetadataEvent::DisablePushSource(v) => MetadataEvent::DisablePushSource(v),
            dtos::MetadataEvent::DisablePollingSource(v) => MetadataEvent::DisablePollingSource(v),
        }
    }
}

impl Into<dtos::MetadataEvent> for MetadataEvent<'_> {
    fn into(self) -> dtos::MetadataEvent {
        match self {
            MetadataEvent::AddData(v) => dtos::MetadataEvent::AddData(v.into()),
            MetadataEvent::ExecuteTransform(v) => dtos::MetadataEvent::ExecuteTransform(v.into()),
            MetadataEvent::Seed(v) => dtos::MetadataEvent::Seed(v.into()),
            MetadataEvent::SetPollingSource(v) => dtos::MetadataEvent::SetPollingSource(v.into()),
            MetadataEvent::SetTransform(v) => dtos::MetadataEvent::SetTransform(v.into()),
            MetadataEvent::SetVocab(v) => dtos::MetadataEvent::SetVocab(v.into()),
            MetadataEvent::SetAttachments(v) => dtos::MetadataEvent::SetAttachments(v.into()),
            MetadataEvent::SetInfo(v) => dtos::MetadataEvent::SetInfo(v.into()),
            MetadataEvent::SetLicense(v) => dtos::MetadataEvent::SetLicense(v.into()),
            MetadataEvent::SetDataSchema(v) => dtos::MetadataEvent::SetDataSchema(v.into()),
            MetadataEvent::AddPushSource(v) => dtos::MetadataEvent::AddPushSource(v.into()),
            MetadataEvent::DisablePushSource(v) => dtos::MetadataEvent::DisablePushSource(v.into()),
            MetadataEvent::DisablePollingSource(v) => {
                dtos::MetadataEvent::DisablePollingSource(v.into())
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// MqttQos
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mqttqos-schema
////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////
// MqttTopicSubscription
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mqtttopicsubscription-schema
////////////////////////////////////////////////////////////////////////////////

pub trait MqttTopicSubscription {
    fn path(&self) -> &str;
    fn qos(&self) -> Option<MqttQos>;
}

impl MqttTopicSubscription for dtos::MqttTopicSubscription {
    fn path(&self) -> &str {
        self.path.as_ref()
    }
    fn qos(&self) -> Option<MqttQos> {
        self.qos.as_ref().map(|v| -> MqttQos { *v })
    }
}

impl Into<dtos::MqttTopicSubscription> for &dyn MqttTopicSubscription {
    fn into(self) -> dtos::MqttTopicSubscription {
        dtos::MqttTopicSubscription {
            path: self.path().to_owned(),
            qos: self.qos().map(|v| v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// OffsetInterval
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#offsetinterval-schema
////////////////////////////////////////////////////////////////////////////////

pub trait OffsetInterval {
    fn start(&self) -> u64;
    fn end(&self) -> u64;
}

impl OffsetInterval for dtos::OffsetInterval {
    fn start(&self) -> u64 {
        self.start
    }
    fn end(&self) -> u64 {
        self.end
    }
}

impl Into<dtos::OffsetInterval> for &dyn OffsetInterval {
    fn into(self) -> dtos::OffsetInterval {
        dtos::OffsetInterval {
            start: self.start(),
            end: self.end(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// PrepStep
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#prepstep-schema
////////////////////////////////////////////////////////////////////////////////

pub enum PrepStep<'a> {
    Decompress(&'a dyn PrepStepDecompress),
    Pipe(&'a dyn PrepStepPipe),
}

impl<'a> From<&'a dtos::PrepStep> for PrepStep<'a> {
    fn from(other: &'a dtos::PrepStep) -> Self {
        match other {
            dtos::PrepStep::Decompress(v) => PrepStep::Decompress(v),
            dtos::PrepStep::Pipe(v) => PrepStep::Pipe(v),
        }
    }
}

impl Into<dtos::PrepStep> for PrepStep<'_> {
    fn into(self) -> dtos::PrepStep {
        match self {
            PrepStep::Decompress(v) => dtos::PrepStep::Decompress(v.into()),
            PrepStep::Pipe(v) => dtos::PrepStep::Pipe(v.into()),
        }
    }
}

pub trait PrepStepDecompress {
    fn format(&self) -> CompressionFormat;
    fn sub_path(&self) -> Option<&str>;
}

pub trait PrepStepPipe {
    fn command(&self) -> Box<dyn Iterator<Item = &str> + '_>;
}

impl PrepStepDecompress for dtos::PrepStepDecompress {
    fn format(&self) -> CompressionFormat {
        self.format
    }
    fn sub_path(&self) -> Option<&str> {
        self.sub_path.as_ref().map(|v| -> &str { v.as_ref() })
    }
}

impl PrepStepPipe for dtos::PrepStepPipe {
    fn command(&self) -> Box<dyn Iterator<Item = &str> + '_> {
        Box::new(self.command.iter().map(|i| -> &str { i.as_ref() }))
    }
}

impl Into<dtos::PrepStepDecompress> for &dyn PrepStepDecompress {
    fn into(self) -> dtos::PrepStepDecompress {
        dtos::PrepStepDecompress {
            format: self.format().into(),
            sub_path: self.sub_path().map(|v| v.to_owned()),
        }
    }
}

impl Into<dtos::PrepStepPipe> for &dyn PrepStepPipe {
    fn into(self) -> dtos::PrepStepPipe {
        dtos::PrepStepPipe {
            command: self.command().map(|i| i.to_owned()).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// RawQueryRequest
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#rawqueryrequest-schema
////////////////////////////////////////////////////////////////////////////////

pub trait RawQueryRequest {
    fn input_data_paths(&self) -> Box<dyn Iterator<Item = &Path> + '_>;
    fn transform(&self) -> Transform;
    fn output_data_path(&self) -> &Path;
}

impl RawQueryRequest for dtos::RawQueryRequest {
    fn input_data_paths(&self) -> Box<dyn Iterator<Item = &Path> + '_> {
        Box::new(
            self.input_data_paths
                .iter()
                .map(|i| -> &Path { i.as_ref() }),
        )
    }
    fn transform(&self) -> Transform {
        (&self.transform).into()
    }
    fn output_data_path(&self) -> &Path {
        self.output_data_path.as_ref()
    }
}

impl Into<dtos::RawQueryRequest> for &dyn RawQueryRequest {
    fn into(self) -> dtos::RawQueryRequest {
        dtos::RawQueryRequest {
            input_data_paths: self.input_data_paths().map(|i| i.to_owned()).collect(),
            transform: self.transform().into(),
            output_data_path: self.output_data_path().to_owned(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// RawQueryResponse
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#rawqueryresponse-schema
////////////////////////////////////////////////////////////////////////////////

pub enum RawQueryResponse<'a> {
    Progress(&'a dyn RawQueryResponseProgress),
    Success(&'a dyn RawQueryResponseSuccess),
    InvalidQuery(&'a dyn RawQueryResponseInvalidQuery),
    InternalError(&'a dyn RawQueryResponseInternalError),
}

impl<'a> From<&'a dtos::RawQueryResponse> for RawQueryResponse<'a> {
    fn from(other: &'a dtos::RawQueryResponse) -> Self {
        match other {
            dtos::RawQueryResponse::Progress(v) => RawQueryResponse::Progress(v),
            dtos::RawQueryResponse::Success(v) => RawQueryResponse::Success(v),
            dtos::RawQueryResponse::InvalidQuery(v) => RawQueryResponse::InvalidQuery(v),
            dtos::RawQueryResponse::InternalError(v) => RawQueryResponse::InternalError(v),
        }
    }
}

impl Into<dtos::RawQueryResponse> for RawQueryResponse<'_> {
    fn into(self) -> dtos::RawQueryResponse {
        match self {
            RawQueryResponse::Progress(v) => dtos::RawQueryResponse::Progress(v.into()),
            RawQueryResponse::Success(v) => dtos::RawQueryResponse::Success(v.into()),
            RawQueryResponse::InvalidQuery(v) => dtos::RawQueryResponse::InvalidQuery(v.into()),
            RawQueryResponse::InternalError(v) => dtos::RawQueryResponse::InternalError(v.into()),
        }
    }
}

pub trait RawQueryResponseProgress {}

pub trait RawQueryResponseSuccess {
    fn num_records(&self) -> u64;
}

pub trait RawQueryResponseInvalidQuery {
    fn message(&self) -> &str;
}

pub trait RawQueryResponseInternalError {
    fn message(&self) -> &str;
    fn backtrace(&self) -> Option<&str>;
}

impl RawQueryResponseProgress for dtos::RawQueryResponseProgress {}

impl RawQueryResponseSuccess for dtos::RawQueryResponseSuccess {
    fn num_records(&self) -> u64 {
        self.num_records
    }
}

impl RawQueryResponseInvalidQuery for dtos::RawQueryResponseInvalidQuery {
    fn message(&self) -> &str {
        self.message.as_ref()
    }
}

impl RawQueryResponseInternalError for dtos::RawQueryResponseInternalError {
    fn message(&self) -> &str {
        self.message.as_ref()
    }
    fn backtrace(&self) -> Option<&str> {
        self.backtrace.as_ref().map(|v| -> &str { v.as_ref() })
    }
}

impl Into<dtos::RawQueryResponseProgress> for &dyn RawQueryResponseProgress {
    fn into(self) -> dtos::RawQueryResponseProgress {
        dtos::RawQueryResponseProgress {}
    }
}

impl Into<dtos::RawQueryResponseSuccess> for &dyn RawQueryResponseSuccess {
    fn into(self) -> dtos::RawQueryResponseSuccess {
        dtos::RawQueryResponseSuccess {
            num_records: self.num_records(),
        }
    }
}

impl Into<dtos::RawQueryResponseInvalidQuery> for &dyn RawQueryResponseInvalidQuery {
    fn into(self) -> dtos::RawQueryResponseInvalidQuery {
        dtos::RawQueryResponseInvalidQuery {
            message: self.message().to_owned(),
        }
    }
}

impl Into<dtos::RawQueryResponseInternalError> for &dyn RawQueryResponseInternalError {
    fn into(self) -> dtos::RawQueryResponseInternalError {
        dtos::RawQueryResponseInternalError {
            message: self.message().to_owned(),
            backtrace: self.backtrace().map(|v| v.to_owned()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// ReadStep
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#readstep-schema
////////////////////////////////////////////////////////////////////////////////

pub enum ReadStep<'a> {
    Csv(&'a dyn ReadStepCsv),
    GeoJson(&'a dyn ReadStepGeoJson),
    EsriShapefile(&'a dyn ReadStepEsriShapefile),
    Parquet(&'a dyn ReadStepParquet),
    Json(&'a dyn ReadStepJson),
    NdJson(&'a dyn ReadStepNdJson),
    NdGeoJson(&'a dyn ReadStepNdGeoJson),
}

impl<'a> From<&'a dtos::ReadStep> for ReadStep<'a> {
    fn from(other: &'a dtos::ReadStep) -> Self {
        match other {
            dtos::ReadStep::Csv(v) => ReadStep::Csv(v),
            dtos::ReadStep::GeoJson(v) => ReadStep::GeoJson(v),
            dtos::ReadStep::EsriShapefile(v) => ReadStep::EsriShapefile(v),
            dtos::ReadStep::Parquet(v) => ReadStep::Parquet(v),
            dtos::ReadStep::Json(v) => ReadStep::Json(v),
            dtos::ReadStep::NdJson(v) => ReadStep::NdJson(v),
            dtos::ReadStep::NdGeoJson(v) => ReadStep::NdGeoJson(v),
        }
    }
}

impl Into<dtos::ReadStep> for ReadStep<'_> {
    fn into(self) -> dtos::ReadStep {
        match self {
            ReadStep::Csv(v) => dtos::ReadStep::Csv(v.into()),
            ReadStep::GeoJson(v) => dtos::ReadStep::GeoJson(v.into()),
            ReadStep::EsriShapefile(v) => dtos::ReadStep::EsriShapefile(v.into()),
            ReadStep::Parquet(v) => dtos::ReadStep::Parquet(v.into()),
            ReadStep::Json(v) => dtos::ReadStep::Json(v.into()),
            ReadStep::NdJson(v) => dtos::ReadStep::NdJson(v.into()),
            ReadStep::NdGeoJson(v) => dtos::ReadStep::NdGeoJson(v.into()),
        }
    }
}

pub trait ReadStepCsv {
    fn schema(&self) -> Option<Box<dyn Iterator<Item = &str> + '_>>;
    fn separator(&self) -> Option<&str>;
    fn encoding(&self) -> Option<&str>;
    fn quote(&self) -> Option<&str>;
    fn escape(&self) -> Option<&str>;
    fn header(&self) -> Option<bool>;
    fn infer_schema(&self) -> Option<bool>;
    fn null_value(&self) -> Option<&str>;
    fn date_format(&self) -> Option<&str>;
    fn timestamp_format(&self) -> Option<&str>;
}

pub trait ReadStepGeoJson {
    fn schema(&self) -> Option<Box<dyn Iterator<Item = &str> + '_>>;
}

pub trait ReadStepEsriShapefile {
    fn schema(&self) -> Option<Box<dyn Iterator<Item = &str> + '_>>;
    fn sub_path(&self) -> Option<&str>;
}

pub trait ReadStepParquet {
    fn schema(&self) -> Option<Box<dyn Iterator<Item = &str> + '_>>;
}

pub trait ReadStepJson {
    fn sub_path(&self) -> Option<&str>;
    fn schema(&self) -> Option<Box<dyn Iterator<Item = &str> + '_>>;
    fn date_format(&self) -> Option<&str>;
    fn encoding(&self) -> Option<&str>;
    fn timestamp_format(&self) -> Option<&str>;
}

pub trait ReadStepNdJson {
    fn schema(&self) -> Option<Box<dyn Iterator<Item = &str> + '_>>;
    fn date_format(&self) -> Option<&str>;
    fn encoding(&self) -> Option<&str>;
    fn timestamp_format(&self) -> Option<&str>;
}

pub trait ReadStepNdGeoJson {
    fn schema(&self) -> Option<Box<dyn Iterator<Item = &str> + '_>>;
}

impl ReadStepCsv for dtos::ReadStepCsv {
    fn schema(&self) -> Option<Box<dyn Iterator<Item = &str> + '_>> {
        self.schema
            .as_ref()
            .map(|v| -> Box<dyn Iterator<Item = &str> + '_> {
                Box::new(v.iter().map(|i| -> &str { i.as_ref() }))
            })
    }
    fn separator(&self) -> Option<&str> {
        self.separator.as_ref().map(|v| -> &str { v.as_ref() })
    }
    fn encoding(&self) -> Option<&str> {
        self.encoding.as_ref().map(|v| -> &str { v.as_ref() })
    }
    fn quote(&self) -> Option<&str> {
        self.quote.as_ref().map(|v| -> &str { v.as_ref() })
    }
    fn escape(&self) -> Option<&str> {
        self.escape.as_ref().map(|v| -> &str { v.as_ref() })
    }
    fn header(&self) -> Option<bool> {
        self.header.as_ref().map(|v| -> bool { *v })
    }
    fn infer_schema(&self) -> Option<bool> {
        self.infer_schema.as_ref().map(|v| -> bool { *v })
    }
    fn null_value(&self) -> Option<&str> {
        self.null_value.as_ref().map(|v| -> &str { v.as_ref() })
    }
    fn date_format(&self) -> Option<&str> {
        self.date_format.as_ref().map(|v| -> &str { v.as_ref() })
    }
    fn timestamp_format(&self) -> Option<&str> {
        self.timestamp_format
            .as_ref()
            .map(|v| -> &str { v.as_ref() })
    }
}

impl ReadStepGeoJson for dtos::ReadStepGeoJson {
    fn schema(&self) -> Option<Box<dyn Iterator<Item = &str> + '_>> {
        self.schema
            .as_ref()
            .map(|v| -> Box<dyn Iterator<Item = &str> + '_> {
                Box::new(v.iter().map(|i| -> &str { i.as_ref() }))
            })
    }
}

impl ReadStepEsriShapefile for dtos::ReadStepEsriShapefile {
    fn schema(&self) -> Option<Box<dyn Iterator<Item = &str> + '_>> {
        self.schema
            .as_ref()
            .map(|v| -> Box<dyn Iterator<Item = &str> + '_> {
                Box::new(v.iter().map(|i| -> &str { i.as_ref() }))
            })
    }
    fn sub_path(&self) -> Option<&str> {
        self.sub_path.as_ref().map(|v| -> &str { v.as_ref() })
    }
}

impl ReadStepParquet for dtos::ReadStepParquet {
    fn schema(&self) -> Option<Box<dyn Iterator<Item = &str> + '_>> {
        self.schema
            .as_ref()
            .map(|v| -> Box<dyn Iterator<Item = &str> + '_> {
                Box::new(v.iter().map(|i| -> &str { i.as_ref() }))
            })
    }
}

impl ReadStepJson for dtos::ReadStepJson {
    fn sub_path(&self) -> Option<&str> {
        self.sub_path.as_ref().map(|v| -> &str { v.as_ref() })
    }
    fn schema(&self) -> Option<Box<dyn Iterator<Item = &str> + '_>> {
        self.schema
            .as_ref()
            .map(|v| -> Box<dyn Iterator<Item = &str> + '_> {
                Box::new(v.iter().map(|i| -> &str { i.as_ref() }))
            })
    }
    fn date_format(&self) -> Option<&str> {
        self.date_format.as_ref().map(|v| -> &str { v.as_ref() })
    }
    fn encoding(&self) -> Option<&str> {
        self.encoding.as_ref().map(|v| -> &str { v.as_ref() })
    }
    fn timestamp_format(&self) -> Option<&str> {
        self.timestamp_format
            .as_ref()
            .map(|v| -> &str { v.as_ref() })
    }
}

impl ReadStepNdJson for dtos::ReadStepNdJson {
    fn schema(&self) -> Option<Box<dyn Iterator<Item = &str> + '_>> {
        self.schema
            .as_ref()
            .map(|v| -> Box<dyn Iterator<Item = &str> + '_> {
                Box::new(v.iter().map(|i| -> &str { i.as_ref() }))
            })
    }
    fn date_format(&self) -> Option<&str> {
        self.date_format.as_ref().map(|v| -> &str { v.as_ref() })
    }
    fn encoding(&self) -> Option<&str> {
        self.encoding.as_ref().map(|v| -> &str { v.as_ref() })
    }
    fn timestamp_format(&self) -> Option<&str> {
        self.timestamp_format
            .as_ref()
            .map(|v| -> &str { v.as_ref() })
    }
}

impl ReadStepNdGeoJson for dtos::ReadStepNdGeoJson {
    fn schema(&self) -> Option<Box<dyn Iterator<Item = &str> + '_>> {
        self.schema
            .as_ref()
            .map(|v| -> Box<dyn Iterator<Item = &str> + '_> {
                Box::new(v.iter().map(|i| -> &str { i.as_ref() }))
            })
    }
}

impl Into<dtos::ReadStepCsv> for &dyn ReadStepCsv {
    fn into(self) -> dtos::ReadStepCsv {
        dtos::ReadStepCsv {
            schema: self.schema().map(|v| v.map(|i| i.to_owned()).collect()),
            separator: self.separator().map(|v| v.to_owned()),
            encoding: self.encoding().map(|v| v.to_owned()),
            quote: self.quote().map(|v| v.to_owned()),
            escape: self.escape().map(|v| v.to_owned()),
            header: self.header().map(|v| v),
            infer_schema: self.infer_schema().map(|v| v),
            null_value: self.null_value().map(|v| v.to_owned()),
            date_format: self.date_format().map(|v| v.to_owned()),
            timestamp_format: self.timestamp_format().map(|v| v.to_owned()),
        }
    }
}

impl Into<dtos::ReadStepGeoJson> for &dyn ReadStepGeoJson {
    fn into(self) -> dtos::ReadStepGeoJson {
        dtos::ReadStepGeoJson {
            schema: self.schema().map(|v| v.map(|i| i.to_owned()).collect()),
        }
    }
}

impl Into<dtos::ReadStepEsriShapefile> for &dyn ReadStepEsriShapefile {
    fn into(self) -> dtos::ReadStepEsriShapefile {
        dtos::ReadStepEsriShapefile {
            schema: self.schema().map(|v| v.map(|i| i.to_owned()).collect()),
            sub_path: self.sub_path().map(|v| v.to_owned()),
        }
    }
}

impl Into<dtos::ReadStepParquet> for &dyn ReadStepParquet {
    fn into(self) -> dtos::ReadStepParquet {
        dtos::ReadStepParquet {
            schema: self.schema().map(|v| v.map(|i| i.to_owned()).collect()),
        }
    }
}

impl Into<dtos::ReadStepJson> for &dyn ReadStepJson {
    fn into(self) -> dtos::ReadStepJson {
        dtos::ReadStepJson {
            sub_path: self.sub_path().map(|v| v.to_owned()),
            schema: self.schema().map(|v| v.map(|i| i.to_owned()).collect()),
            date_format: self.date_format().map(|v| v.to_owned()),
            encoding: self.encoding().map(|v| v.to_owned()),
            timestamp_format: self.timestamp_format().map(|v| v.to_owned()),
        }
    }
}

impl Into<dtos::ReadStepNdJson> for &dyn ReadStepNdJson {
    fn into(self) -> dtos::ReadStepNdJson {
        dtos::ReadStepNdJson {
            schema: self.schema().map(|v| v.map(|i| i.to_owned()).collect()),
            date_format: self.date_format().map(|v| v.to_owned()),
            encoding: self.encoding().map(|v| v.to_owned()),
            timestamp_format: self.timestamp_format().map(|v| v.to_owned()),
        }
    }
}

impl Into<dtos::ReadStepNdGeoJson> for &dyn ReadStepNdGeoJson {
    fn into(self) -> dtos::ReadStepNdGeoJson {
        dtos::ReadStepNdGeoJson {
            schema: self.schema().map(|v| v.map(|i| i.to_owned()).collect()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// RequestHeader
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#requestheader-schema
////////////////////////////////////////////////////////////////////////////////

pub trait RequestHeader {
    fn name(&self) -> &str;
    fn value(&self) -> &str;
}

impl RequestHeader for dtos::RequestHeader {
    fn name(&self) -> &str {
        self.name.as_ref()
    }
    fn value(&self) -> &str {
        self.value.as_ref()
    }
}

impl Into<dtos::RequestHeader> for &dyn RequestHeader {
    fn into(self) -> dtos::RequestHeader {
        dtos::RequestHeader {
            name: self.name().to_owned(),
            value: self.value().to_owned(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Seed
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#seed-schema
////////////////////////////////////////////////////////////////////////////////

pub trait Seed {
    fn dataset_id(&self) -> &DatasetID;
    fn dataset_kind(&self) -> DatasetKind;
}

impl Seed for dtos::Seed {
    fn dataset_id(&self) -> &DatasetID {
        &self.dataset_id
    }
    fn dataset_kind(&self) -> DatasetKind {
        self.dataset_kind
    }
}

impl Into<dtos::Seed> for &dyn Seed {
    fn into(self) -> dtos::Seed {
        dtos::Seed {
            dataset_id: self.dataset_id().clone(),
            dataset_kind: self.dataset_kind().into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// SetAttachments
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setattachments-schema
////////////////////////////////////////////////////////////////////////////////

pub trait SetAttachments {
    fn attachments(&self) -> Attachments;
}

impl SetAttachments for dtos::SetAttachments {
    fn attachments(&self) -> Attachments {
        (&self.attachments).into()
    }
}

impl Into<dtos::SetAttachments> for &dyn SetAttachments {
    fn into(self) -> dtos::SetAttachments {
        dtos::SetAttachments {
            attachments: self.attachments().into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// SetDataSchema
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setdataschema-schema
////////////////////////////////////////////////////////////////////////////////

pub trait SetDataSchema {
    fn schema(&self) -> &[u8];
}

impl SetDataSchema for dtos::SetDataSchema {
    fn schema(&self) -> &[u8] {
        &self.schema
    }
}

impl Into<dtos::SetDataSchema> for &dyn SetDataSchema {
    fn into(self) -> dtos::SetDataSchema {
        dtos::SetDataSchema {
            schema: self.schema().to_vec(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// SetInfo
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setinfo-schema
////////////////////////////////////////////////////////////////////////////////

pub trait SetInfo {
    fn description(&self) -> Option<&str>;
    fn keywords(&self) -> Option<Box<dyn Iterator<Item = &str> + '_>>;
}

impl SetInfo for dtos::SetInfo {
    fn description(&self) -> Option<&str> {
        self.description.as_ref().map(|v| -> &str { v.as_ref() })
    }
    fn keywords(&self) -> Option<Box<dyn Iterator<Item = &str> + '_>> {
        self.keywords
            .as_ref()
            .map(|v| -> Box<dyn Iterator<Item = &str> + '_> {
                Box::new(v.iter().map(|i| -> &str { i.as_ref() }))
            })
    }
}

impl Into<dtos::SetInfo> for &dyn SetInfo {
    fn into(self) -> dtos::SetInfo {
        dtos::SetInfo {
            description: self.description().map(|v| v.to_owned()),
            keywords: self.keywords().map(|v| v.map(|i| i.to_owned()).collect()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// SetLicense
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setlicense-schema
////////////////////////////////////////////////////////////////////////////////

pub trait SetLicense {
    fn short_name(&self) -> &str;
    fn name(&self) -> &str;
    fn spdx_id(&self) -> Option<&str>;
    fn website_url(&self) -> &str;
}

impl SetLicense for dtos::SetLicense {
    fn short_name(&self) -> &str {
        self.short_name.as_ref()
    }
    fn name(&self) -> &str {
        self.name.as_ref()
    }
    fn spdx_id(&self) -> Option<&str> {
        self.spdx_id.as_ref().map(|v| -> &str { v.as_ref() })
    }
    fn website_url(&self) -> &str {
        self.website_url.as_ref()
    }
}

impl Into<dtos::SetLicense> for &dyn SetLicense {
    fn into(self) -> dtos::SetLicense {
        dtos::SetLicense {
            short_name: self.short_name().to_owned(),
            name: self.name().to_owned(),
            spdx_id: self.spdx_id().map(|v| v.to_owned()),
            website_url: self.website_url().to_owned(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// SetPollingSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setpollingsource-schema
////////////////////////////////////////////////////////////////////////////////

pub trait SetPollingSource {
    fn fetch(&self) -> FetchStep;
    fn prepare(&self) -> Option<Box<dyn Iterator<Item = PrepStep> + '_>>;
    fn read(&self) -> ReadStep;
    fn preprocess(&self) -> Option<Transform>;
    fn merge(&self) -> MergeStrategy;
}

impl SetPollingSource for dtos::SetPollingSource {
    fn fetch(&self) -> FetchStep {
        (&self.fetch).into()
    }
    fn prepare(&self) -> Option<Box<dyn Iterator<Item = PrepStep> + '_>> {
        self.prepare
            .as_ref()
            .map(|v| -> Box<dyn Iterator<Item = PrepStep> + '_> {
                Box::new(v.iter().map(|i| -> PrepStep { i.into() }))
            })
    }
    fn read(&self) -> ReadStep {
        (&self.read).into()
    }
    fn preprocess(&self) -> Option<Transform> {
        self.preprocess.as_ref().map(|v| -> Transform { v.into() })
    }
    fn merge(&self) -> MergeStrategy {
        (&self.merge).into()
    }
}

impl Into<dtos::SetPollingSource> for &dyn SetPollingSource {
    fn into(self) -> dtos::SetPollingSource {
        dtos::SetPollingSource {
            fetch: self.fetch().into(),
            prepare: self.prepare().map(|v| v.map(|i| i.into()).collect()),
            read: self.read().into(),
            preprocess: self.preprocess().map(|v| v.into()),
            merge: self.merge().into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// SetTransform
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#settransform-schema
////////////////////////////////////////////////////////////////////////////////

pub trait SetTransform {
    fn inputs(&self) -> Box<dyn Iterator<Item = &dyn TransformInput> + '_>;
    fn transform(&self) -> Transform;
}

impl SetTransform for dtos::SetTransform {
    fn inputs(&self) -> Box<dyn Iterator<Item = &dyn TransformInput> + '_> {
        Box::new(self.inputs.iter().map(|i| -> &dyn TransformInput { i }))
    }
    fn transform(&self) -> Transform {
        (&self.transform).into()
    }
}

impl Into<dtos::SetTransform> for &dyn SetTransform {
    fn into(self) -> dtos::SetTransform {
        dtos::SetTransform {
            inputs: self.inputs().map(|i| i.into()).collect(),
            transform: self.transform().into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// SetVocab
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setvocab-schema
////////////////////////////////////////////////////////////////////////////////

pub trait SetVocab {
    fn offset_column(&self) -> Option<&str>;
    fn operation_type_column(&self) -> Option<&str>;
    fn system_time_column(&self) -> Option<&str>;
    fn event_time_column(&self) -> Option<&str>;
}

impl SetVocab for dtos::SetVocab {
    fn offset_column(&self) -> Option<&str> {
        self.offset_column.as_ref().map(|v| -> &str { v.as_ref() })
    }
    fn operation_type_column(&self) -> Option<&str> {
        self.operation_type_column
            .as_ref()
            .map(|v| -> &str { v.as_ref() })
    }
    fn system_time_column(&self) -> Option<&str> {
        self.system_time_column
            .as_ref()
            .map(|v| -> &str { v.as_ref() })
    }
    fn event_time_column(&self) -> Option<&str> {
        self.event_time_column
            .as_ref()
            .map(|v| -> &str { v.as_ref() })
    }
}

impl Into<dtos::SetVocab> for &dyn SetVocab {
    fn into(self) -> dtos::SetVocab {
        dtos::SetVocab {
            offset_column: self.offset_column().map(|v| v.to_owned()),
            operation_type_column: self.operation_type_column().map(|v| v.to_owned()),
            system_time_column: self.system_time_column().map(|v| v.to_owned()),
            event_time_column: self.event_time_column().map(|v| v.to_owned()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// SourceCaching
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#sourcecaching-schema
////////////////////////////////////////////////////////////////////////////////

pub enum SourceCaching<'a> {
    Forever(&'a dyn SourceCachingForever),
}

impl<'a> From<&'a dtos::SourceCaching> for SourceCaching<'a> {
    fn from(other: &'a dtos::SourceCaching) -> Self {
        match other {
            dtos::SourceCaching::Forever(v) => SourceCaching::Forever(v),
        }
    }
}

impl Into<dtos::SourceCaching> for SourceCaching<'_> {
    fn into(self) -> dtos::SourceCaching {
        match self {
            SourceCaching::Forever(v) => dtos::SourceCaching::Forever(v.into()),
        }
    }
}

pub trait SourceCachingForever {}

impl SourceCachingForever for dtos::SourceCachingForever {}

impl Into<dtos::SourceCachingForever> for &dyn SourceCachingForever {
    fn into(self) -> dtos::SourceCachingForever {
        dtos::SourceCachingForever {}
    }
}

////////////////////////////////////////////////////////////////////////////////
// SourceState
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#sourcestate-schema
////////////////////////////////////////////////////////////////////////////////

pub trait SourceState {
    fn source_name(&self) -> &str;
    fn kind(&self) -> &str;
    fn value(&self) -> &str;
}

impl SourceState for dtos::SourceState {
    fn source_name(&self) -> &str {
        self.source_name.as_ref()
    }
    fn kind(&self) -> &str {
        self.kind.as_ref()
    }
    fn value(&self) -> &str {
        self.value.as_ref()
    }
}

impl Into<dtos::SourceState> for &dyn SourceState {
    fn into(self) -> dtos::SourceState {
        dtos::SourceState {
            source_name: self.source_name().to_owned(),
            kind: self.kind().to_owned(),
            value: self.value().to_owned(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// SqlQueryStep
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#sqlquerystep-schema
////////////////////////////////////////////////////////////////////////////////

pub trait SqlQueryStep {
    fn alias(&self) -> Option<&str>;
    fn query(&self) -> &str;
}

impl SqlQueryStep for dtos::SqlQueryStep {
    fn alias(&self) -> Option<&str> {
        self.alias.as_ref().map(|v| -> &str { v.as_ref() })
    }
    fn query(&self) -> &str {
        self.query.as_ref()
    }
}

impl Into<dtos::SqlQueryStep> for &dyn SqlQueryStep {
    fn into(self) -> dtos::SqlQueryStep {
        dtos::SqlQueryStep {
            alias: self.alias().map(|v| v.to_owned()),
            query: self.query().to_owned(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// TemporalTable
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#temporaltable-schema
////////////////////////////////////////////////////////////////////////////////

pub trait TemporalTable {
    fn name(&self) -> &str;
    fn primary_key(&self) -> Box<dyn Iterator<Item = &str> + '_>;
}

impl TemporalTable for dtos::TemporalTable {
    fn name(&self) -> &str {
        self.name.as_ref()
    }
    fn primary_key(&self) -> Box<dyn Iterator<Item = &str> + '_> {
        Box::new(self.primary_key.iter().map(|i| -> &str { i.as_ref() }))
    }
}

impl Into<dtos::TemporalTable> for &dyn TemporalTable {
    fn into(self) -> dtos::TemporalTable {
        dtos::TemporalTable {
            name: self.name().to_owned(),
            primary_key: self.primary_key().map(|i| i.to_owned()).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Transform
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transform-schema
////////////////////////////////////////////////////////////////////////////////

pub enum Transform<'a> {
    Sql(&'a dyn TransformSql),
}

impl<'a> From<&'a dtos::Transform> for Transform<'a> {
    fn from(other: &'a dtos::Transform) -> Self {
        match other {
            dtos::Transform::Sql(v) => Transform::Sql(v),
        }
    }
}

impl Into<dtos::Transform> for Transform<'_> {
    fn into(self) -> dtos::Transform {
        match self {
            Transform::Sql(v) => dtos::Transform::Sql(v.into()),
        }
    }
}

pub trait TransformSql {
    fn engine(&self) -> &str;
    fn version(&self) -> Option<&str>;
    fn query(&self) -> Option<&str>;
    fn queries(&self) -> Option<Box<dyn Iterator<Item = &dyn SqlQueryStep> + '_>>;
    fn temporal_tables(&self) -> Option<Box<dyn Iterator<Item = &dyn TemporalTable> + '_>>;
}

impl TransformSql for dtos::TransformSql {
    fn engine(&self) -> &str {
        self.engine.as_ref()
    }
    fn version(&self) -> Option<&str> {
        self.version.as_ref().map(|v| -> &str { v.as_ref() })
    }
    fn query(&self) -> Option<&str> {
        self.query.as_ref().map(|v| -> &str { v.as_ref() })
    }
    fn queries(&self) -> Option<Box<dyn Iterator<Item = &dyn SqlQueryStep> + '_>> {
        self.queries
            .as_ref()
            .map(|v| -> Box<dyn Iterator<Item = &dyn SqlQueryStep> + '_> {
                Box::new(v.iter().map(|i| -> &dyn SqlQueryStep { i }))
            })
    }
    fn temporal_tables(&self) -> Option<Box<dyn Iterator<Item = &dyn TemporalTable> + '_>> {
        self.temporal_tables.as_ref().map(
            |v| -> Box<dyn Iterator<Item = &dyn TemporalTable> + '_> {
                Box::new(v.iter().map(|i| -> &dyn TemporalTable { i }))
            },
        )
    }
}

impl Into<dtos::TransformSql> for &dyn TransformSql {
    fn into(self) -> dtos::TransformSql {
        dtos::TransformSql {
            engine: self.engine().to_owned(),
            version: self.version().map(|v| v.to_owned()),
            query: self.query().map(|v| v.to_owned()),
            queries: self.queries().map(|v| v.map(|i| i.into()).collect()),
            temporal_tables: self
                .temporal_tables()
                .map(|v| v.map(|i| i.into()).collect()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// TransformInput
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transforminput-schema
////////////////////////////////////////////////////////////////////////////////

pub trait TransformInput {
    fn dataset_ref(&self) -> &DatasetRef;
    fn alias(&self) -> Option<&str>;
}

impl TransformInput for dtos::TransformInput {
    fn dataset_ref(&self) -> &DatasetRef {
        &self.dataset_ref
    }
    fn alias(&self) -> Option<&str> {
        self.alias.as_ref().map(|v| -> &str { v.as_ref() })
    }
}

impl Into<dtos::TransformInput> for &dyn TransformInput {
    fn into(self) -> dtos::TransformInput {
        dtos::TransformInput {
            dataset_ref: self.dataset_ref().to_owned(),
            alias: self.alias().map(|v| v.to_owned()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// TransformRequest
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transformrequest-schema
////////////////////////////////////////////////////////////////////////////////

pub trait TransformRequest {
    fn dataset_id(&self) -> &DatasetID;
    fn dataset_alias(&self) -> &DatasetAlias;
    fn system_time(&self) -> DateTime<Utc>;
    fn vocab(&self) -> &dyn DatasetVocabulary;
    fn transform(&self) -> Transform;
    fn query_inputs(&self) -> Box<dyn Iterator<Item = &dyn TransformRequestInput> + '_>;
    fn next_offset(&self) -> u64;
    fn prev_checkpoint_path(&self) -> Option<&Path>;
    fn new_checkpoint_path(&self) -> &Path;
    fn new_data_path(&self) -> &Path;
}

impl TransformRequest for dtos::TransformRequest {
    fn dataset_id(&self) -> &DatasetID {
        &self.dataset_id
    }
    fn dataset_alias(&self) -> &DatasetAlias {
        &self.dataset_alias
    }
    fn system_time(&self) -> DateTime<Utc> {
        self.system_time
    }
    fn vocab(&self) -> &dyn DatasetVocabulary {
        &self.vocab
    }
    fn transform(&self) -> Transform {
        (&self.transform).into()
    }
    fn query_inputs(&self) -> Box<dyn Iterator<Item = &dyn TransformRequestInput> + '_> {
        Box::new(
            self.query_inputs
                .iter()
                .map(|i| -> &dyn TransformRequestInput { i }),
        )
    }
    fn next_offset(&self) -> u64 {
        self.next_offset
    }
    fn prev_checkpoint_path(&self) -> Option<&Path> {
        self.prev_checkpoint_path
            .as_ref()
            .map(|v| -> &Path { v.as_ref() })
    }
    fn new_checkpoint_path(&self) -> &Path {
        self.new_checkpoint_path.as_ref()
    }
    fn new_data_path(&self) -> &Path {
        self.new_data_path.as_ref()
    }
}

impl Into<dtos::TransformRequest> for &dyn TransformRequest {
    fn into(self) -> dtos::TransformRequest {
        dtos::TransformRequest {
            dataset_id: self.dataset_id().clone(),
            dataset_alias: self.dataset_alias().to_owned(),
            system_time: self.system_time(),
            vocab: self.vocab().into(),
            transform: self.transform().into(),
            query_inputs: self.query_inputs().map(|i| i.into()).collect(),
            next_offset: self.next_offset(),
            prev_checkpoint_path: self.prev_checkpoint_path().map(|v| v.to_owned()),
            new_checkpoint_path: self.new_checkpoint_path().to_owned(),
            new_data_path: self.new_data_path().to_owned(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// TransformRequestInput
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transformrequestinput-schema
////////////////////////////////////////////////////////////////////////////////

pub trait TransformRequestInput {
    fn dataset_id(&self) -> &DatasetID;
    fn dataset_alias(&self) -> &DatasetAlias;
    fn query_alias(&self) -> &str;
    fn vocab(&self) -> &dyn DatasetVocabulary;
    fn offset_interval(&self) -> Option<&dyn OffsetInterval>;
    fn data_paths(&self) -> Box<dyn Iterator<Item = &Path> + '_>;
    fn schema_file(&self) -> &Path;
    fn explicit_watermarks(&self) -> Box<dyn Iterator<Item = &dyn Watermark> + '_>;
}

impl TransformRequestInput for dtos::TransformRequestInput {
    fn dataset_id(&self) -> &DatasetID {
        &self.dataset_id
    }
    fn dataset_alias(&self) -> &DatasetAlias {
        &self.dataset_alias
    }
    fn query_alias(&self) -> &str {
        self.query_alias.as_ref()
    }
    fn vocab(&self) -> &dyn DatasetVocabulary {
        &self.vocab
    }
    fn offset_interval(&self) -> Option<&dyn OffsetInterval> {
        self.offset_interval
            .as_ref()
            .map(|v| -> &dyn OffsetInterval { v })
    }
    fn data_paths(&self) -> Box<dyn Iterator<Item = &Path> + '_> {
        Box::new(self.data_paths.iter().map(|i| -> &Path { i.as_ref() }))
    }
    fn schema_file(&self) -> &Path {
        self.schema_file.as_ref()
    }
    fn explicit_watermarks(&self) -> Box<dyn Iterator<Item = &dyn Watermark> + '_> {
        Box::new(
            self.explicit_watermarks
                .iter()
                .map(|i| -> &dyn Watermark { i }),
        )
    }
}

impl Into<dtos::TransformRequestInput> for &dyn TransformRequestInput {
    fn into(self) -> dtos::TransformRequestInput {
        dtos::TransformRequestInput {
            dataset_id: self.dataset_id().clone(),
            dataset_alias: self.dataset_alias().to_owned(),
            query_alias: self.query_alias().to_owned(),
            vocab: self.vocab().into(),
            offset_interval: self.offset_interval().map(|v| v.into()),
            data_paths: self.data_paths().map(|i| i.to_owned()).collect(),
            schema_file: self.schema_file().to_owned(),
            explicit_watermarks: self.explicit_watermarks().map(|i| i.into()).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// TransformResponse
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transformresponse-schema
////////////////////////////////////////////////////////////////////////////////

pub enum TransformResponse<'a> {
    Progress(&'a dyn TransformResponseProgress),
    Success(&'a dyn TransformResponseSuccess),
    InvalidQuery(&'a dyn TransformResponseInvalidQuery),
    InternalError(&'a dyn TransformResponseInternalError),
}

impl<'a> From<&'a dtos::TransformResponse> for TransformResponse<'a> {
    fn from(other: &'a dtos::TransformResponse) -> Self {
        match other {
            dtos::TransformResponse::Progress(v) => TransformResponse::Progress(v),
            dtos::TransformResponse::Success(v) => TransformResponse::Success(v),
            dtos::TransformResponse::InvalidQuery(v) => TransformResponse::InvalidQuery(v),
            dtos::TransformResponse::InternalError(v) => TransformResponse::InternalError(v),
        }
    }
}

impl Into<dtos::TransformResponse> for TransformResponse<'_> {
    fn into(self) -> dtos::TransformResponse {
        match self {
            TransformResponse::Progress(v) => dtos::TransformResponse::Progress(v.into()),
            TransformResponse::Success(v) => dtos::TransformResponse::Success(v.into()),
            TransformResponse::InvalidQuery(v) => dtos::TransformResponse::InvalidQuery(v.into()),
            TransformResponse::InternalError(v) => dtos::TransformResponse::InternalError(v.into()),
        }
    }
}

pub trait TransformResponseProgress {}

pub trait TransformResponseSuccess {
    fn new_offset_interval(&self) -> Option<&dyn OffsetInterval>;
    fn new_watermark(&self) -> Option<DateTime<Utc>>;
}

pub trait TransformResponseInvalidQuery {
    fn message(&self) -> &str;
}

pub trait TransformResponseInternalError {
    fn message(&self) -> &str;
    fn backtrace(&self) -> Option<&str>;
}

impl TransformResponseProgress for dtos::TransformResponseProgress {}

impl TransformResponseSuccess for dtos::TransformResponseSuccess {
    fn new_offset_interval(&self) -> Option<&dyn OffsetInterval> {
        self.new_offset_interval
            .as_ref()
            .map(|v| -> &dyn OffsetInterval { v })
    }
    fn new_watermark(&self) -> Option<DateTime<Utc>> {
        self.new_watermark.as_ref().map(|v| -> DateTime<Utc> { *v })
    }
}

impl TransformResponseInvalidQuery for dtos::TransformResponseInvalidQuery {
    fn message(&self) -> &str {
        self.message.as_ref()
    }
}

impl TransformResponseInternalError for dtos::TransformResponseInternalError {
    fn message(&self) -> &str {
        self.message.as_ref()
    }
    fn backtrace(&self) -> Option<&str> {
        self.backtrace.as_ref().map(|v| -> &str { v.as_ref() })
    }
}

impl Into<dtos::TransformResponseProgress> for &dyn TransformResponseProgress {
    fn into(self) -> dtos::TransformResponseProgress {
        dtos::TransformResponseProgress {}
    }
}

impl Into<dtos::TransformResponseSuccess> for &dyn TransformResponseSuccess {
    fn into(self) -> dtos::TransformResponseSuccess {
        dtos::TransformResponseSuccess {
            new_offset_interval: self.new_offset_interval().map(|v| v.into()),
            new_watermark: self.new_watermark().map(|v| v),
        }
    }
}

impl Into<dtos::TransformResponseInvalidQuery> for &dyn TransformResponseInvalidQuery {
    fn into(self) -> dtos::TransformResponseInvalidQuery {
        dtos::TransformResponseInvalidQuery {
            message: self.message().to_owned(),
        }
    }
}

impl Into<dtos::TransformResponseInternalError> for &dyn TransformResponseInternalError {
    fn into(self) -> dtos::TransformResponseInternalError {
        dtos::TransformResponseInternalError {
            message: self.message().to_owned(),
            backtrace: self.backtrace().map(|v| v.to_owned()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Watermark
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#watermark-schema
////////////////////////////////////////////////////////////////////////////////

pub trait Watermark {
    fn system_time(&self) -> DateTime<Utc>;
    fn event_time(&self) -> DateTime<Utc>;
}

impl Watermark for dtos::Watermark {
    fn system_time(&self) -> DateTime<Utc> {
        self.system_time
    }
    fn event_time(&self) -> DateTime<Utc> {
        self.event_time
    }
}

impl Into<dtos::Watermark> for &dyn Watermark {
    fn into(self) -> dtos::Watermark {
        dtos::Watermark {
            system_time: self.system_time(),
            event_time: self.event_time(),
        }
    }
}
