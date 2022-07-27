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

use crate::dtos;
use crate::dtos::{CompressionFormat, DatasetKind, SourceOrdering};
use crate::formats::*;
use crate::identity::{DatasetID, DatasetName};
use chrono::{DateTime, Utc};
use std::path::Path;

////////////////////////////////////////////////////////////////////////////////
// AddData
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#adddata-schema
////////////////////////////////////////////////////////////////////////////////

pub trait AddData {
    fn input_checkpoint(&self) -> Option<&Multihash>;
    fn output_data(&self) -> &dyn DataSlice;
    fn output_checkpoint(&self) -> Option<&dyn Checkpoint>;
    fn output_watermark(&self) -> Option<DateTime<Utc>>;
}

impl AddData for dtos::AddData {
    fn input_checkpoint(&self) -> Option<&Multihash> {
        self.input_checkpoint.as_ref().map(|v| -> &Multihash { v })
    }
    fn output_data(&self) -> &dyn DataSlice {
        &self.output_data
    }
    fn output_checkpoint(&self) -> Option<&dyn Checkpoint> {
        self.output_checkpoint
            .as_ref()
            .map(|v| -> &dyn Checkpoint { v })
    }
    fn output_watermark(&self) -> Option<DateTime<Utc>> {
        self.output_watermark
            .as_ref()
            .map(|v| -> DateTime<Utc> { *v })
    }
}

impl Into<dtos::AddData> for &dyn AddData {
    fn into(self) -> dtos::AddData {
        dtos::AddData {
            input_checkpoint: self.input_checkpoint().map(|v| v.clone()),
            output_data: self.output_data().into(),
            output_checkpoint: self.output_checkpoint().map(|v| v.into()),
            output_watermark: self.output_watermark().map(|v| v),
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
// BlockInterval
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#blockinterval-schema
////////////////////////////////////////////////////////////////////////////////

pub trait BlockInterval {
    fn start(&self) -> &Multihash;
    fn end(&self) -> &Multihash;
}

impl BlockInterval for dtos::BlockInterval {
    fn start(&self) -> &Multihash {
        &self.start
    }
    fn end(&self) -> &Multihash {
        &self.end
    }
}

impl Into<dtos::BlockInterval> for &dyn BlockInterval {
    fn into(self) -> dtos::BlockInterval {
        dtos::BlockInterval {
            start: self.start().clone(),
            end: self.end().clone(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Checkpoint
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#checkpoint-schema
////////////////////////////////////////////////////////////////////////////////

pub trait Checkpoint {
    fn physical_hash(&self) -> &Multihash;
    fn size(&self) -> i64;
}

impl Checkpoint for dtos::Checkpoint {
    fn physical_hash(&self) -> &Multihash {
        &self.physical_hash
    }
    fn size(&self) -> i64 {
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
    fn interval(&self) -> &dyn OffsetInterval;
    fn size(&self) -> i64;
}

impl DataSlice for dtos::DataSlice {
    fn logical_hash(&self) -> &Multihash {
        &self.logical_hash
    }
    fn physical_hash(&self) -> &Multihash {
        &self.physical_hash
    }
    fn interval(&self) -> &dyn OffsetInterval {
        &self.interval
    }
    fn size(&self) -> i64 {
        self.size
    }
}

impl Into<dtos::DataSlice> for &dyn DataSlice {
    fn into(self) -> dtos::DataSlice {
        dtos::DataSlice {
            logical_hash: self.logical_hash().clone(),
            physical_hash: self.physical_hash().clone(),
            interval: self.interval().into(),
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
    fn name(&self) -> &DatasetName;
    fn kind(&self) -> DatasetKind;
    fn metadata(&self) -> Box<dyn Iterator<Item = MetadataEvent> + '_>;
}

impl DatasetSnapshot for dtos::DatasetSnapshot {
    fn name(&self) -> &DatasetName {
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
    fn system_time_column(&self) -> Option<&str>;
    fn event_time_column(&self) -> Option<&str>;
    fn offset_column(&self) -> Option<&str>;
}

impl DatasetVocabulary for dtos::DatasetVocabulary {
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
    fn offset_column(&self) -> Option<&str> {
        self.offset_column.as_ref().map(|v| -> &str { v.as_ref() })
    }
}

impl Into<dtos::DatasetVocabulary> for &dyn DatasetVocabulary {
    fn into(self) -> dtos::DatasetVocabulary {
        dtos::DatasetVocabulary {
            system_time_column: self.system_time_column().map(|v| v.to_owned()),
            event_time_column: self.event_time_column().map(|v| v.to_owned()),
            offset_column: self.offset_column().map(|v| v.to_owned()),
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
    FromMetadata,
    FromPath(&'a dyn EventTimeSourceFromPath),
}

impl<'a> From<&'a dtos::EventTimeSource> for EventTimeSource<'a> {
    fn from(other: &'a dtos::EventTimeSource) -> Self {
        match other {
            dtos::EventTimeSource::FromMetadata => EventTimeSource::FromMetadata,
            dtos::EventTimeSource::FromPath(v) => EventTimeSource::FromPath(v),
        }
    }
}

impl Into<dtos::EventTimeSource> for EventTimeSource<'_> {
    fn into(self) -> dtos::EventTimeSource {
        match self {
            EventTimeSource::FromMetadata => dtos::EventTimeSource::FromMetadata,
            EventTimeSource::FromPath(v) => dtos::EventTimeSource::FromPath(v.into()),
        }
    }
}

pub trait EventTimeSourceFromPath {
    fn pattern(&self) -> &str;
    fn timestamp_format(&self) -> Option<&str>;
}

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

impl Into<dtos::EventTimeSourceFromPath> for &dyn EventTimeSourceFromPath {
    fn into(self) -> dtos::EventTimeSourceFromPath {
        dtos::EventTimeSourceFromPath {
            pattern: self.pattern().to_owned(),
            timestamp_format: self.timestamp_format().map(|v| v.to_owned()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// ExecuteQuery
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#executequery-schema
////////////////////////////////////////////////////////////////////////////////

pub trait ExecuteQuery {
    fn input_slices(&self) -> Box<dyn Iterator<Item = &dyn InputSlice> + '_>;
    fn input_checkpoint(&self) -> Option<&Multihash>;
    fn output_data(&self) -> Option<&dyn DataSlice>;
    fn output_checkpoint(&self) -> Option<&dyn Checkpoint>;
    fn output_watermark(&self) -> Option<DateTime<Utc>>;
}

impl ExecuteQuery for dtos::ExecuteQuery {
    fn input_slices(&self) -> Box<dyn Iterator<Item = &dyn InputSlice> + '_> {
        Box::new(self.input_slices.iter().map(|i| -> &dyn InputSlice { i }))
    }
    fn input_checkpoint(&self) -> Option<&Multihash> {
        self.input_checkpoint.as_ref().map(|v| -> &Multihash { v })
    }
    fn output_data(&self) -> Option<&dyn DataSlice> {
        self.output_data.as_ref().map(|v| -> &dyn DataSlice { v })
    }
    fn output_checkpoint(&self) -> Option<&dyn Checkpoint> {
        self.output_checkpoint
            .as_ref()
            .map(|v| -> &dyn Checkpoint { v })
    }
    fn output_watermark(&self) -> Option<DateTime<Utc>> {
        self.output_watermark
            .as_ref()
            .map(|v| -> DateTime<Utc> { *v })
    }
}

impl Into<dtos::ExecuteQuery> for &dyn ExecuteQuery {
    fn into(self) -> dtos::ExecuteQuery {
        dtos::ExecuteQuery {
            input_slices: self.input_slices().map(|i| i.into()).collect(),
            input_checkpoint: self.input_checkpoint().map(|v| v.clone()),
            output_data: self.output_data().map(|v| v.into()),
            output_checkpoint: self.output_checkpoint().map(|v| v.into()),
            output_watermark: self.output_watermark().map(|v| v),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// ExecuteQueryInput
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#executequeryinput-schema
////////////////////////////////////////////////////////////////////////////////

pub trait ExecuteQueryInput {
    fn dataset_id(&self) -> &DatasetID;
    fn dataset_name(&self) -> &DatasetName;
    fn vocab(&self) -> &dyn DatasetVocabulary;
    fn data_interval(&self) -> Option<&dyn OffsetInterval>;
    fn data_paths(&self) -> Box<dyn Iterator<Item = &Path> + '_>;
    fn schema_file(&self) -> &Path;
    fn explicit_watermarks(&self) -> Box<dyn Iterator<Item = &dyn Watermark> + '_>;
}

impl ExecuteQueryInput for dtos::ExecuteQueryInput {
    fn dataset_id(&self) -> &DatasetID {
        &self.dataset_id
    }
    fn dataset_name(&self) -> &DatasetName {
        &self.dataset_name
    }
    fn vocab(&self) -> &dyn DatasetVocabulary {
        &self.vocab
    }
    fn data_interval(&self) -> Option<&dyn OffsetInterval> {
        self.data_interval
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

impl Into<dtos::ExecuteQueryInput> for &dyn ExecuteQueryInput {
    fn into(self) -> dtos::ExecuteQueryInput {
        dtos::ExecuteQueryInput {
            dataset_id: self.dataset_id().clone(),
            dataset_name: self.dataset_name().to_owned(),
            vocab: self.vocab().into(),
            data_interval: self.data_interval().map(|v| v.into()),
            data_paths: self.data_paths().map(|i| i.to_owned()).collect(),
            schema_file: self.schema_file().to_owned(),
            explicit_watermarks: self.explicit_watermarks().map(|i| i.into()).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// ExecuteQueryRequest
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#executequeryrequest-schema
////////////////////////////////////////////////////////////////////////////////

pub trait ExecuteQueryRequest {
    fn dataset_id(&self) -> &DatasetID;
    fn dataset_name(&self) -> &DatasetName;
    fn system_time(&self) -> DateTime<Utc>;
    fn offset(&self) -> i64;
    fn vocab(&self) -> &dyn DatasetVocabulary;
    fn transform(&self) -> Transform;
    fn inputs(&self) -> Box<dyn Iterator<Item = &dyn ExecuteQueryInput> + '_>;
    fn prev_checkpoint_path(&self) -> Option<&Path>;
    fn new_checkpoint_path(&self) -> &Path;
    fn out_data_path(&self) -> &Path;
}

impl ExecuteQueryRequest for dtos::ExecuteQueryRequest {
    fn dataset_id(&self) -> &DatasetID {
        &self.dataset_id
    }
    fn dataset_name(&self) -> &DatasetName {
        &self.dataset_name
    }
    fn system_time(&self) -> DateTime<Utc> {
        self.system_time
    }
    fn offset(&self) -> i64 {
        self.offset
    }
    fn vocab(&self) -> &dyn DatasetVocabulary {
        &self.vocab
    }
    fn transform(&self) -> Transform {
        (&self.transform).into()
    }
    fn inputs(&self) -> Box<dyn Iterator<Item = &dyn ExecuteQueryInput> + '_> {
        Box::new(self.inputs.iter().map(|i| -> &dyn ExecuteQueryInput { i }))
    }
    fn prev_checkpoint_path(&self) -> Option<&Path> {
        self.prev_checkpoint_path
            .as_ref()
            .map(|v| -> &Path { v.as_ref() })
    }
    fn new_checkpoint_path(&self) -> &Path {
        self.new_checkpoint_path.as_ref()
    }
    fn out_data_path(&self) -> &Path {
        self.out_data_path.as_ref()
    }
}

impl Into<dtos::ExecuteQueryRequest> for &dyn ExecuteQueryRequest {
    fn into(self) -> dtos::ExecuteQueryRequest {
        dtos::ExecuteQueryRequest {
            dataset_id: self.dataset_id().clone(),
            dataset_name: self.dataset_name().to_owned(),
            system_time: self.system_time(),
            offset: self.offset(),
            vocab: self.vocab().into(),
            transform: self.transform().into(),
            inputs: self.inputs().map(|i| i.into()).collect(),
            prev_checkpoint_path: self.prev_checkpoint_path().map(|v| v.to_owned()),
            new_checkpoint_path: self.new_checkpoint_path().to_owned(),
            out_data_path: self.out_data_path().to_owned(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// ExecuteQueryResponse
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#executequeryresponse-schema
////////////////////////////////////////////////////////////////////////////////

pub enum ExecuteQueryResponse<'a> {
    Progress,
    Success(&'a dyn ExecuteQueryResponseSuccess),
    InvalidQuery(&'a dyn ExecuteQueryResponseInvalidQuery),
    InternalError(&'a dyn ExecuteQueryResponseInternalError),
}

impl<'a> From<&'a dtos::ExecuteQueryResponse> for ExecuteQueryResponse<'a> {
    fn from(other: &'a dtos::ExecuteQueryResponse) -> Self {
        match other {
            dtos::ExecuteQueryResponse::Progress => ExecuteQueryResponse::Progress,
            dtos::ExecuteQueryResponse::Success(v) => ExecuteQueryResponse::Success(v),
            dtos::ExecuteQueryResponse::InvalidQuery(v) => ExecuteQueryResponse::InvalidQuery(v),
            dtos::ExecuteQueryResponse::InternalError(v) => ExecuteQueryResponse::InternalError(v),
        }
    }
}

impl Into<dtos::ExecuteQueryResponse> for ExecuteQueryResponse<'_> {
    fn into(self) -> dtos::ExecuteQueryResponse {
        match self {
            ExecuteQueryResponse::Progress => dtos::ExecuteQueryResponse::Progress,
            ExecuteQueryResponse::Success(v) => dtos::ExecuteQueryResponse::Success(v.into()),
            ExecuteQueryResponse::InvalidQuery(v) => {
                dtos::ExecuteQueryResponse::InvalidQuery(v.into())
            }
            ExecuteQueryResponse::InternalError(v) => {
                dtos::ExecuteQueryResponse::InternalError(v.into())
            }
        }
    }
}

pub trait ExecuteQueryResponseSuccess {
    fn data_interval(&self) -> Option<&dyn OffsetInterval>;
    fn output_watermark(&self) -> Option<DateTime<Utc>>;
}

pub trait ExecuteQueryResponseInvalidQuery {
    fn message(&self) -> &str;
}

pub trait ExecuteQueryResponseInternalError {
    fn message(&self) -> &str;
    fn backtrace(&self) -> Option<&str>;
}

impl ExecuteQueryResponseSuccess for dtos::ExecuteQueryResponseSuccess {
    fn data_interval(&self) -> Option<&dyn OffsetInterval> {
        self.data_interval
            .as_ref()
            .map(|v| -> &dyn OffsetInterval { v })
    }
    fn output_watermark(&self) -> Option<DateTime<Utc>> {
        self.output_watermark
            .as_ref()
            .map(|v| -> DateTime<Utc> { *v })
    }
}

impl ExecuteQueryResponseInvalidQuery for dtos::ExecuteQueryResponseInvalidQuery {
    fn message(&self) -> &str {
        self.message.as_ref()
    }
}

impl ExecuteQueryResponseInternalError for dtos::ExecuteQueryResponseInternalError {
    fn message(&self) -> &str {
        self.message.as_ref()
    }
    fn backtrace(&self) -> Option<&str> {
        self.backtrace.as_ref().map(|v| -> &str { v.as_ref() })
    }
}

impl Into<dtos::ExecuteQueryResponseSuccess> for &dyn ExecuteQueryResponseSuccess {
    fn into(self) -> dtos::ExecuteQueryResponseSuccess {
        dtos::ExecuteQueryResponseSuccess {
            data_interval: self.data_interval().map(|v| v.into()),
            output_watermark: self.output_watermark().map(|v| v),
        }
    }
}

impl Into<dtos::ExecuteQueryResponseInvalidQuery> for &dyn ExecuteQueryResponseInvalidQuery {
    fn into(self) -> dtos::ExecuteQueryResponseInvalidQuery {
        dtos::ExecuteQueryResponseInvalidQuery {
            message: self.message().to_owned(),
        }
    }
}

impl Into<dtos::ExecuteQueryResponseInternalError> for &dyn ExecuteQueryResponseInternalError {
    fn into(self) -> dtos::ExecuteQueryResponseInternalError {
        dtos::ExecuteQueryResponseInternalError {
            message: self.message().to_owned(),
            backtrace: self.backtrace().map(|v| v.to_owned()),
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
}

impl<'a> From<&'a dtos::FetchStep> for FetchStep<'a> {
    fn from(other: &'a dtos::FetchStep) -> Self {
        match other {
            dtos::FetchStep::Url(v) => FetchStep::Url(v),
            dtos::FetchStep::FilesGlob(v) => FetchStep::FilesGlob(v),
            dtos::FetchStep::Container(v) => FetchStep::Container(v),
        }
    }
}

impl Into<dtos::FetchStep> for FetchStep<'_> {
    fn into(self) -> dtos::FetchStep {
        match self {
            FetchStep::Url(v) => dtos::FetchStep::Url(v.into()),
            FetchStep::FilesGlob(v) => dtos::FetchStep::FilesGlob(v.into()),
            FetchStep::Container(v) => dtos::FetchStep::Container(v.into()),
        }
    }
}

pub trait FetchStepUrl {
    fn url(&self) -> &str;
    fn event_time(&self) -> Option<EventTimeSource>;
    fn cache(&self) -> Option<SourceCaching>;
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

impl Into<dtos::FetchStepUrl> for &dyn FetchStepUrl {
    fn into(self) -> dtos::FetchStepUrl {
        dtos::FetchStepUrl {
            url: self.url().to_owned(),
            event_time: self.event_time().map(|v| v.into()),
            cache: self.cache().map(|v| v.into()),
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

////////////////////////////////////////////////////////////////////////////////
// InputSlice
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#inputslice-schema
////////////////////////////////////////////////////////////////////////////////

pub trait InputSlice {
    fn dataset_id(&self) -> &DatasetID;
    fn block_interval(&self) -> Option<&dyn BlockInterval>;
    fn data_interval(&self) -> Option<&dyn OffsetInterval>;
}

impl InputSlice for dtos::InputSlice {
    fn dataset_id(&self) -> &DatasetID {
        &self.dataset_id
    }
    fn block_interval(&self) -> Option<&dyn BlockInterval> {
        self.block_interval
            .as_ref()
            .map(|v| -> &dyn BlockInterval { v })
    }
    fn data_interval(&self) -> Option<&dyn OffsetInterval> {
        self.data_interval
            .as_ref()
            .map(|v| -> &dyn OffsetInterval { v })
    }
}

impl Into<dtos::InputSlice> for &dyn InputSlice {
    fn into(self) -> dtos::InputSlice {
        dtos::InputSlice {
            dataset_id: self.dataset_id().clone(),
            block_interval: self.block_interval().map(|v| v.into()),
            data_interval: self.data_interval().map(|v| v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// MergeStrategy
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mergestrategy-schema
////////////////////////////////////////////////////////////////////////////////

pub enum MergeStrategy<'a> {
    Append,
    Ledger(&'a dyn MergeStrategyLedger),
    Snapshot(&'a dyn MergeStrategySnapshot),
}

impl<'a> From<&'a dtos::MergeStrategy> for MergeStrategy<'a> {
    fn from(other: &'a dtos::MergeStrategy) -> Self {
        match other {
            dtos::MergeStrategy::Append => MergeStrategy::Append,
            dtos::MergeStrategy::Ledger(v) => MergeStrategy::Ledger(v),
            dtos::MergeStrategy::Snapshot(v) => MergeStrategy::Snapshot(v),
        }
    }
}

impl Into<dtos::MergeStrategy> for MergeStrategy<'_> {
    fn into(self) -> dtos::MergeStrategy {
        match self {
            MergeStrategy::Append => dtos::MergeStrategy::Append,
            MergeStrategy::Ledger(v) => dtos::MergeStrategy::Ledger(v.into()),
            MergeStrategy::Snapshot(v) => dtos::MergeStrategy::Snapshot(v.into()),
        }
    }
}

pub trait MergeStrategyLedger {
    fn primary_key(&self) -> Box<dyn Iterator<Item = &str> + '_>;
}

pub trait MergeStrategySnapshot {
    fn primary_key(&self) -> Box<dyn Iterator<Item = &str> + '_>;
    fn compare_columns(&self) -> Option<Box<dyn Iterator<Item = &str> + '_>>;
    fn observation_column(&self) -> Option<&str>;
    fn obsv_added(&self) -> Option<&str>;
    fn obsv_changed(&self) -> Option<&str>;
    fn obsv_removed(&self) -> Option<&str>;
}

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
    fn observation_column(&self) -> Option<&str> {
        self.observation_column
            .as_ref()
            .map(|v| -> &str { v.as_ref() })
    }
    fn obsv_added(&self) -> Option<&str> {
        self.obsv_added.as_ref().map(|v| -> &str { v.as_ref() })
    }
    fn obsv_changed(&self) -> Option<&str> {
        self.obsv_changed.as_ref().map(|v| -> &str { v.as_ref() })
    }
    fn obsv_removed(&self) -> Option<&str> {
        self.obsv_removed.as_ref().map(|v| -> &str { v.as_ref() })
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
            observation_column: self.observation_column().map(|v| v.to_owned()),
            obsv_added: self.obsv_added().map(|v| v.to_owned()),
            obsv_changed: self.obsv_changed().map(|v| v.to_owned()),
            obsv_removed: self.obsv_removed().map(|v| v.to_owned()),
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
    fn event(&self) -> MetadataEvent;
    fn sequence_number(&self) -> Option<i32>;
}

impl MetadataBlock for dtos::MetadataBlock {
    fn system_time(&self) -> DateTime<Utc> {
        self.system_time
    }
    fn prev_block_hash(&self) -> Option<&Multihash> {
        self.prev_block_hash.as_ref().map(|v| -> &Multihash { v })
    }
    fn event(&self) -> MetadataEvent {
        (&self.event).into()
    }
    fn sequence_number(&self) -> Option<i32> {
        self.sequence_number.as_ref().map(|v| -> i32 { *v })
    }
}

impl Into<dtos::MetadataBlock> for &dyn MetadataBlock {
    fn into(self) -> dtos::MetadataBlock {
        dtos::MetadataBlock {
            system_time: self.system_time(),
            prev_block_hash: self.prev_block_hash().map(|v| v.clone()),
            event: self.event().into(),
            sequence_number: self.sequence_number().map(|v| v),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// MetadataEvent
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#metadataevent-schema
////////////////////////////////////////////////////////////////////////////////

pub enum MetadataEvent<'a> {
    AddData(&'a dyn AddData),
    ExecuteQuery(&'a dyn ExecuteQuery),
    Seed(&'a dyn Seed),
    SetPollingSource(&'a dyn SetPollingSource),
    SetTransform(&'a dyn SetTransform),
    SetVocab(&'a dyn SetVocab),
    SetWatermark(&'a dyn SetWatermark),
    SetAttachments(&'a dyn SetAttachments),
    SetInfo(&'a dyn SetInfo),
    SetLicense(&'a dyn SetLicense),
}

impl<'a> From<&'a dtos::MetadataEvent> for MetadataEvent<'a> {
    fn from(other: &'a dtos::MetadataEvent) -> Self {
        match other {
            dtos::MetadataEvent::AddData(v) => MetadataEvent::AddData(v),
            dtos::MetadataEvent::ExecuteQuery(v) => MetadataEvent::ExecuteQuery(v),
            dtos::MetadataEvent::Seed(v) => MetadataEvent::Seed(v),
            dtos::MetadataEvent::SetPollingSource(v) => MetadataEvent::SetPollingSource(v),
            dtos::MetadataEvent::SetTransform(v) => MetadataEvent::SetTransform(v),
            dtos::MetadataEvent::SetVocab(v) => MetadataEvent::SetVocab(v),
            dtos::MetadataEvent::SetWatermark(v) => MetadataEvent::SetWatermark(v),
            dtos::MetadataEvent::SetAttachments(v) => MetadataEvent::SetAttachments(v),
            dtos::MetadataEvent::SetInfo(v) => MetadataEvent::SetInfo(v),
            dtos::MetadataEvent::SetLicense(v) => MetadataEvent::SetLicense(v),
        }
    }
}

impl Into<dtos::MetadataEvent> for MetadataEvent<'_> {
    fn into(self) -> dtos::MetadataEvent {
        match self {
            MetadataEvent::AddData(v) => dtos::MetadataEvent::AddData(v.into()),
            MetadataEvent::ExecuteQuery(v) => dtos::MetadataEvent::ExecuteQuery(v.into()),
            MetadataEvent::Seed(v) => dtos::MetadataEvent::Seed(v.into()),
            MetadataEvent::SetPollingSource(v) => dtos::MetadataEvent::SetPollingSource(v.into()),
            MetadataEvent::SetTransform(v) => dtos::MetadataEvent::SetTransform(v.into()),
            MetadataEvent::SetVocab(v) => dtos::MetadataEvent::SetVocab(v.into()),
            MetadataEvent::SetWatermark(v) => dtos::MetadataEvent::SetWatermark(v.into()),
            MetadataEvent::SetAttachments(v) => dtos::MetadataEvent::SetAttachments(v.into()),
            MetadataEvent::SetInfo(v) => dtos::MetadataEvent::SetInfo(v.into()),
            MetadataEvent::SetLicense(v) => dtos::MetadataEvent::SetLicense(v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// OffsetInterval
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#offsetinterval-schema
////////////////////////////////////////////////////////////////////////////////

pub trait OffsetInterval {
    fn start(&self) -> i64;
    fn end(&self) -> i64;
}

impl OffsetInterval for dtos::OffsetInterval {
    fn start(&self) -> i64 {
        self.start
    }
    fn end(&self) -> i64 {
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
// ReadStep
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#readstep-schema
////////////////////////////////////////////////////////////////////////////////

pub enum ReadStep<'a> {
    Csv(&'a dyn ReadStepCsv),
    JsonLines(&'a dyn ReadStepJsonLines),
    GeoJson(&'a dyn ReadStepGeoJson),
    EsriShapefile(&'a dyn ReadStepEsriShapefile),
    Parquet(&'a dyn ReadStepParquet),
}

impl<'a> From<&'a dtos::ReadStep> for ReadStep<'a> {
    fn from(other: &'a dtos::ReadStep) -> Self {
        match other {
            dtos::ReadStep::Csv(v) => ReadStep::Csv(v),
            dtos::ReadStep::JsonLines(v) => ReadStep::JsonLines(v),
            dtos::ReadStep::GeoJson(v) => ReadStep::GeoJson(v),
            dtos::ReadStep::EsriShapefile(v) => ReadStep::EsriShapefile(v),
            dtos::ReadStep::Parquet(v) => ReadStep::Parquet(v),
        }
    }
}

impl Into<dtos::ReadStep> for ReadStep<'_> {
    fn into(self) -> dtos::ReadStep {
        match self {
            ReadStep::Csv(v) => dtos::ReadStep::Csv(v.into()),
            ReadStep::JsonLines(v) => dtos::ReadStep::JsonLines(v.into()),
            ReadStep::GeoJson(v) => dtos::ReadStep::GeoJson(v.into()),
            ReadStep::EsriShapefile(v) => dtos::ReadStep::EsriShapefile(v.into()),
            ReadStep::Parquet(v) => dtos::ReadStep::Parquet(v.into()),
        }
    }
}

pub trait ReadStepCsv {
    fn schema(&self) -> Option<Box<dyn Iterator<Item = &str> + '_>>;
    fn separator(&self) -> Option<&str>;
    fn encoding(&self) -> Option<&str>;
    fn quote(&self) -> Option<&str>;
    fn escape(&self) -> Option<&str>;
    fn comment(&self) -> Option<&str>;
    fn header(&self) -> Option<bool>;
    fn enforce_schema(&self) -> Option<bool>;
    fn infer_schema(&self) -> Option<bool>;
    fn ignore_leading_white_space(&self) -> Option<bool>;
    fn ignore_trailing_white_space(&self) -> Option<bool>;
    fn null_value(&self) -> Option<&str>;
    fn empty_value(&self) -> Option<&str>;
    fn nan_value(&self) -> Option<&str>;
    fn positive_inf(&self) -> Option<&str>;
    fn negative_inf(&self) -> Option<&str>;
    fn date_format(&self) -> Option<&str>;
    fn timestamp_format(&self) -> Option<&str>;
    fn multi_line(&self) -> Option<bool>;
}

pub trait ReadStepJsonLines {
    fn schema(&self) -> Option<Box<dyn Iterator<Item = &str> + '_>>;
    fn date_format(&self) -> Option<&str>;
    fn encoding(&self) -> Option<&str>;
    fn multi_line(&self) -> Option<bool>;
    fn primitives_as_string(&self) -> Option<bool>;
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
    fn comment(&self) -> Option<&str> {
        self.comment.as_ref().map(|v| -> &str { v.as_ref() })
    }
    fn header(&self) -> Option<bool> {
        self.header.as_ref().map(|v| -> bool { *v })
    }
    fn enforce_schema(&self) -> Option<bool> {
        self.enforce_schema.as_ref().map(|v| -> bool { *v })
    }
    fn infer_schema(&self) -> Option<bool> {
        self.infer_schema.as_ref().map(|v| -> bool { *v })
    }
    fn ignore_leading_white_space(&self) -> Option<bool> {
        self.ignore_leading_white_space
            .as_ref()
            .map(|v| -> bool { *v })
    }
    fn ignore_trailing_white_space(&self) -> Option<bool> {
        self.ignore_trailing_white_space
            .as_ref()
            .map(|v| -> bool { *v })
    }
    fn null_value(&self) -> Option<&str> {
        self.null_value.as_ref().map(|v| -> &str { v.as_ref() })
    }
    fn empty_value(&self) -> Option<&str> {
        self.empty_value.as_ref().map(|v| -> &str { v.as_ref() })
    }
    fn nan_value(&self) -> Option<&str> {
        self.nan_value.as_ref().map(|v| -> &str { v.as_ref() })
    }
    fn positive_inf(&self) -> Option<&str> {
        self.positive_inf.as_ref().map(|v| -> &str { v.as_ref() })
    }
    fn negative_inf(&self) -> Option<&str> {
        self.negative_inf.as_ref().map(|v| -> &str { v.as_ref() })
    }
    fn date_format(&self) -> Option<&str> {
        self.date_format.as_ref().map(|v| -> &str { v.as_ref() })
    }
    fn timestamp_format(&self) -> Option<&str> {
        self.timestamp_format
            .as_ref()
            .map(|v| -> &str { v.as_ref() })
    }
    fn multi_line(&self) -> Option<bool> {
        self.multi_line.as_ref().map(|v| -> bool { *v })
    }
}

impl ReadStepJsonLines for dtos::ReadStepJsonLines {
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
    fn multi_line(&self) -> Option<bool> {
        self.multi_line.as_ref().map(|v| -> bool { *v })
    }
    fn primitives_as_string(&self) -> Option<bool> {
        self.primitives_as_string.as_ref().map(|v| -> bool { *v })
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

impl Into<dtos::ReadStepCsv> for &dyn ReadStepCsv {
    fn into(self) -> dtos::ReadStepCsv {
        dtos::ReadStepCsv {
            schema: self.schema().map(|v| v.map(|i| i.to_owned()).collect()),
            separator: self.separator().map(|v| v.to_owned()),
            encoding: self.encoding().map(|v| v.to_owned()),
            quote: self.quote().map(|v| v.to_owned()),
            escape: self.escape().map(|v| v.to_owned()),
            comment: self.comment().map(|v| v.to_owned()),
            header: self.header().map(|v| v),
            enforce_schema: self.enforce_schema().map(|v| v),
            infer_schema: self.infer_schema().map(|v| v),
            ignore_leading_white_space: self.ignore_leading_white_space().map(|v| v),
            ignore_trailing_white_space: self.ignore_trailing_white_space().map(|v| v),
            null_value: self.null_value().map(|v| v.to_owned()),
            empty_value: self.empty_value().map(|v| v.to_owned()),
            nan_value: self.nan_value().map(|v| v.to_owned()),
            positive_inf: self.positive_inf().map(|v| v.to_owned()),
            negative_inf: self.negative_inf().map(|v| v.to_owned()),
            date_format: self.date_format().map(|v| v.to_owned()),
            timestamp_format: self.timestamp_format().map(|v| v.to_owned()),
            multi_line: self.multi_line().map(|v| v),
        }
    }
}

impl Into<dtos::ReadStepJsonLines> for &dyn ReadStepJsonLines {
    fn into(self) -> dtos::ReadStepJsonLines {
        dtos::ReadStepJsonLines {
            schema: self.schema().map(|v| v.map(|i| i.to_owned()).collect()),
            date_format: self.date_format().map(|v| v.to_owned()),
            encoding: self.encoding().map(|v| v.to_owned()),
            multi_line: self.multi_line().map(|v| v),
            primitives_as_string: self.primitives_as_string().map(|v| v),
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
    fn system_time_column(&self) -> Option<&str>;
    fn event_time_column(&self) -> Option<&str>;
    fn offset_column(&self) -> Option<&str>;
}

impl SetVocab for dtos::SetVocab {
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
    fn offset_column(&self) -> Option<&str> {
        self.offset_column.as_ref().map(|v| -> &str { v.as_ref() })
    }
}

impl Into<dtos::SetVocab> for &dyn SetVocab {
    fn into(self) -> dtos::SetVocab {
        dtos::SetVocab {
            system_time_column: self.system_time_column().map(|v| v.to_owned()),
            event_time_column: self.event_time_column().map(|v| v.to_owned()),
            offset_column: self.offset_column().map(|v| v.to_owned()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// SetWatermark
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setwatermark-schema
////////////////////////////////////////////////////////////////////////////////

pub trait SetWatermark {
    fn output_watermark(&self) -> DateTime<Utc>;
}

impl SetWatermark for dtos::SetWatermark {
    fn output_watermark(&self) -> DateTime<Utc> {
        self.output_watermark
    }
}

impl Into<dtos::SetWatermark> for &dyn SetWatermark {
    fn into(self) -> dtos::SetWatermark {
        dtos::SetWatermark {
            output_watermark: self.output_watermark(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// SourceCaching
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#sourcecaching-schema
////////////////////////////////////////////////////////////////////////////////

pub enum SourceCaching<'a> {
    Forever,
    _Phantom(std::marker::PhantomData<&'a ()>),
}

impl<'a> From<&'a dtos::SourceCaching> for SourceCaching<'a> {
    fn from(other: &'a dtos::SourceCaching) -> Self {
        match other {
            dtos::SourceCaching::Forever => SourceCaching::Forever,
        }
    }
}

impl Into<dtos::SourceCaching> for SourceCaching<'_> {
    fn into(self) -> dtos::SourceCaching {
        match self {
            SourceCaching::Forever => dtos::SourceCaching::Forever,
            SourceCaching::_Phantom(_) => unreachable!(),
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
    fn id(&self) -> Option<&DatasetID>;
    fn name(&self) -> &DatasetName;
}

impl TransformInput for dtos::TransformInput {
    fn id(&self) -> Option<&DatasetID> {
        self.id.as_ref().map(|v| -> &DatasetID { v })
    }
    fn name(&self) -> &DatasetName {
        &self.name
    }
}

impl Into<dtos::TransformInput> for &dyn TransformInput {
    fn into(self) -> dtos::TransformInput {
        dtos::TransformInput {
            id: self.id().map(|v| v.clone()),
            name: self.name().to_owned(),
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
