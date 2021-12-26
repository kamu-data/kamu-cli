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

use std::path::Path;

use super::{CompressionFormat, DatasetID, DatasetName, Multihash, SourceOrdering};
use chrono::{DateTime, Utc};

////////////////////////////////////////////////////////////////////////////////
// BlockInterval
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#blockinterval-schema
////////////////////////////////////////////////////////////////////////////////

pub trait BlockInterval {
    fn start(&self) -> &Multihash;
    fn end(&self) -> &Multihash;
}

impl BlockInterval for super::BlockInterval {
    fn start(&self) -> &Multihash {
        &self.start
    }
    fn end(&self) -> &Multihash {
        &self.end
    }
}

impl Into<super::BlockInterval> for &dyn BlockInterval {
    fn into(self) -> super::BlockInterval {
        super::BlockInterval {
            start: self.start().clone(),
            end: self.end().clone(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// DatasetSnapshot
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetsnapshot-schema
////////////////////////////////////////////////////////////////////////////////

pub trait DatasetSnapshot {
    fn name(&self) -> &DatasetName;
    fn source(&self) -> DatasetSource;
    fn vocab(&self) -> Option<&dyn DatasetVocabulary>;
}

impl DatasetSnapshot for super::DatasetSnapshot {
    fn name(&self) -> &DatasetName {
        &self.name
    }
    fn source(&self) -> DatasetSource {
        (&self.source).into()
    }
    fn vocab(&self) -> Option<&dyn DatasetVocabulary> {
        self.vocab.as_ref().map(|v| -> &dyn DatasetVocabulary { v })
    }
}

impl Into<super::DatasetSnapshot> for &dyn DatasetSnapshot {
    fn into(self) -> super::DatasetSnapshot {
        super::DatasetSnapshot {
            name: self.name().to_owned(),
            source: self.source().into(),
            vocab: self.vocab().map(|v| v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// DatasetSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetsource-schema
////////////////////////////////////////////////////////////////////////////////

pub enum DatasetSource<'a> {
    Root(&'a dyn DatasetSourceRoot),
    Derivative(&'a dyn DatasetSourceDerivative),
}

impl<'a> From<&'a super::DatasetSource> for DatasetSource<'a> {
    fn from(other: &'a super::DatasetSource) -> Self {
        match other {
            super::DatasetSource::Root(v) => DatasetSource::Root(v),
            super::DatasetSource::Derivative(v) => DatasetSource::Derivative(v),
        }
    }
}

impl Into<super::DatasetSource> for DatasetSource<'_> {
    fn into(self) -> super::DatasetSource {
        match self {
            DatasetSource::Root(v) => super::DatasetSource::Root(v.into()),
            DatasetSource::Derivative(v) => super::DatasetSource::Derivative(v.into()),
        }
    }
}

pub trait DatasetSourceRoot {
    fn fetch(&self) -> FetchStep;
    fn prepare(&self) -> Option<Box<dyn Iterator<Item = PrepStep> + '_>>;
    fn read(&self) -> ReadStep;
    fn preprocess(&self) -> Option<Transform>;
    fn merge(&self) -> MergeStrategy;
}

pub trait DatasetSourceDerivative {
    fn inputs(&self) -> Box<dyn Iterator<Item = &dyn TransformInput> + '_>;
    fn transform(&self) -> Transform;
}

impl DatasetSourceRoot for super::DatasetSourceRoot {
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

impl DatasetSourceDerivative for super::DatasetSourceDerivative {
    fn inputs(&self) -> Box<dyn Iterator<Item = &dyn TransformInput> + '_> {
        Box::new(self.inputs.iter().map(|i| -> &dyn TransformInput { i }))
    }
    fn transform(&self) -> Transform {
        (&self.transform).into()
    }
}

impl Into<super::DatasetSourceRoot> for &dyn DatasetSourceRoot {
    fn into(self) -> super::DatasetSourceRoot {
        super::DatasetSourceRoot {
            fetch: self.fetch().into(),
            prepare: self.prepare().map(|v| v.map(|i| i.into()).collect()),
            read: self.read().into(),
            preprocess: self.preprocess().map(|v| v.into()),
            merge: self.merge().into(),
        }
    }
}

impl Into<super::DatasetSourceDerivative> for &dyn DatasetSourceDerivative {
    fn into(self) -> super::DatasetSourceDerivative {
        super::DatasetSourceDerivative {
            inputs: self.inputs().map(|i| i.into()).collect(),
            transform: self.transform().into(),
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

impl DatasetVocabulary for super::DatasetVocabulary {
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

impl Into<super::DatasetVocabulary> for &dyn DatasetVocabulary {
    fn into(self) -> super::DatasetVocabulary {
        super::DatasetVocabulary {
            system_time_column: self.system_time_column().map(|v| v.to_owned()),
            event_time_column: self.event_time_column().map(|v| v.to_owned()),
            offset_column: self.offset_column().map(|v| v.to_owned()),
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

impl<'a> From<&'a super::EventTimeSource> for EventTimeSource<'a> {
    fn from(other: &'a super::EventTimeSource) -> Self {
        match other {
            super::EventTimeSource::FromMetadata => EventTimeSource::FromMetadata,
            super::EventTimeSource::FromPath(v) => EventTimeSource::FromPath(v),
        }
    }
}

impl Into<super::EventTimeSource> for EventTimeSource<'_> {
    fn into(self) -> super::EventTimeSource {
        match self {
            EventTimeSource::FromMetadata => super::EventTimeSource::FromMetadata,
            EventTimeSource::FromPath(v) => super::EventTimeSource::FromPath(v.into()),
        }
    }
}

pub trait EventTimeSourceFromPath {
    fn pattern(&self) -> &str;
    fn timestamp_format(&self) -> Option<&str>;
}

impl EventTimeSourceFromPath for super::EventTimeSourceFromPath {
    fn pattern(&self) -> &str {
        self.pattern.as_ref()
    }
    fn timestamp_format(&self) -> Option<&str> {
        self.timestamp_format
            .as_ref()
            .map(|v| -> &str { v.as_ref() })
    }
}

impl Into<super::EventTimeSourceFromPath> for &dyn EventTimeSourceFromPath {
    fn into(self) -> super::EventTimeSourceFromPath {
        super::EventTimeSourceFromPath {
            pattern: self.pattern().to_owned(),
            timestamp_format: self.timestamp_format().map(|v| v.to_owned()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// ExecuteQueryInput
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#executequeryinput-schema
////////////////////////////////////////////////////////////////////////////////

pub trait ExecuteQueryInput {
    fn dataset_id(&self) -> &DatasetID;
    fn vocab(&self) -> &dyn DatasetVocabulary;
    fn data_interval(&self) -> Option<&dyn OffsetInterval>;
    fn data_paths(&self) -> Box<dyn Iterator<Item = &Path> + '_>;
    fn schema_file(&self) -> &Path;
    fn explicit_watermarks(&self) -> Box<dyn Iterator<Item = &dyn Watermark> + '_>;
}

impl ExecuteQueryInput for super::ExecuteQueryInput {
    fn dataset_id(&self) -> &DatasetID {
        &self.dataset_id
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

impl Into<super::ExecuteQueryInput> for &dyn ExecuteQueryInput {
    fn into(self) -> super::ExecuteQueryInput {
        super::ExecuteQueryInput {
            dataset_id: self.dataset_id().clone(),
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
    fn prev_checkpoint_dir(&self) -> Option<&Path>;
    fn new_checkpoint_dir(&self) -> &Path;
    fn out_data_path(&self) -> &Path;
}

impl ExecuteQueryRequest for super::ExecuteQueryRequest {
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
    fn prev_checkpoint_dir(&self) -> Option<&Path> {
        self.prev_checkpoint_dir
            .as_ref()
            .map(|v| -> &Path { v.as_ref() })
    }
    fn new_checkpoint_dir(&self) -> &Path {
        self.new_checkpoint_dir.as_ref()
    }
    fn out_data_path(&self) -> &Path {
        self.out_data_path.as_ref()
    }
}

impl Into<super::ExecuteQueryRequest> for &dyn ExecuteQueryRequest {
    fn into(self) -> super::ExecuteQueryRequest {
        super::ExecuteQueryRequest {
            dataset_id: self.dataset_id().clone(),
            dataset_name: self.dataset_name().to_owned(),
            system_time: self.system_time(),
            offset: self.offset(),
            vocab: self.vocab().into(),
            transform: self.transform().into(),
            inputs: self.inputs().map(|i| i.into()).collect(),
            prev_checkpoint_dir: self.prev_checkpoint_dir().map(|v| v.to_owned()),
            new_checkpoint_dir: self.new_checkpoint_dir().to_owned(),
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

impl<'a> From<&'a super::ExecuteQueryResponse> for ExecuteQueryResponse<'a> {
    fn from(other: &'a super::ExecuteQueryResponse) -> Self {
        match other {
            super::ExecuteQueryResponse::Progress => ExecuteQueryResponse::Progress,
            super::ExecuteQueryResponse::Success(v) => ExecuteQueryResponse::Success(v),
            super::ExecuteQueryResponse::InvalidQuery(v) => ExecuteQueryResponse::InvalidQuery(v),
            super::ExecuteQueryResponse::InternalError(v) => ExecuteQueryResponse::InternalError(v),
        }
    }
}

impl Into<super::ExecuteQueryResponse> for ExecuteQueryResponse<'_> {
    fn into(self) -> super::ExecuteQueryResponse {
        match self {
            ExecuteQueryResponse::Progress => super::ExecuteQueryResponse::Progress,
            ExecuteQueryResponse::Success(v) => super::ExecuteQueryResponse::Success(v.into()),
            ExecuteQueryResponse::InvalidQuery(v) => {
                super::ExecuteQueryResponse::InvalidQuery(v.into())
            }
            ExecuteQueryResponse::InternalError(v) => {
                super::ExecuteQueryResponse::InternalError(v.into())
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

impl ExecuteQueryResponseSuccess for super::ExecuteQueryResponseSuccess {
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

impl ExecuteQueryResponseInvalidQuery for super::ExecuteQueryResponseInvalidQuery {
    fn message(&self) -> &str {
        self.message.as_ref()
    }
}

impl ExecuteQueryResponseInternalError for super::ExecuteQueryResponseInternalError {
    fn message(&self) -> &str {
        self.message.as_ref()
    }
    fn backtrace(&self) -> Option<&str> {
        self.backtrace.as_ref().map(|v| -> &str { v.as_ref() })
    }
}

impl Into<super::ExecuteQueryResponseSuccess> for &dyn ExecuteQueryResponseSuccess {
    fn into(self) -> super::ExecuteQueryResponseSuccess {
        super::ExecuteQueryResponseSuccess {
            data_interval: self.data_interval().map(|v| v.into()),
            output_watermark: self.output_watermark().map(|v| v),
        }
    }
}

impl Into<super::ExecuteQueryResponseInvalidQuery> for &dyn ExecuteQueryResponseInvalidQuery {
    fn into(self) -> super::ExecuteQueryResponseInvalidQuery {
        super::ExecuteQueryResponseInvalidQuery {
            message: self.message().to_owned(),
        }
    }
}

impl Into<super::ExecuteQueryResponseInternalError> for &dyn ExecuteQueryResponseInternalError {
    fn into(self) -> super::ExecuteQueryResponseInternalError {
        super::ExecuteQueryResponseInternalError {
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
}

impl<'a> From<&'a super::FetchStep> for FetchStep<'a> {
    fn from(other: &'a super::FetchStep) -> Self {
        match other {
            super::FetchStep::Url(v) => FetchStep::Url(v),
            super::FetchStep::FilesGlob(v) => FetchStep::FilesGlob(v),
        }
    }
}

impl Into<super::FetchStep> for FetchStep<'_> {
    fn into(self) -> super::FetchStep {
        match self {
            FetchStep::Url(v) => super::FetchStep::Url(v.into()),
            FetchStep::FilesGlob(v) => super::FetchStep::FilesGlob(v.into()),
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

impl FetchStepUrl for super::FetchStepUrl {
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

impl FetchStepFilesGlob for super::FetchStepFilesGlob {
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

impl Into<super::FetchStepUrl> for &dyn FetchStepUrl {
    fn into(self) -> super::FetchStepUrl {
        super::FetchStepUrl {
            url: self.url().to_owned(),
            event_time: self.event_time().map(|v| v.into()),
            cache: self.cache().map(|v| v.into()),
        }
    }
}

impl Into<super::FetchStepFilesGlob> for &dyn FetchStepFilesGlob {
    fn into(self) -> super::FetchStepFilesGlob {
        super::FetchStepFilesGlob {
            path: self.path().to_owned(),
            event_time: self.event_time().map(|v| v.into()),
            cache: self.cache().map(|v| v.into()),
            order: self.order().map(|v| v.into()),
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

impl InputSlice for super::InputSlice {
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

impl Into<super::InputSlice> for &dyn InputSlice {
    fn into(self) -> super::InputSlice {
        super::InputSlice {
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

impl<'a> From<&'a super::MergeStrategy> for MergeStrategy<'a> {
    fn from(other: &'a super::MergeStrategy) -> Self {
        match other {
            super::MergeStrategy::Append => MergeStrategy::Append,
            super::MergeStrategy::Ledger(v) => MergeStrategy::Ledger(v),
            super::MergeStrategy::Snapshot(v) => MergeStrategy::Snapshot(v),
        }
    }
}

impl Into<super::MergeStrategy> for MergeStrategy<'_> {
    fn into(self) -> super::MergeStrategy {
        match self {
            MergeStrategy::Append => super::MergeStrategy::Append,
            MergeStrategy::Ledger(v) => super::MergeStrategy::Ledger(v.into()),
            MergeStrategy::Snapshot(v) => super::MergeStrategy::Snapshot(v.into()),
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

impl MergeStrategyLedger for super::MergeStrategyLedger {
    fn primary_key(&self) -> Box<dyn Iterator<Item = &str> + '_> {
        Box::new(self.primary_key.iter().map(|i| -> &str { i.as_ref() }))
    }
}

impl MergeStrategySnapshot for super::MergeStrategySnapshot {
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

impl Into<super::MergeStrategyLedger> for &dyn MergeStrategyLedger {
    fn into(self) -> super::MergeStrategyLedger {
        super::MergeStrategyLedger {
            primary_key: self.primary_key().map(|i| i.to_owned()).collect(),
        }
    }
}

impl Into<super::MergeStrategySnapshot> for &dyn MergeStrategySnapshot {
    fn into(self) -> super::MergeStrategySnapshot {
        super::MergeStrategySnapshot {
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
    fn prev_block_hash(&self) -> Option<&Multihash>;
    fn system_time(&self) -> DateTime<Utc>;
    fn output_slice(&self) -> Option<&dyn OutputSlice>;
    fn output_watermark(&self) -> Option<DateTime<Utc>>;
    fn input_slices(&self) -> Option<Box<dyn Iterator<Item = &dyn InputSlice> + '_>>;
    fn source(&self) -> Option<DatasetSource>;
    fn vocab(&self) -> Option<&dyn DatasetVocabulary>;
    fn seed(&self) -> Option<&DatasetID>;
}

impl MetadataBlock for super::MetadataBlock {
    fn prev_block_hash(&self) -> Option<&Multihash> {
        self.prev_block_hash.as_ref().map(|v| -> &Multihash { v })
    }
    fn system_time(&self) -> DateTime<Utc> {
        self.system_time
    }
    fn output_slice(&self) -> Option<&dyn OutputSlice> {
        self.output_slice
            .as_ref()
            .map(|v| -> &dyn OutputSlice { v })
    }
    fn output_watermark(&self) -> Option<DateTime<Utc>> {
        self.output_watermark
            .as_ref()
            .map(|v| -> DateTime<Utc> { *v })
    }
    fn input_slices(&self) -> Option<Box<dyn Iterator<Item = &dyn InputSlice> + '_>> {
        self.input_slices
            .as_ref()
            .map(|v| -> Box<dyn Iterator<Item = &dyn InputSlice> + '_> {
                Box::new(v.iter().map(|i| -> &dyn InputSlice { i }))
            })
    }
    fn source(&self) -> Option<DatasetSource> {
        self.source.as_ref().map(|v| -> DatasetSource { v.into() })
    }
    fn vocab(&self) -> Option<&dyn DatasetVocabulary> {
        self.vocab.as_ref().map(|v| -> &dyn DatasetVocabulary { v })
    }
    fn seed(&self) -> Option<&DatasetID> {
        self.seed.as_ref().map(|v| -> &DatasetID { v })
    }
}

impl Into<super::MetadataBlock> for &dyn MetadataBlock {
    fn into(self) -> super::MetadataBlock {
        super::MetadataBlock {
            prev_block_hash: self.prev_block_hash().map(|v| v.clone()),
            system_time: self.system_time(),
            output_slice: self.output_slice().map(|v| v.into()),
            output_watermark: self.output_watermark().map(|v| v),
            input_slices: self.input_slices().map(|v| v.map(|i| i.into()).collect()),
            source: self.source().map(|v| v.into()),
            vocab: self.vocab().map(|v| v.into()),
            seed: self.seed().map(|v| v.clone()),
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

impl OffsetInterval for super::OffsetInterval {
    fn start(&self) -> i64 {
        self.start
    }
    fn end(&self) -> i64 {
        self.end
    }
}

impl Into<super::OffsetInterval> for &dyn OffsetInterval {
    fn into(self) -> super::OffsetInterval {
        super::OffsetInterval {
            start: self.start(),
            end: self.end(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// OutputSlice
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#outputslice-schema
////////////////////////////////////////////////////////////////////////////////

pub trait OutputSlice {
    fn data_logical_hash(&self) -> &Multihash;
    fn data_physical_hash(&self) -> &Multihash;
    fn data_interval(&self) -> &dyn OffsetInterval;
}

impl OutputSlice for super::OutputSlice {
    fn data_logical_hash(&self) -> &Multihash {
        &self.data_logical_hash
    }
    fn data_physical_hash(&self) -> &Multihash {
        &self.data_physical_hash
    }
    fn data_interval(&self) -> &dyn OffsetInterval {
        &self.data_interval
    }
}

impl Into<super::OutputSlice> for &dyn OutputSlice {
    fn into(self) -> super::OutputSlice {
        super::OutputSlice {
            data_logical_hash: self.data_logical_hash().clone(),
            data_physical_hash: self.data_physical_hash().clone(),
            data_interval: self.data_interval().into(),
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

impl<'a> From<&'a super::PrepStep> for PrepStep<'a> {
    fn from(other: &'a super::PrepStep) -> Self {
        match other {
            super::PrepStep::Decompress(v) => PrepStep::Decompress(v),
            super::PrepStep::Pipe(v) => PrepStep::Pipe(v),
        }
    }
}

impl Into<super::PrepStep> for PrepStep<'_> {
    fn into(self) -> super::PrepStep {
        match self {
            PrepStep::Decompress(v) => super::PrepStep::Decompress(v.into()),
            PrepStep::Pipe(v) => super::PrepStep::Pipe(v.into()),
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

impl PrepStepDecompress for super::PrepStepDecompress {
    fn format(&self) -> CompressionFormat {
        self.format
    }
    fn sub_path(&self) -> Option<&str> {
        self.sub_path.as_ref().map(|v| -> &str { v.as_ref() })
    }
}

impl PrepStepPipe for super::PrepStepPipe {
    fn command(&self) -> Box<dyn Iterator<Item = &str> + '_> {
        Box::new(self.command.iter().map(|i| -> &str { i.as_ref() }))
    }
}

impl Into<super::PrepStepDecompress> for &dyn PrepStepDecompress {
    fn into(self) -> super::PrepStepDecompress {
        super::PrepStepDecompress {
            format: self.format().into(),
            sub_path: self.sub_path().map(|v| v.to_owned()),
        }
    }
}

impl Into<super::PrepStepPipe> for &dyn PrepStepPipe {
    fn into(self) -> super::PrepStepPipe {
        super::PrepStepPipe {
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
}

impl<'a> From<&'a super::ReadStep> for ReadStep<'a> {
    fn from(other: &'a super::ReadStep) -> Self {
        match other {
            super::ReadStep::Csv(v) => ReadStep::Csv(v),
            super::ReadStep::JsonLines(v) => ReadStep::JsonLines(v),
            super::ReadStep::GeoJson(v) => ReadStep::GeoJson(v),
            super::ReadStep::EsriShapefile(v) => ReadStep::EsriShapefile(v),
        }
    }
}

impl Into<super::ReadStep> for ReadStep<'_> {
    fn into(self) -> super::ReadStep {
        match self {
            ReadStep::Csv(v) => super::ReadStep::Csv(v.into()),
            ReadStep::JsonLines(v) => super::ReadStep::JsonLines(v.into()),
            ReadStep::GeoJson(v) => super::ReadStep::GeoJson(v.into()),
            ReadStep::EsriShapefile(v) => super::ReadStep::EsriShapefile(v.into()),
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

impl ReadStepCsv for super::ReadStepCsv {
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

impl ReadStepJsonLines for super::ReadStepJsonLines {
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

impl ReadStepGeoJson for super::ReadStepGeoJson {
    fn schema(&self) -> Option<Box<dyn Iterator<Item = &str> + '_>> {
        self.schema
            .as_ref()
            .map(|v| -> Box<dyn Iterator<Item = &str> + '_> {
                Box::new(v.iter().map(|i| -> &str { i.as_ref() }))
            })
    }
}

impl ReadStepEsriShapefile for super::ReadStepEsriShapefile {
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

impl Into<super::ReadStepCsv> for &dyn ReadStepCsv {
    fn into(self) -> super::ReadStepCsv {
        super::ReadStepCsv {
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

impl Into<super::ReadStepJsonLines> for &dyn ReadStepJsonLines {
    fn into(self) -> super::ReadStepJsonLines {
        super::ReadStepJsonLines {
            schema: self.schema().map(|v| v.map(|i| i.to_owned()).collect()),
            date_format: self.date_format().map(|v| v.to_owned()),
            encoding: self.encoding().map(|v| v.to_owned()),
            multi_line: self.multi_line().map(|v| v),
            primitives_as_string: self.primitives_as_string().map(|v| v),
            timestamp_format: self.timestamp_format().map(|v| v.to_owned()),
        }
    }
}

impl Into<super::ReadStepGeoJson> for &dyn ReadStepGeoJson {
    fn into(self) -> super::ReadStepGeoJson {
        super::ReadStepGeoJson {
            schema: self.schema().map(|v| v.map(|i| i.to_owned()).collect()),
        }
    }
}

impl Into<super::ReadStepEsriShapefile> for &dyn ReadStepEsriShapefile {
    fn into(self) -> super::ReadStepEsriShapefile {
        super::ReadStepEsriShapefile {
            schema: self.schema().map(|v| v.map(|i| i.to_owned()).collect()),
            sub_path: self.sub_path().map(|v| v.to_owned()),
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

impl<'a> From<&'a super::SourceCaching> for SourceCaching<'a> {
    fn from(other: &'a super::SourceCaching) -> Self {
        match other {
            super::SourceCaching::Forever => SourceCaching::Forever,
        }
    }
}

impl Into<super::SourceCaching> for SourceCaching<'_> {
    fn into(self) -> super::SourceCaching {
        match self {
            SourceCaching::Forever => super::SourceCaching::Forever,
            SourceCaching::_Phantom(_) => panic!(),
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

impl SqlQueryStep for super::SqlQueryStep {
    fn alias(&self) -> Option<&str> {
        self.alias.as_ref().map(|v| -> &str { v.as_ref() })
    }
    fn query(&self) -> &str {
        self.query.as_ref()
    }
}

impl Into<super::SqlQueryStep> for &dyn SqlQueryStep {
    fn into(self) -> super::SqlQueryStep {
        super::SqlQueryStep {
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
    fn id(&self) -> &str;
    fn primary_key(&self) -> Box<dyn Iterator<Item = &str> + '_>;
}

impl TemporalTable for super::TemporalTable {
    fn id(&self) -> &str {
        self.id.as_ref()
    }
    fn primary_key(&self) -> Box<dyn Iterator<Item = &str> + '_> {
        Box::new(self.primary_key.iter().map(|i| -> &str { i.as_ref() }))
    }
}

impl Into<super::TemporalTable> for &dyn TemporalTable {
    fn into(self) -> super::TemporalTable {
        super::TemporalTable {
            id: self.id().to_owned(),
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

impl<'a> From<&'a super::Transform> for Transform<'a> {
    fn from(other: &'a super::Transform) -> Self {
        match other {
            super::Transform::Sql(v) => Transform::Sql(v),
        }
    }
}

impl Into<super::Transform> for Transform<'_> {
    fn into(self) -> super::Transform {
        match self {
            Transform::Sql(v) => super::Transform::Sql(v.into()),
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

impl TransformSql for super::TransformSql {
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

impl Into<super::TransformSql> for &dyn TransformSql {
    fn into(self) -> super::TransformSql {
        super::TransformSql {
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

impl TransformInput for super::TransformInput {
    fn id(&self) -> Option<&DatasetID> {
        self.id.as_ref().map(|v| -> &DatasetID { v })
    }
    fn name(&self) -> &DatasetName {
        &self.name
    }
}

impl Into<super::TransformInput> for &dyn TransformInput {
    fn into(self) -> super::TransformInput {
        super::TransformInput {
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

impl Watermark for super::Watermark {
    fn system_time(&self) -> DateTime<Utc> {
        self.system_time
    }
    fn event_time(&self) -> DateTime<Utc> {
        self.event_time
    }
}

impl Into<super::Watermark> for &dyn Watermark {
    fn into(self) -> super::Watermark {
        super::Watermark {
            system_time: self.system_time(),
            event_time: self.event_time(),
        }
    }
}
