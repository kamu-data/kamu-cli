// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use opendatafabric::Multihash;
use serde::{Deserialize, Serialize};
use url::Url;

/////////////////////////////////////////////////////////////////////////////////////////

/// Initial dataset pull request message
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetPullRequest {
    pub begin_after: Option<Multihash>,
    pub stop_at: Option<Multihash>,
}

/// Response to initial dataset pull request message
pub type DatasetPullResponse = Result<DatasetPullSuccessResponse, DatasetPullRequestError>;

/// Succesful response to initial dataset pull request
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetPullSuccessResponse {
    pub size_estimate: TransferSizeEstimate,
}

// Unsuccesful response to initial dataset pull request
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub enum DatasetPullRequestError {
    Internal(DatasetInternalError),
    InvalidInterval(DatasetPullInvalidIntervalError),
}

// Error: pulling a range that is not a valid chain interval
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetPullInvalidIntervalError {
    pub head: Multihash,
    pub tail: Multihash,
}

/// Pull stage 1: request metadata update
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetPullMetadataRequest {}

/// Pull stage 1: metadata update response
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetMetadataPullResponse {
    pub blocks: ObjectsBatch,
}

/// Pull stage 2: object transfer request
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetPullObjectsTransferRequest {
    pub object_files: Vec<ObjectFileReference>,
}

// Pull stage 2: object transfer response
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetPullObjectsTransferResponse {
    pub object_transfer_strategies: Vec<PullObjectTransferStrategy>,
}

/// Pull object transfer strategy
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct PullObjectTransferStrategy {
    pub object_file: ObjectFileReference,
    pub pull_strategy: ObjectPullStrategy,
    pub download_from: TransferUrl,
}

// Object pull strategy enumeration
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub enum ObjectPullStrategy {
    HttpDownload,
}

/// Initial dataset push request message
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetPushRequest {
    pub current_head: Option<Multihash>,
    pub size_estimate: TransferSizeEstimate,
}

/// Response to initial dataset push request message
pub type DatasetPushResponse = Result<DatasetPushRequestAccepted, DatasetPushRequestError>;

/// Succes response to initial dataset push request message
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetPushRequestAccepted {}

// Unsuccesful response to initial dataset push request
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub enum DatasetPushRequestError {
    Internal(DatasetInternalError),
    InvalidHead(DatasetPushInvalidHeadError),
}

// Wrong head suggested during push. Client's data on what the head is got out
// of date.
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetPushInvalidHeadError {
    pub actual_head: Option<Multihash>,
    pub expected_head: Option<Multihash>,
}

/// Push phase 1: push metadata request
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetPushMetadataRequest {
    pub new_blocks: ObjectsBatch,
}

/// Push phase 1: push metadata accepted
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetPushMetadataAccepted {}

/// Push phase 2: object transfer request
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetPushObjectsTransferRequest {
    pub object_files: Vec<ObjectFileReference>,
    pub is_truncated: bool,
}

/// Push phase 2: object transfer response
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetPushObjectsTransferResponse {
    pub object_transfer_strategies: Vec<PushObjectTransferStrategy>,
}

/// Push object transfer strategy
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct PushObjectTransferStrategy {
    pub object_file: ObjectFileReference,
    pub push_strategy: ObjectPushStrategy,
    pub upload_to: Option<TransferUrl>,
}

// Object push strategy enumeration
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub enum ObjectPushStrategy {
    SkipUpload,
    HttpUpload,
}

/// Push stage 3: object upload progress request
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetPushObjectsUploadProgressRequest {}

/// Push stage 3: object upload progress response
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetPushObjectsUploadProgressResponse {
    pub details: ObjectsUploadProgressDetails,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub enum ObjectsUploadProgressDetails {
    Running(ObjectsUploadProgressDetailsRunning),
    Complete,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct ObjectsUploadProgressDetailsRunning {
    pub uploaded_objects_count: i32,
}

/// Push stage 4: complete handshake indication
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetPushComplete {}

/// Push stage 3: complete handshake acknowledge
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetPushCompleteConfirmed {}

/// Packed object batch
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct ObjectsBatch {
    pub objects_count: u32,
    pub object_type: ObjectType,
    pub media_type: String,
    pub encoding: String,
    pub payload: Vec<u8>,
}

/// Transfer size estimate
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct TransferSizeEstimate {
    pub num_blocks: u32,
    pub num_objects: u32,
    pub bytes_in_raw_blocks: u64,
    pub bytes_in_raw_objects: u64,
}

// Object file reference
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct ObjectFileReference {
    pub object_type: ObjectType,
    pub physical_hash: Multihash,
    pub size: i64,
}

// Object type enumeration
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub enum ObjectType {
    MetadataBlock,
    DataSlice,
    Checkpoint,
}

// Transfer URL
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct TransferUrl {
    pub url: Url,
    pub expires_at: Option<DateTime<Utc>>,
}

// Transfer URL
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetInternalError {
    pub error_message: String,
}

/////////////////////////////////////////////////////////////////////////////////////////
