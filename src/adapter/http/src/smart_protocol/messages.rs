// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use url::Url;

use super::phases::TransferPhase;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const SMART_TRANSFER_PROTOCOL_VERSION: i32 = 1;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Initial dataset pull request message
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetPullRequest {
    pub begin_after: Option<odf::Multihash>,
    pub stop_at: Option<odf::Multihash>,
    pub force_update_if_diverged: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Response to initial dataset pull request message
pub type DatasetPullResponse = Result<DatasetPullSuccessResponse, DatasetPullRequestError>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Successful response to initial dataset pull request
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetPullSuccessResponse {
    pub transfer_plan: TransferPlan,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Unsuccessful response to initial dataset pull request
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub enum DatasetPullRequestError {
    SeedBlockOverwriteRestricted(DatasetPushMetadataSeedBlockOverwriteRestrictedError),
    Internal(TransferInternalError),
    InvalidInterval(DatasetPullInvalidIntervalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Error: pulling a range that is not a valid chain interval
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetPullInvalidIntervalError {
    pub head: odf::Multihash,
    pub tail: odf::Multihash,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Pull stage 1: request metadata update
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetPullMetadataRequest {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Pull stage 1: metadata update response
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetMetadataPullResponse {
    pub blocks: MetadataBlocksBatch,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Pull stage 2: object transfer request
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetPullObjectsTransferRequest {
    pub object_files: Vec<ObjectFileReference>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Pull stage 2: object transfer response
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetPullObjectsTransferResponse {
    pub object_transfer_strategies: Vec<PullObjectTransferStrategy>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Pull object transfer strategy
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct PullObjectTransferStrategy {
    pub object_file: ObjectFileReference,
    pub pull_strategy: ObjectPullStrategy,
    pub download_from: TransferUrl,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Object pull strategy enumeration
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub enum ObjectPullStrategy {
    HttpDownload,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Initial dataset push request message
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetPushRequest {
    pub current_head: Option<odf::Multihash>,
    pub transfer_plan: TransferPlan,
    pub force_update_if_diverged: bool,
    pub visibility_for_created_dataset: odf::DatasetVisibility,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Response to initial dataset push request message
pub type DatasetPushResponse = Result<DatasetPushRequestAccepted, DatasetPushRequestError>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Success response to initial dataset push request message
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetPushRequestAccepted {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Unsuccessful response to initial dataset push request
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub enum DatasetPushRequestError {
    Internal(TransferInternalError),
    InvalidHead(DatasetPushInvalidHeadError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Wrong head suggested during push. Client's data on what the head is got out
/// of date.
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetPushInvalidHeadError {
    pub actual_head: Option<odf::Multihash>,
    pub expected_head: Option<odf::Multihash>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Push phase 1: push metadata request
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetPushMetadataRequest {
    pub new_blocks: MetadataBlocksBatch,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Push phase 1: push metadata accepted
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetPushMetadataAccepted {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Push phase 1: unsuccessful response push metadata request
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub enum DatasetPushMetadataError {
    SeedBlockOverwriteRestricted(DatasetPushMetadataSeedBlockOverwriteRestrictedError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Rewriting seed block is restricted
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetPushMetadataSeedBlockOverwriteRestrictedError {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Push phase 2: object transfer response
pub type DatasetPushMetadataResponse =
    Result<DatasetPushMetadataAccepted, DatasetPushMetadataError>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Push phase 2: object transfer request
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetPushObjectsTransferRequest {
    pub object_files: Vec<ObjectFileReference>,
    pub is_truncated: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Push phase 2: object transfer response
pub type DatasetPushObjectsTransferResponse =
    Result<DatasetPushObjectsTransferAccepted, DatasetPushObjectsTransferError>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Push phase 2: object transfer accepted
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetPushObjectsTransferAccepted {
    pub object_transfer_strategies: Vec<PushObjectTransferStrategy>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Push phase 2: unsuccessful response push object transfer request
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub enum DatasetPushObjectsTransferError {
    Internal(TransferInternalError),
    RefCollision(DatasetPushObjectsTransferRefCollisionError),
    NameCollision(DatasetPushObjectsTransferNameCollisionError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Dataset with such id already exists with different alias
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetPushObjectsTransferRefCollisionError {
    pub dataset_id: odf::DatasetID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Dataset with such id already exists
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetPushObjectsTransferNameCollisionError {
    pub dataset_alias: odf::DatasetAlias,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Push object transfer strategy
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct PushObjectTransferStrategy {
    pub object_file: ObjectFileReference,
    pub push_strategy: ObjectPushStrategy,
    pub upload_to: Option<TransferUrl>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Object push strategy enumeration
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub enum ObjectPushStrategy {
    SkipUpload,
    HttpUpload,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Push stage 3: object upload progress request
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetPushObjectsUploadProgressRequest {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Push stage 3: object upload progress response
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetPushObjectsUploadProgressResponse {
    pub details: ObjectsUploadProgressDetails,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub enum ObjectsUploadProgressDetails {
    Running(ObjectsUploadProgressDetailsRunning),
    Complete,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct ObjectsUploadProgressDetailsRunning {
    pub uploaded_objects_count: i32,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Push stage 4: complete handshake indication
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetPushComplete {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Push stage 3: complete handshake acknowledge
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetPushCompleteConfirmed {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Packed metadata blocks batch
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct MetadataBlocksBatch {
    pub num_blocks: u32,
    pub media_type: String,
    pub encoding: String,
    pub payload: Vec<u8>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Transfer plan
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct TransferPlan {
    pub num_blocks: u32,
    pub num_objects: u32,
    pub num_records: u64,
    pub bytes_in_raw_blocks: u64,
    pub bytes_in_raw_objects: u64,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Object file reference
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct ObjectFileReference {
    pub object_type: ObjectType,
    pub physical_hash: odf::Multihash,
    pub size: u64,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Object type enumeration
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub enum ObjectType {
    DataSlice,
    Checkpoint,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Transfer URL
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct TransferUrl {
    pub url: Url,
    pub headers: Vec<HeaderRow>,
    pub expires_at: Option<DateTime<Utc>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct HeaderRow {
    pub name: String,
    pub value: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct TransferInternalError {
    pub phase: TransferPhase,
    pub error_message: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
