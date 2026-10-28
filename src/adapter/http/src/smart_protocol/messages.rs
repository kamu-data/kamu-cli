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
    /// The server's `HEAD`
    pub actual_head: Option<odf::Multihash>,
    /// The `HEAD` the client planned the push from
    pub expected_head: Option<odf::Multihash>,
}

impl From<&odf::dataset::RefCASError> for DatasetPushInvalidHeadError {
    fn from(e: &odf::dataset::RefCASError) -> Self {
        Self {
            actual_head: e.actual.clone(),
            expected_head: e.expected.clone(),
        }
    }
}

impl From<DatasetPushInvalidHeadError> for odf::dataset::RefCASError {
    fn from(e: DatasetPushInvalidHeadError) -> Self {
        Self {
            reference: odf::BlockRef::Head,
            expected: e.expected_head,
            actual: e.actual_head,
        }
    }
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

/// Push stage 4: response to the complete request.
///
/// A client that predates it reads `{"Ok": {}}` as a bare
/// `DatasetPushCompleteConfirmed`, ignoring the unknown key.
pub type DatasetPushCompleteResponse =
    Result<DatasetPushCompleteConfirmed, DatasetPushCompleteError>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Push stage 4: the pushed blocks could not be committed
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub enum DatasetPushCompleteError {
    Internal(TransferInternalError),
    UpdatedConcurrently(DatasetPushUpdatedConcurrentlyError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The destination `HEAD` moved while the pushed blocks were being committed;
/// nothing was committed.
///
/// It carries no heads: errors of this stage travel as the close frame reason,
/// which is limited to 123 bytes. A text message would not do, since a client
/// that predates `DatasetPushCompleteResponse` takes any text reply for a
/// confirmation.
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetPushUpdatedConcurrentlyError {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Push stage 4: the complete response as a client reads it, accepting the
/// bare confirmation sent by a server that predates
/// `DatasetPushCompleteResponse`
#[derive(Deserialize, Clone, PartialEq, Eq, Debug)]
#[serde(untagged)]
pub enum DatasetPushCompleteReply {
    Response(DatasetPushCompleteResponse),
    Legacy(DatasetPushCompleteLegacyConfirmed),
}

/// The bare confirmation of a server that predates
/// `DatasetPushCompleteResponse`. Only an empty object matches, so an error
/// this client does not know is never taken for a confirmation.
#[derive(Deserialize, Clone, PartialEq, Eq, Debug)]
#[serde(deny_unknown_fields)]
pub struct DatasetPushCompleteLegacyConfirmed {}

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
