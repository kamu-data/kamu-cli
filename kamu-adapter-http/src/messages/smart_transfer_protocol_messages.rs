// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use url::Url;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use opendatafabric::Multihash;


/// Initial dataset pull request message
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetPullRequest {
    pub begin_after: Option<Multihash>,
    pub stop_at: Option<Multihash>,
}


/// Response to initial dataset pull request
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetPullResponse {
    pub size_estimation: TransferSizeEstimation,
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
    object_transfer_strategies: Vec<PullObjectTransferStrategy>,
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
    pub current_head: Multihash,
    pub size_estimation: TransferSizeEstimation,
}


/// Response to initial dataset push request
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetPushRequestAccepted {}


/// Push phase 1: push metadata request
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetPushMetadata {
    pub new_blocks: ObjectsBatch,
}


/// Push phase 1: push metadata accepted
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetPushMetadataAccepted {}


/// Push phase 2: object transfer request
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetPushObjectsTransferRequest {
    pub object_files: Vec<ObjectFileReference>,
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


/// Push stage 3: complete handshake indication
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetPushComplete {}


/// Push stage 3: complete handshake acknowledge
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetPushCompleteConfirmed {}


/// Error message
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DatasetError {
    pub error_details: ErrorDetails,
}


/// Error message details
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct ErrorDetails {
    pub error_code: String,
    pub description: String,
}

/// Packed object batch
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct ObjectsBatch {
    pub objects_count: u32,
    pub object_type: ObjectType,
    pub media_type: String,
    pub encoding: String,
    pub payload: Vec<u8>,
}


/// Transfer size estimation
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct TransferSizeEstimation {
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
