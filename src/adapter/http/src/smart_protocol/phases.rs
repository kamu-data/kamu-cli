// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

use std::fmt;

use serde::{Deserialize, Serialize};

#[allow(clippy::enum_variant_names)]
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum PullPhase {
    InitialRequest,
    MetadataRequest,
    ObjectsRequest,
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum PushPhase {
    InitialRequest,
    MetadataRequest,
    ObjectsRequest,
    EnsuringTargetExists,
    ObjectsUploadProgress,
    CompleteRequest,
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum TransferPhase {
    Pull(PullPhase),
    Push(PushPhase),
}

impl From<PullPhase> for TransferPhase {
    fn from(value: PullPhase) -> Self {
        Self::Pull(value)
    }
}

impl From<PushPhase> for TransferPhase {
    fn from(value: PushPhase) -> Self {
        Self::Push(value)
    }
}

impl fmt::Display for PullPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let phase = match self {
            PullPhase::InitialRequest => "Initial Request",
            PullPhase::MetadataRequest => "Metadata Request",
            PullPhase::ObjectsRequest => "Objects Request",
        };
        write!(f, "Pull Phase: {phase}")
    }
}

impl fmt::Display for PushPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let phase = match self {
            PushPhase::InitialRequest => "Initial Request",
            PushPhase::MetadataRequest => "Metadata Request",
            PushPhase::ObjectsRequest => "Objects Request",
            PushPhase::EnsuringTargetExists => "Ensuring Target Dataset Exists",
            PushPhase::ObjectsUploadProgress => "Objects Upload Progress",
            PushPhase::CompleteRequest => "Complete Request",
        };
        write!(f, "Push Phase: {phase}")
    }
}
impl fmt::Display for TransferPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransferPhase::Pull(phase) => write!(f, "{phase}"),
            TransferPhase::Push(phase) => write!(f, "{phase}"),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
