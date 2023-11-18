// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric::{AccountID, AccountName, DatasetID};

use crate::*;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpdateTrigger {
    Manual(UpdateTriggerManual),
    AutoPolling(UpdateTriggerAutoPolling),
    Push(UpdateTriggerPush),
    InputDataset(UpdateTriggerInputDataset),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateTriggerManual {
    pub initiator_account_id: AccountID,
    pub initiator_account_name: AccountName,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateTriggerAutoPolling {}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateTriggerPush {
    // TODO: source (HTTP, MQTT, CMD, ...)
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateTriggerInputDataset {
    pub input_dataset_id: DatasetID,
    pub input_update_id: UpdateID,
}

/////////////////////////////////////////////////////////////////////////////////////////
