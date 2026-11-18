// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::CompactionOptions;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_task_system::task_definition_struct! {
    pub struct TaskDefinitionDatasetHardCompact {
        pub target: odf::DatasetHandle,
        pub options: CompactionOptions,
    }
    => "dev.kamu.tasks.dataset.hard_compact"
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
