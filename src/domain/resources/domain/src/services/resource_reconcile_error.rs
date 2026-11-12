// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub trait ResourceReconcileError: std::error::Error + Send + Sync {
    /// Machine-readable kebab-case code of the reported `ReconciliationError`
    fn code(&self) -> &'static str;
    fn user_message(&self) -> String;
    fn is_transient(&self) -> bool;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
