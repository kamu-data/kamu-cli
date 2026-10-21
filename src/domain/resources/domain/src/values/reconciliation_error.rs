// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{
    RESOURCE_CONDITION_RECONCILIATION_ERROR_SCHEMA_URI,
    ResourceConditions,
    TypeRef,
    TypeUri,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Why the main controller failed to reconcile the latest generation of a
/// resource. Present in the conditions only while that failure stands.
pub type ReconciliationError = odf::metadata::resources::ReconciliationError;

/// [`ReconciliationError`] codes shared by resource types; each type adds its
/// own for the failures specific to it
pub const RECONCILIATION_ERROR_CODE_INTERNAL_ERROR: &str = "internal-error";
pub const RECONCILIATION_ERROR_CODE_CONCURRENT_MODIFICATION: &str = "concurrent-modification";
/// Cleanup of a resource being deleted failed
pub const RECONCILIATION_ERROR_CODE_CLEANUP_FAILED: &str = "cleanup-failed";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn reconciliation_error_type_ref() -> TypeRef {
    TypeRef::Uri(TypeUri::new_unchecked(
        RESOURCE_CONDITION_RECONCILIATION_ERROR_SCHEMA_URI,
    ))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait ResourceConditionsReconciliationErrorExt {
    fn set_reconciliation_error(&mut self, error: ReconciliationError);

    fn clear_reconciliation_error(&mut self);
}

impl ResourceConditionsReconciliationErrorExt for ResourceConditions {
    fn set_reconciliation_error(&mut self, error: ReconciliationError) {
        let proxy = odf::metadata::serde::yaml::resources::ReconciliationError::from(error);
        self.entries.insert(
            reconciliation_error_type_ref(),
            serde_json::to_value(proxy).unwrap(),
        );
    }

    fn clear_reconciliation_error(&mut self) {
        self.entries.remove(&reconciliation_error_type_ref());
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
