// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Message of every result reporting a failed compare-and-swap of a dataset
/// `HEAD`
pub(crate) const CAS_FAILED_MESSAGE: &str =
    "Expected head didn't match, dataset was likely updated concurrently";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Heads of a failed compare-and-swap of a dataset `HEAD`, as reported to
/// clients
pub(crate) struct CasFailedHeads {
    pub expected_head: Multihash<'static>,
    pub actual_head: Option<Multihash<'static>>,
}

impl CasFailedHeads {
    /// Fails when the swap expected no head: these mutations always commit on
    /// top of an existing one
    pub(crate) fn try_from_ref_cas_error(
        e: odf::dataset::RefCASError,
    ) -> Result<Self, InternalError> {
        let Some(expected_head) = e.expected.clone() else {
            return Err(e.int_err());
        };
        Ok(Self {
            expected_head: expected_head.into(),
            actual_head: e.actual.map(Into::into),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
