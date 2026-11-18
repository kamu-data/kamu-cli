// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{ErrorIntoInternal, InternalError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Maps a failed `HEAD` update onto an operation error: a lost compare-and-swap
/// keeps its own variant, a missing block is internal.
pub fn map_set_chain_ref_error<E>(e: odf::dataset::SetChainRefError) -> E
where
    E: From<odf::dataset::RefCASError> + From<odf::AccessError> + From<InternalError>,
{
    use odf::dataset::SetChainRefError as SE;

    match e {
        SE::CASFailed(e) => e.into(),
        SE::Access(e) => e.into(),
        SE::Internal(e) => e.into(),
        e @ SE::BlockNotFound(_) => e.int_err().into(),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
