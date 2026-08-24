// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Registry key for a per-type resource dispatcher.
///
/// `schema` is intentionally a `&'static str` (not a `TypeUri`): dill's
/// `#[meta(...)]` emits the metadata as a `const`, so the value must be
/// const-evaluable. It is only ever used as an internal registry key; the
/// public schema identity type is `TypeUri` everywhere else.
#[derive(Debug, Clone)]
pub struct ResourceDispatcherMeta {
    pub schema: &'static str,
    pub canonical_selector: &'static str,
    pub selector_aliases: &'static [&'static str],
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
