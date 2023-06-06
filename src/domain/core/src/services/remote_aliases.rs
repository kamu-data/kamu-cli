// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric::DatasetRefRemote;

use crate::InternalError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteAliasKind {
    Pull,
    Push,
}

pub trait RemoteAliases: Send {
    fn get_by_kind<'a>(
        &'a self,
        kind: RemoteAliasKind,
    ) -> Box<dyn Iterator<Item = &'a DatasetRefRemote> + 'a>;

    fn contains(&self, remore_ref: &DatasetRefRemote, kind: RemoteAliasKind) -> bool;

    fn is_empty(&self, kind: RemoteAliasKind) -> bool;

    fn add(
        &mut self,
        remote_ref: &DatasetRefRemote,
        kind: RemoteAliasKind,
    ) -> Result<bool, InternalError>;

    fn delete(
        &mut self,
        remote_ref: &DatasetRefRemote,
        kind: RemoteAliasKind,
    ) -> Result<bool, InternalError>;

    fn clear(&mut self, kind: RemoteAliasKind) -> Result<usize, InternalError>;
}
