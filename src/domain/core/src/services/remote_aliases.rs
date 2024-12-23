// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_trait::async_trait;
use internal_error::InternalError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteAliasKind {
    Pull,
    Push,
}

#[async_trait]
pub trait RemoteAliases: Send + Sync {
    fn get_by_kind<'a>(
        &'a self,
        kind: RemoteAliasKind,
    ) -> Box<dyn Iterator<Item = &'a odf::DatasetRefRemote> + 'a>;

    fn contains(&self, remote_ref: &odf::DatasetRefRemote, kind: RemoteAliasKind) -> bool;

    fn is_empty(&self, kind: RemoteAliasKind) -> bool;

    async fn add(
        &mut self,
        remote_ref: &odf::DatasetRefRemote,
        kind: RemoteAliasKind,
    ) -> Result<bool, InternalError>;

    async fn delete(
        &mut self,
        remote_ref: &odf::DatasetRefRemote,
        kind: RemoteAliasKind,
    ) -> Result<bool, InternalError>;

    async fn clear(&mut self, kind: RemoteAliasKind) -> Result<usize, InternalError>;
}
