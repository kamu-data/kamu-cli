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
use thiserror::Error;

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

    /// The dataset's only alias of the kind; `None` when it has none
    fn single_by_kind(
        &self,
        kind: RemoteAliasKind,
    ) -> Result<Option<&odf::DatasetRefRemote>, AmbiguousRemoteAliasError> {
        let mut aliases = self.get_by_kind(kind);
        let first = aliases.next();
        if aliases.next().is_some() {
            return Err(AmbiguousRemoteAliasError { kind });
        }
        Ok(first)
    }

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A dataset has several aliases of a kind, so none of them can be chosen
#[derive(Debug, Error)]
#[error("Dataset has several {kind:?} aliases")]
pub struct AmbiguousRemoteAliasError {
    pub kind: RemoteAliasKind,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
