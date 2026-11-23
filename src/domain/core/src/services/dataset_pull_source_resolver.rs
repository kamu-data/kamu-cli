// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::{FindPullAliasError, ResolvePullSourceError, ResolveRemoteSourceError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Finds the remote a local dataset is pulled from.
///
/// A dataset with one pull alias is pulled from it. Several pull aliases are
/// ambiguous.
#[cfg_attr(feature = "testing", mockall::automock)]
#[async_trait::async_trait]
pub trait DatasetPullSourceResolver: Send + Sync {
    /// Whether the dataset has any pull alias, ambiguous ones included. Such a
    /// dataset is updated by pulling only, so writing to it locally would
    /// diverge its history from the remote. Reads the alias registry only
    async fn has_pull_source(&self, target: &odf::DatasetHandle) -> Result<bool, InternalError>;

    /// The dataset's pull alias. Reads the alias registry only
    async fn find_pull_alias(
        &self,
        target: &odf::DatasetHandle,
    ) -> Result<Option<odf::DatasetRefRemote>, FindPullAliasError>;

    /// The dataset's pull alias with the URL to pull from
    async fn resolve_pull_source(
        &self,
        target: &odf::DatasetHandle,
    ) -> Result<Option<DatasetPullSource>, ResolvePullSourceError>;

    /// The URL to pull a remote reference from. A repository on the ODF
    /// protocol may be asked for the account when the reference names none
    async fn resolve_remote_source(
        &self,
        remote_ref: &odf::DatasetRefRemote,
    ) -> Result<DatasetPullSource, ResolveRemoteSourceError>;

    /// The current account's dataset that has this pull alias
    async fn find_dataset_by_pull_alias(
        &self,
        remote_ref: &odf::DatasetRefRemote,
    ) -> Result<Option<odf::DatasetHandle>, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A remote a local dataset is pulled from, with the URL it resolves to
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetPullSource {
    pub remote_ref: odf::DatasetRefRemote,
    pub url: url::Url,
}

impl DatasetPullSource {
    /// The endpoint a sync task reads, without credentials
    pub fn to_sync_endpoint(&self) -> odf::metadata::tasks::SyncEndpoint {
        odf::metadata::tasks::SyncEndpoint {
            url: self.url.to_string(),
            auth: None,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use url::Url;

    use super::*;

    #[test]
    fn test_pull_source_endpoint_carries_the_url_without_credentials() {
        let source = DatasetPullSource {
            remote_ref: odf::DatasetRefRemote::try_from("repo/bar").unwrap(),
            url: Url::parse("odf+https://example.com/bar/").unwrap(),
        };

        assert_eq!(
            source.to_sync_endpoint(),
            odf::metadata::tasks::SyncEndpoint {
                url: "odf+https://example.com/bar/".to_owned(),
                auth: None,
            }
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
