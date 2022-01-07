// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;
use opendatafabric::*;

use url::Url;

pub trait RemoteRepositoryRegistry: Send + Sync {
    fn get_all_repositories<'s>(&'s self) -> Box<dyn Iterator<Item = RepositoryName> + 's>;

    fn get_repository(&self, repo_name: &RepositoryName) -> Result<Repository, DomainError>;

    fn add_repository(&self, repo_name: &RepositoryName, url: Url) -> Result<(), DomainError>;

    fn delete_repository(&self, repo_name: &RepositoryName) -> Result<(), DomainError>;
}
