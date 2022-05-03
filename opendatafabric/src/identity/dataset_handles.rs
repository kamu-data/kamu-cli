// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;

use super::dataset_identity::*;
use super::dataset_refs::*;

////////////////////////////////////////////////////////////////////////////////

/// A resolved handle to the local dataset
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct DatasetHandle {
    pub id: DatasetID,
    pub name: DatasetName,
}

impl DatasetHandle {
    pub fn new(id: DatasetID, name: DatasetName) -> Self {
        Self { id, name }
    }

    pub fn as_local_ref(&self) -> DatasetRefLocal {
        DatasetRefLocal::Handle(self.clone())
    }

    pub fn as_any_ref(&self) -> DatasetRefAny {
        DatasetRefAny::Handle(self.clone())
    }
}

impl fmt::Display for DatasetHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", &self.name)
    }
}

impl fmt::Debug for DatasetHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("DatasetHandle")
            .field(&self.id)
            .field(&self.name)
            .finish()
    }
}

////////////////////////////////////////////////////////////////////////////////

/// A resolved handle to the remote dataset
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct RemoteDatasetHandle {
    pub id: DatasetID,
    pub name: RemoteDatasetName,
}

impl RemoteDatasetHandle {
    pub fn new(id: DatasetID, name: RemoteDatasetName) -> Self {
        Self { id, name }
    }

    pub fn as_remote_ref(&self) -> DatasetRefRemote {
        DatasetRefRemote::RemoteHandle(self.clone())
    }

    pub fn as_any_ref(&self) -> DatasetRefAny {
        DatasetRefAny::RemoteHandle(self.clone())
    }
}

impl fmt::Display for RemoteDatasetHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", &self.name)
    }
}

impl fmt::Debug for RemoteDatasetHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("RemoteDatasetHandle")
            .field(&self.id)
            .field(&self.name)
            .finish()
    }
}

////////////////////////////////////////////////////////////////////////////////

/// A resolved handle to the dataset inside a repository
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct RepoDatasetHandle {
    pub id: DatasetID,
    pub name: DatasetNameWithOwner,
}

impl RepoDatasetHandle {
    pub fn new(id: DatasetID, name: DatasetNameWithOwner) -> Self {
        Self { id, name }
    }
}

impl fmt::Display for RepoDatasetHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", &self.name)
    }
}

impl fmt::Debug for RepoDatasetHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("RepoDatasetHandle")
            .field(&self.id)
            .field(&self.name)
            .finish()
    }
}
