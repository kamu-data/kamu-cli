// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;

use super::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A resolved handle to the local dataset
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct DatasetHandle {
    pub id: DatasetID,
    pub alias: DatasetAlias,
}

impl DatasetHandle {
    pub fn new(id: DatasetID, alias: DatasetAlias) -> Self {
        Self { id, alias }
    }

    pub fn as_local_ref(&self) -> DatasetRef {
        DatasetRef::from(self)
    }

    pub fn into_local_ref(self) -> DatasetRef {
        DatasetRef::from(self)
    }

    pub fn as_any_ref(&self) -> DatasetRefAny {
        DatasetRefAny::from(self)
    }

    pub fn into_any_ref(self) -> DatasetRefAny {
        DatasetRefAny::from(self)
    }
}

impl fmt::Display for DatasetHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", &self.alias)
    }
}

impl fmt::Debug for DatasetHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("DatasetHandle")
            .field(&self.id)
            .field(&self.alias)
            .finish()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A resolved handle to the remote dataset
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct DatasetHandleRemote {
    pub id: DatasetID,
    pub alias: DatasetAliasRemote,
}

impl DatasetHandleRemote {
    pub fn new(id: DatasetID, alias: DatasetAliasRemote) -> Self {
        Self { id, alias }
    }

    pub fn as_remote_ref(&self) -> DatasetRefRemote {
        DatasetRefRemote::from(self)
    }

    pub fn into_remote_ref(self) -> DatasetRefRemote {
        DatasetRefRemote::from(self)
    }

    pub fn as_any_ref(&self) -> DatasetRefAny {
        DatasetRefAny::from(self)
    }

    pub fn into_any_ref(self) -> DatasetRefAny {
        DatasetRefAny::from(self)
    }
}

impl fmt::Display for DatasetHandleRemote {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", &self.alias)
    }
}

impl fmt::Debug for DatasetHandleRemote {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("RemoteDatasetHandle")
            .field(&self.id)
            .field(&self.alias)
            .finish()
    }
}
