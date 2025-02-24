// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::Deref;
use std::path::{Path, PathBuf};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const KAMU_WORKSPACE_DIR_NAME: &str = ".kamu";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Newtype path wrapper that designates a directory meant for transient data
/// that can be frequently cleaned up. This directory is suitable for writing
/// operation logs or creating temporary files to pass some state to
/// subprocesses. In case of a local workspace it is guaranteed to be on the
/// same file system as data, thus allowing atomic move operations into the
/// workspace.
///
/// TODO: This type should be replaced by a service
pub struct RunInfoDir(PathBuf);

impl RunInfoDir {
    pub fn new(inner: impl Into<PathBuf>) -> Self {
        Self(inner.into())
    }
    pub fn inner(&self) -> &PathBuf {
        &self.0
    }

    pub fn into_inner(self) -> PathBuf {
        self.0
    }
}

impl AsRef<Path> for RunInfoDir {
    fn as_ref(&self) -> &Path {
        self.0.as_path()
    }
}

impl Deref for RunInfoDir {
    type Target = PathBuf;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Newtype path wrapper that designates a directory meant for data that should
/// be persisted for some unspecified time but can be safely cleaned up.
///
/// TODO: This type should be replaced by a service
pub struct CacheDir(PathBuf);

impl CacheDir {
    pub fn new(inner: impl Into<PathBuf>) -> Self {
        Self(inner.into())
    }
    pub fn inner(&self) -> &PathBuf {
        &self.0
    }

    pub fn into_inner(self) -> PathBuf {
        self.0
    }
}

impl AsRef<Path> for CacheDir {
    fn as_ref(&self) -> &Path {
        self.0.as_path()
    }
}

impl Deref for CacheDir {
    type Target = PathBuf;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
