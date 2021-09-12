// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;

use crate::infra::WorkspaceLayout;

// TODO
pub struct WorkspaceFactory {
    _workspace_layout: WorkspaceLayout,
}

/// Allows easily creating kamu workspaces with a desired state
impl WorkspaceFactory {
    /// Create new factory instance for an existing workspace directory
    pub fn new(workspace_root: &Path) -> Self {
        let workspace_layout = WorkspaceLayout::new(workspace_root);
        Self {
            _workspace_layout: workspace_layout,
        }
    }

    /// Create new workspace in specified directory
    pub fn create(workspace_root: &Path) -> Self {
        let workspace_layout = WorkspaceLayout::create(workspace_root).unwrap();
        Self {
            _workspace_layout: workspace_layout,
        }
    }
}
