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
