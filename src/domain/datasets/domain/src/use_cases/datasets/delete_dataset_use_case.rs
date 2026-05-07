// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use thiserror::Error;

use crate::ClassifyByAllowanceDatasetActionUnauthorizedError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DeleteDatasetUseCase: Send + Sync {
    async fn plan_delete(
        &self,
        seed_dataset_handles: Vec<odf::DatasetHandle>,
        recursive: bool,
    ) -> Result<DeleteDatasetPlanningResult, DeleteDatasetPlanningError>;

    async fn execute_plan(
        &self,
        plan: DeleteDatasetPlan,
    ) -> Result<DeleteDatasetExecutionSummary, DeleteDatasetError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, Default)]
pub struct DeleteDatasetPlanningOptions {
    pub allow_orphan_foreign_downstream: bool,
    pub allow_orphan_dangling_references: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Default)]
pub struct DeleteDatasetPlanningResult {
    pub plan: DeleteDatasetPlan,
    pub issues: DeleteDatasetPlanIssues,
}

impl DeleteDatasetPlanningResult {
    pub fn has_blocking_issues(&self, options: DeleteDatasetPlanningOptions) -> bool {
        self.issues.has_blockers(options)
    }

    pub fn is_empty(&self) -> bool {
        self.plan.authorized_targets.is_empty() && self.issues.is_empty()
    }

    pub fn into_executable_plan(
        self,
        options: DeleteDatasetPlanningOptions,
    ) -> Result<DeleteDatasetPlan, DeleteDatasetPlanEvaluationError> {
        self.issues.evaluate_blockers(options)?;
        Ok(self.plan)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Default)]
pub struct DeleteDatasetPlan {
    pub authorized_targets: Vec<DeleteDatasetPlanTarget>,
}

#[derive(Debug)]
pub struct DeleteDatasetPlanTarget {
    pub dataset_handle: odf::DatasetHandle,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Default)]
pub struct DeleteDatasetPlanIssues {
    pub unauthorized_selected_handles: Vec<(
        odf::DatasetHandle,
        ClassifyByAllowanceDatasetActionUnauthorizedError,
    )>,
    pub unauthorized_recursive_handles: Vec<(
        odf::DatasetHandle,
        ClassifyByAllowanceDatasetActionUnauthorizedError,
    )>,
    pub dangling_references: Vec<DanglingReferenceError>,
}

impl DeleteDatasetPlanIssues {
    pub fn is_empty(&self) -> bool {
        self.unauthorized_selected_handles.is_empty()
            && self.unauthorized_recursive_handles.is_empty()
            && self.dangling_references.is_empty()
    }

    pub fn has_blockers(&self, options: DeleteDatasetPlanningOptions) -> bool {
        !self.unauthorized_selected_handles.is_empty()
            || (!options.allow_orphan_dangling_references && !self.dangling_references.is_empty())
            || (!options.allow_orphan_foreign_downstream
                && !self.unauthorized_recursive_handles.is_empty())
    }

    pub fn evaluate_blockers(
        mut self,
        options: DeleteDatasetPlanningOptions,
    ) -> Result<(), DeleteDatasetPlanEvaluationError> {
        if !self.unauthorized_selected_handles.is_empty() {
            let (_, error) = self.unauthorized_selected_handles.remove(0);
            return Err(classify_error_to_delete_plan_evaluation_error(error));
        }

        if !self.dangling_references.is_empty() && !options.allow_orphan_dangling_references {
            return Err(DeleteDatasetPlanEvaluationError::DanglingReference(
                self.dangling_references.remove(0),
            ));
        }

        if !options.allow_orphan_foreign_downstream
            && !self.unauthorized_recursive_handles.is_empty()
        {
            let (_, error) = self.unauthorized_recursive_handles.remove(0);
            return Err(classify_error_to_delete_plan_evaluation_error(error));
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Default)]
pub struct DeleteDatasetExecutionSummary {
    pub deleted_dataset_handles: Vec<odf::DatasetHandle>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum DeleteDatasetPlanningError {
    #[error(transparent)]
    NotFound(#[from] odf::DatasetNotFoundError),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum DeleteDatasetPlanEvaluationError {
    #[error(transparent)]
    NotFound(#[from] odf::DatasetNotFoundError),

    #[error(transparent)]
    DanglingReference(#[from] DanglingReferenceError),

    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        odf::AccessError,
    ),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

fn classify_error_to_delete_plan_evaluation_error(
    error: ClassifyByAllowanceDatasetActionUnauthorizedError,
) -> DeleteDatasetPlanEvaluationError {
    match error {
        ClassifyByAllowanceDatasetActionUnauthorizedError::NotFound(e) => {
            DeleteDatasetPlanEvaluationError::NotFound(e)
        }
        ClassifyByAllowanceDatasetActionUnauthorizedError::Access(e) => {
            DeleteDatasetPlanEvaluationError::Access(e)
        }
        ClassifyByAllowanceDatasetActionUnauthorizedError::Internal(e) => {
            DeleteDatasetPlanEvaluationError::Internal(e)
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Clone, PartialEq, Eq, Debug)]
pub struct DanglingReferenceError {
    pub dataset_handle: odf::DatasetHandle,
    pub children: Vec<odf::DatasetHandle>,
}

impl std::fmt::Display for DanglingReferenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Dataset {} is referenced by: ", self.dataset_handle)?;
        for (i, h) in self.children.iter().enumerate() {
            if i != 0 {
                write!(f, ", ")?;
            }
            write!(f, "{h}")?;
        }
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum DeleteDatasetError {
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

impl From<DeleteDatasetPlanningError> for DeleteDatasetPlanEvaluationError {
    fn from(value: DeleteDatasetPlanningError) -> Self {
        match value {
            DeleteDatasetPlanningError::NotFound(e) => Self::NotFound(e),
            DeleteDatasetPlanningError::Internal(e) => Self::Internal(e),
        }
    }
}
