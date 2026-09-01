// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::{ApplyResourceOutcome, ApplyResourceRejectionCategory, Resource, ResourceWarning};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The desired-state pair carried by every accepted apply outcome.
///
/// Both sides are **canonical manifest documents** (see
/// [`crate::ResourceManifest::from_resource`]) rather than a pre-computed list
/// of field-level changes: the backend states what the resource looks like on
/// each side and leaves "how to diff" and "how to visualize" to the client.
/// This keeps diff granularity out of the wire contract entirely — a client can
/// render a one-line change as one line without the backend having to
/// anticipate that granularity.
///
/// Because the canonical form excludes `generation`, timestamps, and `status`,
/// an unchanged apply yields `before == Some(after)`, and there are no spurious
/// differences to normalize away.
#[derive(Debug, Clone)]
pub struct ApplyManifestDocuments {
    /// Canonical manifest before the apply. `None` **iff** the resource is
    /// being created — an unchanged apply carries `Some(doc)` equal to `after`.
    pub before: Option<serde_json::Value>,

    /// Canonical manifest that the apply produced (or would produce).
    pub after: serde_json::Value,
}

impl ApplyManifestDocuments {
    /// Canonicalizes both sides of an apply.
    ///
    /// `previous_resource` is `None` exactly when the resource is being
    /// created. See [`ApplyManifestPlan::set_documents`] for the ordering
    /// requirement around `headers.account`.
    pub fn build(
        previous_resource: Option<&Resource>,
        resource: &Resource,
    ) -> Result<Self, InternalError> {
        Ok(Self {
            before: previous_resource
                .map(crate::resource_to_manifest_value)
                .transpose()?,
            after: crate::resource_to_manifest_value(resource)?,
        })
    }

    /// Whether the two sides differ. An apply that changed nothing, and a
    /// create, are both answered correctly here without inspecting the outcome.
    pub fn has_changes(&self) -> bool {
        self.before.as_ref() != Some(&self.after)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ApplyManifestPlan {
    pub resource: Resource,
    pub outcome: ApplyResourceOutcome,
    pub reconciliation_required: bool,
    pub executable: bool,
    pub warnings: Vec<ResourceWarning>,

    /// How to obtain the canonical [`ApplyManifestDocuments`] for this apply.
    pub documents: ApplyManifestDocumentSource,
}

impl ApplyManifestPlan {
    /// Canonical documents describing this apply. See
    /// [`ApplyManifestDocumentSource::resolve`] for the ordering requirement.
    pub fn documents(&self) -> Result<ApplyManifestDocuments, InternalError> {
        self.documents.resolve(&self.resource)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Where an accepted apply's canonical documents come from.
///
/// The two facades legitimately differ here, and collapsing them into one
/// representation is what makes invalid states expressible:
///
/// - The **local** facade holds the raw pre-apply resource and cannot
///   canonicalize until `headers.account` is final, which happens *after* the
///   dispatcher returns.
/// - The **remote** facade receives already-canonicalized documents and has no
///   resource pair to rebuild them from.
///
/// Modeling this as an enum means there is no moment where a value looks like
/// finished documents but is not — and no need to spell "not computed yet" as
/// an empty/`null` document that `has_changes()` would misread.
#[derive(Debug, Clone)]
pub enum ApplyManifestDocumentSource {
    /// Canonicalize from the resource pair. `previous` is `None` **iff** the
    /// resource is being created.
    Pair { previous: Option<Resource> },

    /// Already-canonical documents, as received over the wire.
    Canonical(ApplyManifestDocuments),
}

impl ApplyManifestDocumentSource {
    /// Resolves to canonical documents for `resource`.
    ///
    /// For [`Self::Pair`], call this **after** `headers.account` has been
    /// finalized: the account is part of the canonical manifest, so
    /// canonicalizing earlier would bake in a stale account and surface it as a
    /// spurious difference. Nothing is cached, so there is no half-built value
    /// to observe or accidentally send.
    pub fn resolve(&self, resource: &Resource) -> Result<ApplyManifestDocuments, InternalError> {
        match self {
            Self::Pair { previous } => ApplyManifestDocuments::build(previous.as_ref(), resource),
            Self::Canonical(documents) => Ok(documents.clone()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ApplyManifestResult {
    pub resource: Resource,
    pub outcome: ApplyResourceOutcome,
    pub warnings: Vec<ResourceWarning>,

    /// See [`ApplyManifestPlan::documents`].
    pub documents: ApplyManifestDocumentSource,
}

impl ApplyManifestResult {
    /// See [`ApplyManifestPlan::documents`] — same ordering requirement.
    pub fn documents(&self) -> Result<ApplyManifestDocuments, InternalError> {
        self.documents.resolve(&self.resource)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ApplyManifestRejection {
    pub category: ApplyResourceRejectionCategory,
    pub message: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub enum ApplyManifestPlanningDecision {
    Planned(ApplyManifestPlan),
    Rejected(ApplyManifestRejection),
}

impl ApplyManifestPlanningDecision {
    pub fn expect_planned(self) -> ApplyManifestPlan {
        let ApplyManifestPlanningDecision::Planned(plan) = self else {
            panic!("expected Planned decision, got Rejected");
        };
        plan
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub enum ApplyManifestApplicationDecision {
    Applied(ApplyManifestResult),
    Rejected(ApplyManifestRejection),
}

impl ApplyManifestApplicationDecision {
    pub fn expect_applied(self) -> ApplyManifestResult {
        let ApplyManifestApplicationDecision::Applied(result) = self else {
            panic!("expected Applied decision, got Rejected");
        };
        result
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
