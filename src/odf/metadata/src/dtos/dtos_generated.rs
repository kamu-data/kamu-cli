// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// WARNING: This file is auto-generated from Open Data Fabric Schemas
// See: http://opendatafabric.org/
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#![allow(clippy::all)]
#![allow(clippy::pedantic)]

use std::path::PathBuf;

use bitflags::bitflags;
use chrono::{DateTime, Utc};
use enum_variants::*;
use serde::{Deserialize, Serialize};
use setty::types::{ByteSize, DurationString};

use crate::auth::*;
use crate::datasets::*;
use crate::formats::*;
use crate::resources::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// auth
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod auth {
    #[allow(unused_imports)]
    use super::*;

    /// Create `PolicyBinding` resources that reference this resource as `object`.
    ///
    /// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/actions/BindPolicy
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct BindPolicy {
        /// Subject of the binding being created, allowing policies to restrict granting access to
        /// e.g. `Everyone`.
        pub subject: auth::SubjectHandle,
    }

    /// A named, possibly nested set of principals. Policies can be bound to groups so that
    /// permissions are granted to all members at once. Some group names are reserved, e.g.
    /// `{org}/admins` and `{org}/members` are created together with an organization.
    ///
    /// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/Group
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct Group {
        /// Container for identity and ownership information of a resource.
        pub headers: resources::ResourceHeadersInput,
        /// Specifies the desired state of the resource.
        pub spec: auth::GroupSpecInput,
    }

    impl Group {
        pub fn schema() -> &'static TypeUri {
            &GROUP_SCHEMA
        }
        pub const fn schema_str() -> &'static str {
            GROUP_SCHEMA_STR
        }
    }

    static GROUP_SCHEMA_STR: &str = "https://opendatafabric.org/schemas/auth/v1alpha1/Group";

    static GROUP_SCHEMA: std::sync::LazyLock<TypeUri> =
        std::sync::LazyLock::new(|| TypeUri::new_unchecked(GROUP_SCHEMA_STR));

    /// Group specification.
    ///
    /// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/GroupSpec
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct GroupSpec {
        /// Members of the group. Only the `Resource` subject kind is allowed, so that well-known
        /// subjects like `Everyone` cannot be nested into a group.
        pub members: Option<Vec<auth::SubjectHandle>>,
    }

    /// Group specification.
    ///
    /// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/GroupSpecInput
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct GroupSpecInput {
        /// Members of the group. Only the `Resource` subject kind is allowed, so that well-known
        /// subjects like `Everyone` cannot be nested into a group.
        pub members: Option<Vec<auth::SubjectRef>>,
    }

    /// An entire ODF deployment acting under a single identity. The node acts as a principal in
    /// system tasks and node-to-node communications and owns node-wide resources like the reserved
    /// `{node}/admins` group.
    ///
    /// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/Node
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct Node {
        /// Container for identity and ownership information of a resource.
        pub headers: resources::ResourceHeadersInput,
        /// Specifies the desired state of the resource.
        pub spec: auth::NodeSpecInput,
    }

    impl Node {
        pub fn schema() -> &'static TypeUri {
            &NODE_SCHEMA
        }
        pub const fn schema_str() -> &'static str {
            NODE_SCHEMA_STR
        }
    }

    static NODE_SCHEMA_STR: &str = "https://opendatafabric.org/schemas/auth/v1alpha1/Node";

    static NODE_SCHEMA: std::sync::LazyLock<TypeUri> =
        std::sync::LazyLock::new(|| TypeUri::new_unchecked(NODE_SCHEMA_STR));

    /// Node account specification.
    ///
    /// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/NodeSpec
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct NodeSpec {
        /// DID associated with the account by ODF or an external system.
        pub did: Option<AccountID>,
        /// Human-friendly display name.
        pub display_name: Option<String>,
    }

    /// Node account specification.
    ///
    /// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/NodeSpecInput
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct NodeSpecInput {
        /// DID associated with the account by ODF or an external system.
        pub did: Option<AccountID>,
        /// Human-friendly display name.
        pub display_name: Option<String>,
    }

    /// An account that owns resources on behalf of a group of users but cannot authenticate or act
    /// itself. Upon creation the node creates reserved groups `{org}/admins`, whose members have
    /// owner rights over all resources of the organization, and `{org}/members`.
    ///
    /// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/Organization
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct Organization {
        /// Container for identity and ownership information of a resource.
        pub headers: resources::ResourceHeadersInput,
        /// Specifies the desired state of the resource.
        pub spec: auth::OrganizationSpecInput,
    }

    impl Organization {
        pub fn schema() -> &'static TypeUri {
            &ORGANIZATION_SCHEMA
        }
        pub const fn schema_str() -> &'static str {
            ORGANIZATION_SCHEMA_STR
        }
    }

    static ORGANIZATION_SCHEMA_STR: &str =
        "https://opendatafabric.org/schemas/auth/v1alpha1/Organization";

    static ORGANIZATION_SCHEMA: std::sync::LazyLock<TypeUri> =
        std::sync::LazyLock::new(|| TypeUri::new_unchecked(ORGANIZATION_SCHEMA_STR));

    /// Organization account specification.
    ///
    /// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/OrganizationSpec
    #[derive(Clone, Debug, Eq, Default)]
    pub struct OrganizationSpec {
        /// Human-friendly display name.
        pub display_name: Option<String>,
        /// URL of the account's avatar image.
        pub avatar_url: Option<String>,
        /// Suspends the account. Effects of suspension are implementation-specific.
        ///
        /// Defaults to: false
        pub disabled: Option<bool>,
    }

    impl OrganizationSpec {
        pub fn default_disabled() -> bool {
            false
        }
        pub fn disabled(&self) -> bool {
            self.disabled.unwrap_or(Self::default_disabled())
        }
    }

    impl PartialEq for OrganizationSpec {
        fn eq(&self, other: &Self) -> bool {
            self.display_name == other.display_name
                && self.avatar_url == other.avatar_url
                && self.disabled.or_else(|| Some(Self::default_disabled()))
                    == other.disabled.or_else(|| Some(Self::default_disabled()))
        }
    }

    /// Organization account specification.
    ///
    /// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/OrganizationSpecInput
    #[derive(Clone, Debug, Eq, Default)]
    pub struct OrganizationSpecInput {
        /// Human-friendly display name.
        pub display_name: Option<String>,
        /// URL of the account's avatar image.
        pub avatar_url: Option<String>,
        /// Suspends the account. Effects of suspension are implementation-specific.
        ///
        /// Defaults to: false
        pub disabled: Option<bool>,
    }

    impl OrganizationSpecInput {
        pub fn default_disabled() -> bool {
            false
        }
        pub fn disabled(&self) -> bool {
            self.disabled.unwrap_or(Self::default_disabled())
        }
    }

    impl PartialEq for OrganizationSpecInput {
        fn eq(&self, other: &Self) -> bool {
            self.display_name == other.display_name
                && self.avatar_url == other.avatar_url
                && self.disabled.or_else(|| Some(Self::default_disabled()))
                    == other.disabled.or_else(|| Some(Self::default_disabled()))
        }
    }

    /// A reusable grant of permissions. Policies with subject and object slots are templates
    /// instantiated via `PolicyBinding`, while policies without slots take effect on their own.
    ///
    /// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/Policy
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct Policy {
        /// Container for identity and ownership information of a resource.
        pub headers: resources::ResourceHeadersInput,
        /// Specifies the desired state of the resource.
        pub spec: auth::PolicySpecInput,
    }

    impl Policy {
        pub fn schema() -> &'static TypeUri {
            &POLICY_SCHEMA
        }
        pub const fn schema_str() -> &'static str {
            POLICY_SCHEMA_STR
        }
    }

    static POLICY_SCHEMA_STR: &str = "https://opendatafabric.org/schemas/auth/v1alpha1/Policy";

    static POLICY_SCHEMA: std::sync::LazyLock<TypeUri> =
        std::sync::LazyLock::new(|| TypeUri::new_unchecked(POLICY_SCHEMA_STR));

    /// Instantiates a `Policy` template for a concrete subject and object.
    ///
    /// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/PolicyBinding
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct PolicyBinding {
        /// Container for identity and ownership information of a resource.
        pub headers: resources::ResourceHeadersInput,
        /// Specifies the desired state of the resource.
        pub spec: auth::PolicyBindingSpecInput,
    }

    impl PolicyBinding {
        pub fn schema() -> &'static TypeUri {
            &POLICY_BINDING_SCHEMA
        }
        pub const fn schema_str() -> &'static str {
            POLICY_BINDING_SCHEMA_STR
        }
    }

    static POLICY_BINDING_SCHEMA_STR: &str =
        "https://opendatafabric.org/schemas/auth/v1alpha1/PolicyBinding";

    static POLICY_BINDING_SCHEMA: std::sync::LazyLock<TypeUri> =
        std::sync::LazyLock::new(|| TypeUri::new_unchecked(POLICY_BINDING_SCHEMA_STR));

    /// Policy binding specification.
    ///
    /// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/PolicyBindingSpec
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct PolicyBindingSpec {
        /// Policy template to instantiate.
        pub policy: resources::ResourceHandle,
        /// Party the permissions are granted to.
        pub subject: auth::SubjectHandle,
        /// Resource the permissions are granted on. Refers to both the resource and the entity
        /// materialized from it.
        pub object: resources::ResourceHandle,
    }

    /// Policy binding specification.
    ///
    /// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/PolicyBindingSpecInput
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct PolicyBindingSpecInput {
        /// Policy template to instantiate.
        pub policy: auth::PolicyRef,
        /// Party the permissions are granted to.
        pub subject: auth::SubjectRef,
        /// Resource the permissions are granted on. Refers to both the resource and the entity
        /// materialized from it.
        pub object: resources::ResourceRef,
    }

    /// Reference to a `Policy`.
    ///
    /// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/PolicyRef
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct PolicyRef {
        /// Reference to an account that owns the `Policy`.
        pub account: Option<auth::ResourceOwnerRef>,
        /// ID of the resource.
        pub id: Option<ResourceID>,
        /// Name of the resource.
        pub name: Option<ResourceName>,
    }
    impl IntoResourceRef for PolicyRef {}

    /// Policy specification. The `kind` selects the policy language.
    ///
    /// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/PolicySpec
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum PolicySpec {
        Generic(auth::PolicySpecGeneric),
        Cedar(auth::PolicySpecCedar),
    }

    impl_enum_with_variants!(PolicySpec);
    impl_enum_variant!(PolicySpec::Generic(auth::PolicySpecGeneric));
    impl_enum_variant!(PolicySpec::Cedar(auth::PolicySpecCedar));

    /// Policy expressed in the Cedar policy language. Policies that declare `?principal` and
    /// `?resource` slots are templates instantiated via `PolicyBinding`, otherwise they take effect
    /// on their own. Only supported by nodes that use Cedar.
    ///
    /// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/PolicySpec#/$defs/Cedar
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct PolicySpecCedar {
        /// Cedar policy source.
        pub body: String,
    }

    /// Engine-agnostic policy that grants a set of actions. Subject and object are always supplied
    /// by a `PolicyBinding`.
    ///
    /// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/PolicySpec#/$defs/Generic
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct PolicySpecGeneric {
        /// Schema URIs of actions granted by the policy.
        pub actions: Vec<TypeUri>,
    }

    /// Policy specification. The `kind` selects the policy language.
    ///
    /// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/PolicySpecInput
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum PolicySpecInput {
        Generic(auth::PolicySpecInputGeneric),
        Cedar(auth::PolicySpecInputCedar),
    }

    impl_enum_with_variants!(PolicySpecInput);
    impl_enum_variant!(PolicySpecInput::Generic(auth::PolicySpecInputGeneric));
    impl_enum_variant!(PolicySpecInput::Cedar(auth::PolicySpecInputCedar));

    /// Policy expressed in the Cedar policy language. Policies that declare `?principal` and
    /// `?resource` slots are templates instantiated via `PolicyBinding`, otherwise they take effect
    /// on their own. Only supported by nodes that use Cedar.
    ///
    /// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/PolicySpecInput#/$defs/Cedar
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct PolicySpecInputCedar {
        /// Cedar policy source.
        pub body: String,
    }

    /// Engine-agnostic policy that grants a set of actions. Subject and object are always supplied
    /// by a `PolicyBinding`.
    ///
    /// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/PolicySpecInput#/$defs/Generic
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct PolicySpecInputGeneric {
        /// Schema URIs of actions granted by the policy.
        pub actions: Vec<TypeUri>,
    }

    /// Link to a resolved identity that can act as a principal: `User`, `ServiceAccount`, or
    /// `Node`.
    ///
    /// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/PrincipalHandle
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct PrincipalHandle {
        /// Type URI of the principal.
        pub r#type: TypeUri,
        /// Account that owns the service account.
        pub account: Option<auth::ResourceOwnerHandle>,
        /// ID of the principal resource.
        pub id: ResourceID,
        /// DID of the principal, if applicable.
        pub did: Option<Did>,
        /// Name of the principal resource.
        pub name: ResourceName,
    }
    impl IntoResourceRef for PrincipalHandle {}

    /// Reference to an identity that can act as a principal: `User`, `ServiceAccount`, or `Node`.
    /// Short form requires a type prefix, e.g. `ServiceAccount:acme/ingest-prod`.
    ///
    /// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/PrincipalRef
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct PrincipalRef {
        /// Short type name or full type URI of the principal.
        pub r#type: Option<TypeRef>,
        /// Reference to an account that owns the service account.
        pub account: Option<auth::ResourceOwnerRef>,
        /// ID of the principal resource.
        pub id: Option<ResourceID>,
        /// DID of the principal.
        pub did: Option<Did>,
        /// Name of the principal resource.
        pub name: Option<ResourceName>,
    }
    impl IntoResourceRef for PrincipalRef {}

    /// Link to a resolved account that can own resources: `User`, `Organization`, or `Node`.
    ///
    /// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/ResourceOwnerHandle
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct ResourceOwnerHandle {
        /// ID of the account resource.
        pub id: ResourceID,
        /// DID of the account.
        pub did: AccountID,
        /// Name of the account.
        pub name: AccountName,
    }
    impl IntoResourceRef for ResourceOwnerHandle {}

    /// Reference to an account that can own resources: `User`, `Organization`, or `Node`. Short
    /// form is the account name, as names of these accounts are unique within a node.
    ///
    /// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/ResourceOwnerRef
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct ResourceOwnerRef {
        /// UUID of the account resource.
        pub id: Option<ResourceID>,
        /// DID of the account.
        pub did: Option<AccountID>,
        /// Name of the account.
        pub name: Option<AccountName>,
    }
    impl IntoResourceRef for ResourceOwnerRef {}

    /// A non-human identity used by automation (scheduled flows, CI, installed applications).
    /// Service accounts are scoped under an owning account and can act as principals, but cannot
    /// own resources.
    ///
    /// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/ServiceAccount
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct ServiceAccount {
        /// Container for identity and ownership information of a resource.
        pub headers: resources::ResourceHeadersInput,
        /// Specifies the desired state of the resource.
        pub spec: auth::ServiceAccountSpecInput,
    }

    impl ServiceAccount {
        pub fn schema() -> &'static TypeUri {
            &SERVICE_ACCOUNT_SCHEMA
        }
        pub const fn schema_str() -> &'static str {
            SERVICE_ACCOUNT_SCHEMA_STR
        }
    }

    static SERVICE_ACCOUNT_SCHEMA_STR: &str =
        "https://opendatafabric.org/schemas/auth/v1alpha1/ServiceAccount";

    static SERVICE_ACCOUNT_SCHEMA: std::sync::LazyLock<TypeUri> =
        std::sync::LazyLock::new(|| TypeUri::new_unchecked(SERVICE_ACCOUNT_SCHEMA_STR));

    /// Service account specification.
    ///
    /// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/ServiceAccountSpec
    #[derive(Clone, Debug, Eq, Default)]
    pub struct ServiceAccountSpec {
        /// Human-friendly display name.
        pub display_name: Option<String>,
        /// Suspends the account. Effects of suspension are implementation-specific.
        ///
        /// Defaults to: false
        pub disabled: Option<bool>,
    }

    impl ServiceAccountSpec {
        pub fn default_disabled() -> bool {
            false
        }
        pub fn disabled(&self) -> bool {
            self.disabled.unwrap_or(Self::default_disabled())
        }
    }

    impl PartialEq for ServiceAccountSpec {
        fn eq(&self, other: &Self) -> bool {
            self.display_name == other.display_name
                && self.disabled.or_else(|| Some(Self::default_disabled()))
                    == other.disabled.or_else(|| Some(Self::default_disabled()))
        }
    }

    /// Service account specification.
    ///
    /// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/ServiceAccountSpecInput
    #[derive(Clone, Debug, Eq, Default)]
    pub struct ServiceAccountSpecInput {
        /// Human-friendly display name.
        pub display_name: Option<String>,
        /// Suspends the account. Effects of suspension are implementation-specific.
        ///
        /// Defaults to: false
        pub disabled: Option<bool>,
    }

    impl ServiceAccountSpecInput {
        pub fn default_disabled() -> bool {
            false
        }
        pub fn disabled(&self) -> bool {
            self.disabled.unwrap_or(Self::default_disabled())
        }
    }

    impl PartialEq for ServiceAccountSpecInput {
        fn eq(&self, other: &Self) -> bool {
            self.display_name == other.display_name
                && self.disabled.or_else(|| Some(Self::default_disabled()))
                    == other.disabled.or_else(|| Some(Self::default_disabled()))
        }
    }

    /// Resolved reference to a party a grant is given to.
    ///
    /// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/SubjectHandle
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum SubjectHandle {
        Resource(auth::SubjectHandleResource),
        Authenticated(auth::SubjectHandleAuthenticated),
        Anonymous(auth::SubjectHandleAnonymous),
        Everyone(auth::SubjectHandleEveryone),
    }

    impl_enum_with_variants!(SubjectHandle);
    impl_enum_variant!(SubjectHandle::Resource(auth::SubjectHandleResource));
    impl_enum_variant!(SubjectHandle::Authenticated(
        auth::SubjectHandleAuthenticated
    ));
    impl_enum_variant!(SubjectHandle::Anonymous(auth::SubjectHandleAnonymous));
    impl_enum_variant!(SubjectHandle::Everyone(auth::SubjectHandleEveryone));

    /// Matches any unauthenticated principal.
    ///
    /// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/SubjectHandle#/$defs/Anonymous
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct SubjectHandleAnonymous {}

    /// Matches any authenticated principal.
    ///
    /// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/SubjectHandle#/$defs/Authenticated
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct SubjectHandleAuthenticated {}

    /// Matches any principal, authenticated or not.
    ///
    /// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/SubjectHandle#/$defs/Everyone
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct SubjectHandleEveryone {}

    /// A specific `User`, `ServiceAccount`, `Node`, or members of a `Group`.
    ///
    /// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/SubjectHandle#/$defs/Resource
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct SubjectHandleResource {
        /// Type URI of the subject resource.
        pub r#type: TypeUri,
        /// Account that owns the subject resource (for service accounts and groups).
        pub account: Option<auth::ResourceOwnerHandle>,
        /// ID of the subject resource.
        pub id: ResourceID,
        /// DID of the subject resource, if applicable.
        pub did: Option<Did>,
        /// Name of the subject resource.
        pub name: ResourceName,
    }

    /// Reference to a party a grant is given to. Unlike a principal, a subject may stand for many
    /// principals at once. Short form is either a well-known subject name (e.g. `Authenticated`) or
    /// a resource reference with a type prefix, DID, or ID (e.g. `Group:acme/engineering`).
    ///
    /// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/SubjectRef
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum SubjectRef {
        Resource(auth::SubjectRefResource),
        Authenticated(auth::SubjectRefAuthenticated),
        Anonymous(auth::SubjectRefAnonymous),
        Everyone(auth::SubjectRefEveryone),
    }

    impl_enum_with_variants!(SubjectRef);
    impl_enum_variant!(SubjectRef::Resource(auth::SubjectRefResource));
    impl_enum_variant!(SubjectRef::Authenticated(auth::SubjectRefAuthenticated));
    impl_enum_variant!(SubjectRef::Anonymous(auth::SubjectRefAnonymous));
    impl_enum_variant!(SubjectRef::Everyone(auth::SubjectRefEveryone));

    /// Matches any unauthenticated principal.
    ///
    /// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/SubjectRef#/$defs/Anonymous
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct SubjectRefAnonymous {}

    /// Matches any authenticated principal.
    ///
    /// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/SubjectRef#/$defs/Authenticated
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct SubjectRefAuthenticated {}

    /// Matches any principal, authenticated or not.
    ///
    /// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/SubjectRef#/$defs/Everyone
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct SubjectRefEveryone {}

    /// A specific `User`, `ServiceAccount`, `Node`, or members of a `Group`.
    ///
    /// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/SubjectRef#/$defs/Resource
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct SubjectRefResource {
        /// Short type name or full type URI of the subject resource.
        pub r#type: Option<TypeRef>,
        /// Reference to an account that owns the subject resource (for service accounts and
        /// groups).
        pub account: Option<auth::ResourceOwnerRef>,
        /// ID of the subject resource.
        pub id: Option<ResourceID>,
        /// DID of the subject resource.
        pub did: Option<Did>,
        /// Name of the subject resource.
        pub name: Option<ResourceName>,
    }

    /// An individual person who can authenticate and act as a principal.
    ///
    /// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/User
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct User {
        /// Container for identity and ownership information of a resource.
        pub headers: resources::ResourceHeadersInput,
        /// Specifies the desired state of the resource.
        pub spec: auth::UserSpecInput,
    }

    impl User {
        pub fn schema() -> &'static TypeUri {
            &USER_SCHEMA
        }
        pub const fn schema_str() -> &'static str {
            USER_SCHEMA_STR
        }
    }

    static USER_SCHEMA_STR: &str = "https://opendatafabric.org/schemas/auth/v1alpha1/User";

    static USER_SCHEMA: std::sync::LazyLock<TypeUri> =
        std::sync::LazyLock::new(|| TypeUri::new_unchecked(USER_SCHEMA_STR));

    /// User account specification.
    ///
    /// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/UserSpec
    #[derive(Clone, Debug, Eq, Default)]
    pub struct UserSpec {
        /// DID associated with the account by ODF or an external system.
        pub did: Option<AccountID>,
        /// Human-friendly display name.
        pub display_name: Option<String>,
        /// Email address of the user.
        pub email: Option<String>,
        /// URL of the account's avatar image.
        pub avatar_url: Option<String>,
        /// Password for local authentication, stored as a hash upon apply. Absent for SSO or
        /// DID-based accounts.
        pub password: Option<config::Secret>,
        /// Suspends the account. Effects of suspension are implementation-specific.
        ///
        /// Defaults to: false
        pub disabled: Option<bool>,
    }

    impl UserSpec {
        pub fn default_disabled() -> bool {
            false
        }
        pub fn disabled(&self) -> bool {
            self.disabled.unwrap_or(Self::default_disabled())
        }
    }

    impl PartialEq for UserSpec {
        fn eq(&self, other: &Self) -> bool {
            self.did == other.did
                && self.display_name == other.display_name
                && self.email == other.email
                && self.avatar_url == other.avatar_url
                && self.password == other.password
                && self.disabled.or_else(|| Some(Self::default_disabled()))
                    == other.disabled.or_else(|| Some(Self::default_disabled()))
        }
    }

    /// User account specification.
    ///
    /// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/UserSpecInput
    #[derive(Clone, Debug, Eq, Default)]
    pub struct UserSpecInput {
        /// DID associated with the account by ODF or an external system.
        pub did: Option<AccountID>,
        /// Human-friendly display name.
        pub display_name: Option<String>,
        /// Email address of the user.
        pub email: Option<String>,
        /// URL of the account's avatar image.
        pub avatar_url: Option<String>,
        /// Password for local authentication, stored as a hash upon apply. Absent for SSO or
        /// DID-based accounts.
        pub password: Option<config::Secret>,
        /// Suspends the account. Effects of suspension are implementation-specific.
        ///
        /// Defaults to: false
        pub disabled: Option<bool>,
    }

    impl UserSpecInput {
        pub fn default_disabled() -> bool {
            false
        }
        pub fn disabled(&self) -> bool {
            self.disabled.unwrap_or(Self::default_disabled())
        }
    }

    impl PartialEq for UserSpecInput {
        fn eq(&self, other: &Self) -> bool {
            self.did == other.did
                && self.display_name == other.display_name
                && self.email == other.email
                && self.avatar_url == other.avatar_url
                && self.password == other.password
                && self.disabled.or_else(|| Some(Self::default_disabled()))
                    == other.disabled.or_else(|| Some(Self::default_disabled()))
        }
    }
}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// config
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod config {
    #[allow(unused_imports)]
    use super::*;

    /// Decrypt a secret from a `SecretSet`.
    ///
    /// Schema: https://opendatafabric.org/schemas/config/v1alpha1/actions/Decrypt
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct Decrypt {}

    /// Individual secret in raw or encrypted form.
    ///
    /// Schema: https://opendatafabric.org/schemas/config/v1alpha1/Secret
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct Secret {
        /// A secret value in raw or encoded form.
        pub value: String,
        /// Represents the encoding of the value. Typically will be `jwe` after a raw secret gets
        /// encrypted.
        pub content_encoding: Option<String>,
    }

    /// Defines a set of secrets stored and managed by the ODF node.
    ///
    /// Schema: https://opendatafabric.org/schemas/config/v1alpha1/SecretSet
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct SecretSet {
        /// Container for identity and ownership information of a resource.
        pub headers: resources::ResourceHeadersInput,
        /// Specifies the desired state of the secret set.
        pub spec: config::SecretSetSpecInput,
    }

    impl SecretSet {
        pub fn schema() -> &'static TypeUri {
            &SECRET_SET_SCHEMA
        }
        pub const fn schema_str() -> &'static str {
            SECRET_SET_SCHEMA_STR
        }
    }

    static SECRET_SET_SCHEMA_STR: &str =
        "https://opendatafabric.org/schemas/config/v1alpha1/SecretSet";

    static SECRET_SET_SCHEMA: std::sync::LazyLock<TypeUri> =
        std::sync::LazyLock::new(|| TypeUri::new_unchecked(SECRET_SET_SCHEMA_STR));

    /// Defines a set of secrets stored and managed by the ODF node and accessible via embedded
    /// sercets provider.
    ///
    /// Schema: https://opendatafabric.org/schemas/config/v1alpha1/SecretSetSpec
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct SecretSetSpec {
        /// Key value pairs of secrets.
        pub secrets: config::Secrets,
    }

    /// Defines a set of secrets stored and managed by the ODF node and accessible via embedded
    /// sercets provider.
    ///
    /// Schema: https://opendatafabric.org/schemas/config/v1alpha1/SecretSetSpecInput
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct SecretSetSpecInput {
        /// Key value pairs of secrets.
        pub secrets: config::Secrets,
    }

    /// Container for key-value secrets. Every key must be a string. Values may be strings with raw
    /// unencrypted data or objects that signify the encoding.
    ///
    /// Schema: https://opendatafabric.org/schemas/config/v1alpha1/Secrets
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub struct Secrets {
        pub entries: std::collections::BTreeMap<String, config::Secret>,
    }

    /// Reference to a value within a `VariableSet` or a `SecretSet`.
    ///
    /// Schema: https://opendatafabric.org/schemas/config/v1alpha1/ValueHandle
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct ValueHandle {
        /// Account that owns the target resource.
        pub account: auth::ResourceOwnerHandle,
        /// Type URI of the target resource.
        pub r#type: TypeUri,
        /// ID of the resource within a node.
        pub id: ResourceID,
        /// Name of a resource.
        pub name: ResourceName,
        /// JSON path to a value within a `VariableSet` or a `SecretSet`.
        ///
        /// Examples:
        /// - "port"
        /// - "postgres.schemaName"
        pub path: Option<String>,
    }
    impl IntoResourceRef for ValueHandle {}

    /// Reference to a value within a `VariableSet` or a `SecretSet`.
    ///
    /// Schema: https://opendatafabric.org/schemas/config/v1alpha1/ValueRef
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct ValueRef {
        /// Reference to an account that owns the `VariableSet` or the `SecretSet`.
        pub account: Option<auth::ResourceOwnerRef>,
        /// ID of a resource.
        pub id: Option<ResourceID>,
        /// Short type name or full type URI of the target resource.
        ///
        /// Examples:
        /// - "SecretSet"
        /// - "VariableSet"
        /// - "https://opendatafabric.org/schemas/config/v1alpha1/SecretSet"
        pub r#type: Option<TypeRef>,
        /// Name of a resource.
        pub name: Option<ResourceName>,
        /// JSON path to a value within a `VariableSet` or a `SecretSet`.
        ///
        /// Examples:
        /// - "port"
        /// - "postgres.schemaName"
        pub path: Option<String>,
    }
    impl IntoResourceRef for ValueRef {}

    /// Container for key-value variables. Every key must be a string. Values shoud reference fields
    /// in `SecretSet`s and `VariableSet`s.
    ///
    /// Schema: https://opendatafabric.org/schemas/config/v1alpha1/ValueRefs
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub struct ValueRefs {
        pub entries: std::collections::BTreeMap<String, config::ValueRef>,
    }

    /// Individual variable.
    ///
    /// Schema: https://opendatafabric.org/schemas/config/v1alpha1/Variable
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct Variable {
        /// A value in raw or encoded form.
        pub value: String,
    }

    /// Defines a set of variables stored and managed by the ODF node.
    ///
    /// Schema: https://opendatafabric.org/schemas/config/v1alpha1/VariableSet
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct VariableSet {
        /// Container for identity and ownership information of a resource.
        pub headers: resources::ResourceHeadersInput,
        /// Specifies the desired state of the variable set.
        pub spec: config::VariableSetSpecInput,
    }

    impl VariableSet {
        pub fn schema() -> &'static TypeUri {
            &VARIABLE_SET_SCHEMA
        }
        pub const fn schema_str() -> &'static str {
            VARIABLE_SET_SCHEMA_STR
        }
    }

    static VARIABLE_SET_SCHEMA_STR: &str =
        "https://opendatafabric.org/schemas/config/v1alpha1/VariableSet";

    static VARIABLE_SET_SCHEMA: std::sync::LazyLock<TypeUri> =
        std::sync::LazyLock::new(|| TypeUri::new_unchecked(VARIABLE_SET_SCHEMA_STR));

    /// Defines a set of variables stored and managed by the ODF node and accessible via embedded
    /// variables provider.
    ///
    /// Schema: https://opendatafabric.org/schemas/config/v1alpha1/VariableSetSpec
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct VariableSetSpec {
        /// Key value pairs of variables.
        pub variables: config::Variables,
    }

    /// Defines a set of variables stored and managed by the ODF node and accessible via embedded
    /// variables provider.
    ///
    /// Schema: https://opendatafabric.org/schemas/config/v1alpha1/VariableSetSpecInput
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct VariableSetSpecInput {
        /// Key value pairs of variables.
        pub variables: config::Variables,
    }

    /// Container for key-value variables. Every key must be a string. Values may be raw strings or
    /// objects that incorporate the encoding.
    ///
    /// Schema: https://opendatafabric.org/schemas/config/v1alpha1/Variables
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub struct Variables {
        pub entries: std::collections::BTreeMap<String, config::Variable>,
    }
}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// data
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod data {
    #[allow(unused_imports)]
    use super::*;

    /// Represents a named field (column) in a root or nested struct schema
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataField
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct DataField {
        /// Name of the field
        pub name: String,
        /// Logical type of the field that defines its semantic behavior and value ranges
        pub r#type: data::DataType,
        /// ODF extensions
        pub extra: Option<data::ExtraAttributes>,
    }

    /// This schema aims to be a human-friendly variant of Arrow. Arrow currently specifies only the [flatbuffer format](https://github.com/apache/arrow/blob/f9301c0ba8a7ed1b0b63275cfdd4c44c26b04675/format/Schema.fbs) which has many legacy to it and is not suited to be defined by humans, so we had to define our own schema format. While inspired by Arrow - this format makes a clear separation between logical data types and encoding (physical layout) of data in the chunks.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataSchema
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct DataSchema {
        /// Top-level fields (columns) of the schema.
        pub fields: Vec<data::DataField>,
        /// ODF extensions
        pub extra: Option<data::ExtraAttributes>,
    }

    /// Defines a logical type of the field. Logical type determines the semantics and boudaries of
    /// a type and how it can be operated on, without a concern about encoding and physical layout
    /// of the data in chunks.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum DataType {
        Binary(data::DataTypeBinary),
        Bool(data::DataTypeBool),
        Date(data::DataTypeDate),
        Decimal(data::DataTypeDecimal),
        Duration(data::DataTypeDuration),
        Float16(data::DataTypeFloat16),
        Float32(data::DataTypeFloat32),
        Float64(data::DataTypeFloat64),
        Int8(data::DataTypeInt8),
        Int16(data::DataTypeInt16),
        Int32(data::DataTypeInt32),
        Int64(data::DataTypeInt64),
        UInt8(data::DataTypeUInt8),
        UInt16(data::DataTypeUInt16),
        UInt32(data::DataTypeUInt32),
        UInt64(data::DataTypeUInt64),
        List(data::DataTypeList),
        Map(data::DataTypeMap),
        Null(data::DataTypeNull),
        Option(data::DataTypeOption),
        Struct(data::DataTypeStruct),
        Time(data::DataTypeTime),
        Timestamp(data::DataTypeTimestamp),
        String(data::DataTypeString),
    }

    impl_enum_with_variants!(DataType);
    impl_enum_variant!(DataType::Binary(data::DataTypeBinary));
    impl_enum_variant!(DataType::Bool(data::DataTypeBool));
    impl_enum_variant!(DataType::Date(data::DataTypeDate));
    impl_enum_variant!(DataType::Decimal(data::DataTypeDecimal));
    impl_enum_variant!(DataType::Duration(data::DataTypeDuration));
    impl_enum_variant!(DataType::Float16(data::DataTypeFloat16));
    impl_enum_variant!(DataType::Float32(data::DataTypeFloat32));
    impl_enum_variant!(DataType::Float64(data::DataTypeFloat64));
    impl_enum_variant!(DataType::Int8(data::DataTypeInt8));
    impl_enum_variant!(DataType::Int16(data::DataTypeInt16));
    impl_enum_variant!(DataType::Int32(data::DataTypeInt32));
    impl_enum_variant!(DataType::Int64(data::DataTypeInt64));
    impl_enum_variant!(DataType::UInt8(data::DataTypeUInt8));
    impl_enum_variant!(DataType::UInt16(data::DataTypeUInt16));
    impl_enum_variant!(DataType::UInt32(data::DataTypeUInt32));
    impl_enum_variant!(DataType::UInt64(data::DataTypeUInt64));
    impl_enum_variant!(DataType::List(data::DataTypeList));
    impl_enum_variant!(DataType::Map(data::DataTypeMap));
    impl_enum_variant!(DataType::Null(data::DataTypeNull));
    impl_enum_variant!(DataType::Option(data::DataTypeOption));
    impl_enum_variant!(DataType::Struct(data::DataTypeStruct));
    impl_enum_variant!(DataType::Time(data::DataTypeTime));
    impl_enum_variant!(DataType::Timestamp(data::DataTypeTimestamp));
    impl_enum_variant!(DataType::String(data::DataTypeString));

    /// A sequence of bytes. Used for arbitrary binary data.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Binary
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct DataTypeBinary {
        /// Number of bytes per value for fixed-size binary. If omitted, the binary is
        /// variable-length.
        pub fixed_length: Option<u64>,
    }

    /// A boolean value representing true or false.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Bool
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct DataTypeBool {}

    /// A calendar date.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Date
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct DataTypeDate {}

    /// A fixed-point decimal number with a specified precision and scale.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Decimal
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct DataTypeDecimal {
        /// Total number of decimal digits that can be stored.
        pub precision: u32,
        /// Number of digits after the decimal point. In certain situations, scale could be
        /// negative number. For negative scale, it is the number of padding 0 to the right of the
        /// digits.
        ///
        /// For example the number 12300 could be treated as a decimal has precision 3 and scale
        /// -2.
        pub scale: i32,
    }

    /// An elapsed time interval with a specified time unit.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Duration
    #[derive(Clone, Debug, Eq, Default)]
    pub struct DataTypeDuration {
        /// The unit of the duration measurement.
        ///
        /// Defaults to: "Millisecond"
        pub unit: Option<data::TimeUnit>,
    }

    impl DataTypeDuration {
        pub fn default_unit() -> data::TimeUnit {
            data::TimeUnit::Millisecond
        }
        pub fn unit(&self) -> data::TimeUnit {
            self.unit.unwrap_or(Self::default_unit())
        }
    }

    impl PartialEq for DataTypeDuration {
        fn eq(&self, other: &Self) -> bool {
            self.unit.or_else(|| Some(Self::default_unit()))
                == other.unit.or_else(|| Some(Self::default_unit()))
        }
    }

    /// A floating-point number.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Float16
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct DataTypeFloat16 {}

    /// A floating-point number.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Float32
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct DataTypeFloat32 {}

    /// A floating-point number.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Float64
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct DataTypeFloat64 {}

    /// An integer value.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Int16
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct DataTypeInt16 {}

    /// An integer value.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Int32
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct DataTypeInt32 {}

    /// An integer value.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Int64
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct DataTypeInt64 {}

    /// An integer value.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Int8
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct DataTypeInt8 {}

    /// A list of values, all having the same data type.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/List
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct DataTypeList {
        /// Data type of list items.
        pub item_type: Box<data::DataType>,
        /// Number of list items per value for fixed-size lists. If omitted, the list is
        /// variable-length.
        pub fixed_length: Option<u64>,
    }

    /// A map of key-value pairs, represented as a list of entries (structs with key and value
    /// fields).
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Map
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct DataTypeMap {
        /// Data type of the map's keys.
        pub key_type: Box<data::DataType>,
        /// Data type of the map's values.
        pub value_type: Box<data::DataType>,
        /// Set to true if the keys within each value are sorted.
        pub keys_sorted: Option<bool>,
    }

    /// A type representing the absence of a value (null).
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Null
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct DataTypeNull {}

    /// A type representing an optional (nullable) value of another data type.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Option
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct DataTypeOption {
        /// Inner data type for the optional value.
        pub inner: Box<data::DataType>,
    }

    /// A Unicode string.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/String
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct DataTypeString {}

    /// A collection of named fields, each with its own data type.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Struct
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct DataTypeStruct {
        /// Fields that make up the struct.
        pub fields: Vec<data::DataField>,
    }

    /// A time of day value, without a date, with a specified unit of granularity.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Time
    #[derive(Clone, Debug, Eq, Default)]
    pub struct DataTypeTime {
        /// The unit of the time value.
        ///
        /// Defaults to: "Millisecond"
        pub unit: Option<data::TimeUnit>,
    }

    impl DataTypeTime {
        pub fn default_unit() -> data::TimeUnit {
            data::TimeUnit::Millisecond
        }
        pub fn unit(&self) -> data::TimeUnit {
            self.unit.unwrap_or(Self::default_unit())
        }
    }

    impl PartialEq for DataTypeTime {
        fn eq(&self, other: &Self) -> bool {
            self.unit.or_else(|| Some(Self::default_unit()))
                == other.unit.or_else(|| Some(Self::default_unit()))
        }
    }

    /// A point in time, represented as an offset from the Unix epoch in a specific timezone.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Timestamp
    #[derive(Clone, Debug, Eq, Default)]
    pub struct DataTypeTimestamp {
        /// The unit of the timestamp value that determines its precision.
        ///
        /// Defaults to: "Millisecond"
        pub unit: Option<data::TimeUnit>,
        /// The timezone is an optional string indicating the name of a timezone
        /// one of
        ///
        /// * As used in the Olson timezone database (the "tz database" or "tzdata"), such as
        ///   "America/New_York".
        /// * An absolute timezone offset of the form "+XX:XX" or "-XX:XX", such as "+07:30".
        ///
        /// Defaults to: "UTC"
        pub timezone: Option<String>,
    }

    impl DataTypeTimestamp {
        pub fn default_unit() -> data::TimeUnit {
            data::TimeUnit::Millisecond
        }
        pub fn unit(&self) -> data::TimeUnit {
            self.unit.unwrap_or(Self::default_unit())
        }
        pub fn default_timezone() -> &'static str {
            "UTC"
        }
        pub fn timezone(&self) -> &str {
            self.timezone.as_deref().unwrap_or(Self::default_timezone())
        }
    }

    impl PartialEq for DataTypeTimestamp {
        fn eq(&self, other: &Self) -> bool {
            self.unit.or_else(|| Some(Self::default_unit()))
                == other.unit.or_else(|| Some(Self::default_unit()))
                && self
                    .timezone
                    .as_deref()
                    .or_else(|| Some(Self::default_timezone()))
                    == other
                        .timezone
                        .as_deref()
                        .or_else(|| Some(Self::default_timezone()))
        }
    }

    /// An integer value.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/UInt16
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct DataTypeUInt16 {}

    /// An integer value.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/UInt32
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct DataTypeUInt32 {}

    /// An integer value.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/UInt64
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct DataTypeUInt64 {}

    /// An integer value.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/UInt8
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct DataTypeUInt8 {}

    /// Container for custom key-value extension attributes. Every key must be in the form of
    /// `<domain>/<path>` (e.g. `kamu.dev/archetype`) in order to fully disambiguate the value in
    /// the face of multiple extensions. Values may be any valid JSON including nested objects.
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/ExtraAttributes
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub struct ExtraAttributes {
        pub entries: std::collections::BTreeMap<String, serde_json::Value>,
    }

    pub use crate::data::OperationType;

    /// Defines the unit of measurement of time
    ///
    /// Schema: https://opendatafabric.org/schemas/data/v1alpha1/TimeUnit
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    pub enum TimeUnit {
        Second,
        Millisecond,
        Microsecond,
        Nanosecond,
    }
}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// datasets
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod datasets {
    #[allow(unused_imports)]
    use super::*;

    /// Indicates that data has been ingested into a root dataset.
    ///
    /// Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/AddData
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct AddData {
        /// Hash of the checkpoint file used to restore ingestion state, if any.
        pub prev_checkpoint: Option<Multihash>,
        /// Last offset of the previous data slice, if any. Must be equal to the last non-empty
        /// `newData.offsetInterval.end`.
        pub prev_offset: Option<u64>,
        /// Describes output data written during this transaction, if any.
        pub new_data: Option<datasets::DataSlice>,
        /// Describes checkpoint written during this transaction, if any. If an engine operation
        /// resulted in no updates to the checkpoint, but checkpoint is still relevant for
        /// subsequent runs - a hash of the previous checkpoint should be specified.
        pub new_checkpoint: Option<datasets::Checkpoint>,
        /// Last watermark of the output data stream, if any. Initial blocks may not have
        /// watermarks, but once watermark is set - all subsequent blocks should either carry the
        /// same watermark or specify a new (greater) one. Thus, watermarks are monotonically
        /// non-decreasing.
        pub new_watermark: Option<DateTime<Utc>>,
        /// The state of the source the data was added from to allow fast resuming. If the state
        /// did not change but is still relevant for subsequent runs it should be carried, i.e.
        /// only the last state per source is considered when resuming.
        pub new_source_state: Option<sources::SourceState>,
        /// ODF extensions.
        pub extra: Option<data::ExtraAttributes>,
    }

    /// Rewrite or truncate dataset history.
    ///
    /// Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/actions/AlterHistory
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct AlterHistory {}

    /// Embedded attachment item.
    ///
    /// Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/AttachmentEmbedded
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct AttachmentEmbedded {
        /// Path to an attachment if it was materialized into a file.
        pub path: String,
        /// Content of the attachment.
        pub content: String,
    }

    /// Defines the source of attachment files.
    ///
    /// Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/Attachments
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum Attachments {
        Embedded(datasets::AttachmentsEmbedded),
    }

    impl_enum_with_variants!(Attachments);
    impl_enum_variant!(Attachments::Embedded(datasets::AttachmentsEmbedded));

    /// For attachments that are specified inline and are embedded in the metadata.
    ///
    /// Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/Attachments#/$defs/Embedded
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct AttachmentsEmbedded {
        /// List of embedded items.
        pub items: Vec<datasets::AttachmentEmbedded>,
    }

    /// Describes a checkpoint produced by an engine
    ///
    /// Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/Checkpoint
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct Checkpoint {
        /// Hash sum of the checkpoint file.
        pub physical_hash: Multihash,
        /// Size of checkpoint file in bytes.
        pub size: u64,
    }

    /// Describes a slice of data added to a dataset or produced via transformation
    ///
    /// Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/DataSlice
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct DataSlice {
        /// Logical hash sum of the data in this slice.
        pub logical_hash: Multihash,
        /// Hash sum of the data part file.
        pub physical_hash: Multihash,
        /// Data slice produced by the transaction.
        pub offset_interval: datasets::OffsetInterval,
        /// Size of data file in bytes.
        pub size: u64,
    }

    /// Represents a desired state of a dataset.
    ///
    /// Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/Dataset
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct Dataset {
        /// Container for identity and ownership information of a resource.
        pub headers: resources::ResourceHeadersInput,
        /// Specifies the desired state of the resource.
        pub spec: datasets::DatasetSpecInput,
    }

    impl Dataset {
        pub fn schema() -> &'static TypeUri {
            &DATASET_SCHEMA
        }
        pub const fn schema_str() -> &'static str {
            DATASET_SCHEMA_STR
        }
    }

    static DATASET_SCHEMA_STR: &str =
        "https://opendatafabric.org/schemas/datasets/v1alpha1/Dataset";

    static DATASET_SCHEMA: std::sync::LazyLock<TypeUri> =
        std::sync::LazyLock::new(|| TypeUri::new_unchecked(DATASET_SCHEMA_STR));

    /// Link to a dataset.
    ///
    /// Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/DatasetHandle
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct DatasetHandle {
        /// Reference to an account that owns the dataset.
        pub account: Option<auth::ResourceOwnerHandle>,
        /// ID of the dataset resource.
        pub id: ResourceID,
        /// DID of the dataset.
        pub did: DatasetID,
        /// Name of the dataset.
        pub name: ResourceName,
    }
    impl IntoResourceRef for DatasetHandle {}

    /// Represents type of the dataset.
    ///
    /// Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/DatasetKind
    #[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
    pub enum DatasetKind {
        Root,
        Derivative,
    }

    /// Reference to a dataset.
    ///
    /// Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/DatasetRef
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct DatasetRef {
        /// Reference to an account that owns the dataset.
        pub account: Option<auth::ResourceOwnerRef>,
        /// UUID of the dataset resource.
        pub id: Option<ResourceID>,
        /// DID of the dataset.
        pub did: Option<DatasetID>,
        /// Name of the dataset.
        pub name: Option<ResourceName>,
    }
    impl IntoResourceRef for DatasetRef {}

    pub use crate::datasets::DatasetSelector;

    /// Represents a desired state of the dataset metadata.
    ///
    /// Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/DatasetSpec
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct DatasetSpec {
        /// DID of the dataset in global ODF network
        pub did: DatasetID,
        /// Type of the dataset.
        pub kind: datasets::DatasetKind,
        /// An array of metadata events that will be used to populate the chain, e.g. to set info,
        /// licenses, and attachments. As the dataset evolves, it maintains a snapshot of the key
        /// events that describe the dataset in its latest state, excluding intermediate
        /// transitions (e.g. previous schema changes) and events related to ingestions and
        /// transformations.
        pub metadata: Vec<datasets::MetadataEvent>,
        /// Reference to a storage volume where dataset data will be stored. If omitted, the node's
        /// default storage is used.
        pub volume: resources::ResourceHandle,
    }

    /// Represents a desired state of the dataset metadata.
    ///
    /// Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/DatasetSpecInput
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct DatasetSpecInput {
        /// DID of the dataset in global ODF network
        pub did: Option<DatasetID>,
        /// Type of the dataset.
        pub kind: datasets::DatasetKind,
        /// An array of metadata events that will be used to populate the chain, e.g. to set info,
        /// licenses, and attachments. As the dataset evolves, it maintains a snapshot of the key
        /// events that describe the dataset in its latest state, excluding intermediate
        /// transitions (e.g. previous schema changes) and events related to ingestions and
        /// transformations.
        pub metadata: Vec<datasets::MetadataEvent>,
        /// Reference to a storage volume where dataset data will be stored. If omitted, the node's
        /// default storage is used.
        pub volume: Option<storage::PersistentVolumeRef>,
    }

    /// Specifies the mapping of system columns onto dataset schema.
    ///
    /// Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/DatasetVocabulary
    #[derive(Clone, Debug, Eq, Default)]
    pub struct DatasetVocabulary {
        /// Name of the offset column.
        ///
        /// Defaults to: "offset"
        pub offset_column: Option<String>,
        /// Name of the operation type column.
        ///
        /// Defaults to: "op"
        pub operation_type_column: Option<String>,
        /// Name of the system time column.
        ///
        /// Defaults to: "system_time"
        pub system_time_column: Option<String>,
        /// Name of the event time column.
        ///
        /// Defaults to: "event_time"
        pub event_time_column: Option<String>,
    }

    impl DatasetVocabulary {
        pub fn default_offset_column() -> &'static str {
            "offset"
        }
        pub fn offset_column(&self) -> &str {
            self.offset_column
                .as_deref()
                .unwrap_or(Self::default_offset_column())
        }
        pub fn default_operation_type_column() -> &'static str {
            "op"
        }
        pub fn operation_type_column(&self) -> &str {
            self.operation_type_column
                .as_deref()
                .unwrap_or(Self::default_operation_type_column())
        }
        pub fn default_system_time_column() -> &'static str {
            "system_time"
        }
        pub fn system_time_column(&self) -> &str {
            self.system_time_column
                .as_deref()
                .unwrap_or(Self::default_system_time_column())
        }
        pub fn default_event_time_column() -> &'static str {
            "event_time"
        }
        pub fn event_time_column(&self) -> &str {
            self.event_time_column
                .as_deref()
                .unwrap_or(Self::default_event_time_column())
        }
    }

    impl PartialEq for DatasetVocabulary {
        fn eq(&self, other: &Self) -> bool {
            self.offset_column
                .as_deref()
                .or_else(|| Some(Self::default_offset_column()))
                == other
                    .offset_column
                    .as_deref()
                    .or_else(|| Some(Self::default_offset_column()))
                && self
                    .operation_type_column
                    .as_deref()
                    .or_else(|| Some(Self::default_operation_type_column()))
                    == other
                        .operation_type_column
                        .as_deref()
                        .or_else(|| Some(Self::default_operation_type_column()))
                && self
                    .system_time_column
                    .as_deref()
                    .or_else(|| Some(Self::default_system_time_column()))
                    == other
                        .system_time_column
                        .as_deref()
                        .or_else(|| Some(Self::default_system_time_column()))
                && self
                    .event_time_column
                    .as_deref()
                    .or_else(|| Some(Self::default_event_time_column()))
                    == other
                        .event_time_column
                        .as_deref()
                        .or_else(|| Some(Self::default_event_time_column()))
        }
    }

    /// Indicates that derivative transformation has been performed.
    ///
    /// Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/ExecuteTransform
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct ExecuteTransform {
        /// Defines inputs used in this transaction. Slices corresponding to every input dataset
        /// must be present.
        pub query_inputs: Vec<datasets::ExecuteTransformInput>,
        /// Hash of the checkpoint file used to restore transformation state, if any.
        pub prev_checkpoint: Option<Multihash>,
        /// Last offset of the previous data slice, if any. Must be equal to the last non-empty
        /// `newData.offsetInterval.end`.
        pub prev_offset: Option<u64>,
        /// Describes output data written during this transaction, if any.
        pub new_data: Option<datasets::DataSlice>,
        /// Describes checkpoint written during this transaction, if any. If an engine operation
        /// resulted in no updates to the checkpoint, but checkpoint is still relevant for
        /// subsequent runs - a hash of the previous checkpoint should be specified.
        pub new_checkpoint: Option<datasets::Checkpoint>,
        /// Last watermark of the output data stream, if any. Initial blocks may not have
        /// watermarks, but once watermark is set - all subsequent blocks should either carry the
        /// same watermark or specify a new (greater) one. Thus, watermarks are monotonically
        /// non-decreasing.
        pub new_watermark: Option<DateTime<Utc>>,
    }

    /// Describes a slice of the input dataset used during a transformation
    ///
    /// Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/ExecuteTransformInput
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct ExecuteTransformInput {
        /// Input dataset identifier.
        pub dataset_id: DatasetID,
        /// Last block of the input dataset that was previously incorporated into the derivative
        /// transformation, if any. Must be equal to the last non-empty `newBlockHash`. Together
        /// with `newBlockHash` defines a half-open `(prevBlockHash, newBlockHash]` interval of
        /// blocks that will be considered in this transaction.
        pub prev_block_hash: Option<Multihash>,
        /// Hash of the last block that will be incorporated into the derivative transformation.
        /// When present, defines a half-open `(prevBlockHash, newBlockHash]` interval of blocks
        /// that will be considered in this transaction.
        pub new_block_hash: Option<Multihash>,
        /// Last data record offset in the input dataset that was previously incorporated into the
        /// derivative transformation, if any. Must be equal to the last non-empty `newOffset`.
        /// Together with `newOffset` defines a half-open `(prevOffset, newOffset]` interval of
        /// data records that will be considered in this transaction.
        pub prev_offset: Option<u64>,
        /// Offset of the last data record that will be incorporated into the derivative
        /// transformation, if any. When present, defines a half-open `(prevOffset, newOffset]`
        /// interval of data records that will be considered in this transaction.
        pub new_offset: Option<u64>,
    }

    /// An individual block in the metadata chain that captures the history of modifications of a
    /// dataset.
    ///
    /// Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/MetadataBlock
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct MetadataBlock {
        /// System time when this block was written.
        pub system_time: DateTime<Utc>,
        /// Hash sum of the preceding block.
        pub prev_block_hash: Option<Multihash>,
        /// Block sequence number, starting from zero at the seed block.
        pub sequence_number: u64,
        /// Event data.
        pub event: datasets::MetadataEvent,
    }

    /// Represents a transaction that occurred on a dataset.
    ///
    /// Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/MetadataEvent
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum MetadataEvent {
        AddData(datasets::AddData),
        ExecuteTransform(datasets::ExecuteTransform),
        Seed(datasets::Seed),
        SetPollingSource(legacy::SetPollingSource),
        SetTransform(datasets::SetTransform),
        SetVocab(datasets::SetVocab),
        SetAttachments(datasets::SetAttachments),
        SetInfo(datasets::SetInfo),
        SetLicense(datasets::SetLicense),
        SetDataSchema(datasets::SetDataSchema),
        AddPushSource(legacy::AddPushSource),
        DisablePushSource(legacy::DisablePushSource),
        DisablePollingSource(legacy::DisablePollingSource),
    }

    impl_enum_with_variants!(MetadataEvent);
    impl_enum_variant!(MetadataEvent::AddData(datasets::AddData));
    impl_enum_variant!(MetadataEvent::ExecuteTransform(datasets::ExecuteTransform));
    impl_enum_variant!(MetadataEvent::Seed(datasets::Seed));
    impl_enum_variant!(MetadataEvent::SetPollingSource(legacy::SetPollingSource));
    impl_enum_variant!(MetadataEvent::SetTransform(datasets::SetTransform));
    impl_enum_variant!(MetadataEvent::SetVocab(datasets::SetVocab));
    impl_enum_variant!(MetadataEvent::SetAttachments(datasets::SetAttachments));
    impl_enum_variant!(MetadataEvent::SetInfo(datasets::SetInfo));
    impl_enum_variant!(MetadataEvent::SetLicense(datasets::SetLicense));
    impl_enum_variant!(MetadataEvent::SetDataSchema(datasets::SetDataSchema));
    impl_enum_variant!(MetadataEvent::AddPushSource(legacy::AddPushSource));
    impl_enum_variant!(MetadataEvent::DisablePushSource(legacy::DisablePushSource));
    impl_enum_variant!(MetadataEvent::DisablePollingSource(
        legacy::DisablePollingSource
    ));

    bitflags! {
        #[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
        pub struct MetadataEventTypeFlags: u32 {
            const ADD_DATA = 1 << 0;
            const EXECUTE_TRANSFORM = 1 << 1;
            const SEED = 1 << 2;
            const SET_POLLING_SOURCE = 1 << 3;
            const SET_TRANSFORM = 1 << 4;
            const SET_VOCAB = 1 << 5;
            const SET_ATTACHMENTS = 1 << 6;
            const SET_INFO = 1 << 7;
            const SET_LICENSE = 1 << 8;
            const SET_DATA_SCHEMA = 1 << 9;
            const ADD_PUSH_SOURCE = 1 << 10;
            const DISABLE_PUSH_SOURCE = 1 << 11;
            const DISABLE_POLLING_SOURCE = 1 << 12;
        }
    }

    impl From<&MetadataEvent> for MetadataEventTypeFlags {
        fn from(v: &MetadataEvent) -> Self {
            match v {
                MetadataEvent::AddData(_) => Self::ADD_DATA,
                MetadataEvent::ExecuteTransform(_) => Self::EXECUTE_TRANSFORM,
                MetadataEvent::Seed(_) => Self::SEED,
                MetadataEvent::SetPollingSource(_) => Self::SET_POLLING_SOURCE,
                MetadataEvent::SetTransform(_) => Self::SET_TRANSFORM,
                MetadataEvent::SetVocab(_) => Self::SET_VOCAB,
                MetadataEvent::SetAttachments(_) => Self::SET_ATTACHMENTS,
                MetadataEvent::SetInfo(_) => Self::SET_INFO,
                MetadataEvent::SetLicense(_) => Self::SET_LICENSE,
                MetadataEvent::SetDataSchema(_) => Self::SET_DATA_SCHEMA,
                MetadataEvent::AddPushSource(_) => Self::ADD_PUSH_SOURCE,
                MetadataEvent::DisablePushSource(_) => Self::DISABLE_PUSH_SOURCE,
                MetadataEvent::DisablePollingSource(_) => Self::DISABLE_POLLING_SOURCE,
            }
        }
    }

    /// Describes a range of data as a closed arithmetic interval of offsets
    ///
    /// Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/OffsetInterval
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct OffsetInterval {
        /// Start of the closed interval [start; end].
        pub start: u64,
        /// End of the closed interval [start; end].
        pub end: u64,
    }

    /// Represents a projection of a dataaset history into a state for fast lookups.
    ///
    /// Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/Projection
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct Projection {
        /// Container for identity and ownership information of a resource.
        pub headers: resources::ResourceHeadersInput,
        /// Specifies the desired state of the resource.
        pub spec: datasets::ProjectionSpecInput,
    }

    impl Projection {
        pub fn schema() -> &'static TypeUri {
            &PROJECTION_SCHEMA
        }
        pub const fn schema_str() -> &'static str {
            PROJECTION_SCHEMA_STR
        }
    }

    static PROJECTION_SCHEMA_STR: &str =
        "https://opendatafabric.org/schemas/datasets/v1alpha1/Projection";

    static PROJECTION_SCHEMA: std::sync::LazyLock<TypeUri> =
        std::sync::LazyLock::new(|| TypeUri::new_unchecked(PROJECTION_SCHEMA_STR));

    /// Represents a projection of a dataaset history into a state for fast lookups.
    ///
    /// Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/ProjectionSpec
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct ProjectionSpec {
        /// Datasets that will be used as sources.
        pub inputs: Vec<datasets::TransformInput>,
        /// Transformation that will be applied to produce new data.
        pub project: datasets::Transform,
    }

    /// Represents a projection of a dataaset history into a state for fast lookups.
    ///
    /// Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/ProjectionSpecInput
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct ProjectionSpecInput {
        /// Datasets that will be used as sources.
        pub inputs: Vec<datasets::TransformInput>,
        /// Transformation that will be applied to produce new data.
        pub project: datasets::Transform,
    }

    /// Read dataset data (query, download).
    ///
    /// Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/actions/Read
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct Read {}

    /// Establishes the identity of the dataset. Always the first metadata event in the chain.
    ///
    /// Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/Seed
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct Seed {
        /// Unique identity of the dataset.
        pub dataset_id: DatasetID,
        /// Type of the dataset.
        pub dataset_kind: datasets::DatasetKind,
    }

    /// Associates a set of files with this dataset.
    ///
    /// Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/SetAttachments
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct SetAttachments {
        /// One of the supported attachment sources.
        pub attachments: datasets::Attachments,
    }

    /// Specifies the complete schema of Data Slices added to the Dataset following this event.
    ///
    /// Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/SetDataSchema
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct SetDataSchema {
        /// DEPRECATED: Apache Arrow schema encoded in its native flatbuffers representation.
        pub raw_arrow_schema: Option<Vec<u8>>,
        /// Defines the logical schema of the data files that follow this event. Will become a
        /// required field after migration.
        pub schema: Option<data::DataSchema>,
    }

    /// Provides basic human-readable information about a dataset.
    ///
    /// Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/SetInfo
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct SetInfo {
        /// Brief single-sentence summary of a dataset.
        pub description: Option<String>,
        /// Keywords, search terms, or tags used to describe the dataset.
        pub keywords: Option<Vec<String>>,
    }

    /// Defines a license that applies to this dataset.
    ///
    /// Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/SetLicense
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct SetLicense {
        /// Abbreviated name of the license.
        pub short_name: String,
        /// Full name of the license.
        pub name: String,
        /// License identifier from the SPDX License List.
        pub spdx_id: Option<String>,
        /// URL where licensing terms can be found.
        pub website_url: String,
    }

    /// Defines a transformation that produces data in a derivative dataset.
    ///
    /// Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/SetTransform
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct SetTransform {
        /// Datasets that will be used as sources.
        pub inputs: Vec<datasets::TransformInput>,
        /// Transformation that will be applied to produce new data.
        pub transform: datasets::Transform,
    }

    /// Lets you manipulate names of the system columns to avoid conflicts.
    ///
    /// Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/SetVocab
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct SetVocab {
        /// Name of the offset column.
        pub offset_column: Option<String>,
        /// Name of the operation type column.
        pub operation_type_column: Option<String>,
        /// Name of the system time column.
        pub system_time_column: Option<String>,
        /// Name of the event time column.
        pub event_time_column: Option<String>,
    }

    /// Defines a query in a multi-step SQL transformation.
    ///
    /// Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/SqlQueryStep
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct SqlQueryStep {
        /// Name of the temporary view that will be created from result of the query. Step without
        /// this alias will be treated as an output of the transformation.
        pub alias: Option<String>,
        /// SQL query the result of which will be exposed under the alias.
        pub query: String,
    }

    /// Temporary Flink-specific extension for creating temporal tables from streams.
    ///
    /// Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/TemporalTable
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct TemporalTable {
        /// Name of the dataset to be converted into a temporal table.
        pub name: String,
        /// Column names used as the primary key for creating a table.
        pub primary_key: Vec<String>,
    }

    /// Engine-specific processing queries that shape the resulting data.
    ///
    /// Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/Transform
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum Transform {
        Sql(datasets::TransformSql),
    }

    impl_enum_with_variants!(Transform);
    impl_enum_variant!(Transform::Sql(datasets::TransformSql));

    /// Describes a derivative transformation input
    ///
    /// Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/TransformInput
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct TransformInput {
        /// A local or remote dataset reference. When block is accepted this MUST be in the form of
        /// a DatasetId to guarantee reproducibility, as aliases can change over time.
        pub dataset_ref: crate::datasets::legacy::DatasetRef,
        /// An alias under which this input will be available in queries. Will be populated from
        /// `datasetRef` if not provided before resolving it to DatasetId.
        pub alias: Option<String>,
    }

    /// Transform using one of the SQL dialects.
    ///
    /// Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/Transform#/$defs/Sql
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct TransformSql {
        /// Identifier of the engine used for this transformation.
        pub engine: String,
        /// Version of the engine to use.
        pub version: Option<String>,
        /// SQL query the result of which will be used as an output. This is a convenience property
        /// meant only for defining queries by hand. When stored in the metadata this property will
        /// never be set and instead will be converted into a single-iter `queries` array.
        pub query: Option<String>,
        /// Specifies multi-step SQL transformations. Each step acts as a shorthand for `CREATE
        /// TEMPORARY VIEW <alias> AS (<query>)`. Last query in the array should have no alias and
        /// will be treated as an output.
        pub queries: Option<Vec<datasets::SqlQueryStep>>,
        /// Temporary Flink-specific extension for creating temporal tables from streams.
        pub temporal_tables: Option<Vec<datasets::TemporalTable>>,
    }

    /// Represents a watermark in the event stream.
    ///
    /// Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/Watermark
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct Watermark {
        /// Moment in processing time when watermark was emitted.
        pub system_time: DateTime<Utc>,
        /// Moment in event time which watermark has reached.
        pub event_time: DateTime<Utc>,
    }

    /// Append new data to the dataset.
    ///
    /// Schema: https://opendatafabric.org/schemas/datasets/v1alpha1/actions/Write
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct Write {}
}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// engines
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod engines {
    #[allow(unused_imports)]
    use super::*;

    /// Sent by the coordinator to an engine to perform query on raw input data, usually as part of
    /// ingest preprocessing step
    ///
    /// Schema: https://opendatafabric.org/schemas/engines/v1alpha1/RawQueryRequest
    #[derive(Clone, Debug, Eq)]
    pub struct RawQueryRequest {
        /// Paths to input data files to perform query over. Must all have identical schema.
        pub input_data_paths: Vec<PathBuf>,
        /// Transformation that will be applied to produce new data.
        pub transform: datasets::Transform,
        /// Path where query result will be written.
        pub output_data_path: PathBuf,
        /// Requires the engine to keep the order of records read from the input in the query
        /// result, or fail the request if it cannot.
        ///
        /// Defaults to: false
        pub preserve_order: Option<bool>,
    }

    impl RawQueryRequest {
        pub fn default_preserve_order() -> bool {
            false
        }
        pub fn preserve_order(&self) -> bool {
            self.preserve_order
                .unwrap_or(Self::default_preserve_order())
        }
    }

    impl PartialEq for RawQueryRequest {
        fn eq(&self, other: &Self) -> bool {
            self.input_data_paths == other.input_data_paths
                && self.transform == other.transform
                && self.output_data_path == other.output_data_path
                && self
                    .preserve_order
                    .or_else(|| Some(Self::default_preserve_order()))
                    == other
                        .preserve_order
                        .or_else(|| Some(Self::default_preserve_order()))
        }
    }

    /// Sent by an engine to coordinator when performing the raw query operation
    ///
    /// Schema: https://opendatafabric.org/schemas/engines/v1alpha1/RawQueryResponse
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum RawQueryResponse {
        Progress(engines::RawQueryResponseProgress),
        Success(engines::RawQueryResponseSuccess),
        InvalidQuery(engines::RawQueryResponseInvalidQuery),
        InternalError(engines::RawQueryResponseInternalError),
    }

    impl_enum_with_variants!(RawQueryResponse);
    impl_enum_variant!(RawQueryResponse::Progress(
        engines::RawQueryResponseProgress
    ));
    impl_enum_variant!(RawQueryResponse::Success(engines::RawQueryResponseSuccess));
    impl_enum_variant!(RawQueryResponse::InvalidQuery(
        engines::RawQueryResponseInvalidQuery
    ));
    impl_enum_variant!(RawQueryResponse::InternalError(
        engines::RawQueryResponseInternalError
    ));

    /// Internal error during query execution
    ///
    /// Schema: https://opendatafabric.org/schemas/engines/v1alpha1/RawQueryResponse#/$defs/InternalError
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct RawQueryResponseInternalError {
        /// Brief description of an error
        pub message: String,
        /// Details of an error (e.g. a backtrace)
        pub backtrace: Option<String>,
    }

    /// Query did not pass validation
    ///
    /// Schema: https://opendatafabric.org/schemas/engines/v1alpha1/RawQueryResponse#/$defs/InvalidQuery
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct RawQueryResponseInvalidQuery {
        /// Explanation of an error
        pub message: String,
    }

    /// Reports query progress
    ///
    /// Schema: https://opendatafabric.org/schemas/engines/v1alpha1/RawQueryResponse#/$defs/Progress
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct RawQueryResponseProgress {}

    /// Query executed successfully
    ///
    /// Schema: https://opendatafabric.org/schemas/engines/v1alpha1/RawQueryResponse#/$defs/Success
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct RawQueryResponseSuccess {
        /// Number of records produced by the query
        pub num_records: u64,
    }

    /// Sent by the coordinator to an engine to perform the next step of data transformation
    ///
    /// Schema: https://opendatafabric.org/schemas/engines/v1alpha1/TransformRequest
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct TransformRequest {
        /// Unique identifier of the output dataset.
        pub dataset_id: DatasetID,
        /// Alias of the output dataset, for logging purposes only.
        pub dataset_alias: crate::datasets::legacy::DatasetAlias,
        /// System time to use for new records.
        pub system_time: DateTime<Utc>,
        /// Vocabulary of the output dataset.
        pub vocab: datasets::DatasetVocabulary,
        /// Transformation that will be applied to produce new data.
        pub transform: datasets::Transform,
        /// Defines inputs used in this transaction. Slices corresponding to every input dataset
        /// must be present.
        pub query_inputs: Vec<engines::TransformRequestInput>,
        /// Starting offset to use for new data records.
        pub next_offset: u64,
        /// TODO: This will be removed when coordinator will be speaking to engines purely through
        /// Arrow.
        pub prev_checkpoint_path: Option<PathBuf>,
        /// TODO: This will be removed when coordinator will be speaking to engines purely through
        /// Arrow.
        pub new_checkpoint_path: PathBuf,
        /// TODO: This will be removed when coordinator will be speaking to engines purely through
        /// Arrow.
        pub new_data_path: PathBuf,
    }

    /// Sent as part of the engine transform request operation to describe the input
    ///
    /// Schema: https://opendatafabric.org/schemas/engines/v1alpha1/TransformRequestInput
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct TransformRequestInput {
        /// Unique identifier of the dataset.
        pub dataset_id: DatasetID,
        /// Alias of the output dataset, for logging purposes only.
        pub dataset_alias: crate::datasets::legacy::DatasetAlias,
        /// An alias of this input to be used in queries.
        pub query_alias: String,
        /// Vocabulary of the input dataset.
        pub vocab: datasets::DatasetVocabulary,
        /// Subset of data that goes into this transaction.
        pub offset_interval: Option<datasets::OffsetInterval>,
        /// TODO: This will be removed when coordinator will be slicing data for the engine.
        pub data_paths: Vec<PathBuf>,
        /// TODO: replace with actual DDL or Parquet schema.
        pub schema_file: PathBuf,
        /// Watermarks that should be injected into the stream to separate micro batches for
        /// reproducibility.
        pub explicit_watermarks: Vec<datasets::Watermark>,
    }

    /// Sent by an engine to coordinator when performing the data transformation
    ///
    /// Schema: https://opendatafabric.org/schemas/engines/v1alpha1/TransformResponse
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum TransformResponse {
        Progress(engines::TransformResponseProgress),
        Success(engines::TransformResponseSuccess),
        InvalidQuery(engines::TransformResponseInvalidQuery),
        InternalError(engines::TransformResponseInternalError),
    }

    impl_enum_with_variants!(TransformResponse);
    impl_enum_variant!(TransformResponse::Progress(
        engines::TransformResponseProgress
    ));
    impl_enum_variant!(TransformResponse::Success(
        engines::TransformResponseSuccess
    ));
    impl_enum_variant!(TransformResponse::InvalidQuery(
        engines::TransformResponseInvalidQuery
    ));
    impl_enum_variant!(TransformResponse::InternalError(
        engines::TransformResponseInternalError
    ));

    /// Internal error during query execution
    ///
    /// Schema: https://opendatafabric.org/schemas/engines/v1alpha1/TransformResponse#/$defs/InternalError
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct TransformResponseInternalError {
        /// Brief description of an error
        pub message: String,
        /// Details of an error (e.g. a backtrace)
        pub backtrace: Option<String>,
    }

    /// Query did not pass validation
    ///
    /// Schema: https://opendatafabric.org/schemas/engines/v1alpha1/TransformResponse#/$defs/InvalidQuery
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct TransformResponseInvalidQuery {
        /// Explanation of an error
        pub message: String,
    }

    /// Reports query progress
    ///
    /// Schema: https://opendatafabric.org/schemas/engines/v1alpha1/TransformResponse#/$defs/Progress
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct TransformResponseProgress {}

    /// Query executed successfully
    ///
    /// Schema: https://opendatafabric.org/schemas/engines/v1alpha1/TransformResponse#/$defs/Success
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct TransformResponseSuccess {
        /// Data slice produced by the transaction, if any.
        pub new_offset_interval: Option<datasets::OffsetInterval>,
        /// Watermark advanced by the transaction, if any.
        pub new_watermark: Option<DateTime<Utc>>,
    }
}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// events
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod events {
    #[allow(unused_imports)]
    use super::*;

    /// Filters that work on domain event types and fields.
    ///
    /// Schema: https://opendatafabric.org/schemas/events/v1alpha1/EventFilter
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub struct EventFilter {
        pub entries: std::collections::BTreeMap<String, serde_json::Value>,
    }
}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// flows
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod flows {
    #[allow(unused_imports)]
    use super::*;

    /// Defines a sequence of tasks to be executed upon certain trigger conditions.
    ///
    /// Schema: https://opendatafabric.org/schemas/flows/v1alpha1/Flow
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct Flow {
        /// Container for identity and ownership information of a resource.
        pub headers: resources::ResourceHeadersInput,
        /// Specifies the desired state of the flow.
        pub spec: flows::FlowSpecInput,
    }

    impl Flow {
        pub fn schema() -> &'static TypeUri {
            &FLOW_SCHEMA
        }
        pub const fn schema_str() -> &'static str {
            FLOW_SCHEMA_STR
        }
    }

    static FLOW_SCHEMA_STR: &str = "https://opendatafabric.org/schemas/flows/v1alpha1/Flow";

    static FLOW_SCHEMA: std::sync::LazyLock<TypeUri> =
        std::sync::LazyLock::new(|| TypeUri::new_unchecked(FLOW_SCHEMA_STR));

    /// Defines a set of tasks to be executed in a sequence.
    ///
    /// Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowRun
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct FlowRun {
        /// Container for identity and ownership information of a resource.
        pub headers: resources::ResourceHeadersInput,
        /// Specifies the desired state of the flow run.
        pub spec: flows::FlowRunSpecInput,
    }

    impl FlowRun {
        pub fn schema() -> &'static TypeUri {
            &FLOW_RUN_SCHEMA
        }
        pub const fn schema_str() -> &'static str {
            FLOW_RUN_SCHEMA_STR
        }
    }

    static FLOW_RUN_SCHEMA_STR: &str = "https://opendatafabric.org/schemas/flows/v1alpha1/FlowRun";

    static FLOW_RUN_SCHEMA: std::sync::LazyLock<TypeUri> =
        std::sync::LazyLock::new(|| TypeUri::new_unchecked(FLOW_RUN_SCHEMA_STR));

    /// Cause of the flow run activation
    ///
    /// Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowRunActivationCause
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct FlowRunActivationCause {
        /// Time at which the trigger fired.
        pub activation_time: DateTime<Utc>,
        /// Principal that initiated the run, if applicable.
        pub initiator: Option<auth::PrincipalHandle>,
        /// Copy of the trigger configuration from the parent Flow that fired.
        pub trigger: flows::FlowTrigger,
    }

    /// Condition capturing what caused this FlowRun to be scheduled. Set by the controller at
    /// creation time; never written by users. In case of a retry, the causes of the original run
    /// are preserved.
    ///
    /// Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowRunActivationCauses
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct FlowRunActivationCauses {
        /// Triggers that caused this run to be scheduled.
        pub activation_causes: Vec<flows::FlowRunActivationCause>,
        /// Additional triggers that fired while this run was already queued or executing.
        pub late_activation_causes: Option<Vec<flows::FlowRunActivationCause>>,
    }

    /// Condition linking this FlowRun to the previous FlowRun it is retrying. Set by the
    /// controller; never written by users.
    ///
    /// Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowRunRetry
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct FlowRunRetry {
        /// Reference to the FlowRun this run is retrying.
        pub retry_of: resources::ResourceHandle,
        /// Retry attempt number (1 = first retry, i.e. second run total).
        pub retry_number: u32,
    }

    /// Defines a set of tasks to be executed in a sequence.
    ///
    /// Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowRunSpec
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct FlowRunSpec {
        /// Defines the default target resources on which tasks will be performed.
        pub target: Option<resources::ResourceHandle>,
        /// Service account whose permissions the tasks of this run execute with. Must be an
        /// account of service account type. A run created by the flow controller copies it from
        /// the `Flow`.
        pub service_account: Option<auth::PrincipalHandle>,
        /// List of tasks to run consecutively.
        pub tasks: Vec<tasks::TaskSpec>,
    }

    /// Defines a set of tasks to be executed in a sequence.
    ///
    /// Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowRunSpecInput
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct FlowRunSpecInput {
        /// Defines the default target resources on which tasks will be performed.
        pub target: Option<resources::ResourceRef>,
        /// Service account whose permissions the tasks of this run execute with. Must be an
        /// account of service account type. A run created by the flow controller copies it from
        /// the `Flow`.
        pub service_account: Option<auth::PrincipalRef>,
        /// List of tasks to run consecutively.
        pub tasks: Vec<tasks::TaskSpecInput>,
    }

    /// Condition tracking the overall execution status of a FlowRun and its spawned tasks.
    ///
    /// Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowRunStatus
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct FlowRunStatus {
        /// Overall execution status of the FlowRun.
        pub status: flows::FlowRunStatusValue,
        /// Tasks spawned by this FlowRun, in execution order.
        pub tasks: Option<Vec<flows::FlowRunStatusTaskEntry>>,
    }

    /// Describes a task spawned by this FlowRun.
    ///
    /// Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowRunStatus#/$defs/TaskEntry
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct FlowRunStatusTaskEntry {
        /// Corresponds to the task name in spec.tasks.
        pub name: String,
        /// Reference to the spawned Task resource.
        pub task: resources::ResourceHandle,
        /// Current execution phase of this task.
        pub status: tasks::TaskStatus,
        /// Outcome kind once the task has finished.
        pub outcome: Option<tasks::TaskOutcome>,
        /// Last time at which this task's status has been updated.
        pub last_updated_at: DateTime<Utc>,
    }

    /// Overall execution status of the FlowRun.
    ///
    /// Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowRunStatus#/$defs/Value
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    pub enum FlowRunStatusValue {
        Waiting,
        Running,
        Finished,
    }

    /// Defines a sequence of tasks to be executed upon certain trigger conditions.
    ///
    /// Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowSpec
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct FlowSpec {
        /// Defines resources for which this flow will be instantiated.
        pub target: resources::ResourceSelector,
        /// Service account whose permissions the tasks of this flow execute with. Must be an
        /// account of service account type. Required for flows owned by an organization. When
        /// omitted, tasks execute with the permissions of the user account that owns the flow.
        pub service_account: Option<auth::PrincipalHandle>,
        /// Conditions that cause this flow to execute.
        pub triggers: Vec<flows::FlowTrigger>,
        /// List of tasks to run consecutively.
        pub tasks: Vec<tasks::TaskSpec>,
        /// Defines how a flow should react to failures.
        pub retry_policy: Option<flows::RetryPolicy>,
        /// Controls retention of `FlowStatus.recentBindings`.
        pub recent_bindings_retention: Option<flows::RecentBindingsRetention>,
        /// Controls retention of `FlowStatus.recentRuns`.
        pub recent_runs_retention: Option<flows::RecentRunsRetention>,
    }

    /// Defines a sequence of tasks to be executed upon certain trigger conditions.
    ///
    /// Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowSpecInput
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct FlowSpecInput {
        /// Defines resources for which this flow will be instantiated.
        pub target: resources::ResourceSelector,
        /// Service account whose permissions the tasks of this flow execute with. Must be an
        /// account of service account type. Required for flows owned by an organization. When
        /// omitted, tasks execute with the permissions of the user account that owns the flow.
        pub service_account: Option<auth::PrincipalRef>,
        /// Conditions that cause this flow to execute.
        pub triggers: Vec<flows::FlowTriggerInput>,
        /// List of tasks to run consecutively.
        pub tasks: Vec<tasks::TaskSpecInput>,
        /// Defines how a flow should react to failures.
        pub retry_policy: Option<flows::RetryPolicy>,
        /// Controls retention of `FlowStatus.recentBindings`.
        pub recent_bindings_retention: Option<flows::RecentBindingsRetention>,
        /// Controls retention of `FlowStatus.recentRuns`.
        pub recent_runs_retention: Option<flows::RecentRunsRetention>,
    }

    /// Condition tracking the overall status of a Flow and a summary of recent runs.
    ///
    /// Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowStatus
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct FlowStatus {
        /// Overall operational status of the Flow.
        pub status: flows::FlowStatusValue,
        /// Last N resources matched by the flow's target selector. Bounded by
        /// spec.recentBindingsRetention.
        pub recent_bindings: Option<Vec<flows::FlowStatusBindingEntry>>,
        /// Total number of resources currently matched by the flow's target selector.
        pub bindings_total: Option<u64>,
        /// Summaries of recent FlowRuns. Bounded by spec.recentRunsRetention.
        pub recent_runs: Option<Vec<flows::FlowStatusRunEntry>>,
    }

    /// A resource currently matched by the flow's target selector.
    ///
    /// Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowStatus#/$defs/BindingEntry
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct FlowStatusBindingEntry {
        /// The matched resource.
        pub target: resources::ResourceHandle,
        /// Time at which this binding was established.
        pub bound_at: DateTime<Utc>,
    }

    /// Summary of a recent FlowRun.
    ///
    /// Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowStatus#/$defs/RunEntry
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct FlowStatusRunEntry {
        /// Reference to the FlowRun resource.
        pub flow_run: resources::ResourceHandle,
        /// Execution status of this run.
        pub status: String,
        /// Outcome of this run, if finished.
        pub outcome: Option<String>,
        /// Time at which this run finished.
        pub finished_at: Option<DateTime<Utc>>,
        /// Reference to the FlowRun this run is retrying, if applicable.
        pub retry_of: Option<resources::ResourceHandle>,
        /// Retry attempt number (1 = first retry, i.e. second run total).
        pub retry_number: Option<u32>,
    }

    /// Overall operational status of the Flow.
    ///
    /// Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowStatus#/$defs/Value
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    pub enum FlowStatusValue {
        Active,
        Paused,
    }

    /// Condition that causes a flow to be executed.
    ///
    /// Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowTrigger
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum FlowTrigger {
        Manual(flows::FlowTriggerManual),
        Schedule(flows::FlowTriggerSchedule),
        Interval(flows::FlowTriggerInterval),
        Event(flows::FlowTriggerEvent),
        Source(flows::FlowTriggerSource),
        InputsUpdated(flows::FlowTriggerInputsUpdated),
    }

    impl_enum_with_variants!(FlowTrigger);
    impl_enum_variant!(FlowTrigger::Manual(flows::FlowTriggerManual));
    impl_enum_variant!(FlowTrigger::Schedule(flows::FlowTriggerSchedule));
    impl_enum_variant!(FlowTrigger::Interval(flows::FlowTriggerInterval));
    impl_enum_variant!(FlowTrigger::Event(flows::FlowTriggerEvent));
    impl_enum_variant!(FlowTrigger::Source(flows::FlowTriggerSource));
    impl_enum_variant!(FlowTrigger::InputsUpdated(flows::FlowTriggerInputsUpdated));

    /// Triggers the flow when an event bus event matching one of the filters is observed.
    ///
    /// Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowTrigger#/$defs/Event
    #[derive(Clone, Debug, Eq)]
    pub struct FlowTriggerEvent {
        /// Allows to pause an individual trigger.
        ///
        /// Defaults to: true
        pub enabled: Option<bool>,
        /// Filters the event by type and fields.
        pub events: events::EventFilter,
        /// Minimum interval between activations of the trigger. If the trigger fires again within
        /// the interval, the activation is postponed until the interval ends, batching multiple
        /// activations into one flow run.
        pub cooldown: Option<DurationString>,
        /// If the trigger is activated this many times during the `cooldown` interval, it fires
        /// without waiting for the interval to end.
        pub cooldown_max_batch: Option<u64>,
    }

    impl FlowTriggerEvent {
        pub fn default_enabled() -> bool {
            true
        }
        pub fn enabled(&self) -> bool {
            self.enabled.unwrap_or(Self::default_enabled())
        }
    }

    impl PartialEq for FlowTriggerEvent {
        fn eq(&self, other: &Self) -> bool {
            self.enabled.or_else(|| Some(Self::default_enabled()))
                == other.enabled.or_else(|| Some(Self::default_enabled()))
                && self.events == other.events
                && self.cooldown == other.cooldown
                && self.cooldown_max_batch == other.cooldown_max_batch
        }
    }

    /// Condition that causes a flow to be executed.
    ///
    /// Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowTriggerInput
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum FlowTriggerInput {
        Manual(flows::FlowTriggerInputManual),
        Schedule(flows::FlowTriggerInputSchedule),
        Interval(flows::FlowTriggerInputInterval),
        Event(flows::FlowTriggerInputEvent),
        Source(flows::FlowTriggerInputSource),
        InputsUpdated(flows::FlowTriggerInputInputsUpdated),
    }

    impl_enum_with_variants!(FlowTriggerInput);
    impl_enum_variant!(FlowTriggerInput::Manual(flows::FlowTriggerInputManual));
    impl_enum_variant!(FlowTriggerInput::Schedule(flows::FlowTriggerInputSchedule));
    impl_enum_variant!(FlowTriggerInput::Interval(flows::FlowTriggerInputInterval));
    impl_enum_variant!(FlowTriggerInput::Event(flows::FlowTriggerInputEvent));
    impl_enum_variant!(FlowTriggerInput::Source(flows::FlowTriggerInputSource));
    impl_enum_variant!(FlowTriggerInput::InputsUpdated(
        flows::FlowTriggerInputInputsUpdated
    ));

    /// Triggers the flow when an event bus event matching one of the filters is observed.
    ///
    /// Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowTriggerInput#/$defs/Event
    #[derive(Clone, Debug, Eq)]
    pub struct FlowTriggerInputEvent {
        /// Allows to pause an individual trigger.
        ///
        /// Defaults to: true
        pub enabled: Option<bool>,
        /// Filters the event by type and fields.
        pub events: events::EventFilter,
        /// Minimum interval between activations of the trigger. If the trigger fires again within
        /// the interval, the activation is postponed until the interval ends, batching multiple
        /// activations into one flow run.
        pub cooldown: Option<DurationString>,
        /// If the trigger is activated this many times during the `cooldown` interval, it fires
        /// without waiting for the interval to end.
        pub cooldown_max_batch: Option<u64>,
    }

    impl FlowTriggerInputEvent {
        pub fn default_enabled() -> bool {
            true
        }
        pub fn enabled(&self) -> bool {
            self.enabled.unwrap_or(Self::default_enabled())
        }
    }

    impl PartialEq for FlowTriggerInputEvent {
        fn eq(&self, other: &Self) -> bool {
            self.enabled.or_else(|| Some(Self::default_enabled()))
                == other.enabled.or_else(|| Some(Self::default_enabled()))
                && self.events == other.events
                && self.cooldown == other.cooldown
                && self.cooldown_max_batch == other.cooldown_max_batch
        }
    }

    /// Triggers the flow when inputs of a derivative dataset receive new data, with optional
    /// batching controls.
    ///
    /// Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowTriggerInput#/$defs/InputsUpdated
    #[derive(Clone, Debug, Eq, Default)]
    pub struct FlowTriggerInputInputsUpdated {
        /// Allows to pause an individual trigger.
        ///
        /// Defaults to: true
        pub enabled: Option<bool>,
        /// Minimum interval between activations of the trigger. If the trigger fires again within
        /// the interval, the activation is postponed until the interval ends, batching multiple
        /// activations into one flow run.
        pub cooldown: Option<DurationString>,
        /// If the trigger is activated this many times during the `cooldown` interval, it fires
        /// without waiting for the interval to end.
        pub cooldown_max_batch: Option<u64>,
        /// Selector of derivative datasets whose inputs are watched. Defaults to the flow-level
        /// target when omitted.
        pub target: Option<datasets::DatasetSelector>,
        /// Minimum number of new input records to accumulate before triggering.
        pub min_records_to_await: Option<u64>,
        /// Maximum time to wait for `minRecordsToAwait` before triggering anyway e.g. `1h`.
        pub max_await_interval: Option<DurationString>,
    }

    impl FlowTriggerInputInputsUpdated {
        pub fn default_enabled() -> bool {
            true
        }
        pub fn enabled(&self) -> bool {
            self.enabled.unwrap_or(Self::default_enabled())
        }
    }

    impl PartialEq for FlowTriggerInputInputsUpdated {
        fn eq(&self, other: &Self) -> bool {
            self.enabled.or_else(|| Some(Self::default_enabled()))
                == other.enabled.or_else(|| Some(Self::default_enabled()))
                && self.cooldown == other.cooldown
                && self.cooldown_max_batch == other.cooldown_max_batch
                && self.target == other.target
                && self.min_records_to_await == other.min_records_to_await
                && self.max_await_interval == other.max_await_interval
        }
    }

    /// Triggers the flow at regular intervals.
    ///
    /// Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowTriggerInput#/$defs/Interval
    #[derive(Clone, Debug, Eq)]
    pub struct FlowTriggerInputInterval {
        /// Allows to pause an individual trigger.
        ///
        /// Defaults to: true
        pub enabled: Option<bool>,
        /// Minimum interval between activations of the trigger. If the trigger fires again within
        /// the interval, the activation is postponed until the interval ends, batching multiple
        /// activations into one flow run.
        pub cooldown: Option<DurationString>,
        /// If the trigger is activated this many times during the `cooldown` interval, it fires
        /// without waiting for the interval to end.
        pub cooldown_max_batch: Option<u64>,
        /// Interval between activations e.g. `15m`.
        pub interval: DurationString,
    }

    impl FlowTriggerInputInterval {
        pub fn default_enabled() -> bool {
            true
        }
        pub fn enabled(&self) -> bool {
            self.enabled.unwrap_or(Self::default_enabled())
        }
    }

    impl PartialEq for FlowTriggerInputInterval {
        fn eq(&self, other: &Self) -> bool {
            self.enabled.or_else(|| Some(Self::default_enabled()))
                == other.enabled.or_else(|| Some(Self::default_enabled()))
                && self.cooldown == other.cooldown
                && self.cooldown_max_batch == other.cooldown_max_batch
                && self.interval == other.interval
        }
    }

    /// Triggers the flow via an API call or UI action.
    ///
    /// Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowTriggerInput#/$defs/Manual
    #[derive(Clone, Debug, Eq, Default)]
    pub struct FlowTriggerInputManual {
        /// Allows to pause an individual trigger.
        ///
        /// Defaults to: true
        pub enabled: Option<bool>,
        /// Minimum interval between activations of the trigger. If the trigger fires again within
        /// the interval, the activation is postponed until the interval ends, batching multiple
        /// activations into one flow run.
        pub cooldown: Option<DurationString>,
        /// If the trigger is activated this many times during the `cooldown` interval, it fires
        /// without waiting for the interval to end.
        pub cooldown_max_batch: Option<u64>,
    }

    impl FlowTriggerInputManual {
        pub fn default_enabled() -> bool {
            true
        }
        pub fn enabled(&self) -> bool {
            self.enabled.unwrap_or(Self::default_enabled())
        }
    }

    impl PartialEq for FlowTriggerInputManual {
        fn eq(&self, other: &Self) -> bool {
            self.enabled.or_else(|| Some(Self::default_enabled()))
                == other.enabled.or_else(|| Some(Self::default_enabled()))
                && self.cooldown == other.cooldown
                && self.cooldown_max_batch == other.cooldown_max_batch
        }
    }

    /// Triggers the flow on a cron schedule.
    ///
    /// Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowTriggerInput#/$defs/Schedule
    #[derive(Clone, Debug, Eq)]
    pub struct FlowTriggerInputSchedule {
        /// Allows to pause an individual trigger.
        ///
        /// Defaults to: true
        pub enabled: Option<bool>,
        /// Minimum interval between activations of the trigger. If the trigger fires again within
        /// the interval, the activation is postponed until the interval ends, batching multiple
        /// activations into one flow run.
        pub cooldown: Option<DurationString>,
        /// If the trigger is activated this many times during the `cooldown` interval, it fires
        /// without waiting for the interval to end.
        pub cooldown_max_batch: Option<u64>,
        /// Cron5 expression defining the schedule e.g. `@daily` or `*/30 * * * *`.
        pub cron: String,
    }

    impl FlowTriggerInputSchedule {
        pub fn default_enabled() -> bool {
            true
        }
        pub fn enabled(&self) -> bool {
            self.enabled.unwrap_or(Self::default_enabled())
        }
    }

    impl PartialEq for FlowTriggerInputSchedule {
        fn eq(&self, other: &Self) -> bool {
            self.enabled.or_else(|| Some(Self::default_enabled()))
                == other.enabled.or_else(|| Some(Self::default_enabled()))
                && self.cooldown == other.cooldown
                && self.cooldown_max_batch == other.cooldown_max_batch
                && self.cron == other.cron
        }
    }

    /// Triggers the flow when a source receives new data, with optional batching controls.
    ///
    /// Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowTriggerInput#/$defs/Source
    #[derive(Clone, Debug, Eq)]
    pub struct FlowTriggerInputSource {
        /// Allows to pause an individual trigger.
        ///
        /// Defaults to: true
        pub enabled: Option<bool>,
        /// Minimum interval between activations of the trigger. If the trigger fires again within
        /// the interval, the activation is postponed until the interval ends, batching multiple
        /// activations into one flow run.
        pub cooldown: Option<DurationString>,
        /// If the trigger is activated this many times during the `cooldown` interval, it fires
        /// without waiting for the interval to end.
        pub cooldown_max_batch: Option<u64>,
        /// Reference to the source resource that drives this trigger.
        pub source: resources::ResourceRef,
        /// Minimum number of new records to accumulate before triggering.
        pub min_records_to_await: Option<u64>,
        /// Maximum time to wait for `minRecordsToAwait` before triggering anyway e.g. `1h`.
        pub max_await_interval: Option<DurationString>,
    }

    impl FlowTriggerInputSource {
        pub fn default_enabled() -> bool {
            true
        }
        pub fn enabled(&self) -> bool {
            self.enabled.unwrap_or(Self::default_enabled())
        }
    }

    impl PartialEq for FlowTriggerInputSource {
        fn eq(&self, other: &Self) -> bool {
            self.enabled.or_else(|| Some(Self::default_enabled()))
                == other.enabled.or_else(|| Some(Self::default_enabled()))
                && self.cooldown == other.cooldown
                && self.cooldown_max_batch == other.cooldown_max_batch
                && self.source == other.source
                && self.min_records_to_await == other.min_records_to_await
                && self.max_await_interval == other.max_await_interval
        }
    }

    /// Triggers the flow when inputs of a derivative dataset receive new data, with optional
    /// batching controls.
    ///
    /// Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowTrigger#/$defs/InputsUpdated
    #[derive(Clone, Debug, Eq, Default)]
    pub struct FlowTriggerInputsUpdated {
        /// Allows to pause an individual trigger.
        ///
        /// Defaults to: true
        pub enabled: Option<bool>,
        /// Minimum interval between activations of the trigger. If the trigger fires again within
        /// the interval, the activation is postponed until the interval ends, batching multiple
        /// activations into one flow run.
        pub cooldown: Option<DurationString>,
        /// If the trigger is activated this many times during the `cooldown` interval, it fires
        /// without waiting for the interval to end.
        pub cooldown_max_batch: Option<u64>,
        /// Selector of derivative datasets whose inputs are watched. Defaults to the flow-level
        /// target when omitted.
        pub target: Option<datasets::DatasetSelector>,
        /// Minimum number of new input records to accumulate before triggering.
        pub min_records_to_await: Option<u64>,
        /// Maximum time to wait for `minRecordsToAwait` before triggering anyway e.g. `1h`.
        pub max_await_interval: Option<DurationString>,
    }

    impl FlowTriggerInputsUpdated {
        pub fn default_enabled() -> bool {
            true
        }
        pub fn enabled(&self) -> bool {
            self.enabled.unwrap_or(Self::default_enabled())
        }
    }

    impl PartialEq for FlowTriggerInputsUpdated {
        fn eq(&self, other: &Self) -> bool {
            self.enabled.or_else(|| Some(Self::default_enabled()))
                == other.enabled.or_else(|| Some(Self::default_enabled()))
                && self.cooldown == other.cooldown
                && self.cooldown_max_batch == other.cooldown_max_batch
                && self.target == other.target
                && self.min_records_to_await == other.min_records_to_await
                && self.max_await_interval == other.max_await_interval
        }
    }

    /// Triggers the flow at regular intervals.
    ///
    /// Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowTrigger#/$defs/Interval
    #[derive(Clone, Debug, Eq)]
    pub struct FlowTriggerInterval {
        /// Allows to pause an individual trigger.
        ///
        /// Defaults to: true
        pub enabled: Option<bool>,
        /// Minimum interval between activations of the trigger. If the trigger fires again within
        /// the interval, the activation is postponed until the interval ends, batching multiple
        /// activations into one flow run.
        pub cooldown: Option<DurationString>,
        /// If the trigger is activated this many times during the `cooldown` interval, it fires
        /// without waiting for the interval to end.
        pub cooldown_max_batch: Option<u64>,
        /// Interval between activations e.g. `15m`.
        pub interval: DurationString,
    }

    impl FlowTriggerInterval {
        pub fn default_enabled() -> bool {
            true
        }
        pub fn enabled(&self) -> bool {
            self.enabled.unwrap_or(Self::default_enabled())
        }
    }

    impl PartialEq for FlowTriggerInterval {
        fn eq(&self, other: &Self) -> bool {
            self.enabled.or_else(|| Some(Self::default_enabled()))
                == other.enabled.or_else(|| Some(Self::default_enabled()))
                && self.cooldown == other.cooldown
                && self.cooldown_max_batch == other.cooldown_max_batch
                && self.interval == other.interval
        }
    }

    /// Triggers the flow via an API call or UI action.
    ///
    /// Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowTrigger#/$defs/Manual
    #[derive(Clone, Debug, Eq, Default)]
    pub struct FlowTriggerManual {
        /// Allows to pause an individual trigger.
        ///
        /// Defaults to: true
        pub enabled: Option<bool>,
        /// Minimum interval between activations of the trigger. If the trigger fires again within
        /// the interval, the activation is postponed until the interval ends, batching multiple
        /// activations into one flow run.
        pub cooldown: Option<DurationString>,
        /// If the trigger is activated this many times during the `cooldown` interval, it fires
        /// without waiting for the interval to end.
        pub cooldown_max_batch: Option<u64>,
    }

    impl FlowTriggerManual {
        pub fn default_enabled() -> bool {
            true
        }
        pub fn enabled(&self) -> bool {
            self.enabled.unwrap_or(Self::default_enabled())
        }
    }

    impl PartialEq for FlowTriggerManual {
        fn eq(&self, other: &Self) -> bool {
            self.enabled.or_else(|| Some(Self::default_enabled()))
                == other.enabled.or_else(|| Some(Self::default_enabled()))
                && self.cooldown == other.cooldown
                && self.cooldown_max_batch == other.cooldown_max_batch
        }
    }

    /// Triggers the flow on a cron schedule.
    ///
    /// Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowTrigger#/$defs/Schedule
    #[derive(Clone, Debug, Eq)]
    pub struct FlowTriggerSchedule {
        /// Allows to pause an individual trigger.
        ///
        /// Defaults to: true
        pub enabled: Option<bool>,
        /// Minimum interval between activations of the trigger. If the trigger fires again within
        /// the interval, the activation is postponed until the interval ends, batching multiple
        /// activations into one flow run.
        pub cooldown: Option<DurationString>,
        /// If the trigger is activated this many times during the `cooldown` interval, it fires
        /// without waiting for the interval to end.
        pub cooldown_max_batch: Option<u64>,
        /// Cron5 expression defining the schedule e.g. `@daily` or `*/30 * * * *`.
        pub cron: String,
    }

    impl FlowTriggerSchedule {
        pub fn default_enabled() -> bool {
            true
        }
        pub fn enabled(&self) -> bool {
            self.enabled.unwrap_or(Self::default_enabled())
        }
    }

    impl PartialEq for FlowTriggerSchedule {
        fn eq(&self, other: &Self) -> bool {
            self.enabled.or_else(|| Some(Self::default_enabled()))
                == other.enabled.or_else(|| Some(Self::default_enabled()))
                && self.cooldown == other.cooldown
                && self.cooldown_max_batch == other.cooldown_max_batch
                && self.cron == other.cron
        }
    }

    /// Triggers the flow when a source receives new data, with optional batching controls.
    ///
    /// Schema: https://opendatafabric.org/schemas/flows/v1alpha1/FlowTrigger#/$defs/Source
    #[derive(Clone, Debug, Eq)]
    pub struct FlowTriggerSource {
        /// Allows to pause an individual trigger.
        ///
        /// Defaults to: true
        pub enabled: Option<bool>,
        /// Minimum interval between activations of the trigger. If the trigger fires again within
        /// the interval, the activation is postponed until the interval ends, batching multiple
        /// activations into one flow run.
        pub cooldown: Option<DurationString>,
        /// If the trigger is activated this many times during the `cooldown` interval, it fires
        /// without waiting for the interval to end.
        pub cooldown_max_batch: Option<u64>,
        /// Reference to the source resource that drives this trigger.
        pub source: resources::ResourceHandle,
        /// Minimum number of new records to accumulate before triggering.
        pub min_records_to_await: Option<u64>,
        /// Maximum time to wait for `minRecordsToAwait` before triggering anyway e.g. `1h`.
        pub max_await_interval: Option<DurationString>,
    }

    impl FlowTriggerSource {
        pub fn default_enabled() -> bool {
            true
        }
        pub fn enabled(&self) -> bool {
            self.enabled.unwrap_or(Self::default_enabled())
        }
    }

    impl PartialEq for FlowTriggerSource {
        fn eq(&self, other: &Self) -> bool {
            self.enabled.or_else(|| Some(Self::default_enabled()))
                == other.enabled.or_else(|| Some(Self::default_enabled()))
                && self.cooldown == other.cooldown
                && self.cooldown_max_batch == other.cooldown_max_batch
                && self.source == other.source
                && self.min_records_to_await == other.min_records_to_await
                && self.max_await_interval == other.max_await_interval
        }
    }

    /// Controls how many recent bindings are reported in `FlowStatus.recentBindings`.
    ///
    /// Schema: https://opendatafabric.org/schemas/flows/v1alpha1/RecentBindingsRetention
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct RecentBindingsRetention {
        /// Maximum number of recent bindings to report.
        pub max_bindings: Option<u32>,
    }

    /// Controls how many recent runs are reported in `FlowStatus.recentRuns`. Implementations may
    /// additionally cap these values.
    ///
    /// Schema: https://opendatafabric.org/schemas/flows/v1alpha1/RecentRunsRetention
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct RecentRunsRetention {
        /// Maximum number of recent runs to report.
        pub max_runs: Option<u32>,
        /// Maximum age of a finished run to be reported e.g. `30d`.
        pub max_age: Option<DurationString>,
    }

    /// Type of the backoff scaling.
    ///
    /// Schema: https://opendatafabric.org/schemas/flows/v1alpha1/RetryBackoff
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    pub enum RetryBackoff {
        Linear,
        Exponential,
    }

    /// Defines how a flow should react to failures.
    ///
    /// Schema: https://opendatafabric.org/schemas/flows/v1alpha1/RetryPolicy
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct RetryPolicy {
        /// Number of attempts before flow auto-scheduling will be disabled.
        pub max_attempts: Option<u32>,
        /// How long to wait until the first retry.
        pub min_delay: Option<DurationString>,
        /// Type of the backoff scaling. When omitted, retries use a constant `minDelay`.
        pub backoff: Option<flows::RetryBackoff>,
    }
}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// legacy
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod legacy {
    #[allow(unused_imports)]
    use super::*;

    /// Describes how to ingest data into a root dataset from a certain logical source.
    ///
    /// Schema: https://opendatafabric.org/schemas/legacy/v0/AddPushSource
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct AddPushSource {
        /// Identifies the source within this dataset.
        pub source_name: String,
        /// Defines how data is read into structured format.
        pub read: sources::ReadStep,
        /// Pre-processing query that shapes the data.
        pub preprocess: Option<datasets::Transform>,
        /// Determines how newly-ingested data should be merged with existing history.
        pub merge: sources::MergeStrategy,
    }

    /// Represents a projection of the dataset metadata at a single point in time.
    /// This type is typically used for defining new datasets and changing the existing ones.
    ///
    /// Schema: https://opendatafabric.org/schemas/legacy/v0/DatasetSnapshot
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct DatasetSnapshot {
        /// Alias of the dataset.
        pub name: crate::datasets::legacy::DatasetAlias,
        /// Type of the dataset.
        pub kind: datasets::DatasetKind,
        /// An array of metadata events that will be used to populate the chain. Here you can
        /// define polling and push sources, set licenses, add attachments etc.
        pub metadata: Vec<datasets::MetadataEvent>,
    }

    /// Disables the previously defined polling source.
    ///
    /// Schema: https://opendatafabric.org/schemas/legacy/v0/DisablePollingSource
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct DisablePollingSource {}

    /// Disables the previously defined source.
    ///
    /// Schema: https://opendatafabric.org/schemas/legacy/v0/DisablePushSource
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct DisablePushSource {
        /// Identifies the source to be disabled.
        pub source_name: String,
    }

    /// Defines the external source of data.
    ///
    /// Schema: https://opendatafabric.org/schemas/legacy/v0/FetchStep
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum FetchStep {
        Url(legacy::FetchStepUrl),
        FilesGlob(legacy::FetchStepFilesGlob),
        Container(legacy::FetchStepContainer),
        Mqtt(legacy::FetchStepMqtt),
        EthereumLogs(legacy::FetchStepEthereumLogs),
    }

    impl_enum_with_variants!(FetchStep);
    impl_enum_variant!(FetchStep::Url(legacy::FetchStepUrl));
    impl_enum_variant!(FetchStep::FilesGlob(legacy::FetchStepFilesGlob));
    impl_enum_variant!(FetchStep::Container(legacy::FetchStepContainer));
    impl_enum_variant!(FetchStep::Mqtt(legacy::FetchStepMqtt));
    impl_enum_variant!(FetchStep::EthereumLogs(legacy::FetchStepEthereumLogs));

    /// Runs the specified OCI container to fetch data from an arbitrary source.
    ///
    /// Schema: https://opendatafabric.org/schemas/legacy/v0/FetchStep#/$defs/Container
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct FetchStepContainer {
        /// Image name and and an optional tag.
        pub image: String,
        /// Specifies the entrypoint. Not executed within a shell. The default OCI image's
        /// ENTRYPOINT is used if this is not provided.
        pub command: Option<Vec<String>>,
        /// Arguments to the entrypoint. The OCI image's CMD is used if this is not provided.
        pub args: Option<Vec<String>>,
        /// Environment variables to propagate into or set in the container.
        pub env: Option<Vec<sources::EnvVar>>,
    }

    /// Connects to an Ethereum node to stream transaction logs.
    ///
    /// Schema: https://opendatafabric.org/schemas/legacy/v0/FetchStep#/$defs/EthereumLogs
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct FetchStepEthereumLogs {
        /// Identifier of the chain to scan logs from. This parameter may be used for RPC endpoint
        /// lookup as well as asserting that provided `nodeUrl` corresponds to the expected chain.
        pub chain_id: Option<u64>,
        /// Url of the node.
        pub node_url: Option<String>,
        /// An SQL WHERE clause that can be used to pre-filter the logs before fetching them from
        /// the ETH node.
        ///
        /// Examples:
        /// - "block_number > 123 and address = X'5fbdb2315678afecb367f032d93f642f64180aa3' and
        ///   topic1 = X'000000000000000000000000f39fd6e51aad88f6f4ce6ab8827279cfffb92266'"
        pub filter: Option<String>,
        /// Solidity log event signature to use for decoding. Using this field adds `event` to the
        /// output containing decoded log as JSON.
        pub signature: Option<String>,
    }

    /// Uses glob operator to match files on the local file system.
    ///
    /// Schema: https://opendatafabric.org/schemas/legacy/v0/FetchStep#/$defs/FilesGlob
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct FetchStepFilesGlob {
        /// Path with a glob pattern.
        pub path: String,
        /// Describes how event time is extracted from the source metadata.
        pub event_time: Option<sources::EventTimeSource>,
        /// Describes the caching settings used for this source.
        pub cache: Option<sources::SourceCaching>,
        /// Specifies how input files should be ordered before ingestion.
        /// Order is important as every file will be processed individually
        /// and will advance the dataset's watermark.
        pub order: Option<sources::SourceOrdering>,
    }

    /// Connects to an MQTT broker to fetch events from the specified topic.
    ///
    /// Schema: https://opendatafabric.org/schemas/legacy/v0/FetchStep#/$defs/Mqtt
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct FetchStepMqtt {
        /// Hostname of the MQTT broker.
        pub host: String,
        /// Port of the MQTT broker.
        pub port: i32,
        /// Username to use for auth with the broker.
        pub username: Option<String>,
        /// Password to use for auth with the broker (can be templated).
        pub password: Option<String>,
        /// List of topic subscription parameters.
        pub topics: Vec<sources::MqttTopicSubscription>,
    }

    /// Pulls data from one of the supported sources by its URL.
    ///
    /// Schema: https://opendatafabric.org/schemas/legacy/v0/FetchStep#/$defs/Url
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct FetchStepUrl {
        /// URL of the data source
        pub url: String,
        /// Describes how event time is extracted from the source metadata.
        pub event_time: Option<sources::EventTimeSource>,
        /// Describes the caching settings used for this source.
        pub cache: Option<sources::SourceCaching>,
        /// Headers to pass during the request (e.g. HTTP Authorization)
        pub headers: Option<Vec<sources::RequestHeader>>,
    }

    /// An object that wraps the metadata resources providing versioning and type identification.
    /// All root-level resources are wrapped with a manifest when serialized to disk.
    ///
    /// Schema: https://opendatafabric.org/schemas/legacy/v0/Manifest
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct Manifest<ContentT> {
        /// Type of the resource.
        pub kind: String,
        /// Major version number of the resource contained in this manifest. It provides the
        /// mechanism for introducing compatibility breaking changes.
        pub version: i32,
        /// Resource data.
        pub content: ContentT,
    }

    /// Contains information on how externally-hosted data can be ingested into the root dataset.
    ///
    /// Schema: https://opendatafabric.org/schemas/legacy/v0/SetPollingSource
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct SetPollingSource {
        /// Determines where data is sourced from.
        pub fetch: legacy::FetchStep,
        /// Defines how raw data is prepared before reading.
        pub prepare: Option<Vec<sources::PrepStep>>,
        /// Defines how data is read into structured format.
        pub read: sources::ReadStep,
        /// Pre-processing query that shapes the data.
        pub preprocess: Option<datasets::Transform>,
        /// Determines how newly-ingested data should be merged with existing history.
        pub merge: sources::MergeStrategy,
    }
}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// resources
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod resources {
    #[allow(unused_imports)]
    use super::*;

    /// Apply a specific label to the resource.
    ///
    /// Schema: https://opendatafabric.org/schemas/resources/v1alpha1/actions/AddLabel
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct AddLabel {
        /// Schema URI of the label being added.
        pub label_type: TypeUri,
        /// Value of the label being added.
        pub label_value: serde_json::Value,
    }

    /// Create a new resource instance of a type (object is a type URI) under an account.
    ///
    /// Schema: https://opendatafabric.org/schemas/resources/v1alpha1/actions/Create
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct Create {
        /// Account under which the resource is being created.
        pub target_owner: auth::ResourceOwnerHandle,
    }

    /// Delete the resource instance.
    ///
    /// Schema: https://opendatafabric.org/schemas/resources/v1alpha1/actions/Delete
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct Delete {}

    /// Modify the resource manifest.
    ///
    /// Schema: https://opendatafabric.org/schemas/resources/v1alpha1/actions/Edit
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct Edit {}

    /// Filters that work on resource labels and identity headers.
    ///
    /// Schema: https://opendatafabric.org/schemas/resources/v1alpha1/LabelFilter
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub struct LabelFilter {
        pub entries: std::collections::BTreeMap<String, serde_json::Value>,
    }

    /// List resource instances of a type (object is a type URI) under an account.
    ///
    /// Schema: https://opendatafabric.org/schemas/resources/v1alpha1/actions/List
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct List {
        /// Account under which resources are being listed.
        pub target_owner: auth::ResourceOwnerHandle,
    }

    /// Condition describing why the controller failed to reconcile the latest generation of a
    /// resource. Set by the controller; never written by users.
    ///
    /// Schema: https://opendatafabric.org/schemas/resources/v1alpha1/ReconciliationError
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct ReconciliationError {
        /// Machine-readable code identifying the type of the error.
        ///
        /// Examples:
        /// - "unresolved-reference"
        pub code: String,
        /// Human-readable description of the error.
        pub message: String,
    }

    /// Remove a specific label from the resource.
    ///
    /// Schema: https://opendatafabric.org/schemas/resources/v1alpha1/actions/RemoveLabel
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct RemoveLabel {
        /// Schema URI of the label being removed.
        pub label_type: TypeUri,
        /// Existing value of the label being removed.
        pub label_value: serde_json::Value,
    }

    /// Top-level container for canonical representation of a resource that specifies the type and
    /// version of the resource, carries identity, ownership, and status information.
    ///
    /// Schema: https://opendatafabric.org/schemas/resources/v1alpha1/Resource
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct Resource<SpecT> {
        /// Identifies the controlling entity, a bounded context that this resource belongs to, and the version. Url should follow the pattern `{base-url}/{context}/{version}/{name}` e.g. `https://opendatafabric.org/schemas/datasets/v1/Dataset`.
        pub schema: TypeUri,
        /// Container for identity and ownership information of a resource.
        pub headers: resources::ResourceHeaders,
        /// Specifies the desired state of a resource.
        pub spec: SpecT,
        /// Resource lifecycle and reconciliation information.
        pub status: resources::ResourceStatus,
    }

    /// Annotations is an unstructured key value map stored with a resource that may be set by
    /// external tools to store and retrieve arbitrary metadata. Unlike labels, annotations are not
    /// indexed and cannot be queried by.
    ///
    /// Schema: https://opendatafabric.org/schemas/resources/v1alpha1/ResourceAnnotations
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub struct ResourceAnnotations {
        pub entries: std::collections::BTreeMap<TypeRef, serde_json::Value>,
    }

    /// Container of generic contditions that can be added by contollers to provide additional information about the state of a resource. Keys uniquely identify the condition and should be in the form of a schema URL describing this condition, e.g. `https://opendatafabric.org/schemas/tasks/v1/TaskStatus`.
    ///
    /// Schema: https://opendatafabric.org/schemas/resources/v1alpha1/ResourceConditions
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub struct ResourceConditions {
        pub entries: std::collections::BTreeMap<TypeRef, serde_json::Value>,
    }

    /// Link to another resolved resource.
    ///
    /// Schema: https://opendatafabric.org/schemas/resources/v1alpha1/ResourceHandle
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct ResourceHandle {
        /// Account that owns the target resource.
        pub account: auth::ResourceOwnerHandle,
        /// Type URI of the target resource.
        pub r#type: TypeUri,
        /// ID of the resource within a node.
        pub id: ResourceID,
        /// DID of the resource, if applicable.
        pub did: Option<Did>,
        /// Name of a resource.
        pub name: ResourceName,
    }
    impl IntoResourceRef for ResourceHandle {}

    /// Container for identity and ownership information of a resource.
    ///
    /// Schema: https://opendatafabric.org/schemas/resources/v1alpha1/ResourceHeaders
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct ResourceHeaders {
        /// Unique identifier of a resource within entire ODF node. Automatically assigned upon
        /// resource creation.
        pub id: ResourceID,
        /// Symbolic name of a resource that identifies it within a scope of an onwing account.
        pub name: ResourceName,
        /// Link to the account that owns the resource.
        pub account: auth::ResourceOwnerHandle,
        /// Map of string keys and values that can be used to organize, categorize, and query
        /// resources.
        pub labels: resources::ResourceLabels,
        /// Annotations is a key value map stored with a resource that may be set by external tools
        /// to store and retrieve arbitrary metadata. Unlike labels, annotations are not indexed
        /// and cannot be queried by.
        pub annotations: resources::ResourceAnnotations,
        /// References to resources that created this resource. Used for lineage tracking and
        /// cascading cleanup.
        pub owner_references: Option<Vec<resources::ResourceHandle>>,
        /// A sequential number that changes every time the resource header and spec are updated.
        /// Does not increment on status changes, thus signifying changes to the desired state.
        /// Populated by the system. Starts with `1`.
        pub generation: u64,
        /// A sequential number that changes every time the resource status is updated. Can be
        /// passed back as a precondition of a status update to make sure the status was not
        /// modified since it was read. Populated by the system. Starts with `1`.
        pub status_generation: u64,
        /// Time when the resource was first applied and assigned an identity.
        pub created_at: DateTime<Utc>,
        /// Time when the resource was last updated, including header, spec, and status updates.
        pub updated_at: DateTime<Utc>,
        /// Time when deletion of the resource was requested. The resource remains visible until
        /// its controllers finish cleaning up and the deletion completes.
        pub deletion_requested_at: Option<DateTime<Utc>>,
        /// Time when the resource was deleted.
        pub deleted_at: Option<DateTime<Utc>>,
    }

    /// Container for identity and ownership information of a resource.
    ///
    /// Schema: https://opendatafabric.org/schemas/resources/v1alpha1/ResourceHeadersInput
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct ResourceHeadersInput {
        /// Unique identifier of a resource within entire ODF node. Automatically assigned upon
        /// resource creation.
        pub id: Option<ResourceID>,
        /// Symbolic name of a resource that identifies it within a scope of an onwing account.
        pub name: ResourceName,
        /// Reference to the account that owns the resource.
        pub account: Option<auth::ResourceOwnerRef>,
        /// Map of string keys and values that can be used to organize, categorize, and query
        /// resources.
        pub labels: Option<resources::ResourceLabels>,
        /// Annotations is a key value map stored with a resource that may be set by external tools
        /// to store and retrieve arbitrary metadata. Unlike labels, annotations are not indexed
        /// and cannot be queried by.
        pub annotations: Option<resources::ResourceAnnotations>,
        /// References to resources that created this resource. Used for lineage tracking and
        /// cascading cleanup.
        pub owner_references: Option<Vec<resources::ResourceRef>>,
    }

    /// Top-level container for user-authored representation of a resource that specifies the type
    /// and version of the resource and its desired state.
    ///
    /// Schema: https://opendatafabric.org/schemas/resources/v1alpha1/ResourceInput
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct ResourceInput<SpecT> {
        /// Identifies the controlling entity, a bounded context that this resource belongs to, and the version. Url should follow the pattern `{base-url}/{context}/{version}/{name}` e.g. `https://opendatafabric.org/schemas/datasets/v1/Dataset`.
        pub schema: TypeUri,
        /// Container for identity and ownership information of a resource.
        pub headers: resources::ResourceHeadersInput,
        /// Specifies the desired state of a resource.
        pub spec: SpecT,
    }

    /// Map of string keys and values that can be used to organize, categorize, and query resources.
    ///
    /// Schema: https://opendatafabric.org/schemas/resources/v1alpha1/ResourceLabels
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub struct ResourceLabels {
        pub entries: std::collections::BTreeMap<TypeRef, serde_json::Value>,
    }

    /// Represents the reconciliation or deletion phase of a resource.
    ///
    /// Schema: https://opendatafabric.org/schemas/resources/v1alpha1/ResourcePhase
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    pub enum ResourcePhase {
        Pending,
        Reconciling,
        Ready,
        Degraded,
        Failed,
        Deleting,
        Deleted,
    }

    /// Reference to another resource.
    ///
    /// Schema: https://opendatafabric.org/schemas/resources/v1alpha1/ResourceRef
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct ResourceRef {
        /// Reference to an account that owns the target resource.
        pub account: Option<auth::ResourceOwnerRef>,
        /// ID of the resource within a node.
        pub id: Option<ResourceID>,
        /// DID of the resource.
        pub did: Option<Did>,
        /// Short type name or full type URI of the target resource.
        ///
        /// Examples:
        /// - "SecretSet"
        /// - "https://opendatafabric.org/schemas/config/v1alpha1/SecretSet"
        pub r#type: Option<TypeRef>,
        /// Name of a resource.
        pub name: Option<ResourceName>,
    }
    impl IntoResourceRef for ResourceRef {}

    pub use crate::resources::ResourceSelector;

    /// Resource lifecycle and reconciliation information.
    ///
    /// Schema: https://opendatafabric.org/schemas/resources/v1alpha1/ResourceStatus
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct ResourceStatus {
        /// Represents the reconciliation phase of a resource as seen by the main resource
        /// controller.
        pub phase: resources::ResourcePhase,
        /// Resource generation that was last seen by the main resource controller.
        pub observed_generation: Option<u64>,
        /// Time when the controller seen the resource state as defined in `observedGeneration`.
        pub observed_at: Option<DateTime<Utc>>,
        /// Resource generation that was last successfully reconciled by the main resource
        /// controller.
        pub reconciled_generation: Option<u64>,
        /// Time when the controller last reconciled the desired resource state as defined in
        /// `reconciledGeneration`.
        pub reconciled_at: Option<DateTime<Utc>>,
        /// Detailed conditions describing the state of the resource that are added by controllers.
        pub conditions: resources::ResourceConditions,
    }

    /// Read the resource manifest.
    ///
    /// Schema: https://opendatafabric.org/schemas/resources/v1alpha1/actions/View
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct View {}
}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// sinks
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod sinks {
    #[allow(unused_imports)]
    use super::*;

    /// Defines a webhook endpoint that can receive event notifications and data.
    ///
    /// Schema: https://opendatafabric.org/schemas/sinks/v1alpha1/WebhookEndpoint
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct WebhookEndpoint {
        /// Container for identity and ownership information of a resource.
        pub headers: resources::ResourceHeadersInput,
        /// Specifies the desired state of the resource.
        pub spec: sinks::WebhookEndpointSpecInput,
    }

    impl WebhookEndpoint {
        pub fn schema() -> &'static TypeUri {
            &WEBHOOK_ENDPOINT_SCHEMA
        }
        pub const fn schema_str() -> &'static str {
            WEBHOOK_ENDPOINT_SCHEMA_STR
        }
    }

    static WEBHOOK_ENDPOINT_SCHEMA_STR: &str =
        "https://opendatafabric.org/schemas/sinks/v1alpha1/WebhookEndpoint";

    static WEBHOOK_ENDPOINT_SCHEMA: std::sync::LazyLock<TypeUri> =
        std::sync::LazyLock::new(|| TypeUri::new_unchecked(WEBHOOK_ENDPOINT_SCHEMA_STR));

    /// Defines a webhook endpoint that can receive event notifications and data.
    ///
    /// Schema: https://opendatafabric.org/schemas/sinks/v1alpha1/WebhookEndpointSpec
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct WebhookEndpointSpec {
        /// URL of the webhook endpoint.
        pub url: String,
        /// Shared secret used for HMAC signature of the request payload for authentication.
        pub secret: Option<config::Secret>,
    }

    /// Defines a webhook endpoint that can receive event notifications and data.
    ///
    /// Schema: https://opendatafabric.org/schemas/sinks/v1alpha1/WebhookEndpointSpecInput
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct WebhookEndpointSpecInput {
        /// URL of the webhook endpoint.
        pub url: String,
        /// Shared secret used for HMAC signature of the request payload for authentication.
        pub secret: Option<config::Secret>,
    }
}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// sources
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod sources {
    #[allow(unused_imports)]
    use super::*;

    /// Defines a compression algorithm.
    ///
    /// Schema: https://opendatafabric.org/schemas/sources/v1alpha1/CompressionFormat
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    pub enum CompressionFormat {
        Gzip,
        Zip,
    }

    /// Defines an environment variable passed into some job.
    ///
    /// Schema: https://opendatafabric.org/schemas/sources/v1alpha1/EnvVar
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct EnvVar {
        /// Name of the variable.
        pub name: String,
        /// Value of the variable.
        pub value: Option<String>,
    }

    /// Defines the external source of data.
    ///
    /// Schema: https://opendatafabric.org/schemas/sources/v1alpha1/EventTimeSource
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum EventTimeSource {
        FromMetadata(sources::EventTimeSourceFromMetadata),
        FromPath(sources::EventTimeSourceFromPath),
        FromSystemTime(sources::EventTimeSourceFromSystemTime),
    }

    impl_enum_with_variants!(EventTimeSource);
    impl_enum_variant!(EventTimeSource::FromMetadata(
        sources::EventTimeSourceFromMetadata
    ));
    impl_enum_variant!(EventTimeSource::FromPath(sources::EventTimeSourceFromPath));
    impl_enum_variant!(EventTimeSource::FromSystemTime(
        sources::EventTimeSourceFromSystemTime
    ));

    /// Extracts event time from the source's metadata.
    ///
    /// Schema: https://opendatafabric.org/schemas/sources/v1alpha1/EventTimeSource#/$defs/FromMetadata
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct EventTimeSourceFromMetadata {}

    /// Extracts event time from the path component of the source.
    ///
    /// Schema: https://opendatafabric.org/schemas/sources/v1alpha1/EventTimeSource#/$defs/FromPath
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct EventTimeSourceFromPath {
        /// Regular expression where first group contains the timestamp string.
        pub pattern: String,
        /// Format of the expected timestamp in java.text.SimpleDateFormat form.
        pub timestamp_format: Option<String>,
    }

    /// Assigns event time from the system time source.
    ///
    /// Schema: https://opendatafabric.org/schemas/sources/v1alpha1/EventTimeSource#/$defs/FromSystemTime
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct EventTimeSourceFromSystemTime {}

    /// Defines the point where data enters the system.
    ///
    /// Schema: https://opendatafabric.org/schemas/sources/v1alpha1/Ingress
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum Ingress {
        Url(sources::IngressUrl),
        FilesGlob(sources::IngressFilesGlob),
        Container(sources::IngressContainer),
        Mqtt(sources::IngressMqtt),
        EvmLogs(sources::IngressEvmLogs),
        RestEndpoint(sources::IngressRestEndpoint),
    }

    impl_enum_with_variants!(Ingress);
    impl_enum_variant!(Ingress::Url(sources::IngressUrl));
    impl_enum_variant!(Ingress::FilesGlob(sources::IngressFilesGlob));
    impl_enum_variant!(Ingress::Container(sources::IngressContainer));
    impl_enum_variant!(Ingress::Mqtt(sources::IngressMqtt));
    impl_enum_variant!(Ingress::EvmLogs(sources::IngressEvmLogs));
    impl_enum_variant!(Ingress::RestEndpoint(sources::IngressRestEndpoint));

    /// Buffer configuration for holding pushed records until they are ingested.
    ///
    /// Schema: https://opendatafabric.org/schemas/sources/v1alpha1/IngressBuffer
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum IngressBuffer {
        Memory(sources::IngressBufferMemory),
    }

    impl_enum_with_variants!(IngressBuffer);
    impl_enum_variant!(IngressBuffer::Memory(sources::IngressBufferMemory));

    /// An in-memory buffer.
    ///
    /// Schema: https://opendatafabric.org/schemas/sources/v1alpha1/IngressBuffer#/$defs/Memory
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct IngressBufferMemory {
        /// Maximum number of records to hold in the buffer.
        pub buffer_size: Option<u64>,
        /// Policy applied when the buffer is full.
        pub overflow_policy: Option<String>,
    }

    /// Runs the specified OCI container to fetch data from an arbitrary source.
    ///
    /// Schema: https://opendatafabric.org/schemas/sources/v1alpha1/Ingress#/$defs/Container
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct IngressContainer {
        /// Image name and and an optional tag.
        pub image: String,
        /// Specifies the entrypoint. Not executed within a shell. The default OCI image's
        /// ENTRYPOINT is used if this is not provided.
        pub command: Option<Vec<String>>,
        /// Arguments to the entrypoint. The OCI image's CMD is used if this is not provided.
        pub args: Option<Vec<String>>,
        /// Environment variables to propagate into or set in the container.
        pub env: Option<Vec<sources::EnvVar>>,
    }

    /// Connects to an EVM (Ethereum) node to stream transaction logs.
    ///
    /// Schema: https://opendatafabric.org/schemas/sources/v1alpha1/Ingress#/$defs/EvmLogs
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct IngressEvmLogs {
        /// Identifier of the chain to scan logs from. This parameter may be used for RPC endpoint
        /// lookup as well as asserting that provided `nodeUrl` corresponds to the expected chain.
        pub chain_id: Option<u64>,
        /// Url of the node.
        pub node_url: Option<String>,
        /// An SQL WHERE clause that can be used to pre-filter the logs before fetching them from
        /// the ETH node.
        ///
        /// Examples:
        /// - "block_number > 123 and address = X'5fbdb2315678afecb367f032d93f642f64180aa3' and
        ///   topic1 = X'000000000000000000000000f39fd6e51aad88f6f4ce6ab8827279cfffb92266'"
        pub filter: Option<String>,
        /// Solidity log event signature to use for decoding. Using this field adds `event` to the
        /// output containing decoded log as JSON.
        pub signature: Option<String>,
    }

    /// Uses glob operator to match files on the local file system.
    ///
    /// Schema: https://opendatafabric.org/schemas/sources/v1alpha1/Ingress#/$defs/FilesGlob
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct IngressFilesGlob {
        /// Path with a glob pattern.
        pub path: String,
        /// Describes how event time is extracted from the source metadata.
        pub event_time: Option<sources::EventTimeSource>,
        /// Describes the caching settings used for this source.
        pub cache: Option<sources::SourceCaching>,
        /// Specifies how input files should be ordered before ingestion.
        /// Order is important as every file will be processed individually
        /// and will advance the dataset's watermark.
        pub order: Option<sources::SourceOrdering>,
    }

    /// Connects to an MQTT broker to fetch events from the specified topic.
    ///
    /// Schema: https://opendatafabric.org/schemas/sources/v1alpha1/Ingress#/$defs/Mqtt
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct IngressMqtt {
        /// Hostname of the MQTT broker.
        pub host: String,
        /// Port of the MQTT broker.
        pub port: i32,
        /// Username to use for auth with the broker.
        pub username: Option<String>,
        /// Password to use for auth with the broker (can be templated).
        pub password: Option<String>,
        /// List of topic subscription parameters.
        pub topics: Vec<sources::MqttTopicSubscription>,
    }

    /// Exposes a REST HTTP endpoint that accepts pushed data records.
    ///
    /// Schema: https://opendatafabric.org/schemas/sources/v1alpha1/Ingress#/$defs/RestEndpoint
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct IngressRestEndpoint {
        /// Buffer configuration for holding records until they are ingested.
        pub buffer: Option<sources::IngressBuffer>,
    }

    /// Pulls data from one of the supported sources by its URL.
    ///
    /// Schema: https://opendatafabric.org/schemas/sources/v1alpha1/Ingress#/$defs/Url
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct IngressUrl {
        /// URL of the data source
        pub url: String,
        /// Describes how event time is extracted from the source metadata.
        pub event_time: Option<sources::EventTimeSource>,
        /// Describes the caching settings used for this source.
        pub cache: Option<sources::SourceCaching>,
        /// Headers to pass during the request (e.g. HTTP Authorization)
        pub headers: Option<Vec<sources::RequestHeader>>,
    }

    /// Merge strategy determines how newly ingested data should be combined with the data that
    /// already exists in the dataset.
    ///
    /// Schema: https://opendatafabric.org/schemas/sources/v1alpha1/MergeStrategy
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum MergeStrategy {
        Append(sources::MergeStrategyAppend),
        Ledger(sources::MergeStrategyLedger),
        Snapshot(sources::MergeStrategySnapshot),
        ChangelogStream(sources::MergeStrategyChangelogStream),
        UpsertStream(sources::MergeStrategyUpsertStream),
    }

    impl_enum_with_variants!(MergeStrategy);
    impl_enum_variant!(MergeStrategy::Append(sources::MergeStrategyAppend));
    impl_enum_variant!(MergeStrategy::Ledger(sources::MergeStrategyLedger));
    impl_enum_variant!(MergeStrategy::Snapshot(sources::MergeStrategySnapshot));
    impl_enum_variant!(MergeStrategy::ChangelogStream(
        sources::MergeStrategyChangelogStream
    ));
    impl_enum_variant!(MergeStrategy::UpsertStream(
        sources::MergeStrategyUpsertStream
    ));

    /// Append merge strategy.
    ///
    /// Under this strategy new data will be appended to the dataset in its entirety, without any
    /// deduplication.
    ///
    /// Schema: https://opendatafabric.org/schemas/sources/v1alpha1/MergeStrategy#/$defs/Append
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct MergeStrategyAppend {}

    /// Changelog stream merge strategy.
    ///
    /// This is the native stream format for ODF that accurately describes the evolution of all
    /// event records including appends, retractions, and corrections as per RFC-015. No
    /// pre-processing except for format validation is done.
    ///
    /// Schema: https://opendatafabric.org/schemas/sources/v1alpha1/MergeStrategy#/$defs/ChangelogStream
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct MergeStrategyChangelogStream {
        /// Names of the columns that uniquely identify the record throughout its lifetime
        pub primary_key: Vec<String>,
    }

    /// Ledger merge strategy.
    ///
    /// This strategy should be used for data sources containing ledgers of events. Currently this
    /// strategy will only perform deduplication of events using user-specified primary key columns.
    /// This means that the source data can contain partially overlapping set of records and only
    /// those records that were not previously seen will be appended.
    ///
    /// Schema: https://opendatafabric.org/schemas/sources/v1alpha1/MergeStrategy#/$defs/Ledger
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct MergeStrategyLedger {
        /// Names of the columns that uniquely identify the record throughout its lifetime
        pub primary_key: Vec<String>,
    }

    /// Snapshot merge strategy.
    ///
    /// This strategy can be used for data state snapshots that are taken periodically and contain
    /// only the latest state of the observed entity or system. Over time such snapshots can have
    /// new rows added, and old rows either removed or modified.
    ///
    /// This strategy transforms snapshot data into an append-only event stream where data already
    /// added is immutable. It does so by performing Change Data Capture - essentially diffing the
    /// current state of data against the reconstructed previous state and recording differences as
    /// retractions or corrections. The Operation Type "op" column will contain:
    ///   - append (`+A`) when a row appears for the first time
    ///   - retraction (`-R`) when row disappears
    ///   - correction (`-C`, `+C`) when row data has changed, with `-C` event carrying the old
    ///     value of the row and `+C` carrying the new value.
    ///
    /// To correctly associate rows between old and new snapshots this strategy relies on
    /// user-specified primary key columns.
    ///
    /// To identify whether a row has changed this strategy will compare all other columns one by
    /// one. If the data contains a column that is guaranteed to change whenever any of the data
    /// columns changes (for example a last modification timestamp, an incremental version, or a
    /// data hash), then it can be specified in `compareColumns` property to speed up the detection
    /// of modified rows.
    ///
    /// Schema: https://opendatafabric.org/schemas/sources/v1alpha1/MergeStrategy#/$defs/Snapshot
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct MergeStrategySnapshot {
        /// Names of the columns that uniquely identify the record throughout its lifetime.
        pub primary_key: Vec<String>,
        /// Names of the columns to compared to determine if a row has changed between two
        /// snapshots.
        pub compare_columns: Option<Vec<String>>,
    }

    /// Upsert stream merge strategy.
    ///
    /// This strategy should be used for data sources containing ledgers of insert-or-update and
    /// delete events. Unlike ChangelogStream the insert-or-update events only carry the new values,
    /// so this strategy will use primary key to re-classify the events into an append or a
    /// correction from/to pair, looking up the previous values.
    ///
    /// Schema: https://opendatafabric.org/schemas/sources/v1alpha1/MergeStrategy#/$defs/UpsertStream
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct MergeStrategyUpsertStream {
        /// Names of the columns that uniquely identify the record throughout its lifetime
        pub primary_key: Vec<String>,
    }

    /// MQTT quality of service class.
    ///
    /// Schema: https://opendatafabric.org/schemas/sources/v1alpha1/MqttQos
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    pub enum MqttQos {
        AtMostOnce,
        AtLeastOnce,
        ExactlyOnce,
    }

    /// MQTT topic subscription parameters.
    ///
    /// Schema: https://opendatafabric.org/schemas/sources/v1alpha1/MqttTopicSubscription
    #[derive(Clone, Debug, Eq)]
    pub struct MqttTopicSubscription {
        /// Name of the topic (may include patterns).
        pub path: String,
        /// Quality of service class.
        ///
        /// Defaults to: "AtMostOnce"
        pub qos: Option<sources::MqttQos>,
    }

    impl MqttTopicSubscription {
        pub fn default_qos() -> sources::MqttQos {
            sources::MqttQos::AtMostOnce
        }
        pub fn qos(&self) -> sources::MqttQos {
            self.qos.unwrap_or(Self::default_qos())
        }
    }

    impl PartialEq for MqttTopicSubscription {
        fn eq(&self, other: &Self) -> bool {
            self.path == other.path
                && self.qos.or_else(|| Some(Self::default_qos()))
                    == other.qos.or_else(|| Some(Self::default_qos()))
        }
    }

    /// Defines the steps to prepare raw data for ingestion.
    ///
    /// Schema: https://opendatafabric.org/schemas/sources/v1alpha1/PrepStep
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum PrepStep {
        Decompress(sources::PrepStepDecompress),
        Pipe(sources::PrepStepPipe),
    }

    impl_enum_with_variants!(PrepStep);
    impl_enum_variant!(PrepStep::Decompress(sources::PrepStepDecompress));
    impl_enum_variant!(PrepStep::Pipe(sources::PrepStepPipe));

    /// Pulls data from one of the supported sources by its URL.
    ///
    /// Schema: https://opendatafabric.org/schemas/sources/v1alpha1/PrepStep#/$defs/Decompress
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct PrepStepDecompress {
        /// Name of a compression algorithm used on data.
        pub format: sources::CompressionFormat,
        /// Path to a data file within a multi-file archive. Can contain glob patterns.
        pub sub_path: Option<String>,
    }

    /// Executes external command to process the data using piped input/output.
    ///
    /// Schema: https://opendatafabric.org/schemas/sources/v1alpha1/PrepStep#/$defs/Pipe
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct PrepStepPipe {
        /// Command to execute and its arguments.
        pub command: Vec<String>,
    }

    /// Defines how raw data should be read into the structured form.
    ///
    /// Schema: https://opendatafabric.org/schemas/sources/v1alpha1/ReadStep
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum ReadStep {
        Csv(sources::ReadStepCsv),
        GeoJson(sources::ReadStepGeoJson),
        EsriShapefile(sources::ReadStepEsriShapefile),
        Parquet(sources::ReadStepParquet),
        Json(sources::ReadStepJson),
        NdJson(sources::ReadStepNdJson),
        NdGeoJson(sources::ReadStepNdGeoJson),
    }

    impl_enum_with_variants!(ReadStep);
    impl_enum_variant!(ReadStep::Csv(sources::ReadStepCsv));
    impl_enum_variant!(ReadStep::GeoJson(sources::ReadStepGeoJson));
    impl_enum_variant!(ReadStep::EsriShapefile(sources::ReadStepEsriShapefile));
    impl_enum_variant!(ReadStep::Parquet(sources::ReadStepParquet));
    impl_enum_variant!(ReadStep::Json(sources::ReadStepJson));
    impl_enum_variant!(ReadStep::NdJson(sources::ReadStepNdJson));
    impl_enum_variant!(ReadStep::NdGeoJson(sources::ReadStepNdGeoJson));

    /// Reader for comma-separated files.
    ///
    /// Schema: https://opendatafabric.org/schemas/sources/v1alpha1/ReadStep#/$defs/Csv
    #[derive(Clone, Debug, Eq, Default)]
    pub struct ReadStepCsv {
        /// DEPRECATED: A DDL-formatted schema. Schema can be used to coerce values into more
        /// appropriate data types.
        ///
        /// Examples:
        /// - ["date TIMESTAMP","city STRING","population INT"]
        pub ddl_schema: Option<Vec<String>>,
        /// Sets a single character as a separator for each field and value.
        ///
        /// Defaults to: ","
        pub separator: Option<String>,
        /// Decodes the CSV files by the given encoding type.
        ///
        /// Defaults to: "utf8"
        pub encoding: Option<String>,
        /// Sets a single character used for escaping quoted values where the separator can be part
        /// of the value. Set an empty string to turn off quotations.
        ///
        /// Defaults to: "\""
        pub quote: Option<String>,
        /// Sets a single character used for escaping quotes inside an already quoted value.
        ///
        /// Defaults to: "\\"
        pub escape: Option<String>,
        /// Use the first line as names of columns.
        ///
        /// Defaults to: false
        pub header: Option<bool>,
        /// Infers the input schema automatically from data. It requires one extra pass over the
        /// data.
        ///
        /// Defaults to: false
        pub infer_schema: Option<bool>,
        /// Sets the string representation of a null value.
        ///
        /// Defaults to: ""
        pub null_value: Option<String>,
        /// Sets the string that indicates a date format. The `rfc3339` is the only required
        /// format, the other format strings are implementation-specific.
        ///
        /// Defaults to: "rfc3339"
        pub date_format: Option<String>,
        /// Sets the string that indicates a timestamp format. The `rfc3339` is the only required
        /// format, the other format strings are implementation-specific.
        ///
        /// Defaults to: "rfc3339"
        pub timestamp_format: Option<String>,
        /// Schema used to coerce values into more appropriate data types.
        pub schema: Option<data::DataSchema>,
    }

    impl ReadStepCsv {
        pub fn default_separator() -> &'static str {
            ","
        }
        pub fn separator(&self) -> &str {
            self.separator
                .as_deref()
                .unwrap_or(Self::default_separator())
        }
        pub fn default_encoding() -> &'static str {
            "utf8"
        }
        pub fn encoding(&self) -> &str {
            self.encoding.as_deref().unwrap_or(Self::default_encoding())
        }
        pub fn default_quote() -> &'static str {
            "\""
        }
        pub fn quote(&self) -> &str {
            self.quote.as_deref().unwrap_or(Self::default_quote())
        }
        pub fn default_escape() -> &'static str {
            "\\"
        }
        pub fn escape(&self) -> &str {
            self.escape.as_deref().unwrap_or(Self::default_escape())
        }
        pub fn default_header() -> bool {
            false
        }
        pub fn header(&self) -> bool {
            self.header.unwrap_or(Self::default_header())
        }
        pub fn default_infer_schema() -> bool {
            false
        }
        pub fn infer_schema(&self) -> bool {
            self.infer_schema.unwrap_or(Self::default_infer_schema())
        }
        pub fn default_null_value() -> &'static str {
            ""
        }
        pub fn null_value(&self) -> &str {
            self.null_value
                .as_deref()
                .unwrap_or(Self::default_null_value())
        }
        pub fn default_date_format() -> &'static str {
            "rfc3339"
        }
        pub fn date_format(&self) -> &str {
            self.date_format
                .as_deref()
                .unwrap_or(Self::default_date_format())
        }
        pub fn default_timestamp_format() -> &'static str {
            "rfc3339"
        }
        pub fn timestamp_format(&self) -> &str {
            self.timestamp_format
                .as_deref()
                .unwrap_or(Self::default_timestamp_format())
        }
    }

    impl PartialEq for ReadStepCsv {
        fn eq(&self, other: &Self) -> bool {
            self.ddl_schema == other.ddl_schema
                && self
                    .separator
                    .as_deref()
                    .or_else(|| Some(Self::default_separator()))
                    == other
                        .separator
                        .as_deref()
                        .or_else(|| Some(Self::default_separator()))
                && self
                    .encoding
                    .as_deref()
                    .or_else(|| Some(Self::default_encoding()))
                    == other
                        .encoding
                        .as_deref()
                        .or_else(|| Some(Self::default_encoding()))
                && self
                    .quote
                    .as_deref()
                    .or_else(|| Some(Self::default_quote()))
                    == other
                        .quote
                        .as_deref()
                        .or_else(|| Some(Self::default_quote()))
                && self
                    .escape
                    .as_deref()
                    .or_else(|| Some(Self::default_escape()))
                    == other
                        .escape
                        .as_deref()
                        .or_else(|| Some(Self::default_escape()))
                && self.header.or_else(|| Some(Self::default_header()))
                    == other.header.or_else(|| Some(Self::default_header()))
                && self
                    .infer_schema
                    .or_else(|| Some(Self::default_infer_schema()))
                    == other
                        .infer_schema
                        .or_else(|| Some(Self::default_infer_schema()))
                && self
                    .null_value
                    .as_deref()
                    .or_else(|| Some(Self::default_null_value()))
                    == other
                        .null_value
                        .as_deref()
                        .or_else(|| Some(Self::default_null_value()))
                && self
                    .date_format
                    .as_deref()
                    .or_else(|| Some(Self::default_date_format()))
                    == other
                        .date_format
                        .as_deref()
                        .or_else(|| Some(Self::default_date_format()))
                && self
                    .timestamp_format
                    .as_deref()
                    .or_else(|| Some(Self::default_timestamp_format()))
                    == other
                        .timestamp_format
                        .as_deref()
                        .or_else(|| Some(Self::default_timestamp_format()))
                && self.schema == other.schema
        }
    }

    /// Reader for ESRI Shapefile format.
    ///
    /// Schema: https://opendatafabric.org/schemas/sources/v1alpha1/ReadStep#/$defs/EsriShapefile
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct ReadStepEsriShapefile {
        /// DEPRECATED: A DDL-formatted schema. Schema can be used to coerce values into more
        /// appropriate data types.
        pub ddl_schema: Option<Vec<String>>,
        /// If the ZIP archive contains multiple shapefiles use this field to specify a sub-path to
        /// the desired `.shp` file. Can contain glob patterns to act as a filter.
        pub sub_path: Option<String>,
        /// Schema used to coerce values into more appropriate data types.
        pub schema: Option<data::DataSchema>,
    }

    /// Reader for GeoJSON files. It expects one `FeatureCollection` object in the root and will
    /// create a record per each `Feature` inside it extracting the properties into individual
    /// columns and leaving the feature geometry in its own column.
    ///
    /// Schema: https://opendatafabric.org/schemas/sources/v1alpha1/ReadStep#/$defs/GeoJson
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct ReadStepGeoJson {
        /// DEPRECATED: A DDL-formatted schema. Schema can be used to coerce values into more
        /// appropriate data types.
        pub ddl_schema: Option<Vec<String>>,
        /// Schema used to coerce values into more appropriate data types.
        pub schema: Option<data::DataSchema>,
    }

    /// Reader for JSON files that contain an array of objects within them.
    ///
    /// Schema: https://opendatafabric.org/schemas/sources/v1alpha1/ReadStep#/$defs/Json
    #[derive(Clone, Debug, Eq, Default)]
    pub struct ReadStepJson {
        /// Path in the form of `a.b.c` to a sub-element of the root JSON object that is an array
        /// or objects. If not specified it is assumed that the root element is an array.
        pub sub_path: Option<String>,
        /// DEPRECATED: A DDL-formatted schema. Schema can be used to coerce values into more
        /// appropriate data types.
        pub ddl_schema: Option<Vec<String>>,
        /// Sets the string that indicates a date format. The `rfc3339` is the only required
        /// format, the other format strings are implementation-specific.
        ///
        /// Defaults to: "rfc3339"
        pub date_format: Option<String>,
        /// Allows to forcibly set one of standard basic or extended encodings.
        ///
        /// Defaults to: "utf8"
        pub encoding: Option<String>,
        /// Sets the string that indicates a timestamp format. The `rfc3339` is the only required
        /// format, the other format strings are implementation-specific.
        ///
        /// Defaults to: "rfc3339"
        pub timestamp_format: Option<String>,
        /// Schema used to coerce values into more appropriate data types.
        pub schema: Option<data::DataSchema>,
    }

    impl ReadStepJson {
        pub fn default_date_format() -> &'static str {
            "rfc3339"
        }
        pub fn date_format(&self) -> &str {
            self.date_format
                .as_deref()
                .unwrap_or(Self::default_date_format())
        }
        pub fn default_encoding() -> &'static str {
            "utf8"
        }
        pub fn encoding(&self) -> &str {
            self.encoding.as_deref().unwrap_or(Self::default_encoding())
        }
        pub fn default_timestamp_format() -> &'static str {
            "rfc3339"
        }
        pub fn timestamp_format(&self) -> &str {
            self.timestamp_format
                .as_deref()
                .unwrap_or(Self::default_timestamp_format())
        }
    }

    impl PartialEq for ReadStepJson {
        fn eq(&self, other: &Self) -> bool {
            self.sub_path == other.sub_path
                && self.ddl_schema == other.ddl_schema
                && self
                    .date_format
                    .as_deref()
                    .or_else(|| Some(Self::default_date_format()))
                    == other
                        .date_format
                        .as_deref()
                        .or_else(|| Some(Self::default_date_format()))
                && self
                    .encoding
                    .as_deref()
                    .or_else(|| Some(Self::default_encoding()))
                    == other
                        .encoding
                        .as_deref()
                        .or_else(|| Some(Self::default_encoding()))
                && self
                    .timestamp_format
                    .as_deref()
                    .or_else(|| Some(Self::default_timestamp_format()))
                    == other
                        .timestamp_format
                        .as_deref()
                        .or_else(|| Some(Self::default_timestamp_format()))
                && self.schema == other.schema
        }
    }

    /// Reader for Newline-delimited GeoJSON files. It is similar to `GeoJson` format but instead of
    /// `FeatureCollection` object in the root it expects every individual feature object to appear
    /// on its own line.
    ///
    /// Schema: https://opendatafabric.org/schemas/sources/v1alpha1/ReadStep#/$defs/NdGeoJson
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct ReadStepNdGeoJson {
        /// DEPRECATED: A DDL-formatted schema. Schema can be used to coerce values into more
        /// appropriate data types.
        pub ddl_schema: Option<Vec<String>>,
        /// Schema used to coerce values into more appropriate data types.
        pub schema: Option<data::DataSchema>,
    }

    /// Reader for files containing multiple newline-delimited JSON objects with the same schema.
    ///
    /// Schema: https://opendatafabric.org/schemas/sources/v1alpha1/ReadStep#/$defs/NdJson
    #[derive(Clone, Debug, Eq, Default)]
    pub struct ReadStepNdJson {
        /// DEPRECATED: A DDL-formatted schema. Schema can be used to coerce values into more
        /// appropriate data types.
        pub ddl_schema: Option<Vec<String>>,
        /// Sets the string that indicates a date format. The `rfc3339` is the only required
        /// format, the other format strings are implementation-specific.
        ///
        /// Defaults to: "rfc3339"
        pub date_format: Option<String>,
        /// Allows to forcibly set one of standard basic or extended encodings.
        ///
        /// Defaults to: "utf8"
        pub encoding: Option<String>,
        /// Sets the string that indicates a timestamp format. The `rfc3339` is the only required
        /// format, the other format strings are implementation-specific.
        ///
        /// Defaults to: "rfc3339"
        pub timestamp_format: Option<String>,
        /// Schema used to coerce values into more appropriate data types.
        pub schema: Option<data::DataSchema>,
    }

    impl ReadStepNdJson {
        pub fn default_date_format() -> &'static str {
            "rfc3339"
        }
        pub fn date_format(&self) -> &str {
            self.date_format
                .as_deref()
                .unwrap_or(Self::default_date_format())
        }
        pub fn default_encoding() -> &'static str {
            "utf8"
        }
        pub fn encoding(&self) -> &str {
            self.encoding.as_deref().unwrap_or(Self::default_encoding())
        }
        pub fn default_timestamp_format() -> &'static str {
            "rfc3339"
        }
        pub fn timestamp_format(&self) -> &str {
            self.timestamp_format
                .as_deref()
                .unwrap_or(Self::default_timestamp_format())
        }
    }

    impl PartialEq for ReadStepNdJson {
        fn eq(&self, other: &Self) -> bool {
            self.ddl_schema == other.ddl_schema
                && self
                    .date_format
                    .as_deref()
                    .or_else(|| Some(Self::default_date_format()))
                    == other
                        .date_format
                        .as_deref()
                        .or_else(|| Some(Self::default_date_format()))
                && self
                    .encoding
                    .as_deref()
                    .or_else(|| Some(Self::default_encoding()))
                    == other
                        .encoding
                        .as_deref()
                        .or_else(|| Some(Self::default_encoding()))
                && self
                    .timestamp_format
                    .as_deref()
                    .or_else(|| Some(Self::default_timestamp_format()))
                    == other
                        .timestamp_format
                        .as_deref()
                        .or_else(|| Some(Self::default_timestamp_format()))
                && self.schema == other.schema
        }
    }

    /// Reader for Apache Parquet format.
    ///
    /// Schema: https://opendatafabric.org/schemas/sources/v1alpha1/ReadStep#/$defs/Parquet
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct ReadStepParquet {
        /// DEPRECATED: A DDL-formatted schema. Schema can be used to coerce values into more
        /// appropriate data types.
        pub ddl_schema: Option<Vec<String>>,
        /// Schema used to coerce values into more appropriate data types.
        pub schema: Option<data::DataSchema>,
    }

    /// Defines a header (e.g. HTTP) to be passed into some request.
    ///
    /// Schema: https://opendatafabric.org/schemas/sources/v1alpha1/RequestHeader
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct RequestHeader {
        /// Name of the header.
        pub name: String,
        /// Value of the header.
        pub value: String,
    }

    /// Defines an external source of data for ingestion.
    ///
    /// Schema: https://opendatafabric.org/schemas/sources/v1alpha1/Source
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct Source {
        /// Container for identity and ownership information of a resource.
        pub headers: resources::ResourceHeadersInput,
        /// Specifies the desired state of the resource.
        pub spec: sources::SourceSpecInput,
    }

    impl Source {
        pub fn schema() -> &'static TypeUri {
            &SOURCE_SCHEMA
        }
        pub const fn schema_str() -> &'static str {
            SOURCE_SCHEMA_STR
        }
    }

    static SOURCE_SCHEMA_STR: &str = "https://opendatafabric.org/schemas/sources/v1alpha1/Source";

    static SOURCE_SCHEMA: std::sync::LazyLock<TypeUri> =
        std::sync::LazyLock::new(|| TypeUri::new_unchecked(SOURCE_SCHEMA_STR));

    /// Defines how external data should be cached.
    ///
    /// Schema: https://opendatafabric.org/schemas/sources/v1alpha1/SourceCaching
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum SourceCaching {
        Forever(sources::SourceCachingForever),
    }

    impl_enum_with_variants!(SourceCaching);
    impl_enum_variant!(SourceCaching::Forever(sources::SourceCachingForever));

    /// After source was processed once it will never be ingested again.
    ///
    /// Schema: https://opendatafabric.org/schemas/sources/v1alpha1/SourceCaching#/$defs/Forever
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct SourceCachingForever {}

    /// Specifies how input files should be ordered before ingestion.
    ///
    /// Schema: https://opendatafabric.org/schemas/sources/v1alpha1/SourceOrdering
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    pub enum SourceOrdering {
        ByEventTime,
        ByName,
    }

    /// Specifies an external source of data for ingestion.
    ///
    /// Schema: https://opendatafabric.org/schemas/sources/v1alpha1/SourceSpec
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct SourceSpec {
        /// Brings the configuration values into the local `config` context.
        pub config: Option<config::ValueRefs>,
        /// Determines where data is sourced from.
        pub ingress: Option<sources::Ingress>,
        /// Defines how raw data is prepared before reading.
        pub prepare: Option<Vec<sources::PrepStep>>,
        /// Defines how data is read into structured format.
        pub read: sources::ReadStep,
        /// Pre-processing query that shapes the data.
        pub preprocess: Option<datasets::Transform>,
        /// Determines how newly-ingested data should be merged with existing history.
        pub merge: Option<sources::MergeStrategy>,
        /// Defines the mapping of system fields to dataset column names.
        pub vocab: Option<datasets::DatasetVocabulary>,
    }

    /// Specifies an external source of data for ingestion.
    ///
    /// Schema: https://opendatafabric.org/schemas/sources/v1alpha1/SourceSpecInput
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct SourceSpecInput {
        /// Brings the configuration values into the local `config` context.
        pub config: Option<config::ValueRefs>,
        /// Determines where data is sourced from.
        pub ingress: Option<sources::Ingress>,
        /// Defines how raw data is prepared before reading.
        pub prepare: Option<Vec<sources::PrepStep>>,
        /// Defines how data is read into structured format.
        pub read: sources::ReadStep,
        /// Pre-processing query that shapes the data.
        pub preprocess: Option<datasets::Transform>,
        /// Determines how newly-ingested data should be merged with existing history.
        pub merge: Option<sources::MergeStrategy>,
        /// Defines the mapping of system fields to dataset column names.
        pub vocab: Option<datasets::DatasetVocabulary>,
    }

    /// The state of the source the data was added from to allow fast resuming.
    ///
    /// Schema: https://opendatafabric.org/schemas/sources/v1alpha1/SourceState
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct SourceState {
        /// Identifies the source that the state corresponds to.
        pub source_name: String,
        /// Identifies the type of the state. Standard types include: `odf/etag`,
        /// `odf/last-modified`.
        pub kind: String,
        /// Opaque value representing the state.
        pub value: String,
    }
}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// storage
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod storage {
    #[allow(unused_imports)]
    use super::*;

    /// Access credentials for AWS or an AWS-compatible service.
    ///
    /// Schema: https://opendatafabric.org/schemas/storage/v1alpha1/AwsCredentials
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct AwsCredentials {
        /// Reference to a secret containing the AWS access key ID.
        pub access_key: Option<config::ValueHandle>,
        /// Reference to a secret containing the AWS secret access key.
        pub secret_key: Option<config::ValueHandle>,
    }

    /// Access credentials for AWS or an AWS-compatible service.
    ///
    /// Schema: https://opendatafabric.org/schemas/storage/v1alpha1/AwsCredentialsInput
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct AwsCredentialsInput {
        /// Reference to a secret containing the AWS access key ID.
        pub access_key: Option<config::ValueRef>,
        /// Reference to a secret containing the AWS secret access key.
        pub secret_key: Option<config::ValueRef>,
    }

    /// Defines a storage volume where data can be stored and its access credentials.
    ///
    /// Schema: https://opendatafabric.org/schemas/storage/v1alpha1/PersistentVolume
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct PersistentVolume {
        /// Container for identity and ownership information of a resource.
        pub headers: resources::ResourceHeadersInput,
        /// Specifies the desired state of the resource.
        pub spec: storage::PersistentVolumeSpecInput,
    }

    impl PersistentVolume {
        pub fn schema() -> &'static TypeUri {
            &PERSISTENT_VOLUME_SCHEMA
        }
        pub const fn schema_str() -> &'static str {
            PERSISTENT_VOLUME_SCHEMA_STR
        }
    }

    static PERSISTENT_VOLUME_SCHEMA_STR: &str =
        "https://opendatafabric.org/schemas/storage/v1alpha1/PersistentVolume";

    static PERSISTENT_VOLUME_SCHEMA: std::sync::LazyLock<TypeUri> =
        std::sync::LazyLock::new(|| TypeUri::new_unchecked(PERSISTENT_VOLUME_SCHEMA_STR));

    /// Reference to a `PersistentVolume`.
    ///
    /// Schema: https://opendatafabric.org/schemas/storage/v1alpha1/PersistentVolumeRef
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct PersistentVolumeRef {
        /// Reference to an account that owns the `PersistentVolume`.
        pub account: Option<auth::ResourceOwnerRef>,
        /// ID of the resource.
        pub id: Option<ResourceID>,
        /// Name of the resource.
        pub name: Option<ResourceName>,
    }
    impl IntoResourceRef for PersistentVolumeRef {}

    /// Defines a storage volume where data can be stored and its access credentials.
    ///
    /// Schema: https://opendatafabric.org/schemas/storage/v1alpha1/PersistentVolumeSpec
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum PersistentVolumeSpec {
        S3(storage::PersistentVolumeSpecS3),
    }

    impl_enum_with_variants!(PersistentVolumeSpec);
    impl_enum_variant!(PersistentVolumeSpec::S3(storage::PersistentVolumeSpecS3));

    /// Defines a storage volume where data can be stored and its access credentials.
    ///
    /// Schema: https://opendatafabric.org/schemas/storage/v1alpha1/PersistentVolumeSpecInput
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum PersistentVolumeSpecInput {
        S3(storage::PersistentVolumeSpecInputS3),
    }

    impl_enum_with_variants!(PersistentVolumeSpecInput);
    impl_enum_variant!(PersistentVolumeSpecInput::S3(
        storage::PersistentVolumeSpecInputS3
    ));

    /// An Amazon S3 or S3-compatible object storage bucket.
    ///
    /// Schema: https://opendatafabric.org/schemas/storage/v1alpha1/PersistentVolumeSpecInput#/$defs/S3
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct PersistentVolumeSpecInputS3 {
        /// S3 endpoint URL. If omitted, defaults to AWS S3. Use for S3-compatible stores e.g. `https://s3.amazonaws.com`.
        pub endpoint: Option<String>,
        /// AWS region where the bucket is located e.g. `us-west-2`.
        pub region: Option<String>,
        /// Name of the S3 bucket.
        pub bucket: String,
        /// Optional path prefix within the bucket.
        pub prefix: Option<String>,
        /// Storage capacity allocation.
        pub capacity: Option<storage::VolumeCapacity>,
        /// Access credentials for the bucket.
        pub credentials: Option<storage::AwsCredentialsInput>,
    }

    /// An Amazon S3 or S3-compatible object storage bucket.
    ///
    /// Schema: https://opendatafabric.org/schemas/storage/v1alpha1/PersistentVolumeSpec#/$defs/S3
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct PersistentVolumeSpecS3 {
        /// S3 endpoint URL. If omitted, defaults to AWS S3. Use for S3-compatible stores e.g. `https://s3.amazonaws.com`.
        pub endpoint: Option<String>,
        /// AWS region where the bucket is located e.g. `us-west-2`.
        pub region: Option<String>,
        /// Name of the S3 bucket.
        pub bucket: String,
        /// Optional path prefix within the bucket.
        pub prefix: Option<String>,
        /// Storage capacity allocation.
        pub capacity: Option<storage::VolumeCapacity>,
        /// Access credentials for the bucket.
        pub credentials: Option<storage::AwsCredentials>,
    }

    /// Storage capacity allocation.
    ///
    /// Schema: https://opendatafabric.org/schemas/storage/v1alpha1/VolumeCapacity
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct VolumeCapacity {
        /// Maximum storage size e.g. `10Gi`.
        pub storage: Option<ByteSize>,
    }
}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// tasks
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod tasks {
    #[allow(unused_imports)]
    use super::*;

    /// Determines how a flow run proceeds when a task finishes with a `Failed` outcome.
    ///
    /// Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/FailureBehavior
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    pub enum FailureBehavior {
        Fail,
        Continue,
    }

    /// Determines how a flow run proceeds when a task finishes with a `NoOp` outcome.
    ///
    /// Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/NoOpBehavior
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    pub enum NoOpBehavior {
        Continue,
        Break,
    }

    /// Credentials used to access a remote location during a sync.
    ///
    /// Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/SyncAuth
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum SyncAuth {
        Bearer(tasks::SyncAuthBearer),
        Aws(tasks::SyncAuthAws),
        Headers(tasks::SyncAuthHeaders),
    }

    impl_enum_with_variants!(SyncAuth);
    impl_enum_variant!(SyncAuth::Bearer(tasks::SyncAuthBearer));
    impl_enum_variant!(SyncAuth::Aws(tasks::SyncAuthAws));
    impl_enum_variant!(SyncAuth::Headers(tasks::SyncAuthHeaders));

    /// Authenticates with AWS or an AWS-compatible object storage.
    ///
    /// Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/SyncAuth#/$defs/Aws
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct SyncAuthAws {
        /// S3 endpoint URL. If omitted, defaults to AWS S3. Use for S3-compatible stores.
        pub endpoint: Option<String>,
        /// AWS region where the bucket is located e.g. `us-west-2`.
        pub region: Option<String>,
        /// Reference to a secret containing the AWS access key ID.
        pub access_key: Option<config::ValueHandle>,
        /// Reference to a secret containing the AWS secret access key.
        pub secret_key: Option<config::ValueHandle>,
    }

    /// Authenticates with a bearer token, e.g. an access token of a remote ODF node.
    ///
    /// Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/SyncAuth#/$defs/Bearer
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct SyncAuthBearer {
        /// Reference to a secret containing the token.
        pub token: config::ValueHandle,
    }

    /// A request header whose value is taken from a `VariableSet` or a `SecretSet`.
    ///
    /// Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/SyncAuthHeader
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct SyncAuthHeader {
        /// Name of the header.
        pub name: String,
        /// Reference to the value of the header.
        pub value: config::ValueHandle,
    }

    /// A request header whose value is taken from a `VariableSet` or a `SecretSet`.
    ///
    /// Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/SyncAuthHeaderInput
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct SyncAuthHeaderInput {
        /// Name of the header.
        pub name: String,
        /// Reference to the value of the header.
        pub value: config::ValueRef,
    }

    /// Authenticates by passing custom request headers.
    ///
    /// Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/SyncAuth#/$defs/Headers
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct SyncAuthHeaders {
        /// Headers to pass with every request.
        pub headers: Vec<tasks::SyncAuthHeader>,
    }

    /// Credentials used to access a remote location during a sync.
    ///
    /// Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/SyncAuthInput
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum SyncAuthInput {
        Bearer(tasks::SyncAuthInputBearer),
        Aws(tasks::SyncAuthInputAws),
        Headers(tasks::SyncAuthInputHeaders),
    }

    impl_enum_with_variants!(SyncAuthInput);
    impl_enum_variant!(SyncAuthInput::Bearer(tasks::SyncAuthInputBearer));
    impl_enum_variant!(SyncAuthInput::Aws(tasks::SyncAuthInputAws));
    impl_enum_variant!(SyncAuthInput::Headers(tasks::SyncAuthInputHeaders));

    /// Authenticates with AWS or an AWS-compatible object storage.
    ///
    /// Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/SyncAuthInput#/$defs/Aws
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct SyncAuthInputAws {
        /// S3 endpoint URL. If omitted, defaults to AWS S3. Use for S3-compatible stores.
        pub endpoint: Option<String>,
        /// AWS region where the bucket is located e.g. `us-west-2`.
        pub region: Option<String>,
        /// Reference to a secret containing the AWS access key ID.
        pub access_key: Option<config::ValueRef>,
        /// Reference to a secret containing the AWS secret access key.
        pub secret_key: Option<config::ValueRef>,
    }

    /// Authenticates with a bearer token, e.g. an access token of a remote ODF node.
    ///
    /// Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/SyncAuthInput#/$defs/Bearer
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct SyncAuthInputBearer {
        /// Reference to a secret containing the token.
        pub token: config::ValueRef,
    }

    /// Authenticates by passing custom request headers.
    ///
    /// Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/SyncAuthInput#/$defs/Headers
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct SyncAuthInputHeaders {
        /// Headers to pass with every request.
        pub headers: Vec<tasks::SyncAuthHeaderInput>,
    }

    /// A remote location a dataset is synced with.
    ///
    /// Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/SyncEndpoint
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct SyncEndpoint {
        /// URL of the remote dataset. The scheme selects the transfer protocol e.g. `odf+https://`, `s3://`, `file://`.
        pub url: String,
        /// Credentials used to access the remote location.
        pub auth: Option<tasks::SyncAuth>,
    }

    /// A remote location a dataset is synced with.
    ///
    /// Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/SyncEndpointInput
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct SyncEndpointInput {
        /// URL of the remote dataset. The scheme selects the transfer protocol e.g. `odf+https://`, `s3://`, `file://`.
        pub url: String,
        /// Credentials used to access the remote location.
        pub auth: Option<tasks::SyncAuthInput>,
    }

    /// An individual work item to be executed.
    ///
    /// Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/Task
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct Task {
        /// Container for identity and ownership information of a resource.
        pub headers: resources::ResourceHeadersInput,
        /// Specifies the desired state of the task.
        pub spec: Option<tasks::TaskSpecInput>,
    }

    impl Task {
        pub fn schema() -> &'static TypeUri {
            &TASK_SCHEMA
        }
        pub const fn schema_str() -> &'static str {
            TASK_SCHEMA_STR
        }
    }

    static TASK_SCHEMA_STR: &str = "https://opendatafabric.org/schemas/tasks/v1alpha1/Task";

    static TASK_SCHEMA: std::sync::LazyLock<TypeUri> =
        std::sync::LazyLock::new(|| TypeUri::new_unchecked(TASK_SCHEMA_STR));

    /// Type-specific details of a task failure. The `kind` property identifies the error, e.g.
    /// `InputDatasetCompacted`, and the remaining properties depend on it.
    ///
    /// Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/TaskError
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub struct TaskError {
        pub entries: std::collections::BTreeMap<String, serde_json::Value>,
    }

    /// Result of the execution of a task.
    ///
    /// Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/TaskOutcome
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum TaskOutcome {
        Success(tasks::TaskOutcomeSuccess),
        Failed(tasks::TaskOutcomeFailed),
        NoOp(tasks::TaskOutcomeNoOp),
        Cancelled(tasks::TaskOutcomeCancelled),
    }

    impl_enum_with_variants!(TaskOutcome);
    impl_enum_variant!(TaskOutcome::Success(tasks::TaskOutcomeSuccess));
    impl_enum_variant!(TaskOutcome::Failed(tasks::TaskOutcomeFailed));
    impl_enum_variant!(TaskOutcome::NoOp(tasks::TaskOutcomeNoOp));
    impl_enum_variant!(TaskOutcome::Cancelled(tasks::TaskOutcomeCancelled));

    /// Task was cancelled before completion.
    ///
    /// Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/TaskOutcome#/$defs/Cancelled
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct TaskOutcomeCancelled {}

    /// Task failed.
    ///
    /// Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/TaskOutcome#/$defs/Failed
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct TaskOutcomeFailed {
        /// Human-readable description of the failure.
        pub message: String,
        /// Whether retrying the task could succeed, e.g. after a network error. An unrecoverable
        /// failure, such as an invalid query, would fail the same way again.
        pub recoverable: bool,
        /// Type-specific details of the failure.
        pub error: Option<tasks::TaskError>,
    }

    /// Task completed with no work done (e.g. no new data to process).
    ///
    /// Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/TaskOutcome#/$defs/NoOp
    #[derive(Clone, Debug, Eq, PartialEq, Default)]
    pub struct TaskOutcomeNoOp {}

    /// Task completed successfully.
    ///
    /// Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/TaskOutcome#/$defs/Success
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub struct TaskOutcomeSuccess {
        pub entries: std::collections::BTreeMap<String, serde_json::Value>,
    }

    /// A self-contained logical execution plan of a task.
    ///
    /// Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/TaskPlan
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub struct TaskPlan {
        pub entries: std::collections::BTreeMap<String, serde_json::Value>,
    }

    /// An individual work item to be executed as part of a flow.
    ///
    /// Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/TaskSpec
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum TaskSpec {
        Ingest(tasks::TaskSpecIngest),
        Transform(tasks::TaskSpecTransform),
        SyncFrom(tasks::TaskSpecSyncFrom),
        SyncTo(tasks::TaskSpecSyncTo),
        Compaction(tasks::TaskSpecCompaction),
        Reset(tasks::TaskSpecReset),
        GarbageCollection(tasks::TaskSpecGarbageCollection),
        Verify(tasks::TaskSpecVerify),
        WebhookCall(tasks::TaskSpecWebhookCall),
    }

    impl_enum_with_variants!(TaskSpec);
    impl_enum_variant!(TaskSpec::Ingest(tasks::TaskSpecIngest));
    impl_enum_variant!(TaskSpec::Transform(tasks::TaskSpecTransform));
    impl_enum_variant!(TaskSpec::SyncFrom(tasks::TaskSpecSyncFrom));
    impl_enum_variant!(TaskSpec::SyncTo(tasks::TaskSpecSyncTo));
    impl_enum_variant!(TaskSpec::Compaction(tasks::TaskSpecCompaction));
    impl_enum_variant!(TaskSpec::Reset(tasks::TaskSpecReset));
    impl_enum_variant!(TaskSpec::GarbageCollection(
        tasks::TaskSpecGarbageCollection
    ));
    impl_enum_variant!(TaskSpec::Verify(tasks::TaskSpecVerify));
    impl_enum_variant!(TaskSpec::WebhookCall(tasks::TaskSpecWebhookCall));

    /// Compacts data files in matching datasets to improve query performance.
    ///
    /// Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/TaskSpec#/$defs/Compaction
    #[derive(Clone, Debug, Eq, Default)]
    pub struct TaskSpecCompaction {
        /// An alias for the task used to refer to it in flows and access the results
        pub name: Option<String>,
        /// Reference to the dataset to compact. Defaults to the flow-level target when omitted.
        pub target: Option<datasets::DatasetHandle>,
        /// Service account whose permissions the task executes with. Must be an account of service
        /// account type. Defaults to the service account of the run when omitted.
        pub service_account: Option<auth::PrincipalHandle>,
        /// How the flow run proceeds when this task finishes with a `NoOp` outcome: `Continue`
        /// executes the next task, while `Break` finishes the flow run early without an error.
        ///
        /// Defaults to: "Continue"
        pub on_no_op: Option<tasks::NoOpBehavior>,
        /// How the flow run proceeds when this task finishes with a `Failed` outcome: `Fail`
        /// finishes the flow run with an error, while `Continue` executes the next task.
        ///
        /// Defaults to: "Fail"
        pub on_failure: Option<tasks::FailureBehavior>,
        /// Target maximum size of each compacted data slice e.g. `100MiB`.
        pub max_slice_size: Option<ByteSize>,
        /// Target maximum number of records per compacted data slice.
        pub max_slice_records: Option<u64>,
    }

    impl TaskSpecCompaction {
        pub fn default_on_no_op() -> tasks::NoOpBehavior {
            tasks::NoOpBehavior::Continue
        }
        pub fn on_no_op(&self) -> tasks::NoOpBehavior {
            self.on_no_op.unwrap_or(Self::default_on_no_op())
        }
        pub fn default_on_failure() -> tasks::FailureBehavior {
            tasks::FailureBehavior::Fail
        }
        pub fn on_failure(&self) -> tasks::FailureBehavior {
            self.on_failure.unwrap_or(Self::default_on_failure())
        }
    }

    impl PartialEq for TaskSpecCompaction {
        fn eq(&self, other: &Self) -> bool {
            self.name == other.name
                && self.target == other.target
                && self.service_account == other.service_account
                && self.on_no_op.or_else(|| Some(Self::default_on_no_op()))
                    == other.on_no_op.or_else(|| Some(Self::default_on_no_op()))
                && self.on_failure.or_else(|| Some(Self::default_on_failure()))
                    == other
                        .on_failure
                        .or_else(|| Some(Self::default_on_failure()))
                && self.max_slice_size == other.max_slice_size
                && self.max_slice_records == other.max_slice_records
        }
    }

    /// Removes unreferenced data files from matching datasets.
    ///
    /// Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/TaskSpec#/$defs/GarbageCollection
    #[derive(Clone, Debug, Eq, Default)]
    pub struct TaskSpecGarbageCollection {
        /// An alias for the task used to refer to it in flows and access the results
        pub name: Option<String>,
        /// Reference to the dataset to collect garbage from. Defaults to the flow-level target
        /// when omitted.
        pub target: Option<datasets::DatasetHandle>,
        /// Service account whose permissions the task executes with. Must be an account of service
        /// account type. Defaults to the service account of the run when omitted.
        pub service_account: Option<auth::PrincipalHandle>,
        /// How the flow run proceeds when this task finishes with a `NoOp` outcome: `Continue`
        /// executes the next task, while `Break` finishes the flow run early without an error.
        ///
        /// Defaults to: "Continue"
        pub on_no_op: Option<tasks::NoOpBehavior>,
        /// How the flow run proceeds when this task finishes with a `Failed` outcome: `Fail`
        /// finishes the flow run with an error, while `Continue` executes the next task.
        ///
        /// Defaults to: "Fail"
        pub on_failure: Option<tasks::FailureBehavior>,
    }

    impl TaskSpecGarbageCollection {
        pub fn default_on_no_op() -> tasks::NoOpBehavior {
            tasks::NoOpBehavior::Continue
        }
        pub fn on_no_op(&self) -> tasks::NoOpBehavior {
            self.on_no_op.unwrap_or(Self::default_on_no_op())
        }
        pub fn default_on_failure() -> tasks::FailureBehavior {
            tasks::FailureBehavior::Fail
        }
        pub fn on_failure(&self) -> tasks::FailureBehavior {
            self.on_failure.unwrap_or(Self::default_on_failure())
        }
    }

    impl PartialEq for TaskSpecGarbageCollection {
        fn eq(&self, other: &Self) -> bool {
            self.name == other.name
                && self.target == other.target
                && self.service_account == other.service_account
                && self.on_no_op.or_else(|| Some(Self::default_on_no_op()))
                    == other.on_no_op.or_else(|| Some(Self::default_on_no_op()))
                && self.on_failure.or_else(|| Some(Self::default_on_failure()))
                    == other
                        .on_failure
                        .or_else(|| Some(Self::default_on_failure()))
        }
    }

    /// Fetches data from a source and appends it to a dataset.
    ///
    /// Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/TaskSpec#/$defs/Ingest
    #[derive(Clone, Debug, Eq)]
    pub struct TaskSpecIngest {
        /// An alias for the task used to refer to it in flows and access the results
        pub name: Option<String>,
        /// Reference to the dataset to ingest into. Defaults to the flow-level target when
        /// omitted.
        pub target: Option<datasets::DatasetHandle>,
        /// Service account whose permissions the task executes with. Must be an account of service
        /// account type. Defaults to the service account of the run when omitted.
        pub service_account: Option<auth::PrincipalHandle>,
        /// How the flow run proceeds when this task finishes with a `NoOp` outcome: `Continue`
        /// executes the next task, while `Break` finishes the flow run early without an error.
        ///
        /// Defaults to: "Continue"
        pub on_no_op: Option<tasks::NoOpBehavior>,
        /// How the flow run proceeds when this task finishes with a `Failed` outcome: `Fail`
        /// finishes the flow run with an error, while `Continue` executes the next task.
        ///
        /// Defaults to: "Fail"
        pub on_failure: Option<tasks::FailureBehavior>,
        /// Reference to the source resource that defines how to fetch data.
        pub source: resources::ResourceHandle,
        /// Target number of records to ingest per data slice.
        pub target_records_per_slice: Option<u64>,
    }

    impl TaskSpecIngest {
        pub fn default_on_no_op() -> tasks::NoOpBehavior {
            tasks::NoOpBehavior::Continue
        }
        pub fn on_no_op(&self) -> tasks::NoOpBehavior {
            self.on_no_op.unwrap_or(Self::default_on_no_op())
        }
        pub fn default_on_failure() -> tasks::FailureBehavior {
            tasks::FailureBehavior::Fail
        }
        pub fn on_failure(&self) -> tasks::FailureBehavior {
            self.on_failure.unwrap_or(Self::default_on_failure())
        }
    }

    impl PartialEq for TaskSpecIngest {
        fn eq(&self, other: &Self) -> bool {
            self.name == other.name
                && self.target == other.target
                && self.service_account == other.service_account
                && self.on_no_op.or_else(|| Some(Self::default_on_no_op()))
                    == other.on_no_op.or_else(|| Some(Self::default_on_no_op()))
                && self.on_failure.or_else(|| Some(Self::default_on_failure()))
                    == other
                        .on_failure
                        .or_else(|| Some(Self::default_on_failure()))
                && self.source == other.source
                && self.target_records_per_slice == other.target_records_per_slice
        }
    }

    /// An individual work item to be executed as part of a flow.
    ///
    /// Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/TaskSpecInput
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum TaskSpecInput {
        Ingest(tasks::TaskSpecInputIngest),
        Transform(tasks::TaskSpecInputTransform),
        SyncFrom(tasks::TaskSpecInputSyncFrom),
        SyncTo(tasks::TaskSpecInputSyncTo),
        Compaction(tasks::TaskSpecInputCompaction),
        Reset(tasks::TaskSpecInputReset),
        GarbageCollection(tasks::TaskSpecInputGarbageCollection),
        Verify(tasks::TaskSpecInputVerify),
        WebhookCall(tasks::TaskSpecInputWebhookCall),
    }

    impl_enum_with_variants!(TaskSpecInput);
    impl_enum_variant!(TaskSpecInput::Ingest(tasks::TaskSpecInputIngest));
    impl_enum_variant!(TaskSpecInput::Transform(tasks::TaskSpecInputTransform));
    impl_enum_variant!(TaskSpecInput::SyncFrom(tasks::TaskSpecInputSyncFrom));
    impl_enum_variant!(TaskSpecInput::SyncTo(tasks::TaskSpecInputSyncTo));
    impl_enum_variant!(TaskSpecInput::Compaction(tasks::TaskSpecInputCompaction));
    impl_enum_variant!(TaskSpecInput::Reset(tasks::TaskSpecInputReset));
    impl_enum_variant!(TaskSpecInput::GarbageCollection(
        tasks::TaskSpecInputGarbageCollection
    ));
    impl_enum_variant!(TaskSpecInput::Verify(tasks::TaskSpecInputVerify));
    impl_enum_variant!(TaskSpecInput::WebhookCall(tasks::TaskSpecInputWebhookCall));

    /// Compacts data files in matching datasets to improve query performance.
    ///
    /// Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/TaskSpecInput#/$defs/Compaction
    #[derive(Clone, Debug, Eq, Default)]
    pub struct TaskSpecInputCompaction {
        /// An alias for the task used to refer to it in flows and access the results
        pub name: Option<String>,
        /// Reference to the dataset to compact. Defaults to the flow-level target when omitted.
        pub target: Option<datasets::DatasetRef>,
        /// Service account whose permissions the task executes with. Must be an account of service
        /// account type. Defaults to the service account of the run when omitted.
        pub service_account: Option<auth::PrincipalRef>,
        /// How the flow run proceeds when this task finishes with a `NoOp` outcome: `Continue`
        /// executes the next task, while `Break` finishes the flow run early without an error.
        ///
        /// Defaults to: "Continue"
        pub on_no_op: Option<tasks::NoOpBehavior>,
        /// How the flow run proceeds when this task finishes with a `Failed` outcome: `Fail`
        /// finishes the flow run with an error, while `Continue` executes the next task.
        ///
        /// Defaults to: "Fail"
        pub on_failure: Option<tasks::FailureBehavior>,
        /// Target maximum size of each compacted data slice e.g. `100MiB`.
        pub max_slice_size: Option<ByteSize>,
        /// Target maximum number of records per compacted data slice.
        pub max_slice_records: Option<u64>,
    }

    impl TaskSpecInputCompaction {
        pub fn default_on_no_op() -> tasks::NoOpBehavior {
            tasks::NoOpBehavior::Continue
        }
        pub fn on_no_op(&self) -> tasks::NoOpBehavior {
            self.on_no_op.unwrap_or(Self::default_on_no_op())
        }
        pub fn default_on_failure() -> tasks::FailureBehavior {
            tasks::FailureBehavior::Fail
        }
        pub fn on_failure(&self) -> tasks::FailureBehavior {
            self.on_failure.unwrap_or(Self::default_on_failure())
        }
    }

    impl PartialEq for TaskSpecInputCompaction {
        fn eq(&self, other: &Self) -> bool {
            self.name == other.name
                && self.target == other.target
                && self.service_account == other.service_account
                && self.on_no_op.or_else(|| Some(Self::default_on_no_op()))
                    == other.on_no_op.or_else(|| Some(Self::default_on_no_op()))
                && self.on_failure.or_else(|| Some(Self::default_on_failure()))
                    == other
                        .on_failure
                        .or_else(|| Some(Self::default_on_failure()))
                && self.max_slice_size == other.max_slice_size
                && self.max_slice_records == other.max_slice_records
        }
    }

    /// Removes unreferenced data files from matching datasets.
    ///
    /// Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/TaskSpecInput#/$defs/GarbageCollection
    #[derive(Clone, Debug, Eq, Default)]
    pub struct TaskSpecInputGarbageCollection {
        /// An alias for the task used to refer to it in flows and access the results
        pub name: Option<String>,
        /// Reference to the dataset to collect garbage from. Defaults to the flow-level target
        /// when omitted.
        pub target: Option<datasets::DatasetRef>,
        /// Service account whose permissions the task executes with. Must be an account of service
        /// account type. Defaults to the service account of the run when omitted.
        pub service_account: Option<auth::PrincipalRef>,
        /// How the flow run proceeds when this task finishes with a `NoOp` outcome: `Continue`
        /// executes the next task, while `Break` finishes the flow run early without an error.
        ///
        /// Defaults to: "Continue"
        pub on_no_op: Option<tasks::NoOpBehavior>,
        /// How the flow run proceeds when this task finishes with a `Failed` outcome: `Fail`
        /// finishes the flow run with an error, while `Continue` executes the next task.
        ///
        /// Defaults to: "Fail"
        pub on_failure: Option<tasks::FailureBehavior>,
    }

    impl TaskSpecInputGarbageCollection {
        pub fn default_on_no_op() -> tasks::NoOpBehavior {
            tasks::NoOpBehavior::Continue
        }
        pub fn on_no_op(&self) -> tasks::NoOpBehavior {
            self.on_no_op.unwrap_or(Self::default_on_no_op())
        }
        pub fn default_on_failure() -> tasks::FailureBehavior {
            tasks::FailureBehavior::Fail
        }
        pub fn on_failure(&self) -> tasks::FailureBehavior {
            self.on_failure.unwrap_or(Self::default_on_failure())
        }
    }

    impl PartialEq for TaskSpecInputGarbageCollection {
        fn eq(&self, other: &Self) -> bool {
            self.name == other.name
                && self.target == other.target
                && self.service_account == other.service_account
                && self.on_no_op.or_else(|| Some(Self::default_on_no_op()))
                    == other.on_no_op.or_else(|| Some(Self::default_on_no_op()))
                && self.on_failure.or_else(|| Some(Self::default_on_failure()))
                    == other
                        .on_failure
                        .or_else(|| Some(Self::default_on_failure()))
        }
    }

    /// Fetches data from a source and appends it to a dataset.
    ///
    /// Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/TaskSpecInput#/$defs/Ingest
    #[derive(Clone, Debug, Eq)]
    pub struct TaskSpecInputIngest {
        /// An alias for the task used to refer to it in flows and access the results
        pub name: Option<String>,
        /// Reference to the dataset to ingest into. Defaults to the flow-level target when
        /// omitted.
        pub target: Option<datasets::DatasetRef>,
        /// Service account whose permissions the task executes with. Must be an account of service
        /// account type. Defaults to the service account of the run when omitted.
        pub service_account: Option<auth::PrincipalRef>,
        /// How the flow run proceeds when this task finishes with a `NoOp` outcome: `Continue`
        /// executes the next task, while `Break` finishes the flow run early without an error.
        ///
        /// Defaults to: "Continue"
        pub on_no_op: Option<tasks::NoOpBehavior>,
        /// How the flow run proceeds when this task finishes with a `Failed` outcome: `Fail`
        /// finishes the flow run with an error, while `Continue` executes the next task.
        ///
        /// Defaults to: "Fail"
        pub on_failure: Option<tasks::FailureBehavior>,
        /// Reference to the source resource that defines how to fetch data.
        pub source: resources::ResourceRef,
        /// Target number of records to ingest per data slice.
        pub target_records_per_slice: Option<u64>,
    }

    impl TaskSpecInputIngest {
        pub fn default_on_no_op() -> tasks::NoOpBehavior {
            tasks::NoOpBehavior::Continue
        }
        pub fn on_no_op(&self) -> tasks::NoOpBehavior {
            self.on_no_op.unwrap_or(Self::default_on_no_op())
        }
        pub fn default_on_failure() -> tasks::FailureBehavior {
            tasks::FailureBehavior::Fail
        }
        pub fn on_failure(&self) -> tasks::FailureBehavior {
            self.on_failure.unwrap_or(Self::default_on_failure())
        }
    }

    impl PartialEq for TaskSpecInputIngest {
        fn eq(&self, other: &Self) -> bool {
            self.name == other.name
                && self.target == other.target
                && self.service_account == other.service_account
                && self.on_no_op.or_else(|| Some(Self::default_on_no_op()))
                    == other.on_no_op.or_else(|| Some(Self::default_on_no_op()))
                && self.on_failure.or_else(|| Some(Self::default_on_failure()))
                    == other
                        .on_failure
                        .or_else(|| Some(Self::default_on_failure()))
                && self.source == other.source
                && self.target_records_per_slice == other.target_records_per_slice
        }
    }

    /// Moves a block reference of a dataset to an earlier block, discarding the history that
    /// follows it.
    ///
    /// Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/TaskSpecInput#/$defs/Reset
    #[derive(Clone, Debug, Eq, Default)]
    pub struct TaskSpecInputReset {
        /// An alias for the task used to refer to it in flows and access the results
        pub name: Option<String>,
        /// Reference to the dataset to reset. Defaults to the flow-level target when omitted.
        pub target: Option<datasets::DatasetRef>,
        /// Service account whose permissions the task executes with. Must be an account of service
        /// account type. Defaults to the service account of the run when omitted.
        pub service_account: Option<auth::PrincipalRef>,
        /// How the flow run proceeds when this task finishes with a `NoOp` outcome: `Continue`
        /// executes the next task, while `Break` finishes the flow run early without an error.
        ///
        /// Defaults to: "Continue"
        pub on_no_op: Option<tasks::NoOpBehavior>,
        /// How the flow run proceeds when this task finishes with a `Failed` outcome: `Fail`
        /// finishes the flow run with an error, while `Continue` executes the next task.
        ///
        /// Defaults to: "Fail"
        pub on_failure: Option<tasks::FailureBehavior>,
        /// Name of the block reference to reset.
        ///
        /// Defaults to: "head"
        pub r#ref: Option<String>,
        /// Hash of the block the reference will point to. Defaults to the `Seed` block when
        /// omitted.
        pub new_block_hash: Option<Multihash>,
        /// Expected hash of the block the reference currently points to. When specified, the task
        /// fails if the reference has moved, protecting against concurrent updates.
        pub old_block_hash: Option<Multihash>,
    }

    impl TaskSpecInputReset {
        pub fn default_on_no_op() -> tasks::NoOpBehavior {
            tasks::NoOpBehavior::Continue
        }
        pub fn on_no_op(&self) -> tasks::NoOpBehavior {
            self.on_no_op.unwrap_or(Self::default_on_no_op())
        }
        pub fn default_on_failure() -> tasks::FailureBehavior {
            tasks::FailureBehavior::Fail
        }
        pub fn on_failure(&self) -> tasks::FailureBehavior {
            self.on_failure.unwrap_or(Self::default_on_failure())
        }
        pub fn default_ref() -> &'static str {
            "head"
        }
        pub fn r#ref(&self) -> &str {
            self.r#ref.as_deref().unwrap_or(Self::default_ref())
        }
    }

    impl PartialEq for TaskSpecInputReset {
        fn eq(&self, other: &Self) -> bool {
            self.name == other.name
                && self.target == other.target
                && self.service_account == other.service_account
                && self.on_no_op.or_else(|| Some(Self::default_on_no_op()))
                    == other.on_no_op.or_else(|| Some(Self::default_on_no_op()))
                && self.on_failure.or_else(|| Some(Self::default_on_failure()))
                    == other
                        .on_failure
                        .or_else(|| Some(Self::default_on_failure()))
                && self.r#ref.as_deref().or_else(|| Some(Self::default_ref()))
                    == other.r#ref.as_deref().or_else(|| Some(Self::default_ref()))
                && self.new_block_hash == other.new_block_hash
                && self.old_block_hash == other.old_block_hash
        }
    }

    /// Pulls new blocks from a remote dataset into a local dataset.
    ///
    /// Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/TaskSpecInput#/$defs/SyncFrom
    #[derive(Clone, Debug, Eq)]
    pub struct TaskSpecInputSyncFrom {
        /// An alias for the task used to refer to it in flows and access the results
        pub name: Option<String>,
        /// Reference to the local dataset to pull into. Defaults to the flow-level target when
        /// omitted.
        pub target: Option<datasets::DatasetRef>,
        /// Service account whose permissions the task executes with. Must be an account of service
        /// account type. Defaults to the service account of the run when omitted.
        pub service_account: Option<auth::PrincipalRef>,
        /// How the flow run proceeds when this task finishes with a `NoOp` outcome: `Continue`
        /// executes the next task, while `Break` finishes the flow run early without an error.
        ///
        /// Defaults to: "Continue"
        pub on_no_op: Option<tasks::NoOpBehavior>,
        /// How the flow run proceeds when this task finishes with a `Failed` outcome: `Fail`
        /// finishes the flow run with an error, while `Continue` executes the next task.
        ///
        /// Defaults to: "Fail"
        pub on_failure: Option<tasks::FailureBehavior>,
        /// Remote dataset to pull from.
        pub source: tasks::SyncEndpointInput,
        /// Overwrite the local dataset even if its history has diverged from the source.
        ///
        /// Defaults to: false
        pub force: Option<bool>,
    }

    impl TaskSpecInputSyncFrom {
        pub fn default_on_no_op() -> tasks::NoOpBehavior {
            tasks::NoOpBehavior::Continue
        }
        pub fn on_no_op(&self) -> tasks::NoOpBehavior {
            self.on_no_op.unwrap_or(Self::default_on_no_op())
        }
        pub fn default_on_failure() -> tasks::FailureBehavior {
            tasks::FailureBehavior::Fail
        }
        pub fn on_failure(&self) -> tasks::FailureBehavior {
            self.on_failure.unwrap_or(Self::default_on_failure())
        }
        pub fn default_force() -> bool {
            false
        }
        pub fn force(&self) -> bool {
            self.force.unwrap_or(Self::default_force())
        }
    }

    impl PartialEq for TaskSpecInputSyncFrom {
        fn eq(&self, other: &Self) -> bool {
            self.name == other.name
                && self.target == other.target
                && self.service_account == other.service_account
                && self.on_no_op.or_else(|| Some(Self::default_on_no_op()))
                    == other.on_no_op.or_else(|| Some(Self::default_on_no_op()))
                && self.on_failure.or_else(|| Some(Self::default_on_failure()))
                    == other
                        .on_failure
                        .or_else(|| Some(Self::default_on_failure()))
                && self.source == other.source
                && self.force.or_else(|| Some(Self::default_force()))
                    == other.force.or_else(|| Some(Self::default_force()))
        }
    }

    /// Pushes new blocks of a local dataset to a remote dataset.
    ///
    /// Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/TaskSpecInput#/$defs/SyncTo
    #[derive(Clone, Debug, Eq)]
    pub struct TaskSpecInputSyncTo {
        /// An alias for the task used to refer to it in flows and access the results
        pub name: Option<String>,
        /// Reference to the local dataset to push. Defaults to the flow-level target when omitted.
        pub target: Option<datasets::DatasetRef>,
        /// Service account whose permissions the task executes with. Must be an account of service
        /// account type. Defaults to the service account of the run when omitted.
        pub service_account: Option<auth::PrincipalRef>,
        /// How the flow run proceeds when this task finishes with a `NoOp` outcome: `Continue`
        /// executes the next task, while `Break` finishes the flow run early without an error.
        ///
        /// Defaults to: "Continue"
        pub on_no_op: Option<tasks::NoOpBehavior>,
        /// How the flow run proceeds when this task finishes with a `Failed` outcome: `Fail`
        /// finishes the flow run with an error, while `Continue` executes the next task.
        ///
        /// Defaults to: "Fail"
        pub on_failure: Option<tasks::FailureBehavior>,
        /// Remote dataset to push to.
        pub destination: tasks::SyncEndpointInput,
        /// Overwrite the remote dataset even if its history has diverged from the local one.
        ///
        /// Defaults to: false
        pub force: Option<bool>,
        /// Create the remote dataset if it does not exist.
        ///
        /// Defaults to: true
        pub create_if_not_exists: Option<bool>,
    }

    impl TaskSpecInputSyncTo {
        pub fn default_on_no_op() -> tasks::NoOpBehavior {
            tasks::NoOpBehavior::Continue
        }
        pub fn on_no_op(&self) -> tasks::NoOpBehavior {
            self.on_no_op.unwrap_or(Self::default_on_no_op())
        }
        pub fn default_on_failure() -> tasks::FailureBehavior {
            tasks::FailureBehavior::Fail
        }
        pub fn on_failure(&self) -> tasks::FailureBehavior {
            self.on_failure.unwrap_or(Self::default_on_failure())
        }
        pub fn default_force() -> bool {
            false
        }
        pub fn force(&self) -> bool {
            self.force.unwrap_or(Self::default_force())
        }
        pub fn default_create_if_not_exists() -> bool {
            true
        }
        pub fn create_if_not_exists(&self) -> bool {
            self.create_if_not_exists
                .unwrap_or(Self::default_create_if_not_exists())
        }
    }

    impl PartialEq for TaskSpecInputSyncTo {
        fn eq(&self, other: &Self) -> bool {
            self.name == other.name
                && self.target == other.target
                && self.service_account == other.service_account
                && self.on_no_op.or_else(|| Some(Self::default_on_no_op()))
                    == other.on_no_op.or_else(|| Some(Self::default_on_no_op()))
                && self.on_failure.or_else(|| Some(Self::default_on_failure()))
                    == other
                        .on_failure
                        .or_else(|| Some(Self::default_on_failure()))
                && self.destination == other.destination
                && self.force.or_else(|| Some(Self::default_force()))
                    == other.force.or_else(|| Some(Self::default_force()))
                && self
                    .create_if_not_exists
                    .or_else(|| Some(Self::default_create_if_not_exists()))
                    == other
                        .create_if_not_exists
                        .or_else(|| Some(Self::default_create_if_not_exists()))
        }
    }

    /// Executes transformation of data defined in a derivative dataset.
    ///
    /// Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/TaskSpecInput#/$defs/Transform
    #[derive(Clone, Debug, Eq, Default)]
    pub struct TaskSpecInputTransform {
        /// An alias for the task used to refer to it in flows and access the results
        pub name: Option<String>,
        /// Reference to the derivative dataset that defines how to transform data.
        pub target: Option<datasets::DatasetRef>,
        /// Service account whose permissions the task executes with. Must be an account of service
        /// account type. Defaults to the service account of the run when omitted.
        pub service_account: Option<auth::PrincipalRef>,
        /// How the flow run proceeds when this task finishes with a `NoOp` outcome: `Continue`
        /// executes the next task, while `Break` finishes the flow run early without an error.
        ///
        /// Defaults to: "Continue"
        pub on_no_op: Option<tasks::NoOpBehavior>,
        /// How the flow run proceeds when this task finishes with a `Failed` outcome: `Fail`
        /// finishes the flow run with an error, while `Continue` executes the next task.
        ///
        /// Defaults to: "Fail"
        pub on_failure: Option<tasks::FailureBehavior>,
    }

    impl TaskSpecInputTransform {
        pub fn default_on_no_op() -> tasks::NoOpBehavior {
            tasks::NoOpBehavior::Continue
        }
        pub fn on_no_op(&self) -> tasks::NoOpBehavior {
            self.on_no_op.unwrap_or(Self::default_on_no_op())
        }
        pub fn default_on_failure() -> tasks::FailureBehavior {
            tasks::FailureBehavior::Fail
        }
        pub fn on_failure(&self) -> tasks::FailureBehavior {
            self.on_failure.unwrap_or(Self::default_on_failure())
        }
    }

    impl PartialEq for TaskSpecInputTransform {
        fn eq(&self, other: &Self) -> bool {
            self.name == other.name
                && self.target == other.target
                && self.service_account == other.service_account
                && self.on_no_op.or_else(|| Some(Self::default_on_no_op()))
                    == other.on_no_op.or_else(|| Some(Self::default_on_no_op()))
                && self.on_failure.or_else(|| Some(Self::default_on_failure()))
                    == other
                        .on_failure
                        .or_else(|| Some(Self::default_on_failure()))
        }
    }

    /// Checks dataset metadata for integrity.
    ///
    /// Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/TaskSpecInput#/$defs/Verify
    #[derive(Clone, Debug, Eq, Default)]
    pub struct TaskSpecInputVerify {
        /// An alias for the task used to refer to it in flows and access the results
        pub name: Option<String>,
        /// Reference to the dataset to verify. Defaults to the flow-level target when omitted.
        pub target: Option<datasets::DatasetRef>,
        /// Service account whose permissions the task executes with. Must be an account of service
        /// account type. Defaults to the service account of the run when omitted.
        pub service_account: Option<auth::PrincipalRef>,
        /// How the flow run proceeds when this task finishes with a `NoOp` outcome: `Continue`
        /// executes the next task, while `Break` finishes the flow run early without an error.
        ///
        /// Defaults to: "Continue"
        pub on_no_op: Option<tasks::NoOpBehavior>,
        /// How the flow run proceeds when this task finishes with a `Failed` outcome: `Fail`
        /// finishes the flow run with an error, while `Continue` executes the next task.
        ///
        /// Defaults to: "Fail"
        pub on_failure: Option<tasks::FailureBehavior>,
        /// If true, re-executes transformations on derivative datasets to verify reproducibility.
        pub replay_transform: Option<bool>,
    }

    impl TaskSpecInputVerify {
        pub fn default_on_no_op() -> tasks::NoOpBehavior {
            tasks::NoOpBehavior::Continue
        }
        pub fn on_no_op(&self) -> tasks::NoOpBehavior {
            self.on_no_op.unwrap_or(Self::default_on_no_op())
        }
        pub fn default_on_failure() -> tasks::FailureBehavior {
            tasks::FailureBehavior::Fail
        }
        pub fn on_failure(&self) -> tasks::FailureBehavior {
            self.on_failure.unwrap_or(Self::default_on_failure())
        }
    }

    impl PartialEq for TaskSpecInputVerify {
        fn eq(&self, other: &Self) -> bool {
            self.name == other.name
                && self.target == other.target
                && self.service_account == other.service_account
                && self.on_no_op.or_else(|| Some(Self::default_on_no_op()))
                    == other.on_no_op.or_else(|| Some(Self::default_on_no_op()))
                && self.on_failure.or_else(|| Some(Self::default_on_failure()))
                    == other
                        .on_failure
                        .or_else(|| Some(Self::default_on_failure()))
                && self.replay_transform == other.replay_transform
        }
    }

    /// Dispatches a certain payload to a specific `WebhookEndpoint`.
    ///
    /// Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/TaskSpecInput#/$defs/WebhookCall
    #[derive(Clone, Debug, Eq)]
    pub struct TaskSpecInputWebhookCall {
        /// An alias for the task used to refer to it in flows and access the results
        pub name: Option<String>,
        /// Service account whose permissions the task executes with. Must be an account of service
        /// account type. Defaults to the service account of the run when omitted.
        pub service_account: Option<auth::PrincipalRef>,
        /// How the flow run proceeds when this task finishes with a `NoOp` outcome: `Continue`
        /// executes the next task, while `Break` finishes the flow run early without an error.
        ///
        /// Defaults to: "Continue"
        pub on_no_op: Option<tasks::NoOpBehavior>,
        /// How the flow run proceeds when this task finishes with a `Failed` outcome: `Fail`
        /// finishes the flow run with an error, while `Continue` executes the next task.
        ///
        /// Defaults to: "Fail"
        pub on_failure: Option<tasks::FailureBehavior>,
        /// Reference to the `WebhookEndpoint`.
        pub endpoint: resources::ResourceRef,
        /// The payload to send. May include templating.
        pub payload: Option<String>,
        /// Defines how a webhook should react to failures.
        pub retry_policy: Option<flows::RetryPolicy>,
    }

    impl TaskSpecInputWebhookCall {
        pub fn default_on_no_op() -> tasks::NoOpBehavior {
            tasks::NoOpBehavior::Continue
        }
        pub fn on_no_op(&self) -> tasks::NoOpBehavior {
            self.on_no_op.unwrap_or(Self::default_on_no_op())
        }
        pub fn default_on_failure() -> tasks::FailureBehavior {
            tasks::FailureBehavior::Fail
        }
        pub fn on_failure(&self) -> tasks::FailureBehavior {
            self.on_failure.unwrap_or(Self::default_on_failure())
        }
    }

    impl PartialEq for TaskSpecInputWebhookCall {
        fn eq(&self, other: &Self) -> bool {
            self.name == other.name
                && self.service_account == other.service_account
                && self.on_no_op.or_else(|| Some(Self::default_on_no_op()))
                    == other.on_no_op.or_else(|| Some(Self::default_on_no_op()))
                && self.on_failure.or_else(|| Some(Self::default_on_failure()))
                    == other
                        .on_failure
                        .or_else(|| Some(Self::default_on_failure()))
                && self.endpoint == other.endpoint
                && self.payload == other.payload
                && self.retry_policy == other.retry_policy
        }
    }

    /// Moves a block reference of a dataset to an earlier block, discarding the history that
    /// follows it.
    ///
    /// Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/TaskSpec#/$defs/Reset
    #[derive(Clone, Debug, Eq, Default)]
    pub struct TaskSpecReset {
        /// An alias for the task used to refer to it in flows and access the results
        pub name: Option<String>,
        /// Reference to the dataset to reset. Defaults to the flow-level target when omitted.
        pub target: Option<datasets::DatasetHandle>,
        /// Service account whose permissions the task executes with. Must be an account of service
        /// account type. Defaults to the service account of the run when omitted.
        pub service_account: Option<auth::PrincipalHandle>,
        /// How the flow run proceeds when this task finishes with a `NoOp` outcome: `Continue`
        /// executes the next task, while `Break` finishes the flow run early without an error.
        ///
        /// Defaults to: "Continue"
        pub on_no_op: Option<tasks::NoOpBehavior>,
        /// How the flow run proceeds when this task finishes with a `Failed` outcome: `Fail`
        /// finishes the flow run with an error, while `Continue` executes the next task.
        ///
        /// Defaults to: "Fail"
        pub on_failure: Option<tasks::FailureBehavior>,
        /// Name of the block reference to reset.
        ///
        /// Defaults to: "head"
        pub r#ref: Option<String>,
        /// Hash of the block the reference will point to. Defaults to the `Seed` block when
        /// omitted.
        pub new_block_hash: Option<Multihash>,
        /// Expected hash of the block the reference currently points to. When specified, the task
        /// fails if the reference has moved, protecting against concurrent updates.
        pub old_block_hash: Option<Multihash>,
    }

    impl TaskSpecReset {
        pub fn default_on_no_op() -> tasks::NoOpBehavior {
            tasks::NoOpBehavior::Continue
        }
        pub fn on_no_op(&self) -> tasks::NoOpBehavior {
            self.on_no_op.unwrap_or(Self::default_on_no_op())
        }
        pub fn default_on_failure() -> tasks::FailureBehavior {
            tasks::FailureBehavior::Fail
        }
        pub fn on_failure(&self) -> tasks::FailureBehavior {
            self.on_failure.unwrap_or(Self::default_on_failure())
        }
        pub fn default_ref() -> &'static str {
            "head"
        }
        pub fn r#ref(&self) -> &str {
            self.r#ref.as_deref().unwrap_or(Self::default_ref())
        }
    }

    impl PartialEq for TaskSpecReset {
        fn eq(&self, other: &Self) -> bool {
            self.name == other.name
                && self.target == other.target
                && self.service_account == other.service_account
                && self.on_no_op.or_else(|| Some(Self::default_on_no_op()))
                    == other.on_no_op.or_else(|| Some(Self::default_on_no_op()))
                && self.on_failure.or_else(|| Some(Self::default_on_failure()))
                    == other
                        .on_failure
                        .or_else(|| Some(Self::default_on_failure()))
                && self.r#ref.as_deref().or_else(|| Some(Self::default_ref()))
                    == other.r#ref.as_deref().or_else(|| Some(Self::default_ref()))
                && self.new_block_hash == other.new_block_hash
                && self.old_block_hash == other.old_block_hash
        }
    }

    /// Pulls new blocks from a remote dataset into a local dataset.
    ///
    /// Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/TaskSpec#/$defs/SyncFrom
    #[derive(Clone, Debug, Eq)]
    pub struct TaskSpecSyncFrom {
        /// An alias for the task used to refer to it in flows and access the results
        pub name: Option<String>,
        /// Reference to the local dataset to pull into. Defaults to the flow-level target when
        /// omitted.
        pub target: Option<datasets::DatasetHandle>,
        /// Service account whose permissions the task executes with. Must be an account of service
        /// account type. Defaults to the service account of the run when omitted.
        pub service_account: Option<auth::PrincipalHandle>,
        /// How the flow run proceeds when this task finishes with a `NoOp` outcome: `Continue`
        /// executes the next task, while `Break` finishes the flow run early without an error.
        ///
        /// Defaults to: "Continue"
        pub on_no_op: Option<tasks::NoOpBehavior>,
        /// How the flow run proceeds when this task finishes with a `Failed` outcome: `Fail`
        /// finishes the flow run with an error, while `Continue` executes the next task.
        ///
        /// Defaults to: "Fail"
        pub on_failure: Option<tasks::FailureBehavior>,
        /// Remote dataset to pull from.
        pub source: tasks::SyncEndpoint,
        /// Overwrite the local dataset even if its history has diverged from the source.
        ///
        /// Defaults to: false
        pub force: Option<bool>,
    }

    impl TaskSpecSyncFrom {
        pub fn default_on_no_op() -> tasks::NoOpBehavior {
            tasks::NoOpBehavior::Continue
        }
        pub fn on_no_op(&self) -> tasks::NoOpBehavior {
            self.on_no_op.unwrap_or(Self::default_on_no_op())
        }
        pub fn default_on_failure() -> tasks::FailureBehavior {
            tasks::FailureBehavior::Fail
        }
        pub fn on_failure(&self) -> tasks::FailureBehavior {
            self.on_failure.unwrap_or(Self::default_on_failure())
        }
        pub fn default_force() -> bool {
            false
        }
        pub fn force(&self) -> bool {
            self.force.unwrap_or(Self::default_force())
        }
    }

    impl PartialEq for TaskSpecSyncFrom {
        fn eq(&self, other: &Self) -> bool {
            self.name == other.name
                && self.target == other.target
                && self.service_account == other.service_account
                && self.on_no_op.or_else(|| Some(Self::default_on_no_op()))
                    == other.on_no_op.or_else(|| Some(Self::default_on_no_op()))
                && self.on_failure.or_else(|| Some(Self::default_on_failure()))
                    == other
                        .on_failure
                        .or_else(|| Some(Self::default_on_failure()))
                && self.source == other.source
                && self.force.or_else(|| Some(Self::default_force()))
                    == other.force.or_else(|| Some(Self::default_force()))
        }
    }

    /// Pushes new blocks of a local dataset to a remote dataset.
    ///
    /// Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/TaskSpec#/$defs/SyncTo
    #[derive(Clone, Debug, Eq)]
    pub struct TaskSpecSyncTo {
        /// An alias for the task used to refer to it in flows and access the results
        pub name: Option<String>,
        /// Reference to the local dataset to push. Defaults to the flow-level target when omitted.
        pub target: Option<datasets::DatasetHandle>,
        /// Service account whose permissions the task executes with. Must be an account of service
        /// account type. Defaults to the service account of the run when omitted.
        pub service_account: Option<auth::PrincipalHandle>,
        /// How the flow run proceeds when this task finishes with a `NoOp` outcome: `Continue`
        /// executes the next task, while `Break` finishes the flow run early without an error.
        ///
        /// Defaults to: "Continue"
        pub on_no_op: Option<tasks::NoOpBehavior>,
        /// How the flow run proceeds when this task finishes with a `Failed` outcome: `Fail`
        /// finishes the flow run with an error, while `Continue` executes the next task.
        ///
        /// Defaults to: "Fail"
        pub on_failure: Option<tasks::FailureBehavior>,
        /// Remote dataset to push to.
        pub destination: tasks::SyncEndpoint,
        /// Overwrite the remote dataset even if its history has diverged from the local one.
        ///
        /// Defaults to: false
        pub force: Option<bool>,
        /// Create the remote dataset if it does not exist.
        ///
        /// Defaults to: true
        pub create_if_not_exists: Option<bool>,
    }

    impl TaskSpecSyncTo {
        pub fn default_on_no_op() -> tasks::NoOpBehavior {
            tasks::NoOpBehavior::Continue
        }
        pub fn on_no_op(&self) -> tasks::NoOpBehavior {
            self.on_no_op.unwrap_or(Self::default_on_no_op())
        }
        pub fn default_on_failure() -> tasks::FailureBehavior {
            tasks::FailureBehavior::Fail
        }
        pub fn on_failure(&self) -> tasks::FailureBehavior {
            self.on_failure.unwrap_or(Self::default_on_failure())
        }
        pub fn default_force() -> bool {
            false
        }
        pub fn force(&self) -> bool {
            self.force.unwrap_or(Self::default_force())
        }
        pub fn default_create_if_not_exists() -> bool {
            true
        }
        pub fn create_if_not_exists(&self) -> bool {
            self.create_if_not_exists
                .unwrap_or(Self::default_create_if_not_exists())
        }
    }

    impl PartialEq for TaskSpecSyncTo {
        fn eq(&self, other: &Self) -> bool {
            self.name == other.name
                && self.target == other.target
                && self.service_account == other.service_account
                && self.on_no_op.or_else(|| Some(Self::default_on_no_op()))
                    == other.on_no_op.or_else(|| Some(Self::default_on_no_op()))
                && self.on_failure.or_else(|| Some(Self::default_on_failure()))
                    == other
                        .on_failure
                        .or_else(|| Some(Self::default_on_failure()))
                && self.destination == other.destination
                && self.force.or_else(|| Some(Self::default_force()))
                    == other.force.or_else(|| Some(Self::default_force()))
                && self
                    .create_if_not_exists
                    .or_else(|| Some(Self::default_create_if_not_exists()))
                    == other
                        .create_if_not_exists
                        .or_else(|| Some(Self::default_create_if_not_exists()))
        }
    }

    /// Executes transformation of data defined in a derivative dataset.
    ///
    /// Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/TaskSpec#/$defs/Transform
    #[derive(Clone, Debug, Eq, Default)]
    pub struct TaskSpecTransform {
        /// An alias for the task used to refer to it in flows and access the results
        pub name: Option<String>,
        /// Reference to the derivative dataset that defines how to transform data.
        pub target: Option<datasets::DatasetHandle>,
        /// Service account whose permissions the task executes with. Must be an account of service
        /// account type. Defaults to the service account of the run when omitted.
        pub service_account: Option<auth::PrincipalHandle>,
        /// How the flow run proceeds when this task finishes with a `NoOp` outcome: `Continue`
        /// executes the next task, while `Break` finishes the flow run early without an error.
        ///
        /// Defaults to: "Continue"
        pub on_no_op: Option<tasks::NoOpBehavior>,
        /// How the flow run proceeds when this task finishes with a `Failed` outcome: `Fail`
        /// finishes the flow run with an error, while `Continue` executes the next task.
        ///
        /// Defaults to: "Fail"
        pub on_failure: Option<tasks::FailureBehavior>,
    }

    impl TaskSpecTransform {
        pub fn default_on_no_op() -> tasks::NoOpBehavior {
            tasks::NoOpBehavior::Continue
        }
        pub fn on_no_op(&self) -> tasks::NoOpBehavior {
            self.on_no_op.unwrap_or(Self::default_on_no_op())
        }
        pub fn default_on_failure() -> tasks::FailureBehavior {
            tasks::FailureBehavior::Fail
        }
        pub fn on_failure(&self) -> tasks::FailureBehavior {
            self.on_failure.unwrap_or(Self::default_on_failure())
        }
    }

    impl PartialEq for TaskSpecTransform {
        fn eq(&self, other: &Self) -> bool {
            self.name == other.name
                && self.target == other.target
                && self.service_account == other.service_account
                && self.on_no_op.or_else(|| Some(Self::default_on_no_op()))
                    == other.on_no_op.or_else(|| Some(Self::default_on_no_op()))
                && self.on_failure.or_else(|| Some(Self::default_on_failure()))
                    == other
                        .on_failure
                        .or_else(|| Some(Self::default_on_failure()))
        }
    }

    /// Checks dataset metadata for integrity.
    ///
    /// Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/TaskSpec#/$defs/Verify
    #[derive(Clone, Debug, Eq, Default)]
    pub struct TaskSpecVerify {
        /// An alias for the task used to refer to it in flows and access the results
        pub name: Option<String>,
        /// Reference to the dataset to verify. Defaults to the flow-level target when omitted.
        pub target: Option<datasets::DatasetHandle>,
        /// Service account whose permissions the task executes with. Must be an account of service
        /// account type. Defaults to the service account of the run when omitted.
        pub service_account: Option<auth::PrincipalHandle>,
        /// How the flow run proceeds when this task finishes with a `NoOp` outcome: `Continue`
        /// executes the next task, while `Break` finishes the flow run early without an error.
        ///
        /// Defaults to: "Continue"
        pub on_no_op: Option<tasks::NoOpBehavior>,
        /// How the flow run proceeds when this task finishes with a `Failed` outcome: `Fail`
        /// finishes the flow run with an error, while `Continue` executes the next task.
        ///
        /// Defaults to: "Fail"
        pub on_failure: Option<tasks::FailureBehavior>,
        /// If true, re-executes transformations on derivative datasets to verify reproducibility.
        pub replay_transform: Option<bool>,
    }

    impl TaskSpecVerify {
        pub fn default_on_no_op() -> tasks::NoOpBehavior {
            tasks::NoOpBehavior::Continue
        }
        pub fn on_no_op(&self) -> tasks::NoOpBehavior {
            self.on_no_op.unwrap_or(Self::default_on_no_op())
        }
        pub fn default_on_failure() -> tasks::FailureBehavior {
            tasks::FailureBehavior::Fail
        }
        pub fn on_failure(&self) -> tasks::FailureBehavior {
            self.on_failure.unwrap_or(Self::default_on_failure())
        }
    }

    impl PartialEq for TaskSpecVerify {
        fn eq(&self, other: &Self) -> bool {
            self.name == other.name
                && self.target == other.target
                && self.service_account == other.service_account
                && self.on_no_op.or_else(|| Some(Self::default_on_no_op()))
                    == other.on_no_op.or_else(|| Some(Self::default_on_no_op()))
                && self.on_failure.or_else(|| Some(Self::default_on_failure()))
                    == other
                        .on_failure
                        .or_else(|| Some(Self::default_on_failure()))
                && self.replay_transform == other.replay_transform
        }
    }

    /// Dispatches a certain payload to a specific `WebhookEndpoint`.
    ///
    /// Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/TaskSpec#/$defs/WebhookCall
    #[derive(Clone, Debug, Eq)]
    pub struct TaskSpecWebhookCall {
        /// An alias for the task used to refer to it in flows and access the results
        pub name: Option<String>,
        /// Service account whose permissions the task executes with. Must be an account of service
        /// account type. Defaults to the service account of the run when omitted.
        pub service_account: Option<auth::PrincipalHandle>,
        /// How the flow run proceeds when this task finishes with a `NoOp` outcome: `Continue`
        /// executes the next task, while `Break` finishes the flow run early without an error.
        ///
        /// Defaults to: "Continue"
        pub on_no_op: Option<tasks::NoOpBehavior>,
        /// How the flow run proceeds when this task finishes with a `Failed` outcome: `Fail`
        /// finishes the flow run with an error, while `Continue` executes the next task.
        ///
        /// Defaults to: "Fail"
        pub on_failure: Option<tasks::FailureBehavior>,
        /// Reference to the `WebhookEndpoint`.
        pub endpoint: resources::ResourceHandle,
        /// The payload to send. May include templating.
        pub payload: Option<String>,
        /// Defines how a webhook should react to failures.
        pub retry_policy: Option<flows::RetryPolicy>,
    }

    impl TaskSpecWebhookCall {
        pub fn default_on_no_op() -> tasks::NoOpBehavior {
            tasks::NoOpBehavior::Continue
        }
        pub fn on_no_op(&self) -> tasks::NoOpBehavior {
            self.on_no_op.unwrap_or(Self::default_on_no_op())
        }
        pub fn default_on_failure() -> tasks::FailureBehavior {
            tasks::FailureBehavior::Fail
        }
        pub fn on_failure(&self) -> tasks::FailureBehavior {
            self.on_failure.unwrap_or(Self::default_on_failure())
        }
    }

    impl PartialEq for TaskSpecWebhookCall {
        fn eq(&self, other: &Self) -> bool {
            self.name == other.name
                && self.service_account == other.service_account
                && self.on_no_op.or_else(|| Some(Self::default_on_no_op()))
                    == other.on_no_op.or_else(|| Some(Self::default_on_no_op()))
                && self.on_failure.or_else(|| Some(Self::default_on_failure()))
                    == other
                        .on_failure
                        .or_else(|| Some(Self::default_on_failure()))
                && self.endpoint == other.endpoint
                && self.payload == other.payload
                && self.retry_policy == other.retry_policy
        }
    }

    /// Execution phase of a task.
    ///
    /// Schema: https://opendatafabric.org/schemas/tasks/v1alpha1/TaskStatus
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    pub enum TaskStatus {
        Pending,
        Queued,
        Running,
        Finished,
    }
}
