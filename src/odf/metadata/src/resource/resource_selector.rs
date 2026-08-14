// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use flatbuffers::{FlatBufferBuilder, WIPOffset};

use crate::auth::AccountRef;
use crate::formats::*;
use crate::resource::{LabelFilter, ResourceID, TypeRef};
use crate::serde::flatbuffers::{
    FlatbuffersDeserializable,
    FlatbuffersSerializable,
    proxies_generated as fb,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Matches zero or many resources using identity and label filters.
///
/// Unlike [`ResourceRef`](crate::resource::ResourceRef), which names exactly
/// one resource, `name` here is a SQL `LIKE` pattern.
///
/// Schema: <https://opendatafabric.org/schemas/resource/v1alpha1/ResourceSelector>
///
/// Excluded from codegen via the schema's `rust.dtoType` hint, so it must stay
/// in sync with the generated serde proxy and flatbuffers table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceSelector {
    pub account: Option<AccountRef>,
    /// Short type name (`SecretSet`) or full schema URI.
    pub r#type: TypeRef,
    pub id: Option<ResourceID>,
    /// SQL `LIKE` pattern. A `String`, not a `ResourceName`, since wildcards
    /// are not valid in a name.
    pub name: Option<String>,
    pub labels: Option<LabelFilter>,
}

impl ResourceSelector {
    pub fn new(
        account: Option<AccountRef>,
        r#type: TypeRef,
        id: Option<ResourceID>,
        name: Option<String>,
        labels: Option<LabelFilter>,
    ) -> Self {
        Self {
            account,
            r#type,
            id,
            name,
            labels,
        }
    }

    pub fn account(&self) -> Option<&AccountRef> {
        self.account.as_ref()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl std::str::FromStr for ResourceSelector {
    type Err = ::multiformats::ParseError<Self>;

    /// Parses the short form `Type:[account/]namePattern`.
    ///
    /// Mirrors `ResourceRef`'s short form, but the name position accepts a
    /// `LIKE` pattern. Cannot express `id` or `labels`.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let Some((typ, account, name)) = Grammar::match_resource_selector(s) else {
            return Err(::multiformats::ParseError::<Self>::new(s));
        };

        Ok(Self {
            account: account.map(|s| crate::auth::AccountName::new_unchecked(s).into()),
            r#type: crate::resource::TypeName::new_unchecked(typ).into(),
            id: None,
            name: Some(name.to_string()),
            labels: None,
        })
    }
}

impl_parse_error!(ResourceSelector);
impl_try_from_str!(ResourceSelector);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for ResourceSelector {
    type OffsetT = WIPOffset<fb::ResourceSelector<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let account_offset = self.account.as_ref().map(|v| v.serialize(fb));
        let labels_offset = self.labels.as_ref().map(|v| v.serialize(fb));
        let type_offset = fb.create_string(self.r#type.as_str());
        let id_offset = self.id.as_ref().map(|v| fb.create_vector(v.as_bytes()));
        let name_offset = self.name.as_ref().map(|v| fb.create_string(v));

        let mut builder = fb::ResourceSelectorBuilder::new(fb);
        if let Some(off) = account_offset {
            builder.add_account(off);
        }
        builder.add_type_(type_offset);
        if let Some(off) = id_offset {
            builder.add_id(off);
        }
        if let Some(off) = name_offset {
            builder.add_name(off);
        }
        if let Some(off) = labels_offset {
            builder.add_labels(off);
        }
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::ResourceSelector<'fb>> for ResourceSelector {
    fn deserialize(proxy: fb::ResourceSelector<'fb>) -> Self {
        Self {
            account: proxy.account().map(AccountRef::deserialize),
            r#type: proxy
                .type_()
                .map(|v| TypeRef::try_from(v).unwrap())
                .unwrap(),
            id: proxy
                .id()
                .map(|v| ResourceID::from_bytes(v.bytes()).unwrap()),
            name: proxy.name().map(ToString::to_string),
            labels: proxy.labels().map(LabelFilter::deserialize),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use pretty_assertions::assert_eq;

    use super::*;

    fn parse(s: &str) -> ResourceSelector {
        s.parse().unwrap()
    }

    #[test]
    fn test_parse_type_and_exact_name() {
        let sel = parse("SecretSet:my-secrets");
        assert_eq!(sel.r#type.as_str(), "SecretSet");
        assert_eq!(sel.name.as_deref(), Some("my-secrets"));
        assert!(sel.account.is_none());
        assert!(sel.id.is_none());
        assert!(sel.labels.is_none());
    }

    // The name position is what separates a selector from a ref.
    #[test]
    fn test_parse_name_patterns() {
        assert_eq!(parse("SecretSet:%").name.as_deref(), Some("%"));
        assert_eq!(
            parse("SecretSet:my-resource-%").name.as_deref(),
            Some("my-resource-%")
        );
        assert_eq!(parse("SecretSet:%-prod").name.as_deref(), Some("%-prod"));
    }

    #[test]
    fn test_parse_with_account() {
        let sel = parse("SecretSet:alice/app-%");
        assert_eq!(sel.r#type.as_str(), "SecretSet");
        assert_eq!(sel.name.as_deref(), Some("app-%"));
        assert_matches!(
            sel.account,
            Some(AccountRef { name: Some(ref n), .. }) if n.as_str() == "alice"
        );
    }

    #[test]
    fn test_parse_rejects_malformed() {
        for input in [
            "",
            "SecretSet",            // no `:` separator
            ":my-secrets",          // no type
            "SecretSet:",           // no name
            "Secret Set:my-secret", // space in type
            "SecretSet:a/b/c",      // too many separators
        ] {
            assert_matches!(
                input.parse::<ResourceSelector>(),
                Err(_),
                "expected `{input}` to be rejected"
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
