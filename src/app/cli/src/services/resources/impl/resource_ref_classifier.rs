// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Whether a selector spells a resource ID rather than a name.
///
/// `get`, `delete` and `list` must agree on this, otherwise the same argument
/// means different things depending on the command. Only `UUIDv4` counts —
/// other UUID versions are ordinary names.
pub fn is_resource_id(input: &str) -> bool {
    uuid::Uuid::parse_str(input).is_ok_and(|id| id.get_version() == Some(uuid::Version::Random))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_resource_id_accepts_uuid_v4() {
        assert!(is_resource_id(&uuid::Uuid::new_v4().to_string()));
    }

    #[test]
    fn test_is_resource_id_rejects_other_uuid_versions() {
        // A parseable UUID that is not v4 is a name, not an ID.
        assert!(!is_resource_id("00000000-0000-1000-8000-000000000000"));
    }

    #[test]
    fn test_is_resource_id_rejects_names_and_patterns() {
        for input in ["my-vars", "my-%", "%", "not-a-uuid"] {
            assert!(!is_resource_id(input), "{input} should not parse as an ID");
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
