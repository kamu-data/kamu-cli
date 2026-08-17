// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::CLIError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Renders a scan failure as a caret-annotated usage error.
///
/// Shared by the selector and label grammars so both report position the same
/// way; `kind` names the surface ("resource selector", "label selector").
pub fn usage_error_at(kind: &str, input: &str, offset: usize, message: &str) -> CLIError {
    // Offsets are byte indices, but the caret is padded in characters, so a
    // multi-byte prefix still lands the caret on the right column.
    let caret = " ".repeat(input[..offset.min(input.len())].chars().count());

    CLIError::usage_error(format!("Invalid {kind}:\n  {input}\n  {caret}^\n{message}"))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_a_caret_under_the_offending_column() {
        let rendered =
            usage_error_at("resource selector", "vs/foo/extra", 6, "unexpected `/`").to_string();

        assert!(
            rendered.contains("Invalid resource selector:"),
            "should name the surface: {rendered}"
        );
        assert_eq!(
            rendered.lines().nth(2),
            Some("        ^"),
            "caret should sit under the second `/`: {rendered}"
        );
    }

    #[test]
    fn counts_characters_not_bytes_when_padding() {
        // A multi-byte prefix must not push the caret right by its byte length.
        let rendered = usage_error_at("label selector", "ünï=x", "ünï".len(), "boom").to_string();

        assert_eq!(rendered.lines().nth(2), Some("     ^"), "{rendered}");
    }

    #[test]
    fn clamps_an_offset_past_the_end() {
        let rendered = usage_error_at("resource selector", "vs", 99, "boom").to_string();

        assert!(rendered.contains('^'), "{rendered}");
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
