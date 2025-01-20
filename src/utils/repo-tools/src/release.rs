// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![feature(exit_status_error)]

use std::path::Path;

use chrono::{Datelike, NaiveDate};
use clap::ArgAction;
use regex::Captures;
use semver::Version;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const CHANGE_DATE_YEARS: i32 = 4;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn main() {
    let matches = clap::Command::new("release")
        .args(&[
            clap::Arg::new("version")
                .long("version")
                .short('v')
                .action(ArgAction::Set),
            clap::Arg::new("next-minor")
                .long("minor")
                .action(ArgAction::SetTrue),
            clap::Arg::new("next-patch")
                .long("patch")
                .action(ArgAction::SetTrue),
        ])
        .get_matches();

    let current_version = get_current_version();
    eprintln!("Current version: {current_version}");

    let new_version: Version = if let Some(v) = matches.get_one::<String>("version") {
        v.strip_prefix('v').unwrap_or(v).parse().unwrap()
    } else if matches.get_flag("next-minor") {
        Version {
            minor: current_version.minor + 1,
            patch: 0,
            ..current_version.clone()
        }
    } else if matches.get_flag("next-patch") {
        Version {
            patch: current_version.patch + 1,
            ..current_version.clone()
        }
    } else {
        panic!("Specify a --version or --minor flag");
    };

    eprintln!("New version: {new_version}");

    let current_date = chrono::Utc::now().naive_utc().date();

    update_crates(&new_version);

    update_changelog(Path::new("CHANGELOG.md"), &new_version, current_date);

    update_license(
        Path::new("LICENSE.txt"),
        &current_version,
        &new_version,
        current_date,
    );

    update_openapi_schema(Path::new("resources/openapi.json"), &new_version);
    update_openapi_schema(Path::new("resources/openapi-mt.json"), &new_version);
}

fn get_current_version() -> Version {
    let root_cargo_content = std::fs::read_to_string(Path::new("Cargo.toml"))
        .expect("Could not read root Cargo.toml file");
    let cargo_toml: toml::Value =
        toml::from_str(&root_cargo_content).expect("Failed to parse a Cargo.toml");
    cargo_toml["workspace"]["package"]["version"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap()
}

fn update_crates(new_version: &Version) {
    std::process::Command::new("cargo")
        .args([
            "set-version".to_string(),
            "--workspace".to_string(),
            new_version.to_string(),
        ])
        .status()
        .expect(
            "Failed to execute `cargo set-version` - make sure `cargo-edit` is installed (`cargo \
             install cargo-edit`)",
        )
        .exit_ok()
        .expect("`cargo set-version` returned non-zero exit code");
}

fn update_license(
    license_path: &Path,
    current_version: &Version,
    new_version: &Version,
    current_date: NaiveDate,
) {
    let text = std::fs::read_to_string(license_path).expect("Could not read the license file");
    let new_text = update_license_text(&text, current_version, new_version, current_date);
    assert_ne!(text, new_text);
    std::fs::write(license_path, new_text).expect("Failed to write to license file");
}

fn update_openapi_schema(path: &Path, new_version: &Version) {
    let text = std::fs::read_to_string(path).expect("Could not read the schema file");

    let re = regex::Regex::new(r#""version": "\d+\.\d+\.\d+""#).unwrap();
    let new_text = re
        .replace(&text, |_: &Captures| {
            format!("\"version\": \"{new_version}\"")
        })
        .to_string();

    assert_ne!(text, new_text);
    std::fs::write(path, new_text).expect("Failed to write to schema file");
}

fn update_license_text(
    text: &str,
    current_version: &Version,
    new_version: &Version,
    current_date: NaiveDate,
) -> String {
    let significant_version =
        new_version.major != current_version.major || new_version.minor != current_version.minor;

    eprintln!("Updating license version: {new_version}");
    let re = regex::Regex::new(r"(Licensed Work: +Kamu CLI Version )(\d+\.\d+\.\d+)").unwrap();
    let text = re.replace(text, |c: &Captures| format!("{}{}", &c[1], new_version));

    if significant_version {
        let change_date = add_years(current_date, CHANGE_DATE_YEARS);
        let re = regex::Regex::new(r"(Change Date: +)(\d+-\d+-\d+)").unwrap();

        eprintln!("Updating license change date: {change_date}");
        re.replace(&text, |c: &Captures| format!("{}{}", &c[1], change_date))
    } else {
        text
    }
    .to_string()
}

fn update_changelog(path: &Path, new_version: &Version, current_date: NaiveDate) {
    let text = std::fs::read_to_string(path).expect("Could not read the changelog file");

    let re = regex::Regex::new(r#"## +\[?Unreleased\]? *"#).unwrap();
    let new_text = re
        .replace(&text, |_: &Captures| {
            format!("## [{new_version}] - {current_date}")
        })
        .to_string();

    assert_ne!(text, new_text, "Unreleased changes section not found");

    std::fs::write(path, new_text).expect("Failed to write to changelog file");
}

fn add_years(d: NaiveDate, years: i32) -> NaiveDate {
    NaiveDate::from_ymd_opt(d.year() + years, d.month(), d.day()).unwrap_or_else(|| {
        d + (NaiveDate::from_ymd_opt(d.year() + years, 1, 1).unwrap()
            - NaiveDate::from_ymd_opt(d.year(), 1, 1).unwrap())
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use chrono::NaiveDate;
    use semver::Version;

    use crate::update_license_text;

    #[test]
    fn test_update_license_patch() {
        // During patch release the Change Date stays the same

        let orig_text = indoc::indoc!(
            r#"
            ...
            Licensor:                  Kamu Data, Inc.
            Licensed Work:             Kamu CLI Version 0.63.0
            ...
            Change Date:               2025-01-01
            Change License:            Apache License, Version 2.0
            ...
            "#
        );

        let new_text = update_license_text(
            orig_text,
            &Version::new(0, 63, 0),
            &Version::new(0, 63, 1),
            NaiveDate::from_str("2021-09-01").unwrap(),
        );

        assert_eq!(
            new_text,
            indoc::indoc!(
                r#"
                ...
                Licensor:                  Kamu Data, Inc.
                Licensed Work:             Kamu CLI Version 0.63.1
                ...
                Change Date:               2025-01-01
                Change License:            Apache License, Version 2.0
                ...
                "#
            )
        );
    }

    #[test]
    fn test_update_license_minor() {
        // During patch release the Change Date stays the same

        let orig_text = indoc::indoc!(
            r#"
            ...
            Licensor:                  Kamu Data, Inc.
            Licensed Work:             Kamu CLI Version 0.63.0
            ...
            Change Date:               2025-01-01
            Change License:            Apache License, Version 2.0
            ...
            "#
        );

        let new_text = update_license_text(
            orig_text,
            &Version::new(0, 63, 0),
            &Version::new(0, 64, 0),
            NaiveDate::from_str("2021-09-01").unwrap(),
        );

        assert_eq!(
            new_text,
            indoc::indoc!(
                r#"
                ...
                Licensor:                  Kamu Data, Inc.
                Licensed Work:             Kamu CLI Version 0.64.0
                ...
                Change Date:               2025-09-01
                Change License:            Apache License, Version 2.0
                ...
                "#
            )
        );
    }
}
