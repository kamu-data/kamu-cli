// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

extern crate semver;
extern crate toml_edit;

use std::path::{Path, PathBuf};

use chrono::{Datelike, NaiveDate};
use regex::Captures;
use semver::Version;

const CHANGE_DATE_YEARS: i32 = 4;

fn main() {
    let matches = clap::Command::new("release")
        .args(&[
            clap::Arg::new("version")
                .long("version")
                .short('v')
                .takes_value(true),
            clap::Arg::new("next-minor").long("minor"),
            clap::Arg::new("next-patch").long("patch"),
        ])
        .get_matches();

    let crates = get_all_crates();
    let current_version = get_version(&crates.first().unwrap().cargo_toml_path);
    eprintln!("Current version: {}", current_version);

    let new_version: Version = if let Some(v) = matches.value_of("version") {
        v.strip_prefix('v').unwrap_or(v).parse().unwrap()
    } else if matches.is_present("next-minor") {
        Version {
            minor: current_version.minor + 1,
            patch: 0,
            ..current_version.clone()
        }
    } else if matches.is_present("next-patch") {
        Version {
            patch: current_version.patch + 1,
            ..current_version.clone()
        }
    } else {
        panic!("Specivy a --version or --minor flag");
    };

    eprintln!("New version: {}", new_version);

    for cr in crates.iter().rev() {
        eprintln!("Bumping version in: {}", cr.name);
        set_version(&cr.cargo_toml_path, &new_version);
    }

    update_license(&Path::new("LICENSE.txt"), &current_version, &new_version);

    update_makefile(&Path::new("images/Makefile"), &new_version);

    update_makefile(&Path::new("images/demo/Makefile"), &new_version);
}

fn get_all_crates() -> Vec<Crate> {
    let root_cargo_content = std::fs::read_to_string(Path::new("Cargo.toml"))
        .expect("Could not read root Cargo.toml file");
    let root_cargo: toml::Value = root_cargo_content
        .parse()
        .expect("Failed to parse root Cargo.toml");

    root_cargo["workspace"]["members"]
        .as_array()
        .unwrap()
        .into_iter()
        .map(|v| v.as_str().unwrap())
        .filter(|s| *s != "tools")
        .map(|name| Crate {
            name: name.to_owned(),
            cargo_toml_path: Path::new(name).join("Cargo.toml"),
        })
        .collect()
}

fn get_version(cargo_toml_path: &Path) -> Version {
    let content =
        std::fs::read_to_string(cargo_toml_path).expect("Could not read a Cargo.toml file");
    let cargo_toml: toml::Value = content.parse().expect("Failed to parse a Cargo.toml");
    cargo_toml["package"]["version"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap()
}

fn set_version(cargo_toml_path: &Path, version: &Version) {
    use toml_edit::{value, Document};

    let content =
        std::fs::read_to_string(cargo_toml_path).expect("Could not read a Cargo.toml file");

    let mut doc: Document = content.parse().expect("Invalid toml");
    assert_eq!(doc.to_string(), content);
    doc["package"]["version"] = value(version.to_string());

    std::fs::write(cargo_toml_path, doc.to_string()).expect("Failed to write to Cargo.toml");
}

fn update_license(license_path: &Path, current_version: &Version, new_version: &Version) {
    let text = std::fs::read_to_string(license_path).expect("Could not read the license file");
    let new_text = update_license_text(
        &text,
        current_version,
        new_version,
        &chrono::Utc::now().naive_utc().date(),
    );
    std::fs::write(license_path, new_text).expect("Failed to write to license file");
}

fn update_license_text<'t>(
    text: &'t str,
    current_version: &Version,
    new_version: &Version,
    current_date: &NaiveDate,
) -> String {
    let significant_version =
        new_version.major != current_version.major || new_version.minor != current_version.minor;

    eprintln!("Updating license version: {}", new_version);
    let re = regex::Regex::new(r"(Licensed Work:[ ]+Kamu CLI Version )(\d+\.\d+\.\d+)").unwrap();
    let text = re.replace(text, |c: &Captures| format!("{}{}", &c[1], new_version));

    if significant_version {
        let change_date = add_years(current_date, CHANGE_DATE_YEARS);
        let re = regex::Regex::new(r"(Change Date:[ ]+)(\d+-\d+-\d+)").unwrap();

        eprintln!("Updating license change date: {}", change_date);
        re.replace(&text, |c: &Captures| format!("{}{}", &c[1], change_date))
    } else {
        text
    }
    .to_string()
}

fn update_makefile(makefile_path: &Path, new_version: &Version) {
    eprintln!("Updating version in makefile: {}", makefile_path.display());
    let text = std::fs::read_to_string(makefile_path).expect("Could not read the Makefile");
    let new_text = update_makefile_text(&text, new_version);
    std::fs::write(makefile_path, new_text).expect("Failed to write to Makefile");
}

fn update_makefile_text(text: &str, new_version: &Version) -> String {
    let re = regex::Regex::new(r"(KAMU_VERSION = )(\d+\.\d+\.\d+)").unwrap();
    re.replace(text, |c: &Captures| format!("{}{}", &c[1], new_version))
        .to_string()
}

fn add_years(d: &NaiveDate, years: i32) -> NaiveDate {
    NaiveDate::from_ymd_opt(d.year() + years, d.month(), d.day()).unwrap_or_else(|| {
        *d + (NaiveDate::from_ymd(d.year() + years, 1, 1) - NaiveDate::from_ymd(d.year(), 1, 1))
    })
}

#[derive(Debug, Clone)]
struct Crate {
    name: String,
    cargo_toml_path: PathBuf,
}

/////////////////////////////////////////////////////////////////////////////////////////
// Tests
/////////////////////////////////////////////////////////////////////////////////////////

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
            &NaiveDate::from_str("2021-09-01").unwrap(),
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
        )
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
            &NaiveDate::from_str("2021-09-01").unwrap(),
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
        )
    }
}
