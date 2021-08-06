extern crate semver;
extern crate toml_edit;

use std::path::{Path, PathBuf};

use semver::Version;

fn main() {
    let matches = clap::App::new("release")
        .global_setting(clap::AppSettings::ColoredHelp)
        .args(&[
            clap::Arg::with_name("version")
                .long("version")
                .short("v")
                .takes_value(true),
            clap::Arg::with_name("next-minor").long("minor"),
            clap::Arg::with_name("next-patch").long("patch"),
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
            ..current_version
        }
    } else if matches.is_present("next-patch") {
        Version {
            patch: current_version.patch + 1,
            ..current_version
        }
    } else {
        panic!("Specivy a --version or --minor flag");
    };

    eprintln!("New version: {}", new_version);

    for cr in &crates {
        eprintln!("Bumping version in: {}", cr.name);
        set_version(&cr.cargo_toml_path, &new_version);
    }
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

#[derive(Debug, Clone)]
struct Crate {
    name: String,
    cargo_toml_path: PathBuf,
}
