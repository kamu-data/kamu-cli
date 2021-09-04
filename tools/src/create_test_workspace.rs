#![feature(exit_status_error)]

use std::{
    path::{Path, PathBuf},
    process::Command,
    str::FromStr,
};

fn main() {
    let matches = clap::App::new("create-test-workspace")
        .global_setting(clap::AppSettings::ColoredHelp)
        .args(&[clap::Arg::with_name("force").long("force").short("f")])
        .get_matches();

    let work_dir = PathBuf::from_str("examples/covid").unwrap();

    init_workspace(&work_dir, matches.is_present("force"));

    let source_data = download_source_data(
        &work_dir,
        &[
            "ca.bccdc.covid19.case-details",
            "ca.ontario.data.covid19.case-details",
        ],
    );

    // TODO: Split input into many batches
    for (dataset_id, data_path) in source_data {
        let data_url = url::Url::from_file_path(&data_path.0).unwrap().to_string();
        Command::new("kamu")
            .current_dir(&work_dir)
            .args(&["pull", &dataset_id, "--fetch", &data_url])
            .status()
            .unwrap()
            .exit_ok()
            .unwrap();
    }

    for dataset_id in ["ca.covid19.case-details", "ca.covid19.daily-cases"] {
        Command::new("kamu")
            .current_dir(&work_dir)
            .args(&["pull", dataset_id])
            .status()
            .unwrap()
            .exit_ok()
            .unwrap();
    }

    Command::new("kamu")
        .current_dir(&work_dir)
        .args(&["list"])
        .status()
        .unwrap()
        .exit_ok()
        .unwrap();
}

fn init_workspace(work_dir: &Path, force: bool) {
    let kamu_root = work_dir.join(".kamu");
    let kamu_volume = work_dir.join(".kamu.local");
    if kamu_root.exists() {
        if !force {
            panic!(
                "Workspace already exists, use --force to re-create: {}",
                work_dir.display()
            );
        }

        std::fs::remove_dir_all(kamu_root).unwrap();
        std::fs::remove_dir_all(kamu_volume).unwrap();
    }

    Command::new("kamu")
        .current_dir(&work_dir)
        .arg("init")
        .status()
        .unwrap()
        .exit_ok()
        .unwrap();

    Command::new("kamu")
        .current_dir(&work_dir)
        .args(&["add", ".", "-r"])
        .status()
        .unwrap()
        .exit_ok()
        .unwrap();
}

fn download_source_data(work_dir: &Path, datasets: &[&str]) -> Vec<(String, DropFile)> {
    let mut root_data: Vec<(String, DropFile)> = Vec::new();

    for dataset_id in datasets {
        let dataset_id = (*dataset_id).to_owned();

        let manifest_file = {
            let mut s = dataset_id.clone();
            s.push_str(".yaml");
            s
        };
        let manifest_content = std::fs::read_to_string(work_dir.join(manifest_file)).unwrap();
        let manifest = yaml_rust::YamlLoader::load_from_str(&manifest_content)
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        let url = &manifest["content"]["source"]["fetch"]["url"]
            .as_str()
            .unwrap();

        let data_file = {
            let mut s = dataset_id.clone();
            s.push_str(".bin");
            work_dir.join(s)
        };

        println!(
            "Downloading data:\n  Dataset: {}\n  Source {}\n  Dest {}",
            dataset_id,
            url,
            data_file.display()
        );

        Command::new("wget")
            .args(&["-O", &data_file.to_string_lossy(), url])
            .status()
            .unwrap()
            .exit_ok()
            .unwrap();

        root_data.push((dataset_id, DropFile(data_file.canonicalize().unwrap())));
    }

    root_data
}

struct DropFile(PathBuf);

impl Drop for DropFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
