// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![feature(exit_status_error)]

use std::{
    io::BufRead,
    path::{Path, PathBuf},
    process::Command,
    str::FromStr,
};

fn main() {
    let matches = clap::Command::new("create-test-workspace")
        .args(&[clap::Arg::new("force").long("force").short('f')])
        .get_matches();

    let work_dir = PathBuf::from_str("examples/covid").unwrap();

    init_workspace(&work_dir, matches.is_present("force"));

    // BC dataset does not contain unique IDs so it's a pain to slice and we pull it whole
    Command::new("kamu")
        .current_dir(&work_dir)
        .args(&[
            "pull",
            "alberta.case-details",
            "british-columbia.case-details",
            "quebec.case-details",
        ])
        .status()
        .unwrap()
        .exit_ok()
        .unwrap();

    let source_data = download_source_data(&work_dir, &["ontario.case-details"]);

    for (dataset_id, data_path) in source_data {
        for data_slice_path in slice_data_file(data_path.as_ref(), 50, true) {
            let data_url = url::Url::from_file_path(&data_slice_path)
                .unwrap()
                .to_string();

            Command::new("kamu")
                .current_dir(&work_dir)
                .args(&["pull", &dataset_id, "--fetch", &data_url])
                .status()
                .unwrap()
                .exit_ok()
                .unwrap();
        }
    }

    Command::new("kamu")
        .current_dir(&work_dir)
        .args(&["pull", "--all"])
        .status()
        .unwrap()
        .exit_ok()
        .unwrap();

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
    if kamu_root.exists() {
        if !force {
            panic!(
                "Workspace already exists, use --force to re-create: {}",
                work_dir.display()
            );
        }

        std::fs::remove_dir_all(kamu_root).unwrap();
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
        let url = &manifest["content"]["metadata"][0]["fetch"]["url"]
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

fn slice_data_file(data_path: &Path, num_slices: usize, has_header: bool) -> Slices {
    Slices::new(data_path, num_slices, has_header)
}

///////////////////////////////////////////////////////////////////////////////

struct Slices {
    data_path: PathBuf,
    lines: Vec<String>,
    current_slice: usize,
    num_slices: usize,
    has_header: bool,
}

impl Slices {
    fn new(data_path: &Path, num_slices: usize, has_header: bool) -> Self {
        let file = std::fs::File::open(data_path).unwrap();
        let reader = std::io::BufReader::new(file);
        let lines: Vec<String> = reader.lines().map(|r| r.unwrap()).collect();

        Self {
            data_path: data_path.to_owned(),
            lines,
            current_slice: 0,
            num_slices,
            has_header,
        }
    }
}

impl Iterator for Slices {
    type Item = DropFile;

    fn next(&mut self) -> Option<Self::Item> {
        use std::io::Write;

        let num_data_lines = self.lines.len() - if self.has_header { 1 } else { 0 };
        let lines_per_slice =
            (((num_data_lines as f64) / (self.num_slices as f64)).ceil()) as usize;

        let slice_start =
            lines_per_slice * self.current_slice + if self.has_header { 1 } else { 0 };
        let slice_end = std::cmp::min(slice_start + lines_per_slice, self.lines.len());

        if slice_start >= self.lines.len() {
            return None;
        }

        let mut slice_path = self.data_path.clone();
        let new_file_name = format!(
            "{}.{}.bin",
            slice_path.file_stem().unwrap().to_string_lossy(),
            self.current_slice
        );
        slice_path.set_file_name(new_file_name);

        let mut file = std::fs::File::create(&slice_path).unwrap();

        if self.has_header {
            writeln!(file, "{}", &self.lines[0]).unwrap();
        }

        for line in &self.lines[slice_start..slice_end] {
            writeln!(file, "{}", line).unwrap();
        }

        self.current_slice += 1;

        Some(DropFile(slice_path))
    }
}

///////////////////////////////////////////////////////////////////////////////

struct DropFile(PathBuf);

impl AsRef<Path> for DropFile {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}

impl Drop for DropFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
