// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io;
use std::path::{Path, PathBuf};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn copy_dir_contents_recursively(src: &Path, dst: &Path) -> io::Result<()> {
    if src.exists() {
        std::fs::create_dir_all(dst)?;
        let copy_options = fs_extra::dir::CopyOptions::new().content_only(true);
        fs_extra::dir::copy(src, dst, &copy_options).unwrap();
    }
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn list_files_recursively(dir: &Path) -> io::Result<Vec<PathBuf>> {
    if !dir.exists() {
        return Ok(Vec::new());
    }

    std::fs::read_dir(dir)?.try_fold(Vec::new(), |mut files, entry| {
        let path = entry?.path();
        if path.is_dir() {
            files.extend(list_files_recursively(&path)?);
        } else {
            files.push(path);
        }
        Ok(files)
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn list_relative_files_recursively(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut files = list_files_recursively(dir)?;

    for path in &mut files {
        *path = path.strip_prefix(dir).unwrap().to_owned();
    }

    files.sort();
    Ok(files)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
