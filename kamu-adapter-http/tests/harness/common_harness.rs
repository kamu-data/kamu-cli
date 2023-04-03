// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{fs, io, path::Path};

/////////////////////////////////////////////////////////////////////////////////////////

pub fn copy_folder_recursively(src: &Path, dst: &Path) -> io::Result<()> {
    fs::create_dir_all(&dst)?;
    let copy_options = fs_extra::dir::CopyOptions::new().content_only(true);
    fs_extra::dir::copy(src, dst, &copy_options).unwrap();
    Ok(())
}

/////////////////////////////////////////////////////////////////////////////////////////
