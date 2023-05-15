// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunArgs {
    pub args: Vec<String>,
    pub container_name: Option<String>,
    pub detached: bool,
    pub entry_point: Option<String>,
    pub environment_vars: Vec<(String, String)>,
    pub expose_all_ports: bool,
    pub expose_ports: Vec<u16>,
    pub expose_port_map: Vec<(u16, u16)>,
    pub expose_port_map_addr: Vec<(String, u16, u16)>,
    pub expose_port_map_range: Vec<((u16, u16), (u16, u16))>,
    pub hostname: Option<String>,
    pub image: String,
    pub interactive: bool,
    pub network: Option<String>,
    pub remove: bool,
    pub tty: bool,
    pub user: Option<String>,
    pub volume_map: Vec<(PathBuf, PathBuf)>,
    pub work_dir: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecArgs {
    pub tty: bool,
    pub interactive: bool,
    pub work_dir: Option<PathBuf>,
}

impl Default for RunArgs {
    fn default() -> Self {
        Self {
            args: Vec::new(),
            container_name: None,
            detached: false,
            entry_point: None,
            environment_vars: Vec::new(),
            expose_all_ports: false,
            expose_ports: Vec::new(),
            expose_port_map: Vec::new(),
            expose_port_map_addr: Vec::new(),
            expose_port_map_range: Vec::new(),
            hostname: None,
            image: "".to_owned(),
            interactive: false,
            network: None,
            remove: true,
            tty: false,
            user: None,
            volume_map: Vec::new(),
            work_dir: None,
        }
    }
}

impl Default for ExecArgs {
    fn default() -> Self {
        Self {
            tty: false,
            interactive: false,
            work_dir: None,
        }
    }
}
