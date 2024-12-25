// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
    pub init: bool,
    pub interactive: bool,
    pub network: Option<String>,
    pub remove: bool,
    pub tty: bool,
    pub user: Option<String>,
    pub volumes: Vec<VolumeSpec>,
    pub extra_hosts: Vec<ExtraHostSpec>,
    pub work_dir: Option<PathBuf>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ExecArgs {
    pub tty: bool,
    pub interactive: bool,
    pub work_dir: Option<PathBuf>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumeSpec {
    pub source: PathBuf,
    pub dest: PathBuf,
    pub access: VolumeAccess,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VolumeAccess {
    ReadOnly,
    ReadWrite,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtraHostSpec {
    pub source: String,
    pub dest: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
            image: String::new(),
            init: false,
            interactive: false,
            network: None,
            remove: true,
            tty: false,
            user: None,
            volumes: Vec::new(),
            extra_hosts: Vec::new(),
            work_dir: None,
        }
    }
}

impl<P1, P2> From<(P1, P2)> for VolumeSpec
where
    P1: Into<PathBuf>,
    P2: Into<PathBuf>,
{
    fn from(val: (P1, P2)) -> Self {
        VolumeSpec {
            source: val.0.into(),
            dest: val.1.into(),
            access: VolumeAccess::ReadWrite,
        }
    }
}

impl<P1, P2> From<(P1, P2, VolumeAccess)> for VolumeSpec
where
    P1: Into<PathBuf>,
    P2: Into<PathBuf>,
{
    fn from(val: (P1, P2, VolumeAccess)) -> Self {
        VolumeSpec {
            source: val.0.into(),
            dest: val.1.into(),
            access: val.2,
        }
    }
}

impl<S1, S2> From<(S1, S2)> for ExtraHostSpec
where
    S1: Into<String>,
    S2: Into<String>,
{
    fn from(val: (S1, S2)) -> Self {
        ExtraHostSpec {
            source: val.0.into(),
            dest: val.1.into(),
        }
    }
}
