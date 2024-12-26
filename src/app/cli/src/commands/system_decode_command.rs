// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::{Read, Write};

use internal_error::*;
use opendatafabric::serde::{MetadataBlockDeserializer, MetadataBlockSerializer};
use opendatafabric::{self as odf};

use super::{CLIError, Command};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SystemDecodeCommand {
    manifest: Option<String>,
    stdin: bool,
}

impl SystemDecodeCommand {
    pub fn new(manifest: Option<String>, stdin: bool) -> Self {
        Self { manifest, stdin }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for SystemDecodeCommand {
    fn needs_workspace(&self) -> bool {
        false
    }

    async fn run(&mut self) -> Result<(), CLIError> {
        let data: Vec<u8> = if self.stdin {
            let mut buf = Vec::new();
            std::io::stdin().read_to_end(&mut buf).int_err()?;
            buf
        } else if let Some(manifest) = &self.manifest {
            let url = match url::Url::parse(manifest) {
                Ok(url) => url,
                Err(url::ParseError::RelativeUrlWithoutBase) => {
                    let path = std::path::PathBuf::from(manifest)
                        .canonicalize()
                        .int_err()?;
                    url::Url::from_file_path(&path)
                        .unwrap_or_else(|_| panic!("Invalid path: {}", path.display()))
                }
                Err(err) => return Err(err.int_err().into()),
            };

            match url.scheme() {
                "file" => std::fs::read(url.to_file_path().unwrap()).int_err()?,
                scheme => {
                    return Err(CLIError::usage_error(format!(
                        "Fetchind data from {scheme} is not supported"
                    )))
                }
            }
        } else {
            return Err(CLIError::usage_error("Specify URL, path, or --stdin"));
        };

        let de = odf::serde::flatbuffers::FlatbuffersMetadataBlockDeserializer;
        let block = de.read_manifest(&data).int_err()?;

        let ser = odf::serde::yaml::YamlMetadataBlockSerializer;
        let data_out = ser.write_manifest(&block).int_err()?;

        std::io::stdout().write_all(&data_out).int_err()?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
