// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::*;

use crate::OutputConfig;

///////////////////////////////////////////////////////////////////////////////

pub fn write_output<T: serde::Serialize>(
    value: T,
    output_config: &OutputConfig,
    output_format: Option<impl AsRef<str>>,
) -> Result<(), InternalError> {
    let output_format = if let Some(fmt) = &output_format {
        fmt.as_ref()
    } else {
        if output_config.is_tty {
            "shell"
        } else {
            "json"
        }
    };

    // TODO: Generalize this code in output config, just like we do for tabular
    // output
    match output_format {
        "json" => serde_json::to_writer_pretty(std::io::stdout(), &value).int_err()?,
        "shell" | "yaml" | _ => serde_yaml::to_writer(std::io::stdout(), &value).int_err()?,
    }

    Ok(())
}
