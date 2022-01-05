// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::{CLIError, Command};
use opendatafabric::DatasetName;

use indoc::indoc;
use std::path::{Path, PathBuf};
use std::{fs::OpenOptions, io::Write};

pub struct NewDatasetCommand {
    name: DatasetName,
    is_root: bool,
    is_derivative: bool,
    output_path: Option<PathBuf>,
}

impl NewDatasetCommand {
    pub fn new<N, P>(name: N, is_root: bool, is_derivative: bool, output_path: Option<P>) -> Self
    where
        N: TryInto<DatasetName>,
        <N as TryInto<opendatafabric::DatasetName>>::Error: std::fmt::Debug,
        P: AsRef<Path>,
    {
        Self {
            name: name.try_into().unwrap(),
            is_root: is_root,
            is_derivative: is_derivative,
            output_path: output_path.map(|p| p.as_ref().to_owned()),
        }
    }

    pub fn get_content(name: &str, is_root: bool) -> String {
        if is_root {
            format!(
                indoc!(
                    r#"
                    ---
                    kind: DatasetSnapshot
                    version: 1
                    content:
                      # A human-friendly alias of the dataset
                      name: {}
                      # Root sources are the points of entry of external data into the system
                      kind: root
                      # List of metadata events that get dataset into its initial state
                      # See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#metadataevent-schema
                      metadata:
                        # Specifies the source of data that can be periodically polled to refresh the dataset
                        # See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setpollingsource-schema
                        - kind: setPollingSource
                          # Where to fetch the data from.
                          # Includes source URL, a protocol to use, cache control
                          # See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#fetchstep-schema
                          fetch:
                            kind: url
                            url: https://example.com/city_populations_over_time.zip
                          # OPTIONAL: How to prepare the binary data
                          # Includes decompression, file filtering, format conversions
                          prepare:
                          - kind: decompress
                            format: zip
                          # How to interpret the data.
                          # Includes data format, schema to apply, error handling
                          # See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#readstep-schema
                          read:
                            kind: csv
                            header: true
                            timestampFormat: yyyy-M-d
                            schema:
                            - "date TIMESTAMP"
                            - "city STRING"
                            - "population STRING"
                          # OPTIONAL: Pre-processing query that shapes the data.
                          # Useful for converting text data read from CSVs into strict types
                          # See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transform-schema
                          preprocess:
                            kind: sql
                            # Use one of the supported engines and a query in its dialect
                            # See: https://docs.kamu.dev/cli/transform/supported-engines/
                            engine: spark
                            query: >
                              SELECT
                                date,
                                city,
                                CAST(REPLACE(population, ",", "") as BIGINT)  -- removes commas between thousands
                              FROM input
                          # How to combine data ingested in the past with the new data.
                          # See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mergestrategy-schema
                          merge:
                            kind: ledger
                            primaryKey:
                            - date
                            - city
                        # Lets you manipulate names of the system columns to avoid conflicts or use names better suited for yout data.
                        # See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setvocab-schema
                        - kind: setVocab
                          eventTimeColumn: date
                    "#
                ),
                name
            )
        } else {
            format!(
                indoc!(
                    r#"
                    ---
                    kind: DatasetSnapshot
                    version: 1
                    content:
                      # A human-friendly alias of the dataset
                      name: {}
                      # Derivative sources produce data by transforming and combining one or multiple existing datasets.
                      kind: derivative
                      # List of metadata events that get dataset into its initial state
                      # See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#metadataevent-schema
                      metadata:
                        # Transformation that will be applied to produce new data
                        # See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#settransform-schema
                        - kind: setTransform
                          # References the datasets that will be used as sources.
                          # Note: We are associating inputs by name, but could also use IDs.
                          inputs:
                            - name: com.example.city-populations
                          # Transformation steps that ise one of the supported engines and query dialects
                          # See: https://docs.kamu.dev/cli/transform/supported-engines/
                          transform:
                            kind: sql
                            engine: spark
                            query: >
                              SELECT
                                date,
                                city,
                                population + 1 as population
                              FROM `com.example.city-populations`
                        # Lets you manipulate names of the system columns to avoid conflicts or use names better suited for yout data.
                        # See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setvocab-schema
                        - kind: setVocab
                          eventTimeColumn: date
                    "#
                ),
                name
            )
        }
    }
}

impl Command for NewDatasetCommand {
    fn needs_workspace(&self) -> bool {
        false
    }

    // TODO: link to documentation
    fn run(&mut self) -> Result<(), CLIError> {
        let path = self.output_path.clone().unwrap_or_else(|| {
            let mut p = PathBuf::from(self.name.as_str());
            p.set_extension("yaml");
            p
        });

        let contents = if self.is_root {
            Self::get_content(&self.name, true)
        } else if self.is_derivative {
            Self::get_content(&self.name, false)
        } else {
            return Err(CLIError::usage_error(
                "Please specify --root or --derivative",
            ));
        };

        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;

        file.write_all(contents.as_bytes())?;

        eprintln!(
            "{}: {}",
            console::style("Written new manifest template to")
                .green()
                .bold(),
            path.display()
        );

        eprintln!(
            "Follow directions in the file's comments and use `kamu add {}` when ready.",
            path.display()
        );

        Ok(())
    }
}
