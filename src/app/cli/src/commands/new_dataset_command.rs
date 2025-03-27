// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;

use super::{CLIError, Command};

#[dill::component]
#[dill::interface(dyn Command)]
pub struct NewDatasetCommand {
    #[dill::component(explicit)]
    name: odf::DatasetName,

    #[dill::component(explicit)]
    is_root: bool,

    #[dill::component(explicit)]
    is_derivative: bool,

    #[dill::component(explicit)]
    output_path: Option<PathBuf>,
}

impl NewDatasetCommand {
    pub fn get_content(name: &str, is_root: bool) -> String {
        if is_root {
            format!(
                indoc::indoc!(
                    r#"
                    ---
                    kind: DatasetSnapshot
                    version: 1
                    content:
                      # A human-friendly alias of the dataset
                      name: {}
                      # Root datasets are the points of entry of external data into the system
                      # See: https://docs.kamu.dev/cli/ingest/
                      kind: Root
                      # List of metadata events that get dataset into its initial state
                      # See: https://docs.kamu.dev/odf/reference/#metadataevent
                      metadata:
                        # Specifies the source of data that can be periodically polled to refresh the dataset
                        # See: https://docs.kamu.dev/odf/reference/#setpollingsource
                        - kind: SetPollingSource
                          # Where to fetch the data from.
                          # Includes source URL, a protocol to use, cache control
                          # See: https://docs.kamu.dev/odf/reference/#fetchstep
                          fetch:
                            kind: Url
                            url: https://example.com/city_populations_over_time.zip
                          # OPTIONAL: How to prepare the binary data
                          # Includes decompression, file filtering, format conversions
                          prepare:
                            - kind: Decompress
                              format: Zip
                          # How to interpret the data.
                          # Includes data format, schema to apply, error handling
                          # See: https://docs.kamu.dev/odf/reference/#readstep
                          read:
                            kind: Csv
                            header: true
                            timestampFormat: yyyy-M-d
                            schema:
                              - "date TIMESTAMP"
                              - "city STRING"
                              - "population STRING"
                          # OPTIONAL: Pre-processing query that shapes the data.
                          # Useful for converting text data read from CSVs into strict types
                          # See: https://docs.kamu.dev/odf/reference/#transform
                          preprocess:
                            kind: Sql
                            # Use one of the supported engines and a query in its dialect
                            # See: https://docs.kamu.dev/cli/supported-engines/
                            engine: datafusion
                            query: |
                              select
                                date,
                                city,
                                -- remove commas between thousands
                                cast(replace(population, ",", "") as bigint)
                              from input
                          # How to combine data ingested in the past with the new data.
                          # See: https://docs.kamu.dev/odf/reference/#mergestrategy
                          merge:
                            kind: Ledger
                            primaryKey:
                              - date
                              - city
                          # Lets you manipulate names of the system columns to avoid conflicts
                          # or use names better suited for your data.
                          # See: https://docs.kamu.dev/odf/reference/#setvocab
                        - kind: SetVocab
                          eventTimeColumn: date
                    "#
                ),
                name
            )
        } else {
            format!(
                indoc::indoc!(
                    r#"
                    ---
                    kind: DatasetSnapshot
                    version: 1
                    content:
                      # A human-friendly alias of the dataset
                      name: {}
                      # Derivative datasets produce data by transforming and combining
                      # one or multiple existing datasets.
                      # See: https://docs.kamu.dev/cli/transform/
                      kind: Derivative
                      # List of metadata events that get dataset into its initial state
                      # See: https://docs.kamu.dev/odf/reference/#metadataevent
                      metadata:
                        # Transformation that will be applied to produce new data
                        # See: https://docs.kamu.dev/odf/reference/#settransform
                        - kind: SetTransform
                          # References the datasets that will be used as inputs.
                          # Note: We are associating inputs by name, but could also use IDs.
                          inputs:
                            - datasetRef: com.example.city-populations
                          # Transformation steps that ise one of the supported engines and query dialects
                          # See: https://docs.kamu.dev/cli/supported-engines/
                          transform:
                            kind: Sql
                            engine: datafusion
                            query: |
                              select
                                date,
                                city,
                                population + 1 as population
                              from `com.example.city-populations`
                        # Lets you manipulate names of the system columns to avoid
                        # conflicts or use names better suited for your data.
                        # See: https://docs.kamu.dev/odf/reference/#setvocab
                        - kind: SetVocab
                          eventTimeColumn: date
                    "#
                ),
                name
            )
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for NewDatasetCommand {
    // TODO: link to documentation
    async fn run(&self) -> Result<(), CLIError> {
        let path = self.output_path.clone().unwrap_or_else(|| {
            let mut name = String::with_capacity(100);
            name.push_str(&self.name);
            name.push_str(".yaml");
            PathBuf::from(name)
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
