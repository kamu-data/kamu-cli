use super::{CLIError, Command};

use indoc::indoc;
use std::path::{Path, PathBuf};
use std::{fs::OpenOptions, io::Write};

pub struct NewDatasetCommand {
    id: String,
    is_root: bool,
    is_derivative: bool,
    output_path: Option<PathBuf>,
}

impl NewDatasetCommand {
    pub fn new<P>(id: &str, is_root: bool, is_derivative: bool, output_path: Option<P>) -> Self
    where
        P: AsRef<Path>,
    {
        Self {
            id: id.to_owned(),
            is_root: is_root,
            is_derivative: is_derivative,
            output_path: output_path.map(|p| p.as_ref().to_owned()),
        }
    }

    pub fn get_content(id: &str, is_root: bool) -> String {
        if is_root {
            format!(
                indoc!(
                    r#"
                    ---
                    apiVersion: 1
                    kind: DatasetSnapshot
                    content:
                      id: {}
                      # Reference: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetsource-schema
                      source:
                        # Root sources are the points of entry of external data into the system
                        kind: root
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
                        # OPTIONAL: Pre-processing query that shapes the data
                        # Useful for converting text data read from CSVs into strict types
                        # See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transform-schema
                        preprocess:
                          kind: sql
                          engine: spark
                          query: >
                            SELECT
                              date,
                              city,
                              CAST(REPLACE(population, ",", "") as BIGINT)  -- removes commas between thousands
                            FROM input
                        # How to combine data ingested in the past with the new data:
                        # append as log or diff as a snapshot of the current state.
                        # See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mergestrategy-schema
                        merge:
                          kind: ledger
                          primaryKey:
                          - date
                          - city
                        # Lets you manipulate names of the system columns to avoid conflicts or use names better suited for yout data.
                        # See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetvocabulary-schema
                      vocab:
                        eventTimeColumn: date
                    "#
                ),
                id
            )
        } else {
            format!(
                indoc!(
                    r#"
                    ---
                    apiVersion: 1
                    kind: DatasetSnapshot
                    content:
                      id: {}
                      # Reference: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetsourcederivative-schema
                      source:
                        # Derivative sources produce data by transforming and combining one or multiple existing datasets.
                        kind: derivative
                        # Identifiers of the datasets that will be used as sources.
                        inputs:
                        - com.example.city-populations
                        # Transformation that will be applied to produce new data
                        # See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transform-schema
                        transform:
                          kind: sql
                          engine: spark
                          query: >
                            SELECT
                              date,
                              city,
                              population + 1 as population
                            FROM `com.example.city-populations`
                      vocab:
                        eventTimeColumn: date
                    "#
                ),
                id
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
            let mut p = PathBuf::from(&self.id);
            p.set_extension("yaml");
            p
        });

        let contents = if self.is_root {
            Self::get_content(&self.id, true)
        } else if self.is_derivative {
            Self::get_content(&self.id, false)
        } else {
            return Err(CLIError::UsageError {
                msg: "Please specify --root or --derivative".to_owned(),
            });
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
