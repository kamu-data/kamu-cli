use super::{Command, Error};

use indoc::indoc;
use std::fs;
use std::path::{Path, PathBuf};

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
}

impl Command for NewDatasetCommand {
    fn needs_workspace(&self) -> bool {
        false
    }

    // TODO: link to documentation
    fn run(&mut self) -> Result<(), Error> {
        let path = self.output_path.clone().unwrap_or_else(|| {
            let mut p = PathBuf::from(&self.id);
            p.set_extension("yaml");
            p
        });

        if self.is_root {
            fs::write(
                &path,
                format!(
                    indoc!(
                        "
                        ---
                        apiVersion: 1
                        kind: DatasetSnapshot
                        content:
                          id: {}
                          source:
                            kind: root
                            fetch:
                              kind: url
                              url: https://example.com/city_populations_over_time.csv
                            read:
                              kind: csv
                              header: true
                              schema:
                              - \"date TIMESTAMP\"
                              - \"city STRING\"
                              - \"population BIGINT\"
                            # preprocess:
                            #   kind: sql
                            #   engine: spark
                            #   query: >
                            #     SELECT
                            #       Date as date,
                            #       `USD (AM)` as usd_am,
                            #       `USD (PM)` as usd_pm,
                            #       `GBP (AM)` as gbp_am,
                            #       `GBP (PM)` as gbp_pm,
                            #       `EURO (AM)` as euro_am,
                            #       `EURO (PM)` as euro_pm
                            #     FROM input
                            merge:
                              kind: ledger
                              primaryKey:
                              - date
                              - city
                          vocab:
                            eventTimeColumn: date
                        "
                    ),
                    self.id
                ),
            )?;
        } else if self.is_derivative {
            fs::write(
                &path,
                format!(
                    indoc!(
                        "
                        ---
                        apiVersion: 1
                        kind: DatasetSnapshot
                        content:
                          id: {}
                          source:
                            kind: derivative
                            inputs:
                            - com.example.city-populations
                            transform:
                              kind: sql
                              engine: spark
                              query: >
                                SELECT
                                  date,
                                  city,
                                  population + 1 as population
                                FROM `com.example.city-populations`
                        "
                    ),
                    self.id
                ),
            )?;
        } else {
            return Err(Error::UsageError {
                msg: "Please specify --root or --derivative".to_owned(),
            });
        };

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
