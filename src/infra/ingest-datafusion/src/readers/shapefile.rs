// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::{Path, PathBuf};

use datafusion::datasource::file_format::file_type::FileCompressionType;
use datafusion::prelude::*;
use internal_error::*;
use kamu_core::ingest::ReadError;
use opendatafabric::*;

use crate::*;

///////////////////////////////////////////////////////////////////////////////

pub struct ReaderEsriShapefile {
    temp_path: PathBuf,
}

impl ReaderEsriShapefile {
    // TODO: This is an ugly API that leaves it to the caller to clean up our temp
    // file mess. Ideally we should not produce any temp files at all and stream in
    // all data.
    pub fn new(temp_path: impl Into<PathBuf>) -> Self {
        Self {
            temp_path: temp_path.into(),
        }
    }

    // TODO: PERF: Consider subPath argumemnt to skip extracting unrelated data
    fn extract_zip_to_temp_dir(&self, path: &Path) -> Result<PathBuf, InternalError> {
        std::fs::create_dir(&self.temp_path).int_err()?;
        let mut archive = zip::ZipArchive::new(std::fs::File::open(path).int_err()?).int_err()?;
        archive.extract(&self.temp_path).int_err()?;
        Ok(self.temp_path.clone())
    }

    fn locate_shp_file(&self, dir: &Path, subpath: Option<&str>) -> Result<PathBuf, ReadError> {
        let is_shp_file = |p: &Path| -> bool { p.extension().map(|s| s == "shp").unwrap_or(false) };

        let list_shp_files = || -> Vec<PathBuf> {
            walkdir::WalkDir::new(dir)
                .into_iter()
                .filter_map(|e| e.ok())
                .filter(|e| is_shp_file(e.path()))
                .map(|e| e.into_path())
                .collect()
        };

        let to_relative_paths = |paths: Vec<PathBuf>| -> Vec<String> {
            paths
                .into_iter()
                .filter_map(|p| {
                    p.strip_prefix(dir)
                        .ok()
                        .map(|p| p.to_string_lossy().into_owned())
                })
                .collect()
        };

        if let Some(subpath) = subpath {
            let path = dir.join(subpath);

            // Try exact match
            if path.is_file() {
                Ok(path)
            } else {
                // Try globbed match
                let matches: Vec<_> = glob::glob(path.to_str().unwrap())
                    .int_err()?
                    .filter_map(|e| e.ok())
                    .filter(|p| is_shp_file(&p))
                    .collect();

                if matches.len() == 1 {
                    Ok(matches.into_iter().next().unwrap())
                } else if matches.is_empty() {
                    Err(MalformedError::new(format!(
                        "Archive does not contain any .shp files under '{}' sub-path. Possible \
                         entries are:\n  - {}",
                        subpath,
                        to_relative_paths(list_shp_files()).join("\n  - ")
                    ))
                    .into())
                } else {
                    Err(MalformedError::new(format!(
                        "Archive contains multiple .shp files matching sub-path '{}':\n  - {}",
                        subpath,
                        to_relative_paths(matches).join("\n  - ")
                    ))
                    .into())
                }
            }
        } else {
            let shp_files = list_shp_files();

            if shp_files.len() == 1 {
                Ok(shp_files.into_iter().next().unwrap())
            } else if shp_files.len() > 1 {
                Err(MalformedError::new(format!(
                    "Archive contains multiple .shp files. Specify `subPath` argument to select \
                     one of:\n  - {}",
                    to_relative_paths(shp_files).join("\n  - ")
                ))
                .into())
            } else {
                Err(MalformedError::new("Archive does not contain any .shp files").into())
            }
        }
    }

    fn shp_record_to_json(
        &self,
        record: shapefile::dbase::Record,
    ) -> serde_json::Map<String, serde_json::Value> {
        use serde_json::Value as JsonValue;
        use shapefile::dbase::FieldValue as ShpValue;

        let mut json = serde_json::Map::new();

        for (name, value) in record {
            let json_value = match value {
                ShpValue::Character(v) => v.map(|v| JsonValue::String(v)),
                ShpValue::Numeric(v) => {
                    v.map(|v| JsonValue::Number(serde_json::Number::from_f64(v).unwrap()))
                }
                ShpValue::Logical(v) => v.map(|v| JsonValue::Bool(v)),
                ShpValue::Date(v) => {
                    v.map(|v| JsonValue::String(format!("{}-{}-{}", v.year(), v.month(), v.day())))
                }
                ShpValue::Float(v) => {
                    v.map(|v| JsonValue::Number(serde_json::Number::from_f64(v as f64).unwrap()))
                }
                ShpValue::Integer(v) => Some(JsonValue::Number(v.into())),
                ShpValue::Currency(v) => {
                    Some(JsonValue::Number(serde_json::Number::from_f64(v).unwrap()))
                }
                ShpValue::DateTime(v) => Some(JsonValue::String(format!(
                    "{}-{}-{} {}:{}:{}",
                    v.date().year(),
                    v.date().month(),
                    v.date().day(),
                    v.time().hours(),
                    v.time().minutes(),
                    v.time().seconds(),
                ))),
                ShpValue::Double(v) => {
                    Some(JsonValue::Number(serde_json::Number::from_f64(v).unwrap()))
                }
                ShpValue::Memo(v) => Some(JsonValue::String(v)),
            }
            .unwrap_or(JsonValue::Null);

            json.insert(name, json_value);
        }

        json
    }
}

///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl Reader for ReaderEsriShapefile {
    async fn output_schema(
        &self,
        ctx: &SessionContext,
        conf: &ReadStep,
    ) -> Result<Option<datafusion::arrow::datatypes::Schema>, ReadError> {
        super::output_schema_common(ctx, conf).await
    }

    async fn read(
        &self,
        ctx: &SessionContext,
        path: &Path,
        conf: &ReadStep,
    ) -> Result<DataFrame, ReadError> {
        use std::io::Write;

        use serde_json::Value as JsonValue;

        let schema = self.output_schema(ctx, conf).await?;

        let ReadStep::EsriShapefile(conf) = conf else {
            unreachable!()
        };

        // TODO: PERF: This is a temporary, highly inefficient implementation that
        // decodes Shapefile into NdJson which DataFusion can read natively
        let extracted_path = self.extract_zip_to_temp_dir(path)?;
        let shp_path =
            self.locate_shp_file(&extracted_path, conf.sub_path.as_ref().map(|s| s.as_str()))?;

        let temp_json_path = extracted_path.join("__temp.json");
        let mut file = std::fs::File::create_new(&temp_json_path).int_err()?;

        let mut reader = shapefile::Reader::from_path(&shp_path).int_err()?;
        for rec in reader.iter_shapes_and_records() {
            let (shape, record) = rec.int_err()?;

            let geometry: geo_types::Geometry = shape.try_into().int_err()?;
            let feature = geojson::Feature {
                bbox: None,
                geometry: Some(geojson::Geometry {
                    value: geojson::Value::from(&geometry),
                    bbox: None,
                    foreign_members: None,
                }),
                id: None,
                properties: None,
                foreign_members: None,
            };

            let mut json = self.shp_record_to_json(record);
            json.insert(
                "geometry".to_string(),
                JsonValue::String(feature.to_string()),
            );

            serde_json::to_writer(&mut file, &json).int_err()?;
            writeln!(&mut file).int_err()?;
        }

        file.flush().int_err()?;

        let options = NdJsonReadOptions {
            file_extension: "json",
            table_partition_cols: Vec::new(),
            schema: schema.as_ref(),
            schema_infer_max_records: 1000,
            file_compression_type: FileCompressionType::UNCOMPRESSED,
            infinite: false,
        };

        let df = ctx
            .read_json(temp_json_path.to_str().unwrap(), options)
            .await
            .int_err()?;

        Ok(df)
    }
}
