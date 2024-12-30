// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::prelude::SessionContext;
use kamu_core::ingest::*;
use kamu_ingest_datafusion::*;
use odf::metadata::ReadStep;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DataFormatRegistryImpl {}

#[dill::component(pub)]
#[dill::interface(dyn DataFormatRegistry)]
impl DataFormatRegistryImpl {
    pub const FMT_CSV: DataFormatDesc = DataFormatDesc {
        short_name: "CSV",
        media_type: MediaType::CSV,
        file_extensions: &["csv"],
    };
    pub const FMT_JSON: DataFormatDesc = DataFormatDesc {
        short_name: "JSON",
        media_type: MediaType::JSON,
        file_extensions: &["json"],
    };
    pub const FMT_NDJSON: DataFormatDesc = DataFormatDesc {
        short_name: "NDJSON",
        media_type: MediaType::NDJSON,
        file_extensions: &["ndjson"],
    };
    pub const FMT_GEOJSON: DataFormatDesc = DataFormatDesc {
        short_name: "GeoJSON",
        media_type: MediaType::GEOJSON,
        file_extensions: &["geojson"],
    };
    pub const FMT_NDGEOJSON: DataFormatDesc = DataFormatDesc {
        short_name: "NDGeoJSON",
        media_type: MediaType::NDGEOJSON,
        file_extensions: &["ndgeojson"],
    };
    pub const FMT_PARQUET: DataFormatDesc = DataFormatDesc {
        short_name: "Parquet",
        media_type: MediaType::PARQUET,
        file_extensions: &["parquet"],
    };
    pub const FMT_ESRI_SHAPEFILE: DataFormatDesc = DataFormatDesc {
        short_name: "Shapefile",
        media_type: MediaType::ESRI_SHAPEFILE,
        file_extensions: &["shp", "shx"],
    };

    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl DataFormatRegistry for DataFormatRegistryImpl {
    fn list_formats(&self) -> Vec<DataFormatDesc> {
        vec![
            Self::FMT_CSV,
            Self::FMT_JSON,
            Self::FMT_NDJSON,
            Self::FMT_GEOJSON,
            Self::FMT_NDGEOJSON,
            Self::FMT_PARQUET,
            Self::FMT_ESRI_SHAPEFILE,
        ]
    }

    fn format_by_file_extension(&self, ext: &str) -> Option<DataFormatDesc> {
        let ext = ext.to_lowercase();
        for fmt in self.list_formats() {
            for fext in fmt.file_extensions {
                if *fext == ext {
                    return Some(fmt);
                }
            }
        }
        None
    }

    fn format_of(&self, conf: &ReadStep) -> DataFormatDesc {
        match conf {
            ReadStep::Csv(_) => Self::FMT_CSV,
            ReadStep::Json(_) => Self::FMT_JSON,
            ReadStep::NdJson(_) => Self::FMT_NDJSON,
            ReadStep::GeoJson(_) => Self::FMT_GEOJSON,
            ReadStep::NdGeoJson(_) => Self::FMT_NDGEOJSON,
            ReadStep::Parquet(_) => Self::FMT_PARQUET,
            ReadStep::EsriShapefile(_) => Self::FMT_ESRI_SHAPEFILE,
        }
    }

    async fn get_reader(
        &self,
        ctx: SessionContext,
        conf: ReadStep,
        temp_path: PathBuf,
    ) -> Result<Arc<dyn Reader>, ReadError> {
        let reader: Arc<dyn Reader> = match conf {
            ReadStep::Csv(conf) => Arc::new(ReaderCsv::new(ctx, conf).await?),
            ReadStep::Json(conf) => Arc::new(ReaderJson::new(ctx, conf, temp_path).await?),
            ReadStep::NdJson(conf) => Arc::new(ReaderNdJson::new(ctx, conf).await?),
            ReadStep::GeoJson(conf) => Arc::new(ReaderGeoJson::new(ctx, conf, temp_path).await?),
            ReadStep::NdGeoJson(conf) => {
                Arc::new(ReaderNdGeoJson::new(ctx, conf, temp_path).await?)
            }
            ReadStep::EsriShapefile(conf) => {
                Arc::new(ReaderEsriShapefile::new(ctx, conf, temp_path).await?)
            }
            ReadStep::Parquet(conf) => Arc::new(ReaderParquet::new(ctx, conf).await?),
        };

        Ok(reader)
    }

    fn get_compatible_read_config(
        &self,
        base_conf: ReadStep,
        actual_media_type: &MediaType,
    ) -> Result<ReadStep, UnsupportedMediaTypeError> {
        let base_format = self.format_of(&base_conf);

        if base_format.media_type == *actual_media_type {
            return Ok(base_conf);
        }

        // Perform best-effort conversion
        let schema = base_conf.schema().cloned();
        self.get_best_effort_config(schema, actual_media_type)
    }

    fn get_best_effort_config(
        &self,
        schema: Option<Vec<String>>,
        media_type: &MediaType,
    ) -> Result<ReadStep, UnsupportedMediaTypeError> {
        use odf::metadata::*;
        match MediaTypeRef(media_type.0.as_str()) {
            MediaType::CSV => Ok(ReadStepCsv {
                // Assuming header is present if we don't have any previous schema.
                // TODO: This should be replaced with proper inference
                // See: https://github.com/kamu-data/kamu-node/issues/99
                header: Some(schema.is_none()),
                schema,
                ..Default::default()
            }
            .into()),
            MediaType::JSON => Ok(ReadStepJson {
                schema,
                ..Default::default()
            }
            .into()),
            MediaType::NDJSON => Ok(ReadStepNdJson {
                schema,
                ..Default::default()
            }
            .into()),
            MediaType::GEOJSON => Ok(ReadStepGeoJson { schema }.into()),
            MediaType::NDGEOJSON => Ok(ReadStepNdGeoJson { schema }.into()),
            MediaType::PARQUET => Ok(ReadStepParquet { schema }.into()),
            MediaType::ESRI_SHAPEFILE | MediaTypeRef("x-gis/x-shapefile") => {
                Ok(ReadStepEsriShapefile {
                    schema,
                    ..Default::default()
                }
                .into())
            }
            _ => Err(UnsupportedMediaTypeError::new(media_type.clone())),
        }
    }
}
