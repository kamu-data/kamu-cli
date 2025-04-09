use std::error;
use std::fmt;

#[derive(Debug, Clone)]
pub struct ResponseContent<T> {
    pub status: reqwest::StatusCode,
    pub content: String,
    pub entity: Option<T>,
}

#[derive(Debug)]
pub enum Error<T> {
    Reqwest(reqwest::Error),
    Serde(serde_json::Error),
    Io(std::io::Error),
    ResponseError(ResponseContent<T>),
}

impl <T> fmt::Display for Error<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (module, e) = match self {
            Error::Reqwest(e) => ("reqwest", e.to_string()),
            Error::Serde(e) => ("serde", e.to_string()),
            Error::Io(e) => ("IO", e.to_string()),
            Error::ResponseError(e) => ("response", format!("status code {}", e.status)),
        };
        write!(f, "error in {}: {}", module, e)
    }
}

impl <T: fmt::Debug> error::Error for Error<T> {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        Some(match self {
            Error::Reqwest(e) => e,
            Error::Serde(e) => e,
            Error::Io(e) => e,
            Error::ResponseError(_) => return None,
        })
    }
}

impl <T> From<reqwest::Error> for Error<T> {
    fn from(e: reqwest::Error) -> Self {
        Error::Reqwest(e)
    }
}

impl <T> From<serde_json::Error> for Error<T> {
    fn from(e: serde_json::Error) -> Self {
        Error::Serde(e)
    }
}

impl <T> From<std::io::Error> for Error<T> {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

pub fn urlencode<T: AsRef<str>>(s: T) -> String {
    ::url::form_urlencoded::byte_serialize(s.as_ref().as_bytes()).collect()
}

pub fn parse_deep_object(prefix: &str, value: &serde_json::Value) -> Vec<(String, String)> {
    if let serde_json::Value::Object(object) = value {
        let mut params = vec![];

        for (key, value) in object {
            match value {
                serde_json::Value::Object(_) => params.append(&mut parse_deep_object(
                    &format!("{}[{}]", prefix, key),
                    value,
                )),
                serde_json::Value::Array(array) => {
                    for (i, value) in array.iter().enumerate() {
                        params.append(&mut parse_deep_object(
                            &format!("{}[{}][{}]", prefix, key, i),
                            value,
                        ));
                    }
                },
                serde_json::Value::String(s) => params.push((format!("{}[{}]", prefix, key), s.clone())),
                _ => params.push((format!("{}[{}]", prefix, key), value.to_string())),
            }
        }

        return params;
    }

    unimplemented!("Only objects are supported with style=deepObject")
}

/// Internal use only
/// A content type supported by this client.
#[allow(dead_code)]
enum ContentType {
    Json,
    Text,
    Unsupported(String)
}

impl From<&str> for ContentType {
    fn from(content_type: &str) -> Self {
        if content_type.starts_with("application") && content_type.contains("json") {
            return Self::Json;
        } else if content_type.starts_with("text/plain") {
            return Self::Text;
        } else {
            return Self::Unsupported(content_type.to_string());
        }
    }
}

pub mod kamu_api;
pub mod kamu_odata_api;
pub mod odf_core_api;
pub mod odf_query_api;
pub mod odf_transfer_api;

pub mod configuration;

use std::sync::Arc;

pub trait Api {
    fn kamu_api(&self) -> &dyn kamu_api::KamuApi;
    fn kamu_odata_api(&self) -> &dyn kamu_odata_api::KamuOdataApi;
    fn odf_core_api(&self) -> &dyn odf_core_api::OdfCoreApi;
    fn odf_query_api(&self) -> &dyn odf_query_api::OdfQueryApi;
    fn odf_transfer_api(&self) -> &dyn odf_transfer_api::OdfTransferApi;
}

pub struct ApiClient {
    kamu_api: Box<dyn kamu_api::KamuApi>,
    kamu_odata_api: Box<dyn kamu_odata_api::KamuOdataApi>,
    odf_core_api: Box<dyn odf_core_api::OdfCoreApi>,
    odf_query_api: Box<dyn odf_query_api::OdfQueryApi>,
    odf_transfer_api: Box<dyn odf_transfer_api::OdfTransferApi>,
}

impl ApiClient {
    pub fn new(configuration: Arc<configuration::Configuration>) -> Self {
        Self {
            kamu_api: Box::new(kamu_api::KamuApiClient::new(configuration.clone())),
            kamu_odata_api: Box::new(kamu_odata_api::KamuOdataApiClient::new(configuration.clone())),
            odf_core_api: Box::new(odf_core_api::OdfCoreApiClient::new(configuration.clone())),
            odf_query_api: Box::new(odf_query_api::OdfQueryApiClient::new(configuration.clone())),
            odf_transfer_api: Box::new(odf_transfer_api::OdfTransferApiClient::new(configuration.clone())),
        }
    }
}

impl Api for ApiClient {
    fn kamu_api(&self) -> &dyn kamu_api::KamuApi {
        self.kamu_api.as_ref()
    }
    fn kamu_odata_api(&self) -> &dyn kamu_odata_api::KamuOdataApi {
        self.kamu_odata_api.as_ref()
    }
    fn odf_core_api(&self) -> &dyn odf_core_api::OdfCoreApi {
        self.odf_core_api.as_ref()
    }
    fn odf_query_api(&self) -> &dyn odf_query_api::OdfQueryApi {
        self.odf_query_api.as_ref()
    }
    fn odf_transfer_api(&self) -> &dyn odf_transfer_api::OdfTransferApi {
        self.odf_transfer_api.as_ref()
    }
}


