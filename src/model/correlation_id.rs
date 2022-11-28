//! Correlation-ID is a UUID to use for correlation logs/query together

use hyper::HeaderMap;
use serde_derive::{Deserialize, Serialize};
use std::convert::TryFrom;
use thiserror::*;
use uuid::{fmt::Hyphenated, Uuid};

/// Correlation-ID for correlating logs together
#[derive(Clone, Debug, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub struct CorrelationId(Uuid);

impl std::fmt::Display for CorrelationId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TryFrom<&[u8]> for CorrelationId {
    type Error = InvalidCorrelationId;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Uuid::from_slice(value)
            .map(|uuid| CorrelationId(uuid))
            .map_err(|_| InvalidCorrelationId::InvalidBytes(value.to_vec()))
    }
}

impl From<Uuid> for CorrelationId {
    fn from(c: Uuid) -> Self {
        CorrelationId(c)
    }
}

impl From<CorrelationId> for Uuid {
    fn from(c: CorrelationId) -> Self {
        c.0
    }
}

impl From<CorrelationId> for [u8; 16] {
    fn from(c: CorrelationId) -> Self {
        c.0.as_bytes().clone()
    }
}

impl<'a> TryFrom<&'a str> for CorrelationId {
    type Error = InvalidCorrelationId;

    fn try_from(input: &'a str) -> Result<Self, Self::Error> {
        Uuid::parse_str(input)
            .map(CorrelationId)
            .map_err(|_| InvalidCorrelationId::InvalidString(input.to_string()))
    }
}

/// Creates correlation-id from a String
///
/// # Examples
///
/// ```
/// # use grcon::CorrelationId;
/// # use uuid::Uuid;
/// # use std::convert::TryFrom;
///
/// let cid = CorrelationId::try_from(&"02497eac-edab-4d96-9f6c-a2c8c1766dee".to_string());
/// assert!(cid.is_ok());
/// ```
impl<'a> TryFrom<&'a String> for CorrelationId {
    type Error = InvalidCorrelationId;

    fn try_from(input: &'a String) -> Result<Self, Self::Error> {
        Uuid::parse_str(input)
            .map(CorrelationId)
            .map_err(|_| InvalidCorrelationId::InvalidString(input.clone()))
    }
}

#[derive(Debug, Error)]
pub enum InvalidCorrelationId {
    #[error("correlation-id not found")]
    NotFound(),
    #[error("Invalid correlation-id bytes {0:?}")]
    InvalidBytes(Vec<u8>),
    #[error("Invalid correlation-id string {0}")]
    InvalidString(String),
}

fn tonic_status_missing_correlation_id() -> tonic::Status {
    tonic::Status::new(
        tonic::Code::InvalidArgument,
        format!("correlation-id header is missing"),
    )
}

fn tonic_status_invalid_correlation_id() -> tonic::Status {
    tonic::Status::new(
        tonic::Code::InvalidArgument,
        format!("correlation-id header is in the wrong format, expecting UUID"),
    )
}

impl CorrelationId {
    pub const HEADER_NAME: &'static str = "correlation-id";

    pub fn from_tonic_metadata(
        metadata: &tonic::metadata::MetadataMap,
    ) -> Result<Self, tonic::Status> {
        let correlation_id_r = metadata
            .get(Self::HEADER_NAME)
            .ok_or(tonic_status_missing_correlation_id())
            .and_then(|m| {
                m.to_str()
                    .map_err(|__| tonic_status_invalid_correlation_id())
            })
            .and_then(|input| {
                uuid::Uuid::parse_str(input).map_err(|_| tonic_status_invalid_correlation_id())
            });
        match correlation_id_r {
            Ok(cid) => Ok(cid.into()),
            Err(_) => Ok(CorrelationId(Uuid::nil())),
        }
    }

    pub fn append_to_metadata(&self, metadata_map: &mut tonic::metadata::MetadataMap) {
        metadata_map.append(
            Self::HEADER_NAME,
            tonic::metadata::MetadataValue::from_str(&format!("{}", self.0)).unwrap(),
        );
    }

    /// Extract correlation-id from a set of HTTP headers
    ///
    /// # Examples
    ///
    /// Basic usage
    ///
    /// ```
    /// # use http::{HeaderMap, HeaderValue};
    /// # use grcon::CorrelationId;
    /// # use uuid::Uuid;
    /// let mut headers = HeaderMap::new();
    ///
    /// let cid = CorrelationId::from(uuid::Uuid::new_v4());
    /// cid.insert_into_header_map(&mut headers);
    /// let cid_extracted = CorrelationId::from_header_map(&headers);
    ///
    /// assert_eq!(cid_extracted.unwrap(), cid);
    /// ```
    pub fn from_header_map(h: &HeaderMap) -> Result<Self, InvalidCorrelationId> {
        let res = h
            .get(Self::HEADER_NAME)
            .ok_or(InvalidCorrelationId::NotFound())
            .and_then(|x| {
                x.to_str()
                    .map_err(|err| InvalidCorrelationId::InvalidString(err.to_string()))
            })
            .and_then(|x| {
                uuid::Uuid::parse_str(x)
                    .map_err(|err| InvalidCorrelationId::InvalidString(err.to_string()))
            })
            .map(|cid| cid.into());
        res
    }

    pub fn insert_into_header_map(&self, h: &mut HeaderMap) -> anyhow::Result<()> {
        h.insert(
            Self::HEADER_NAME,
            http::HeaderValue::from_str(
                self.as_hyphenated()
                    .encode_lower(&mut Uuid::encode_buffer()),
            )?,
        );
        Ok(())
    }

    pub fn as_hyphenated(&self) -> Hyphenated {
        self.0.hyphenated()
    }
}
