use serde::Deserialize;
use std::io::Read;
use std::{
    fs::File,
    path::{Path, PathBuf},
};
use thiserror::*;

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("io error {0} when reading config")]
    IoError(#[from] std::io::Error),
    #[error("cannot open config file '{0}' : {1}")]
    OpeningError(PathBuf, std::io::Error),
    #[error("UTF8 format error when reading config")]
    Utf8Error,
    #[error("format error {0} when reading config")]
    FormatError(#[from] serde_yaml::Error),
}

#[derive(Clone, Deserialize)]
pub struct Listen {
    pub ipv6: Option<bool>,
    pub host: Option<String>,
    pub port: u16,
    pub concurrent: Option<usize>,
    pub timeout: Option<u64>,
    pub tls: bool,
    pub identity_cert: Option<PathBuf>,
    pub identity_key: Option<PathBuf>,
    pub cafile: Option<PathBuf>,
}

#[derive(Clone, Deserialize)]
pub struct StorageConfig {
    pub url: String,
}

#[derive(Clone, Deserialize)]
pub struct KeycloakConfig {
    pub url: String,
    pub realm: String,
    pub client_id: String,
    pub secret: String,
    pub timeout_millis: u64,
    pub token_timeout_millis: i64,
    pub group_id: String,
}

#[derive(Clone, Deserialize)]
pub struct HexSafe {
    pub url: String,
    pub api_key: String,
    pub secret: String,
    pub account_id: i32,
    pub webhook_signer_address: String,
    pub client_id: i32,
    pub webhook_sig_verification_enabled: bool,
}

#[derive(Clone, Deserialize)]
pub struct TalosConfig {
    pub host: String,
    pub api_key: String,
    pub secret: String,
}

#[derive(Clone, Deserialize)]
pub struct HtmBackendConfig {
    pub url: String,
}

#[derive(Clone, Deserialize)]
pub struct BookKeepingConfig {
    pub engine_url: String,
    pub service_url: String,
}

#[derive(Clone, Deserialize)]
pub struct ExecutorConfig {
    pub services: ExecutorServicesConfig,
}

#[derive(Clone, Deserialize)]
pub struct ExecutorServicesConfig {
    pub hex_safe: HexSafe,
    pub htm_backend: HtmBackendConfig,
    pub keycloak: KeycloakConfig,
    pub talos: TalosConfig,
    pub book_keeping: BookKeepingConfig,
}

#[derive(Clone, Deserialize)]
pub struct InstanceConfig {
    pub alive_timeout_sec: u32,
}

#[derive(Deserialize)]
pub struct Config {
    pub listen: Listen,
    pub storage: StorageConfig,
    pub log: Option<crate::log::Log>,
    pub executor: ExecutorConfig,
    pub instance: InstanceConfig,
}

impl Config {
    pub fn from_str(s: &str) -> Result<Self, serde_yaml::Error> {
        serde_yaml::from_str(&s)
    }

    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let p = path.as_ref();
        let mut file = File::open(p).map_err(|e| ConfigError::OpeningError(p.to_owned(), e))?;
        let mut contents = vec![];
        file.read_to_end(&mut contents)?;
        let contents = String::from_utf8(contents).map_err(|_| ConfigError::Utf8Error)?;
        let config = Config::from_str(&contents)?;
        Ok(config)
    }
}

pub struct RetryConfig {
    pub max_retries: u32,
    pub interval_ms: u64,
    pub max_interval_ms: u64,
    pub deadline_ms: u64,
}

impl RetryConfig {
    pub fn new(max_retries: u32, interval_ms: u64, max_interval_ms: u64, deadline_ms: u64) -> Self {
        Self {
            max_retries,
            interval_ms,
            max_interval_ms,
            deadline_ms,
        }
    }

    pub fn for_task(task_name: &str) -> RetryConfig {
        // TODO implement query config file for numbers
        // TODO or maybe in DB for live reload
        match task_name {
            "htm_confirm_transaction" => RetryConfig::new(5000, 300000, 86400000, 86400000),
            _ => RetryConfig::new(10, 20, 86400000, 300000),
        }
    }
}

pub mod testdata {
    use super::Config;

    #[allow(dead_code)]
    pub fn test_config() -> Config {
        Config::from_str(
            r#"
        log:
            level: trace
            backend:
                type: Syslog
            structured: false
        instance:
            alive_timeout_sec: 5
        storage:
            url: "mysql://root:root!1AA@localhost:3306/iris"
        listen:
            port: 50055
            concurrent: 10
            timeout: 30
            tls: false
        executor:
            services:
                keycloak:
                    url: http://localhost
                    client_id: admin-cli
                    realm: htm
                    secret: 
                    timeout_millis: 3000
                    token_timeout_millis: 30000
                    group_id: 3b27193d-43d7-4551-b685-a2127b35edb4
                talos:
                    host: http://localhost
                    api_key: 
                    secret: 
                hex_safe:
                    url: http://localhost
                    api_key: 
                    secret: 
                    account_id: 34
                    client_id: 15
                    webhook_signer_address: "0xf4d3549ee3457744a9a5aebd7ec1c9749c3c81f9"
                    webhook_sig_verification_enabled: false
                htm_backend:
                    url: http://127.0.0.1:3000
                book_keeping:
                    engine_url: http://127.0.0.1:8080
                    service_url: http://127.0.0.1:8081
        "#,
        )
        .unwrap()
    }
}
