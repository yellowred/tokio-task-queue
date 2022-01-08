use serde::Deserialize;
use tracing_subscriber::{fmt::Subscriber, EnvFilter};

#[derive(Deserialize)]
#[serde(tag = "type")]
pub enum LogType {
    Syslog {
        process: Option<String>,
        hostname: Option<String>,
    },
    Stdout,
}

#[derive(Deserialize)]
pub struct Log {
    pub level: String,
    pub structured: bool,
    pub backend: LogType,
}

/// setup log an overidde flag, an optional environment filter, and the config file
///
/// if the environemnt filter is present, then the config is not used
pub fn setup(env_filter: Result<EnvFilter, tracing_subscriber::filter::FromEnvError>) {
    match env_filter {
        Ok(env_filter) => {
            let sbuilder = Subscriber::builder()
                .with_timer(tracing_subscriber::fmt::time::ChronoUtc::rfc3339())
                .with_level(true)
                .with_env_filter(env_filter);
            let ss = sbuilder.with_ansi(true).finish();
            tracing::subscriber::set_global_default(ss)
                .expect("setting tracing default subscriber failed");
        }
        Err(_) => {}
    };
}
