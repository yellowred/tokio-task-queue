use serde::Deserialize;
use tracing::Level;
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
pub fn setup(
    override_debug: bool,
    env_filter: Result<EnvFilter, tracing_subscriber::filter::FromEnvError>,
    clog: &Option<Log>,
) {
    match (env_filter, &clog) {
        (Ok(env_filter), _) => {
            let sbuilder = Subscriber::builder()
                .with_timer(tracing_subscriber::fmt::time::ChronoUtc::rfc3339())
                .with_level(true)
                .with_env_filter(env_filter);
            let structured = clog.as_ref().map(|clog| clog.structured).unwrap_or(false);
            if structured {
                let ss = sbuilder.json().finish();
                tracing::subscriber::set_global_default(ss)
                    .expect("setting tracing default subscriber failed");
            } else {
                let ss = sbuilder.with_ansi(true).finish();
                tracing::subscriber::set_global_default(ss)
                    .expect("setting tracing default subscriber failed");
            }
        }
        (Err(_), None) => {}
        (Err(_), Some(clog)) => {
            let log_level = if override_debug {
                Level::DEBUG
            } else {
                match clog.level.as_ref() {
                    "trace" => Level::TRACE,
                    "debug" => Level::DEBUG,
                    "info" => Level::INFO,
                    "error" => Level::ERROR,
                    "warn" => Level::WARN,
                    _ => Level::INFO,
                }
            };
            let sbuilder = Subscriber::builder()
                .with_timer(tracing_subscriber::fmt::time::ChronoUtc::rfc3339())
                .with_level(true)
                .with_max_level(log_level);

            if clog.structured {
                let ss = sbuilder.json().finish();
                tracing::subscriber::set_global_default(ss)
                    .expect("setting tracing default subscriber failed");
            } else {
                let ss = sbuilder.with_ansi(true).finish();
                tracing::subscriber::set_global_default(ss)
                    .expect("setting tracing default subscriber failed");
            };
        }
    };
}
