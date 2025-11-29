#![allow(missing_docs)]

use std::{collections::HashMap, time::Duration};

use bytes::Bytes;
use futures_util::FutureExt;
use http::{Uri, response::Parts};
use serde_with::serde_as;
use snafu::ResultExt;
use vector_lib::{config::LogNamespace, configurable::configurable_component, event::Event};

use crate::{
    Result,
    config::{GenerateConfig, SourceConfig, SourceContext, SourceOutput},
    http::Auth,
    internal_events::PrometheusParseError,
    sources::{
        self,
        prometheus::parser,
        util::{
            http::HttpMethod,
            http_client::{
                GenericHttpClientInputs, HttpClientBuilder, HttpClientContext, call,
                default_interval, default_timeout, warn_if_interval_too_low,
            },
        },
    },
    tls::{TlsConfig, TlsSettings},
};

/// Configuration for the `blackbox_exporter` source.
///
/// This source scrapes metrics from Prometheus Blackbox Exporter instances, automatically
/// constructing probe URLs and injecting target and module labels into the scraped metrics.
///
/// The Blackbox Exporter allows blackbox probing of endpoints over HTTP, HTTPS, DNS, TCP,
/// and ICMP. This source simplifies the configuration compared to using the generic
/// `prometheus_scrape` source by handling URL construction and label injection automatically.
#[serde_as]
#[configurable_component(source(
    "blackbox_exporter",
    "Collect metrics from Prometheus Blackbox Exporter probes."
))]
#[derive(Clone, Debug)]
pub struct BlackboxExporterConfig {
    /// URL of the Blackbox Exporter instance.
    ///
    /// This is the base URL where the Blackbox Exporter is running. The source will
    /// automatically append `/probe` to this URL along with the target and module
    /// query parameters.
    #[configurable(metadata(docs::examples = "http://localhost:9115"))]
    #[configurable(metadata(docs::examples = "http://blackbox.example.com"))]
    #[configurable(metadata(docs::examples = "https://blackbox.nodexeus.io"))]
    url: String,

    /// List of targets to probe.
    ///
    /// Each target will be probed using the specified module. Targets can be URLs,
    /// hostnames, or IP addresses depending on the module type. All targets are
    /// scraped concurrently at the configured interval.
    #[configurable(metadata(docs::examples = "https://example.com"))]
    #[configurable(metadata(docs::examples = "https://app.example.com"))]
    #[configurable(metadata(docs::examples = "https://api.example.com"))]
    #[configurable(metadata(docs::examples = "8.8.8.8"))]
    targets: Vec<String>,

    /// Blackbox Exporter module to use for probing.
    ///
    /// This should match a module defined in the Blackbox Exporter configuration.
    /// Common modules include `http_2xx` for HTTP probes, `icmp` for ICMP pings,
    /// and `tcp_connect` for TCP connection checks.
    #[configurable(metadata(docs::examples = "http_2xx"))]
    #[configurable(metadata(docs::examples = "icmp"))]
    #[configurable(metadata(docs::examples = "tcp_connect"))]
    module: String,

    /// The interval between scrapes.
    ///
    /// Requests are run concurrently so if a scrape takes longer than the interval,
    /// a new scrape will be started. This can take extra resources; set the timeout
    /// to a value lower than the scrape interval to prevent this from happening.
    #[serde(default = "default_interval")]
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    #[serde(rename = "scrape_interval_secs")]
    #[configurable(metadata(docs::human_name = "Scrape Interval"))]
    interval: Duration,

    /// The timeout for each scrape request.
    #[serde(default = "default_timeout")]
    #[serde_as(as = "serde_with::DurationSecondsWithFrac<f64>")]
    #[serde(rename = "scrape_timeout_secs")]
    #[configurable(metadata(docs::human_name = "Scrape Timeout"))]
    timeout: Duration,

    /// TLS configuration for connecting to the Blackbox Exporter.
    #[configurable(derived)]
    tls: Option<TlsConfig>,

    /// Authentication configuration for connecting to the Blackbox Exporter.
    #[configurable(derived)]
    #[configurable(metadata(docs::advanced))]
    auth: Option<Auth>,
}

impl GenerateConfig for BlackboxExporterConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            url: "http://localhost:9115".to_string(),
            targets: vec!["https://example.com".to_string()],
            module: "http_2xx".to_string(),
            interval: default_interval(),
            timeout: default_timeout(),
            tls: None,
            auth: None,
        })
        .unwrap()
    }
}

#[cfg(test)]
mod tests;

#[async_trait::async_trait]
#[typetag::serde(name = "blackbox_exporter")]
impl SourceConfig for BlackboxExporterConfig {
    async fn build(&self, cx: SourceContext) -> Result<sources::Source> {
        // Parse and validate base URL
        let base_url = self.url.parse::<Uri>().context(sources::UriParseSnafu)?;

        // Construct probe URLs for all targets
        let urls = self
            .targets
            .iter()
            .map(|target| {
                construct_probe_url(&base_url, target, &self.module).context(sources::UriParseSnafu)
            })
            .collect::<std::result::Result<Vec<Uri>, sources::BuildError>>()?;

        // Create TLS settings from configuration
        let tls = TlsSettings::from_options(self.tls.as_ref())?;

        // Create BlackboxExporterBuilder instance
        let builder = BlackboxExporterBuilder {
            module: self.module.clone(),
        };

        // Emit warning if timeout >= interval
        warn_if_interval_too_low(self.timeout, self.interval);

        // Set up GenericHttpClientInputs with all probe URLs
        let inputs = GenericHttpClientInputs {
            urls,
            interval: self.interval,
            timeout: self.timeout,
            headers: HashMap::new(),
            content_type: "text/plain".to_string(),
            auth: self.auth.clone(),
            tls,
            proxy: cx.proxy.clone(),
            shutdown: cx.shutdown,
        };

        // Call http_client::call() with inputs and builder
        Ok(call(inputs, builder, cx.out, HttpMethod::Get).boxed())
    }

    fn outputs(&self, _global_log_namespace: LogNamespace) -> Vec<SourceOutput> {
        vec![SourceOutput::new_metrics()]
    }

    fn can_acknowledge(&self) -> bool {
        false
    }
}

/// Constructs a probe URL from base URL, target, and module.
///
/// Format: `<base_url>/probe?target=<encoded_target>&module=<encoded_module>`
fn construct_probe_url(
    base_url: &Uri,
    target: &str,
    module: &str,
) -> std::result::Result<Uri, http::uri::InvalidUri> {
    use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};

    let scheme = base_url.scheme_str().unwrap_or("http");
    let authority = base_url.authority().map(|a| a.as_str()).unwrap_or("");
    let path = base_url.path();

    // Preserve existing path and append /probe
    let probe_path = if path.is_empty() || path == "/" {
        "/probe".to_string()
    } else {
        format!("{}/probe", path.trim_end_matches('/'))
    };

    // URL encode target and module
    let encoded_target = utf8_percent_encode(target, NON_ALPHANUMERIC).to_string();
    let encoded_module = utf8_percent_encode(module, NON_ALPHANUMERIC).to_string();

    // Preserve existing query parameters
    let query = if let Some(existing_query) = base_url.query() {
        format!(
            "{}&target={}&module={}",
            existing_query, encoded_target, encoded_module
        )
    } else {
        format!("target={}&module={}", encoded_target, encoded_module)
    };

    // Construct the full URL
    let url_string = format!("{}://{}{}?{}", scheme, authority, probe_path, query);
    url_string.parse()
}

/// Captures the configuration options required to build request-specific context.
#[derive(Clone)]
struct BlackboxExporterBuilder {
    module: String,
}

impl HttpClientBuilder for BlackboxExporterBuilder {
    type Context = BlackboxExporterContext;

    fn build(&self, url: &Uri) -> Self::Context {
        // Extract target from URL query parameters and decode it
        let target = url
            .query()
            .and_then(|q| {
                q.split('&')
                    .find(|param| param.starts_with("target="))
                    .and_then(|param| param.strip_prefix("target="))
            })
            .and_then(|encoded| {
                percent_encoding::percent_decode_str(encoded)
                    .decode_utf8()
                    .ok()
            })
            .map(|decoded| decoded.to_string())
            .unwrap_or_default();

        BlackboxExporterContext {
            target,
            module: self.module.clone(),
        }
    }
}

/// Request-specific context required for decoding into events.
struct BlackboxExporterContext {
    target: String,
    module: String,
}

impl HttpClientContext for BlackboxExporterContext {
    fn on_response(&mut self, url: &Uri, _header: &Parts, body: &Bytes) -> Option<Vec<Event>> {
        // Parse Prometheus text format
        // Internal events emitted:
        // - PrometheusParseError: Emitted when parsing fails (handled here)
        // - EndpointBytesReceived: Emitted by http_client::call() on successful scrapes
        // - HttpClientEventsReceived: Emitted by http_client::call() after parsing succeeds
        // - HttpClientHttpError: Emitted by http_client::call() on HTTP errors
        // - HttpClientHttpResponseError: Emitted by http_client::call() on non-200 responses
        let body_str = String::from_utf8_lossy(body.as_ref());
        match parser::parse_text(&body_str) {
            Ok(events) => Some(events),
            Err(error) => {
                emit!(PrometheusParseError {
                    error,
                    url: url.clone(),
                    body: body_str,
                });
                None
            }
        }
    }

    fn enrich_events(&mut self, events: &mut Vec<Event>) {
        for event in events.iter_mut() {
            let metric = event.as_mut_metric();

            // Handle target tag
            if let Some(existing_target) = metric.remove_tag("target") {
                // Rename existing tag to exported_target
                metric.replace_tag("exported_target".to_string(), existing_target);
            }
            // Add new target tag
            metric.replace_tag("target".to_string(), self.target.clone());

            // Handle module tag
            if let Some(existing_module) = metric.remove_tag("module") {
                // Rename existing tag to exported_module
                metric.replace_tag("exported_module".to_string(), existing_module);
            }
            // Add new module tag
            metric.replace_tag("module".to_string(), self.module.clone());
        }
    }
}
