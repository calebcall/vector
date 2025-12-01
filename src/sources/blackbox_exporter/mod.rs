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

    /// Geohash of the probe location.
    ///
    /// This label will be added to all scraped metrics to identify the precise
    /// geographic location of the probe.
    #[configurable(metadata(docs::examples = "9qx7hh9jd"))]
    #[configurable(metadata(docs::examples = "u4pruydqqvj"))]
    geohash: Option<String>,

    /// Probe region (e.g., AMER, EMEA, APAC).
    ///
    /// This label will be added to all scraped metrics to identify which broad
    /// geographic region the probe is in.
    #[configurable(metadata(docs::examples = "AMER"))]
    #[configurable(metadata(docs::examples = "EMEA"))]
    #[configurable(metadata(docs::examples = "APAC"))]
    region: Option<String>,

    /// Probe location (city or location name).
    ///
    /// This label will be added to all scraped metrics to identify the specific
    /// city or location name of the probe.
    #[configurable(metadata(docs::examples = "Paris"))]
    #[configurable(metadata(docs::examples = "New York"))]
    #[configurable(metadata(docs::examples = "Oregon"))]
    location: Option<String>,

    /// Two-digit country code.
    ///
    /// This label will be added to all scraped metrics to identify which country
    /// the probe is located in.
    #[configurable(metadata(docs::examples = "US"))]
    #[configurable(metadata(docs::examples = "CA"))]
    #[configurable(metadata(docs::examples = "FR"))]
    country: Option<String>,

    /// Check friendly name.
    ///
    /// This label will be added to all scraped metrics to give the check a
    /// friendly, human-readable identifier.
    #[configurable(metadata(docs::examples = "Google"))]
    #[configurable(metadata(docs::examples = "Homepage"))]
    #[configurable(metadata(docs::examples = "API Health"))]
    name: Option<String>,

    /// Infrastructure provider.
    ///
    /// This label will be added to all scraped metrics to identify which
    /// infrastructure provider the probe is running on.
    #[configurable(metadata(docs::examples = "AWS"))]
    #[configurable(metadata(docs::examples = "GCP"))]
    #[configurable(metadata(docs::examples = "AZURE"))]
    provider: Option<String>,

    /// Additional custom labels to add to all metrics.
    ///
    /// This allows you to add arbitrary key-value pairs as labels to all scraped
    /// metrics. These labels are added after the predefined optional labels.
    #[configurable(metadata(
        docs::additional_props_description = "An arbitrary key-value pair to add as a label."
    ))]
    labels: Option<HashMap<String, String>>,
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
            geohash: Some("9qx7hh9jd".to_string()),
            region: Some("AMER".to_string()),
            location: Some("Oregon".to_string()),
            country: Some("US".to_string()),
            name: Some("Example Check".to_string()),
            provider: Some("AWS".to_string()),
            labels: Some(HashMap::from([
                ("environment".to_string(), "production".to_string()),
                ("team".to_string(), "platform".to_string()),
            ])),
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
            optional_labels: OptionalLabels::from_config(self),
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

/// Optional labels to add to all scraped metrics.
///
/// This struct holds both predefined optional labels (geohash, region, location,
/// country, name, provider) and custom ad-hoc labels from the labels map.
#[derive(Clone, Debug)]
struct OptionalLabels {
    geohash: Option<String>,
    region: Option<String>,
    location: Option<String>,
    country: Option<String>,
    name: Option<String>,
    provider: Option<String>,
    custom: HashMap<String, String>,
}

impl OptionalLabels {
    /// Constructs OptionalLabels from BlackboxExporterConfig.
    fn from_config(config: &BlackboxExporterConfig) -> Self {
        Self {
            geohash: config.geohash.clone(),
            region: config.region.clone(),
            location: config.location.clone(),
            country: config.country.clone(),
            name: config.name.clone(),
            provider: config.provider.clone(),
            custom: config.labels.clone().unwrap_or_default(),
        }
    }
}

/// Captures the configuration options required to build request-specific context.
#[derive(Clone)]
struct BlackboxExporterBuilder {
    module: String,
    optional_labels: OptionalLabels,
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
            optional_labels: self.optional_labels.clone(),
        }
    }
}

/// Request-specific context required for decoding into events.
struct BlackboxExporterContext {
    target: String,
    module: String,
    #[allow(dead_code)]
    optional_labels: OptionalLabels,
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

            // Add predefined optional labels (after target and module)
            self.add_optional_label(metric, "geohash", &self.optional_labels.geohash);
            self.add_optional_label(metric, "region", &self.optional_labels.region);
            self.add_optional_label(metric, "location", &self.optional_labels.location);
            self.add_optional_label(metric, "country", &self.optional_labels.country);
            self.add_optional_label(metric, "name", &self.optional_labels.name);
            self.add_optional_label(metric, "provider", &self.optional_labels.provider);

            // Add ad-hoc custom labels
            // Ad-hoc labels override predefined labels with the same key
            for (key, value) in &self.optional_labels.custom {
                self.add_optional_label(metric, key, &Some(value.clone()));
            }
        }
    }
}

impl BlackboxExporterContext {
    /// Adds an optional label to a metric if the value is present and non-empty.
    ///
    /// If the metric already contains a tag with the same key, the existing tag
    /// is renamed to `exported_<key>` before adding the new tag.
    ///
    /// Empty string values are skipped and no tag is added.
    fn add_optional_label(
        &self,
        metric: &mut vector_lib::event::Metric,
        key: &str,
        value: &Option<String>,
    ) {
        if let Some(val) = value {
            // Skip empty strings
            if val.is_empty() {
                return;
            }

            // Handle conflicts by renaming existing tag
            if let Some(existing) = metric.remove_tag(key) {
                metric.replace_tag(format!("exported_{}", key), existing);
            }

            // Add new tag
            metric.replace_tag(key.to_string(), val.clone());
        }
    }
}
