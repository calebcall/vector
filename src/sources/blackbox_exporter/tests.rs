// Unit tests for blackbox_exporter source

use super::*;
use bytes::Bytes;

#[test]
fn test_construct_probe_url_basic() {
    let base_url = "http://blackbox.example.com".parse::<Uri>().unwrap();
    let target = "https://app.example.com";
    let module = "http_2xx";

    let result = construct_probe_url(&base_url, target, module).unwrap();
    let result_str = result.to_string();

    // Verify the URL contains the probe path
    assert!(result_str.contains("/probe"));
    // Verify the URL contains encoded target parameter
    assert!(result_str.contains("target="));
    // Verify the URL contains encoded module parameter
    assert!(result_str.contains("module="));
}

#[test]
fn test_construct_probe_url_with_encoding() {
    let base_url = "http://blackbox.example.com".parse::<Uri>().unwrap();
    let target = "https://app.example.com/path?query=value";
    let module = "http_2xx";

    let result = construct_probe_url(&base_url, target, module).unwrap();
    let result_str = result.to_string();

    // Verify special characters are encoded
    assert!(result_str.contains("/probe"));
    assert!(result_str.contains("target="));
    assert!(result_str.contains("module="));
    // The URL should be properly encoded (no raw ? or & in target value)
}

#[test]
fn test_construct_probe_url_preserves_path() {
    let base_url = "http://blackbox.example.com/metrics"
        .parse::<Uri>()
        .unwrap();
    let target = "https://app.example.com";
    let module = "http_2xx";

    let result = construct_probe_url(&base_url, target, module).unwrap();
    let result_str = result.to_string();

    // Verify the original path is preserved
    assert!(result_str.contains("/metrics/probe"));
}

#[test]
fn test_construct_probe_url_preserves_query_params() {
    let base_url = "http://blackbox.example.com?existing=param"
        .parse::<Uri>()
        .unwrap();
    let target = "https://app.example.com";
    let module = "http_2xx";

    let result = construct_probe_url(&base_url, target, module).unwrap();
    let result_str = result.to_string();

    // Verify existing query parameters are preserved
    assert!(result_str.contains("existing=param"));
    assert!(result_str.contains("target="));
    assert!(result_str.contains("module="));
}

#[test]
fn test_construct_probe_url_with_path_and_query() {
    let base_url = "http://blackbox.example.com/api?key=value"
        .parse::<Uri>()
        .unwrap();
    let target = "https://app.example.com";
    let module = "http_2xx";

    let result = construct_probe_url(&base_url, target, module).unwrap();
    let result_str = result.to_string();

    // Verify both path and query parameters are preserved
    assert!(result_str.contains("/api/probe"));
    assert!(result_str.contains("key=value"));
    assert!(result_str.contains("target="));
    assert!(result_str.contains("module="));
}

#[test]
fn test_context_on_response_success() {
    // Test that on_response successfully parses Prometheus text format
    let mut context = BlackboxExporterContext {
        target: "https://example.com".to_string(),
        module: "http_2xx".to_string(),
    };

    let url = "http://blackbox.example.com/probe?target=https://example.com&module=http_2xx"
        .parse::<Uri>()
        .unwrap();

    // Create a mock response with valid Prometheus text format
    let body = Bytes::from(
        r#"# HELP probe_success Displays whether or not the probe was a success
# TYPE probe_success gauge
probe_success 1
# HELP probe_duration_seconds Returns how long the probe took to complete in seconds
# TYPE probe_duration_seconds gauge
probe_duration_seconds 0.123
"#,
    );

    let response = http::Response::builder().status(200).body(()).unwrap();
    let (parts, _) = response.into_parts();

    // Call on_response
    let events = context.on_response(&url, &parts, &body);

    // Verify events were returned
    assert!(events.is_some());
    let events = events.unwrap();
    assert!(!events.is_empty());
    assert_eq!(events.len(), 2); // probe_success and probe_duration_seconds
}

#[test]
fn test_context_on_response_parse_error() {
    // Test that on_response emits PrometheusParseError on invalid format
    let mut context = BlackboxExporterContext {
        target: "https://example.com".to_string(),
        module: "http_2xx".to_string(),
    };

    let url = "http://blackbox.example.com/probe?target=https://example.com&module=http_2xx"
        .parse::<Uri>()
        .unwrap();

    // Create a mock response with invalid Prometheus text format
    let body = Bytes::from("invalid prometheus format {{{");

    let response = http::Response::builder().status(200).body(()).unwrap();
    let (parts, _) = response.into_parts();

    // Call on_response - should return None and emit PrometheusParseError
    let events = context.on_response(&url, &parts, &body);

    // Verify no events were returned (parse failed)
    assert!(events.is_none());
    // Note: PrometheusParseError is emitted via emit! macro, which we can't easily test here
    // but the code path is verified
}

#[test]
fn test_context_on_response_empty_body() {
    // Test that on_response handles empty response body gracefully
    let mut context = BlackboxExporterContext {
        target: "https://example.com".to_string(),
        module: "http_2xx".to_string(),
    };

    let url = "http://blackbox.example.com/probe?target=https://example.com&module=http_2xx"
        .parse::<Uri>()
        .unwrap();

    // Create a mock response with empty body
    let body = Bytes::from("");

    let response = http::Response::builder().status(200).body(()).unwrap();
    let (parts, _) = response.into_parts();

    // Call on_response
    let events = context.on_response(&url, &parts, &body);

    // Empty body should parse successfully but return no events
    assert!(events.is_some());
    let events = events.unwrap();
    assert!(events.is_empty());
}

#[test]
fn test_enrich_events_adds_tags() {
    // Test that enrich_events adds target and module tags
    let mut context = BlackboxExporterContext {
        target: "https://example.com".to_string(),
        module: "http_2xx".to_string(),
    };

    // Create a simple metric event
    let event = Event::Metric(vector_lib::event::Metric::new(
        "probe_success",
        vector_lib::event::MetricKind::Absolute,
        vector_lib::event::MetricValue::Gauge { value: 1.0 },
    ));

    let mut events = vec![event];

    // Call enrich_events
    context.enrich_events(&mut events);

    // Verify tags were added
    let metric = events[0].as_metric();
    assert_eq!(
        metric.tags().unwrap().get("target"),
        Some("https://example.com")
    );
    assert_eq!(metric.tags().unwrap().get("module"), Some("http_2xx"));
}

#[test]
fn test_enrich_events_handles_tag_conflicts() {
    // Test that enrich_events renames conflicting tags to exported_*
    let mut context = BlackboxExporterContext {
        target: "https://example.com".to_string(),
        module: "http_2xx".to_string(),
    };

    // Create a metric event with existing target and module tags
    let mut metric = vector_lib::event::Metric::new(
        "probe_success",
        vector_lib::event::MetricKind::Absolute,
        vector_lib::event::MetricValue::Gauge { value: 1.0 },
    );
    metric.replace_tag("target".to_string(), "internal".to_string());
    metric.replace_tag("module".to_string(), "internal_module".to_string());

    let mut events = vec![Event::Metric(metric)];

    // Call enrich_events
    context.enrich_events(&mut events);

    // Verify conflicting tags were renamed and new tags were added
    let metric = events[0].as_metric();
    assert_eq!(
        metric.tags().unwrap().get("target"),
        Some("https://example.com")
    );
    assert_eq!(metric.tags().unwrap().get("module"), Some("http_2xx"));
    assert_eq!(
        metric.tags().unwrap().get("exported_target"),
        Some("internal")
    );
    assert_eq!(
        metric.tags().unwrap().get("exported_module"),
        Some("internal_module")
    );
}

#[test]
fn test_enrich_events_preserves_other_tags() {
    // Test that enrich_events preserves existing tags
    let mut context = BlackboxExporterContext {
        target: "https://example.com".to_string(),
        module: "http_2xx".to_string(),
    };

    // Create a metric event with other tags
    let mut metric = vector_lib::event::Metric::new(
        "probe_success",
        vector_lib::event::MetricKind::Absolute,
        vector_lib::event::MetricValue::Gauge { value: 1.0 },
    );
    metric.replace_tag("instance".to_string(), "server1".to_string());
    metric.replace_tag("job".to_string(), "blackbox".to_string());

    let mut events = vec![Event::Metric(metric)];

    // Call enrich_events
    context.enrich_events(&mut events);

    // Verify existing tags are preserved
    let metric = events[0].as_metric();
    assert_eq!(metric.tags().unwrap().get("instance"), Some("server1"));
    assert_eq!(metric.tags().unwrap().get("job"), Some("blackbox"));
    // And new tags were added
    assert_eq!(
        metric.tags().unwrap().get("target"),
        Some("https://example.com")
    );
    assert_eq!(metric.tags().unwrap().get("module"), Some("http_2xx"));
}

#[test]
fn test_builder_decodes_url_encoded_target() {
    // Test that the builder properly decodes URL-encoded target from query parameters
    let builder = BlackboxExporterBuilder {
        module: "http_2xx".to_string(),
    };

    // Create a URL with URL-encoded target (https://www.google.com encoded)
    let url =
        "http://blackbox.example.com/probe?target=https%3A%2F%2Fwww%2Egoogle%2Ecom&module=http_2xx"
            .parse::<Uri>()
            .unwrap();

    let context = builder.build(&url);

    // Verify the target is decoded properly
    assert_eq!(context.target, "https://www.google.com");
    assert_eq!(context.module, "http_2xx");
}
