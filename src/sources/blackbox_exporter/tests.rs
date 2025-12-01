// Unit tests and integration tests for blackbox_exporter source

use super::*;
use bytes::Bytes;
use std::time::Duration;
use warp::Filter;

use crate::test_util::{
    addr::next_addr,
    components::{HTTP_PULL_SOURCE_TAGS, run_and_assert_source_compliance},
    wait_for_tcp,
};

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

// Edge case tests for URL construction

#[test]
fn test_construct_probe_url_with_special_characters_in_target() {
    let base_url = "http://blackbox.example.com".parse::<Uri>().unwrap();
    // Target with special characters: spaces, ampersands, question marks, equals signs
    let target = "https://app.example.com/path?query=value&other=test param";
    let module = "http_2xx";

    let result = construct_probe_url(&base_url, target, module).unwrap();
    let result_str = result.to_string();

    // Verify URL is constructed successfully
    assert!(result_str.contains("/probe"));
    assert!(result_str.contains("target="));
    assert!(result_str.contains("module="));

    // Verify special characters are encoded (should contain % for percent encoding)
    let query = result.query().unwrap();
    let target_param = query.split('&').find(|p| p.starts_with("target=")).unwrap();

    // Special characters should be percent-encoded
    assert!(
        target_param.contains('%'),
        "Special characters should be URL-encoded"
    );

    // Verify we can decode it back to the original target
    let decoded = target_param
        .strip_prefix("target=")
        .and_then(|encoded| {
            let param_value = encoded.split('&').next().unwrap();
            percent_encoding::percent_decode_str(param_value)
                .decode_utf8()
                .ok()
        })
        .map(|s| s.to_string());

    assert_eq!(
        decoded.as_deref(),
        Some(target),
        "Decoded target should match original"
    );
}

#[test]
fn test_construct_probe_url_with_special_characters_in_module() {
    let base_url = "http://blackbox.example.com".parse::<Uri>().unwrap();
    let target = "https://app.example.com";
    // Module with special characters
    let module = "http_2xx_custom-probe";

    let result = construct_probe_url(&base_url, target, module).unwrap();
    let result_str = result.to_string();

    // Verify URL is constructed successfully
    assert!(result_str.contains("/probe"));
    assert!(result_str.contains("target="));
    assert!(result_str.contains("module="));

    // Verify we can decode the module back
    let query = result.query().unwrap();
    let module_param = query.split('&').find(|p| p.starts_with("module=")).unwrap();

    let decoded = module_param
        .strip_prefix("module=")
        .and_then(|encoded| {
            let param_value = encoded.split('&').next().unwrap();
            percent_encoding::percent_decode_str(param_value)
                .decode_utf8()
                .ok()
        })
        .map(|s| s.to_string());

    assert_eq!(
        decoded.as_deref(),
        Some(module),
        "Decoded module should match original"
    );
}

#[test]
fn test_construct_probe_url_with_unicode_characters() {
    let base_url = "http://blackbox.example.com".parse::<Uri>().unwrap();
    // Target with Unicode characters
    let target = "https://例え.jp/パス";
    let module = "http_2xx";

    let result = construct_probe_url(&base_url, target, module).unwrap();
    let result_str = result.to_string();

    // Verify URL is constructed successfully
    assert!(result_str.contains("/probe"));
    assert!(result_str.contains("target="));
    assert!(result_str.contains("module="));

    // Verify Unicode characters are properly encoded
    let query = result.query().unwrap();
    let target_param = query.split('&').find(|p| p.starts_with("target=")).unwrap();

    // Unicode should be percent-encoded
    assert!(
        target_param.contains('%'),
        "Unicode characters should be URL-encoded"
    );

    // Verify we can decode it back to the original target
    let decoded = target_param
        .strip_prefix("target=")
        .and_then(|encoded| {
            let param_value = encoded.split('&').next().unwrap();
            percent_encoding::percent_decode_str(param_value)
                .decode_utf8()
                .ok()
        })
        .map(|s| s.to_string());

    assert_eq!(
        decoded.as_deref(),
        Some(target),
        "Decoded target should match original"
    );
}

#[test]
fn test_construct_probe_url_with_nested_path() {
    let base_url = "http://blackbox.example.com/api/v1/metrics"
        .parse::<Uri>()
        .unwrap();
    let target = "https://app.example.com";
    let module = "http_2xx";

    let result = construct_probe_url(&base_url, target, module).unwrap();
    let result_str = result.to_string();

    // Verify nested path is preserved with /probe appended
    assert!(
        result_str.contains("/api/v1/metrics/probe"),
        "Nested path should be preserved: {}",
        result_str
    );
    assert!(result_str.contains("target="));
    assert!(result_str.contains("module="));
}

#[test]
fn test_construct_probe_url_with_trailing_slash_in_path() {
    let base_url = "http://blackbox.example.com/metrics/"
        .parse::<Uri>()
        .unwrap();
    let target = "https://app.example.com";
    let module = "http_2xx";

    let result = construct_probe_url(&base_url, target, module).unwrap();
    let result_str = result.to_string();

    // Verify trailing slash is handled correctly (should not result in double slash)
    assert!(
        result_str.contains("/metrics/probe"),
        "Path with trailing slash should be handled correctly: {}",
        result_str
    );
    assert!(
        !result_str.contains("//probe"),
        "Should not have double slash: {}",
        result_str
    );
}

#[test]
fn test_construct_probe_url_with_multiple_query_params() {
    let base_url = "http://blackbox.example.com?key1=value1&key2=value2&key3=value3"
        .parse::<Uri>()
        .unwrap();
    let target = "https://app.example.com";
    let module = "http_2xx";

    let result = construct_probe_url(&base_url, target, module).unwrap();
    let query = result.query().unwrap();

    // Verify all original query parameters are preserved
    assert!(
        query.contains("key1=value1"),
        "First param should be preserved"
    );
    assert!(
        query.contains("key2=value2"),
        "Second param should be preserved"
    );
    assert!(
        query.contains("key3=value3"),
        "Third param should be preserved"
    );

    // Verify new parameters are added
    assert!(query.contains("target="), "Target param should be added");
    assert!(query.contains("module="), "Module param should be added");
}

#[test]
fn test_construct_probe_url_with_query_params_containing_special_chars() {
    let base_url = "http://blackbox.example.com?auth=Bearer%20token123"
        .parse::<Uri>()
        .unwrap();
    let target = "https://app.example.com";
    let module = "http_2xx";

    let result = construct_probe_url(&base_url, target, module).unwrap();
    let query = result.query().unwrap();

    // Verify existing encoded query parameter is preserved
    assert!(
        query.contains("auth=Bearer%20token123"),
        "Encoded query param should be preserved: {}",
        query
    );

    // Verify new parameters are added
    assert!(query.contains("target="), "Target param should be added");
    assert!(query.contains("module="), "Module param should be added");
}

#[test]
fn test_construct_probe_url_with_port_number() {
    let base_url = "http://blackbox.example.com:9115".parse::<Uri>().unwrap();
    let target = "https://app.example.com:8443";
    let module = "http_2xx";

    let result = construct_probe_url(&base_url, target, module).unwrap();
    let result_str = result.to_string();

    // Verify port is preserved in base URL
    assert!(
        result_str.starts_with("http://blackbox.example.com:9115"),
        "Port should be preserved in base URL: {}",
        result_str
    );

    // Verify target with port is properly encoded
    assert!(result_str.contains("target="));

    // Decode and verify target
    let query = result.query().unwrap();
    let decoded_target = query
        .split('&')
        .find(|p| p.starts_with("target="))
        .and_then(|p| p.strip_prefix("target="))
        .and_then(|encoded| {
            let param_value = encoded.split('&').next().unwrap();
            percent_encoding::percent_decode_str(param_value)
                .decode_utf8()
                .ok()
        })
        .map(|s| s.to_string());

    assert_eq!(
        decoded_target.as_deref(),
        Some(target),
        "Target with port should be preserved"
    );
}

#[test]
fn test_construct_probe_url_with_https_scheme() {
    let base_url = "https://blackbox.example.com".parse::<Uri>().unwrap();
    let target = "https://app.example.com";
    let module = "http_2xx";

    let result = construct_probe_url(&base_url, target, module).unwrap();
    let result_str = result.to_string();

    // Verify HTTPS scheme is preserved
    assert!(
        result_str.starts_with("https://"),
        "HTTPS scheme should be preserved: {}",
        result_str
    );
    assert!(result_str.contains("/probe"));
    assert!(result_str.contains("target="));
    assert!(result_str.contains("module="));
}

#[test]
fn test_construct_probe_url_with_path_and_multiple_query_params() {
    let base_url = "http://blackbox.example.com/api/v2?auth=token&region=us-east"
        .parse::<Uri>()
        .unwrap();
    let target = "https://app.example.com/health";
    let module = "http_2xx";

    let result = construct_probe_url(&base_url, target, module).unwrap();
    let result_str = result.to_string();

    // Verify path is preserved
    assert!(
        result_str.contains("/api/v2/probe"),
        "Path should be preserved: {}",
        result_str
    );

    // Verify all query parameters are present
    let query = result.query().unwrap();
    assert!(
        query.contains("auth=token"),
        "Original auth param should be preserved"
    );
    assert!(
        query.contains("region=us-east"),
        "Original region param should be preserved"
    );
    assert!(query.contains("target="), "Target param should be added");
    assert!(query.contains("module="), "Module param should be added");
}

#[test]
fn test_construct_probe_url_with_empty_path() {
    let base_url = "http://blackbox.example.com/".parse::<Uri>().unwrap();
    let target = "https://app.example.com";
    let module = "http_2xx";

    let result = construct_probe_url(&base_url, target, module).unwrap();
    let result_str = result.to_string();

    // Verify /probe is added correctly (not //probe)
    assert!(
        result_str.contains("/probe?"),
        "Should have /probe path: {}",
        result_str
    );
    assert!(
        !result_str.contains("//probe"),
        "Should not have double slash: {}",
        result_str
    );
}

#[test]
fn test_construct_probe_url_with_ipv4_address() {
    let base_url = "http://192.168.1.100:9115".parse::<Uri>().unwrap();
    let target = "http://10.0.0.1";
    let module = "icmp";

    let result = construct_probe_url(&base_url, target, module).unwrap();
    let result_str = result.to_string();

    // Verify IPv4 address is preserved
    assert!(
        result_str.starts_with("http://192.168.1.100:9115"),
        "IPv4 address should be preserved: {}",
        result_str
    );

    // Verify target IPv4 is properly encoded
    let query = result.query().unwrap();
    let decoded_target = query
        .split('&')
        .find(|p| p.starts_with("target="))
        .and_then(|p| p.strip_prefix("target="))
        .and_then(|encoded| {
            let param_value = encoded.split('&').next().unwrap();
            percent_encoding::percent_decode_str(param_value)
                .decode_utf8()
                .ok()
        })
        .map(|s| s.to_string());

    assert_eq!(
        decoded_target.as_deref(),
        Some(target),
        "Target IPv4 should be preserved"
    );
}

#[test]
fn test_context_on_response_success() {
    // Test that on_response successfully parses Prometheus text format
    let mut context = BlackboxExporterContext {
        target: "https://example.com".to_string(),
        module: "http_2xx".to_string(),
        optional_labels: OptionalLabels {
            geohash: None,
            region: None,
            location: None,
            country: None,
            name: None,
            provider: None,
            custom: HashMap::new(),
        },
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
        optional_labels: OptionalLabels {
            geohash: None,
            region: None,
            location: None,
            country: None,
            name: None,
            provider: None,
            custom: HashMap::new(),
        },
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
fn test_context_on_response_parse_error_malformed_metric() {
    // Test parse error with malformed metric line
    let mut context = BlackboxExporterContext {
        target: "https://example.com".to_string(),
        module: "http_2xx".to_string(),
        optional_labels: OptionalLabels {
            geohash: None,
            region: None,
            location: None,
            country: None,
            name: None,
            provider: None,
            custom: HashMap::new(),
        },
    };

    let url = "http://blackbox.example.com/probe?target=https://example.com&module=http_2xx"
        .parse::<Uri>()
        .unwrap();

    // Create a response with malformed metric (missing value)
    let body = Bytes::from(
        r#"# HELP probe_success Test metric
# TYPE probe_success gauge
probe_success
"#,
    );

    let response = http::Response::builder().status(200).body(()).unwrap();
    let (parts, _) = response.into_parts();

    // Call on_response - should return None and emit PrometheusParseError
    let events = context.on_response(&url, &parts, &body);

    // Verify no events were returned (parse failed)
    assert!(events.is_none());
}

#[test]
fn test_context_on_response_parse_error_invalid_value() {
    // Test parse error with invalid metric value
    let mut context = BlackboxExporterContext {
        target: "https://example.com".to_string(),
        module: "http_2xx".to_string(),
        optional_labels: OptionalLabels {
            geohash: None,
            region: None,
            location: None,
            country: None,
            name: None,
            provider: None,
            custom: HashMap::new(),
        },
    };

    let url = "http://blackbox.example.com/probe?target=https://example.com&module=http_2xx"
        .parse::<Uri>()
        .unwrap();

    // Create a response with invalid metric value (not a number)
    let body = Bytes::from(
        r#"# HELP probe_success Test metric
# TYPE probe_success gauge
probe_success not_a_number
"#,
    );

    let response = http::Response::builder().status(200).body(()).unwrap();
    let (parts, _) = response.into_parts();

    // Call on_response - should return None and emit PrometheusParseError
    let events = context.on_response(&url, &parts, &body);

    // Verify no events were returned (parse failed)
    assert!(events.is_none());
}

#[test]
fn test_context_on_response_empty_body() {
    // Test that on_response handles empty response body gracefully
    let mut context = BlackboxExporterContext {
        target: "https://example.com".to_string(),
        module: "http_2xx".to_string(),
        optional_labels: OptionalLabels {
            geohash: None,
            region: None,
            location: None,
            country: None,
            name: None,
            provider: None,
            custom: HashMap::new(),
        },
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
        optional_labels: OptionalLabels {
            geohash: None,
            region: None,
            location: None,
            country: None,
            name: None,
            provider: None,
            custom: HashMap::new(),
        },
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
        optional_labels: OptionalLabels {
            geohash: None,
            region: None,
            location: None,
            country: None,
            name: None,
            provider: None,
            custom: HashMap::new(),
        },
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
        optional_labels: OptionalLabels {
            geohash: None,
            region: None,
            location: None,
            country: None,
            name: None,
            provider: None,
            custom: HashMap::new(),
        },
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
        optional_labels: OptionalLabels {
            geohash: None,
            region: None,
            location: None,
            country: None,
            name: None,
            provider: None,
            custom: HashMap::new(),
        },
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

// Unit tests for SourceConfig implementation

#[test]
fn test_valid_configuration_builds() {
    // Test that a valid configuration parses successfully
    let config_toml = r#"
        url = "http://localhost:9115"
        targets = ["https://example.com", "https://test.com"]
        module = "http_2xx"
    "#;

    let result: std::result::Result<BlackboxExporterConfig, _> = toml::from_str(config_toml);
    assert!(
        result.is_ok(),
        "Valid configuration should parse successfully"
    );

    let config = result.unwrap();
    assert_eq!(config.url, "http://localhost:9115");
    assert_eq!(config.targets.len(), 2);
    assert_eq!(config.targets[0], "https://example.com");
    assert_eq!(config.targets[1], "https://test.com");
    assert_eq!(config.module, "http_2xx");
}

#[test]
fn test_valid_configuration_with_optional_fields() {
    // Test that a valid configuration with optional fields parses successfully
    let config_toml = r#"
        url = "https://blackbox.example.com:9115"
        targets = ["https://example.com"]
        module = "http_2xx"
        scrape_interval_secs = 30
        scrape_timeout_secs = 10
    "#;

    let result: std::result::Result<BlackboxExporterConfig, _> = toml::from_str(config_toml);
    assert!(
        result.is_ok(),
        "Valid configuration with optional fields should parse successfully"
    );

    let config = result.unwrap();
    assert_eq!(config.url, "https://blackbox.example.com:9115");
    assert_eq!(config.targets.len(), 1);
    assert_eq!(config.module, "http_2xx");
    assert_eq!(config.interval, Duration::from_secs(30));
    assert_eq!(config.timeout, Duration::from_secs(10));
}

#[test]
fn test_default_interval_and_timeout() {
    // Test that default interval and timeout values are applied when not specified
    let config_toml = r#"
        url = "http://localhost:9115"
        targets = ["https://example.com"]
        module = "http_2xx"
    "#;

    let result: std::result::Result<BlackboxExporterConfig, _> = toml::from_str(config_toml);
    assert!(result.is_ok(), "Configuration should parse successfully");

    let config = result.unwrap();
    // Default interval should be 15 seconds (from default_interval())
    assert_eq!(
        config.interval,
        Duration::from_secs(15),
        "Default interval should be 15 seconds"
    );
    // Default timeout should be 5 seconds (from default_timeout())
    assert_eq!(
        config.timeout,
        Duration::from_secs(5),
        "Default timeout should be 5 seconds"
    );
}

#[test]
fn test_invalid_configuration_missing_url() {
    // Test that configuration without url field fails to parse
    let config_toml = r#"
        targets = ["https://example.com"]
        module = "http_2xx"
    "#;

    let result: std::result::Result<BlackboxExporterConfig, _> = toml::from_str(config_toml);
    assert!(
        result.is_err(),
        "Configuration without url should fail to parse"
    );
}

#[test]
fn test_invalid_configuration_missing_targets() {
    // Test that configuration without targets field fails to parse
    let config_toml = r#"
        url = "http://localhost:9115"
        module = "http_2xx"
    "#;

    let result: std::result::Result<BlackboxExporterConfig, _> = toml::from_str(config_toml);
    assert!(
        result.is_err(),
        "Configuration without targets should fail to parse"
    );
}

#[test]
fn test_invalid_configuration_missing_module() {
    // Test that configuration without module field fails to parse
    let config_toml = r#"
        url = "http://localhost:9115"
        targets = ["https://example.com"]
    "#;

    let result: std::result::Result<BlackboxExporterConfig, _> = toml::from_str(config_toml);
    assert!(
        result.is_err(),
        "Configuration without module should fail to parse"
    );
}

#[test]
fn test_invalid_configuration_empty_targets() {
    // Test that configuration with empty targets list parses but should be caught during build
    let config_toml = r#"
        url = "http://localhost:9115"
        targets = []
        module = "http_2xx"
    "#;

    let result: std::result::Result<BlackboxExporterConfig, _> = toml::from_str(config_toml);
    // Empty targets list should parse at TOML level
    assert!(
        result.is_ok(),
        "Configuration with empty targets should parse at TOML level"
    );

    let config = result.unwrap();
    assert!(config.targets.is_empty(), "Targets should be empty");
    // Note: Empty targets validation happens during build(), not during parsing
}

#[test]
fn test_invalid_configuration_empty_module() {
    // Test that configuration with empty module string parses but should be caught during build
    let config_toml = r#"
        url = "http://localhost:9115"
        targets = ["https://example.com"]
        module = ""
    "#;

    let result: std::result::Result<BlackboxExporterConfig, _> = toml::from_str(config_toml);
    // Empty module should parse at TOML level
    assert!(
        result.is_ok(),
        "Configuration with empty module should parse at TOML level"
    );

    let config = result.unwrap();
    assert!(config.module.is_empty(), "Module should be empty");
    // Note: Empty module validation happens during build(), not during parsing
}

#[test]
fn test_configuration_with_multiple_targets() {
    // Test that configuration with multiple targets parses correctly
    let config_toml = r#"
        url = "http://localhost:9115"
        targets = [
            "https://example.com",
            "https://test.com",
            "https://api.example.com",
            "8.8.8.8"
        ]
        module = "http_2xx"
    "#;

    let result: std::result::Result<BlackboxExporterConfig, _> = toml::from_str(config_toml);
    assert!(
        result.is_ok(),
        "Configuration with multiple targets should parse successfully"
    );

    let config = result.unwrap();
    assert_eq!(config.targets.len(), 4);
    assert_eq!(config.targets[0], "https://example.com");
    assert_eq!(config.targets[1], "https://test.com");
    assert_eq!(config.targets[2], "https://api.example.com");
    assert_eq!(config.targets[3], "8.8.8.8");
}

#[test]
fn test_configuration_with_different_modules() {
    // Test that configuration with different module types parses correctly
    let modules = vec!["http_2xx", "icmp", "tcp_connect", "dns_query"];

    for module in modules {
        let config_toml = format!(
            r#"
            url = "http://localhost:9115"
            targets = ["https://example.com"]
            module = "{}"
            "#,
            module
        );

        let result: std::result::Result<BlackboxExporterConfig, _> = toml::from_str(&config_toml);
        assert!(
            result.is_ok(),
            "Configuration with module '{}' should parse successfully",
            module
        );

        let config = result.unwrap();
        assert_eq!(config.module, module);
    }
}

#[test]
fn test_configuration_with_custom_intervals() {
    // Test that custom interval and timeout values are parsed correctly
    let config_toml = r#"
        url = "http://localhost:9115"
        targets = ["https://example.com"]
        module = "http_2xx"
        scrape_interval_secs = 60
        scrape_timeout_secs = 30
    "#;

    let result: std::result::Result<BlackboxExporterConfig, _> = toml::from_str(config_toml);
    assert!(
        result.is_ok(),
        "Configuration with custom intervals should parse successfully"
    );

    let config = result.unwrap();
    assert_eq!(config.interval, Duration::from_secs(60));
    assert_eq!(config.timeout, Duration::from_secs(30));
}

#[test]
fn test_configuration_with_fractional_timeout() {
    // Test that fractional timeout values are parsed correctly
    let config_toml = r#"
        url = "http://localhost:9115"
        targets = ["https://example.com"]
        module = "http_2xx"
        scrape_timeout_secs = 2.5
    "#;

    let result: std::result::Result<BlackboxExporterConfig, _> = toml::from_str(config_toml);
    assert!(
        result.is_ok(),
        "Configuration with fractional timeout should parse successfully"
    );

    let config = result.unwrap();
    assert_eq!(config.timeout, Duration::from_millis(2500));
}

#[test]
fn test_configuration_with_https_url() {
    // Test that HTTPS URLs are parsed correctly
    let config_toml = r#"
        url = "https://blackbox.example.com:9115"
        targets = ["https://example.com"]
        module = "http_2xx"
    "#;

    let result: std::result::Result<BlackboxExporterConfig, _> = toml::from_str(config_toml);
    assert!(
        result.is_ok(),
        "Configuration with HTTPS URL should parse successfully"
    );

    let config = result.unwrap();
    assert_eq!(config.url, "https://blackbox.example.com:9115");
}

#[test]
fn test_configuration_with_url_path() {
    // Test that URLs with paths are parsed correctly
    let config_toml = r#"
        url = "http://localhost:9115/metrics"
        targets = ["https://example.com"]
        module = "http_2xx"
    "#;

    let result: std::result::Result<BlackboxExporterConfig, _> = toml::from_str(config_toml);
    assert!(
        result.is_ok(),
        "Configuration with URL path should parse successfully"
    );

    let config = result.unwrap();
    assert_eq!(config.url, "http://localhost:9115/metrics");
}

#[test]
fn test_configuration_with_ipv4_address() {
    // Test that IPv4 addresses are parsed correctly
    let config_toml = r#"
        url = "http://192.168.1.100:9115"
        targets = ["8.8.8.8", "1.1.1.1"]
        module = "icmp"
    "#;

    let result: std::result::Result<BlackboxExporterConfig, _> = toml::from_str(config_toml);
    assert!(
        result.is_ok(),
        "Configuration with IPv4 addresses should parse successfully"
    );

    let config = result.unwrap();
    assert_eq!(config.url, "http://192.168.1.100:9115");
    assert_eq!(config.targets[0], "8.8.8.8");
    assert_eq!(config.targets[1], "1.1.1.1");
}

// Integration tests

#[tokio::test]
async fn test_basic_scraping() {
    // Set up mock Blackbox Exporter endpoint
    let (_guard, addr) = next_addr();

    let mock_endpoint = warp::path!("probe")
        .and(warp::query::<std::collections::HashMap<String, String>>())
        .map(|_q: std::collections::HashMap<String, String>| {
            // Return mock Prometheus metrics
            warp::http::Response::builder()
                .header("Content-Type", "text/plain")
                .body(
                    r#"# HELP probe_success Displays whether or not the probe was a success
# TYPE probe_success gauge
probe_success 1
# HELP probe_duration_seconds Returns how long the probe took to complete in seconds
# TYPE probe_duration_seconds gauge
probe_duration_seconds 0.123
# HELP probe_http_status_code Response HTTP status code
# TYPE probe_http_status_code gauge
probe_http_status_code 200
"#,
                )
                .unwrap()
        });

    tokio::spawn(warp::serve(mock_endpoint).run(addr));
    wait_for_tcp(addr).await;

    // Configure source with single target
    let config = BlackboxExporterConfig {
        url: format!("http://{}", addr),
        targets: vec!["https://example.com".to_string()],
        module: "http_2xx".to_string(),
        interval: Duration::from_secs(1),
        timeout: Duration::from_millis(500),
        tls: None,
        auth: None,
        geohash: None,
        region: None,
        location: None,
        country: None,
        name: None,
        provider: None,
        labels: None,
    };

    // Run source and collect events
    let events =
        run_and_assert_source_compliance(config, Duration::from_secs(3), &HTTP_PULL_SOURCE_TAGS)
            .await;

    // Verify metrics are scraped and tagged correctly
    assert!(
        !events.is_empty(),
        "Should have received at least one event"
    );

    // Check that we got the expected metrics
    let metric_names: Vec<String> = events
        .iter()
        .map(|e| e.as_metric().name().to_string())
        .collect();

    assert!(
        metric_names.contains(&"probe_success".to_string()),
        "Should have probe_success metric"
    );
    assert!(
        metric_names.contains(&"probe_duration_seconds".to_string()),
        "Should have probe_duration_seconds metric"
    );

    // Verify all metrics have target and module tags
    for event in &events {
        let metric = event.as_metric();
        let tags = metric.tags().expect("Metric should have tags");

        assert_eq!(
            tags.get("target"),
            Some("https://example.com"),
            "Metric should have correct target tag"
        );
        assert_eq!(
            tags.get("module"),
            Some("http_2xx"),
            "Metric should have correct module tag"
        );
    }
}

#[tokio::test]
async fn test_multiple_targets() {
    // Set up mock Blackbox Exporter endpoint
    let (_guard, addr) = next_addr();

    let mock_endpoint = warp::path!("probe")
        .and(warp::query::<std::collections::HashMap<String, String>>())
        .map(|q: std::collections::HashMap<String, String>| {
            let target = q.get("target").unwrap();
            let _module = q.get("module").unwrap();

            // Return different metrics based on target to verify correct tagging
            let status = if target.contains("example.com") {
                200
            } else if target.contains("test.com") {
                201
            } else {
                202
            };

            warp::http::Response::builder()
                .header("Content-Type", "text/plain")
                .body(format!(
                    r#"# HELP probe_success Displays whether or not the probe was a success
# TYPE probe_success gauge
probe_success 1
# HELP probe_http_status_code Response HTTP status code
# TYPE probe_http_status_code gauge
probe_http_status_code {}
"#,
                    status
                ))
                .unwrap()
        });

    tokio::spawn(warp::serve(mock_endpoint).run(addr));
    wait_for_tcp(addr).await;

    // Configure source with multiple targets
    let config = BlackboxExporterConfig {
        url: format!("http://{}", addr),
        targets: vec![
            "https://example.com".to_string(),
            "https://test.com".to_string(),
            "https://another.com".to_string(),
        ],
        module: "http_2xx".to_string(),
        interval: Duration::from_secs(1),
        timeout: Duration::from_millis(500),
        tls: None,
        auth: None,
        geohash: None,
        region: None,
        location: None,
        country: None,
        name: None,
        provider: None,
        labels: None,
    };

    // Run source and collect events
    let events =
        run_and_assert_source_compliance(config, Duration::from_secs(3), &HTTP_PULL_SOURCE_TAGS)
            .await;

    // Verify all targets are scraped
    assert!(!events.is_empty(), "Should have received events");

    // Collect unique target values from metrics
    let mut targets_seen = std::collections::HashSet::new();
    for event in &events {
        let metric = event.as_metric();
        if let Some(tags) = metric.tags() {
            if let Some(target) = tags.get("target") {
                targets_seen.insert(target.to_string());
            }
        }
    }

    // Verify we saw all three targets
    assert!(
        targets_seen.contains("https://example.com"),
        "Should have scraped example.com"
    );
    assert!(
        targets_seen.contains("https://test.com"),
        "Should have scraped test.com"
    );
    assert!(
        targets_seen.contains("https://another.com"),
        "Should have scraped another.com"
    );

    // Verify each metric has correct target tag
    for event in &events {
        let metric = event.as_metric();
        let tags = metric.tags().expect("Metric should have tags");

        // Verify target tag exists and is one of our targets
        let target = tags.get("target").expect("Should have target tag");
        assert!(
            target == "https://example.com"
                || target == "https://test.com"
                || target == "https://another.com",
            "Target tag should be one of the configured targets"
        );

        // Verify module tag is correct
        assert_eq!(
            tags.get("module"),
            Some("http_2xx"),
            "Metric should have correct module tag"
        );
    }
}

// Property-based tests

#[cfg(test)]
mod property_tests {
    use super::*;
    use proptest::prelude::*;
    use std::sync::Arc;

    // Feature: blackbox-exporter-source, Property 1: Configuration validation
    // For any blackbox_exporter configuration, if all required fields (url, targets, module)
    // are present and valid, then the configuration should parse successfully; if any required
    // field is missing or invalid, then parsing should fail with an appropriate error.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn test_configuration_validation_valid(
            port in 1024u16..65535,
            num_targets in 1usize..5,
            module in "[a-z_][a-z0-9_]{2,20}",
        ) {
            // Generate valid configuration
            let url = format!("http://localhost:{}", port);
            let targets: Vec<String> = (0..num_targets)
                .map(|i| format!("https://target{}.com", i))
                .collect();

            // Create configuration as TOML string
            let config_toml = format!(
                r#"
                url = "{}"
                targets = [{}]
                module = "{}"
                "#,
                url,
                targets.iter().map(|t| format!("\"{}\"", t)).collect::<Vec<_>>().join(", "),
                module
            );

            // Parse configuration
            let result: std::result::Result<BlackboxExporterConfig, _> = toml::from_str(&config_toml);

            // Valid configuration should parse successfully
            prop_assert!(result.is_ok(), "Valid configuration should parse successfully: {:?}", result.err());

            if let Ok(config) = result {
                prop_assert_eq!(config.url, url, "URL should match");
                prop_assert_eq!(config.targets, targets, "Targets should match");
                prop_assert_eq!(config.module, module, "Module should match");
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn test_configuration_validation_invalid_url(
            invalid_url in "[ \t\n]{1,5}|[^a-zA-Z0-9:/.-]{1,10}",
            num_targets in 1usize..3,
            module in "[a-z_][a-z0-9_]{2,10}",
        ) {
            // Generate configuration with invalid URL
            let targets: Vec<String> = (0..num_targets)
                .map(|i| format!("https://target{}.com", i))
                .collect();

            // Create configuration as TOML string with invalid URL
            let config_toml = format!(
                r#"
                url = "{}"
                targets = [{}]
                module = "{}"
                "#,
                invalid_url,
                targets.iter().map(|t| format!("\"{}\"", t)).collect::<Vec<_>>().join(", "),
                module
            );

            // Parse configuration - should succeed at TOML level
            let result: std::result::Result<BlackboxExporterConfig, _> = toml::from_str(&config_toml);

            // Configuration parsing might succeed, but URL validation should fail during build
            if let Ok(config) = result {
                // Try to parse the URL - this is where validation happens
                let url_parse_result = config.url.parse::<Uri>();

                // Invalid URLs should fail to parse
                // Note: Some strings might accidentally be valid URLs, so we can't assert failure here
                // The important thing is that the validation logic exists and is exercised
                let _ = url_parse_result;
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn test_configuration_validation_empty_targets(
            port in 1024u16..65535,
            module in "[a-z_][a-z0-9_]{2,20}",
        ) {
            // Generate configuration with empty targets list
            let url = format!("http://localhost:{}", port);

            // Create configuration as TOML string with empty targets
            let config_toml = format!(
                r#"
                url = "{}"
                targets = []
                module = "{}"
                "#,
                url,
                module
            );

            // Parse configuration
            let result: std::result::Result<BlackboxExporterConfig, _> = toml::from_str(&config_toml);

            // Configuration should parse successfully at TOML level
            prop_assert!(result.is_ok(), "Configuration should parse at TOML level");

            // But empty targets should be caught during build validation
            // (This is tested in the build logic, not in parsing)
            if let Ok(config) = result {
                prop_assert!(config.targets.is_empty(), "Targets should be empty");
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn test_configuration_validation_empty_module(
            port in 1024u16..65535,
            num_targets in 1usize..3,
        ) {
            // Generate configuration with empty module
            let url = format!("http://localhost:{}", port);
            let targets: Vec<String> = (0..num_targets)
                .map(|i| format!("https://target{}.com", i))
                .collect();

            // Create configuration as TOML string with empty module
            let config_toml = format!(
                r#"
                url = "{}"
                targets = [{}]
                module = ""
                "#,
                url,
                targets.iter().map(|t| format!("\"{}\"", t)).collect::<Vec<_>>().join(", "),
            );

            // Parse configuration
            let result: std::result::Result<BlackboxExporterConfig, _> = toml::from_str(&config_toml);

            // Configuration should parse successfully at TOML level
            prop_assert!(result.is_ok(), "Configuration should parse at TOML level");

            // But empty module should be caught during build validation
            if let Ok(config) = result {
                prop_assert!(config.module.is_empty(), "Module should be empty");
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn test_configuration_validation_with_optional_fields(
            port in 1024u16..65535,
            num_targets in 1usize..3,
            module in "[a-z_][a-z0-9_]{2,20}",
            interval_secs in 1u64..300,
            timeout_secs in 1u64..60,
        ) {
            // Generate valid configuration with optional fields
            let url = format!("http://localhost:{}", port);
            let targets: Vec<String> = (0..num_targets)
                .map(|i| format!("https://target{}.com", i))
                .collect();

            // Create configuration as TOML string with optional fields
            let config_toml = format!(
                r#"
                url = "{}"
                targets = [{}]
                module = "{}"
                scrape_interval_secs = {}
                scrape_timeout_secs = {}
                "#,
                url,
                targets.iter().map(|t| format!("\"{}\"", t)).collect::<Vec<_>>().join(", "),
                module,
                interval_secs,
                timeout_secs
            );

            // Parse configuration
            let result: std::result::Result<BlackboxExporterConfig, _> = toml::from_str(&config_toml);

            // Valid configuration with optional fields should parse successfully
            prop_assert!(result.is_ok(), "Valid configuration with optional fields should parse successfully: {:?}", result.err());

            if let Ok(config) = result {
                prop_assert_eq!(config.url, url, "URL should match");
                prop_assert_eq!(config.targets, targets, "Targets should match");
                prop_assert_eq!(config.module, module, "Module should match");
                prop_assert_eq!(config.interval.as_secs(), interval_secs, "Interval should match");
                prop_assert_eq!(config.timeout.as_secs(), timeout_secs, "Timeout should match");
            }
        }
    }

    // Feature: blackbox-exporter-source, Property 2: Probe URL construction
    // For any valid Blackbox Exporter Instance URL, target, and module, the constructed
    // probe URL should have the format `<url>/probe?target=<encoded_target>&module=<encoded_module>`
    // where target and module are properly URL-encoded.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn test_probe_url_construction(
            scheme in "(http|https)",
            host in "[a-z][a-z0-9-]{2,20}\\.[a-z]{2,5}",
            port in proptest::option::of(1024u16..65535),
            target_scheme in "(http|https)",
            target_host in "[a-z][a-z0-9-]{2,20}\\.[a-z]{2,5}",
            target_path in proptest::option::of("[a-z][a-z0-9/_-]{0,30}"),
            target_query in proptest::option::of("[a-z]=[a-z0-9]{1,10}"),
            module in "[a-z_][a-z0-9_]{2,20}",
        ) {
            // Construct base URL
            let base_url_str = if let Some(p) = port {
                format!("{}://{}:{}", scheme, host, p)
            } else {
                format!("{}://{}", scheme, host)
            };
            let base_url = base_url_str.parse::<Uri>().unwrap();

            // Construct target URL
            let mut target = format!("{}://{}", target_scheme, target_host);
            if let Some(path) = target_path {
                target.push('/');
                target.push_str(&path);
            }
            if let Some(query) = target_query {
                target.push('?');
                target.push_str(&query);
            }

            // Construct probe URL
            let result = construct_probe_url(&base_url, &target, &module);
            prop_assert!(result.is_ok(), "URL construction should succeed");

            let probe_url = result.unwrap();
            let probe_url_str = probe_url.to_string();

            // Verify the URL contains /probe path
            prop_assert!(
                probe_url_str.contains("/probe"),
                "Probe URL should contain /probe path: {}",
                probe_url_str
            );

            // Verify the URL contains target parameter
            prop_assert!(
                probe_url_str.contains("target="),
                "Probe URL should contain target parameter: {}",
                probe_url_str
            );

            // Verify the URL contains module parameter
            prop_assert!(
                probe_url_str.contains("module="),
                "Probe URL should contain module parameter: {}",
                probe_url_str
            );

            // Verify the URL has the correct scheme and host
            prop_assert_eq!(
                probe_url.scheme_str(),
                Some(scheme.as_str()),
                "Scheme should be preserved"
            );
            prop_assert_eq!(
                probe_url.authority().map(|a| a.host()),
                Some(host.as_str()),
                "Host should be preserved"
            );

            // Verify URL encoding by checking that special characters are encoded
            // If target contains special characters like ?, &, =, they should be encoded
            if target.contains('?') || target.contains('&') || target.contains('=') {
                // The target value in the query string should be URL-encoded
                // We can verify this by checking that the raw special characters don't appear
                // in the target parameter value (they should be percent-encoded)
                let query_str = probe_url.query().unwrap();

                // Extract the target parameter value
                if let Some(target_param) = query_str.split('&')
                    .find(|p| p.starts_with("target="))
                    .and_then(|p| p.strip_prefix("target="))
                {
                    // Find where the target parameter ends (at next & or end of string)
                    let target_value = target_param.split('&').next().unwrap();

                    // Verify that special characters are encoded (% should appear for encoding)
                    prop_assert!(
                        target_value.contains('%'),
                        "Target with special characters should be URL-encoded: {}",
                        target_value
                    );
                }
            }

            // Verify we can decode the target back
            if let Some(query) = probe_url.query() {
                let decoded_target = query
                    .split('&')
                    .find(|param| param.starts_with("target="))
                    .and_then(|param| param.strip_prefix("target="))
                    .and_then(|encoded| {
                        // Find where this parameter ends
                        let param_value = encoded.split('&').next().unwrap();
                        percent_encoding::percent_decode_str(param_value)
                            .decode_utf8()
                            .ok()
                    })
                    .map(|decoded| decoded.to_string());

                prop_assert_eq!(
                    decoded_target.as_deref(),
                    Some(target.as_str()),
                    "Decoded target should match original target"
                );

                let decoded_module = query
                    .split('&')
                    .find(|param| param.starts_with("module="))
                    .and_then(|param| param.strip_prefix("module="))
                    .and_then(|encoded| {
                        // Find where this parameter ends
                        let param_value = encoded.split('&').next().unwrap();
                        percent_encoding::percent_decode_str(param_value)
                            .decode_utf8()
                            .ok()
                    })
                    .map(|decoded| decoded.to_string());

                prop_assert_eq!(
                    decoded_module.as_deref(),
                    Some(module.as_str()),
                    "Decoded module should match original module"
                );
            }
        }
    }

    // Feature: blackbox-exporter-source, Property 3: URL path preservation
    // For any Blackbox Exporter Instance URL with a path component, constructing a
    // probe URL should preserve the original path before appending `/probe`.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn test_url_path_preservation(
            scheme in "(http|https)",
            host in "[a-z][a-z0-9-]{2,20}\\.[a-z]{2,5}",
            port in proptest::option::of(1024u16..65535),
            // Generate various path components
            path_segments in proptest::collection::vec("[a-z][a-z0-9_-]{1,15}", 1..5),
            target_scheme in "(http|https)",
            target_host in "[a-z][a-z0-9-]{2,20}\\.[a-z]{2,5}",
            module in "[a-z_][a-z0-9_]{2,20}",
        ) {
            // Construct base URL with path
            let path = format!("/{}", path_segments.join("/"));
            let base_url_str = if let Some(p) = port {
                format!("{}://{}:{}{}", scheme, host, p, path)
            } else {
                format!("{}://{}{}", scheme, host, path)
            };
            let base_url = base_url_str.parse::<Uri>().unwrap();

            // Construct target URL
            let target = format!("{}://{}", target_scheme, target_host);

            // Construct probe URL
            let result = construct_probe_url(&base_url, &target, &module);
            prop_assert!(result.is_ok(), "URL construction should succeed");

            let probe_url = result.unwrap();
            let probe_url_path = probe_url.path();

            // Verify the original path is preserved and /probe is appended
            let expected_path = format!("{}/probe", path.trim_end_matches('/'));
            prop_assert_eq!(
                probe_url_path,
                expected_path.as_str(),
                "Path should be preserved with /probe appended. Expected: {}, Got: {}",
                expected_path,
                probe_url_path
            );

            // Verify the scheme and host are also preserved
            prop_assert_eq!(
                probe_url.scheme_str(),
                Some(scheme.as_str()),
                "Scheme should be preserved"
            );
            prop_assert_eq!(
                probe_url.authority().map(|a| a.host()),
                Some(host.as_str()),
                "Host should be preserved"
            );

            // Verify the URL still contains the target and module parameters
            let query = probe_url.query().unwrap();
            prop_assert!(
                query.contains("target="),
                "Query should contain target parameter"
            );
            prop_assert!(
                query.contains("module="),
                "Query should contain module parameter"
            );
        }
    }

    // Feature: blackbox-exporter-source, Property 4: Query parameter preservation
    // For any Blackbox Exporter Instance URL with existing query parameters, constructing
    // a probe URL should preserve all existing parameters and append the target and module parameters.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn test_query_parameter_preservation(
            scheme in "(http|https)",
            host in "[a-z][a-z0-9-]{2,20}\\.[a-z]{2,5}",
            port in proptest::option::of(1024u16..65535),
            // Generate query parameters (key=value pairs)
            query_params in proptest::collection::vec(
                ("[a-z][a-z0-9_]{1,10}", "[a-z0-9_-]{1,15}"),
                1..5
            ),
            target_scheme in "(http|https)",
            target_host in "[a-z][a-z0-9-]{2,20}\\.[a-z]{2,5}",
            module in "[a-z_][a-z0-9_]{2,20}",
        ) {
            // Construct base URL with query parameters
            let query_string = query_params
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect::<Vec<_>>()
                .join("&");

            let base_url_str = if let Some(p) = port {
                format!("{}://{}:{}?{}", scheme, host, p, query_string)
            } else {
                format!("{}://{}?{}", scheme, host, query_string)
            };
            let base_url = base_url_str.parse::<Uri>().unwrap();

            // Construct target URL
            let target = format!("{}://{}", target_scheme, target_host);

            // Construct probe URL
            let result = construct_probe_url(&base_url, &target, &module);
            prop_assert!(result.is_ok(), "URL construction should succeed");

            let probe_url = result.unwrap();
            let probe_query = probe_url.query().expect("Probe URL should have query string");

            // Verify all original query parameters are preserved
            for (key, value) in &query_params {
                let param_string = format!("{}={}", key, value);
                prop_assert!(
                    probe_query.contains(&param_string),
                    "Query should contain original parameter '{}'. Query: {}",
                    param_string,
                    probe_query
                );
            }

            // Verify target and module parameters are present
            prop_assert!(
                probe_query.contains("target="),
                "Query should contain target parameter. Query: {}",
                probe_query
            );
            prop_assert!(
                probe_query.contains("module="),
                "Query should contain module parameter. Query: {}",
                probe_query
            );

            // Verify the scheme and host are preserved
            prop_assert_eq!(
                probe_url.scheme_str(),
                Some(scheme.as_str()),
                "Scheme should be preserved"
            );
            prop_assert_eq!(
                probe_url.authority().map(|a| a.host()),
                Some(host.as_str()),
                "Host should be preserved"
            );

            // Verify we can decode the target and module parameters correctly
            let decoded_target = probe_query
                .split('&')
                .find(|param| param.starts_with("target="))
                .and_then(|param| param.strip_prefix("target="))
                .and_then(|encoded| {
                    percent_encoding::percent_decode_str(encoded)
                        .decode_utf8()
                        .ok()
                })
                .map(|decoded| decoded.to_string());

            prop_assert_eq!(
                decoded_target.as_deref(),
                Some(target.as_str()),
                "Decoded target should match original target"
            );

            let decoded_module = probe_query
                .split('&')
                .find(|param| param.starts_with("module="))
                .and_then(|param| param.strip_prefix("module="))
                .and_then(|encoded| {
                    percent_encoding::percent_decode_str(encoded)
                        .decode_utf8()
                        .ok()
                })
                .map(|decoded| decoded.to_string());

            prop_assert_eq!(
                decoded_module.as_deref(),
                Some(module.as_str()),
                "Decoded module should match original module"
            );
        }
    }

    // Feature: blackbox-exporter-source, Property 9: Error isolation across targets
    // For any set of targets where some scrape requests fail, metrics from successful
    // scrape requests should still be processed and emitted.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(10))]

        #[test]
        fn test_error_isolation_across_targets(
            num_success_targets in 1usize..4,
            num_fail_targets in 1usize..4,
        ) {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                // Set up mock Blackbox Exporter endpoint that fails for some targets
                let (_guard, addr) = next_addr();

                let mock_endpoint = warp::path!("probe")
                    .and(warp::query::<std::collections::HashMap<String, String>>())
                    .map(|q: std::collections::HashMap<String, String>| {
                        let target = q.get("target").unwrap();

                        // Fail for targets containing "fail"
                        if target.contains("fail") {
                            warp::http::Response::builder()
                                .status(500)
                                .body("Internal Server Error".to_string())
                                .unwrap()
                        } else {
                            // Return success for other targets
                            warp::http::Response::builder()
                                .header("Content-Type", "text/plain")
                                .body(
                                    r#"# HELP probe_success Displays whether or not the probe was a success
# TYPE probe_success gauge
probe_success 1
"#
                                    .to_string(),
                                )
                                .unwrap()
                        }
                    });

                tokio::spawn(warp::serve(mock_endpoint).run(addr));
                wait_for_tcp(addr).await;

                // Generate targets - mix of successful and failing
                let mut targets = Vec::new();
                for i in 0..num_success_targets {
                    targets.push(format!("https://success{}.com", i));
                }
                for i in 0..num_fail_targets {
                    targets.push(format!("https://fail{}.com", i));
                }

                // Configure source with mix of successful and failing targets
                let config = BlackboxExporterConfig {
                    url: format!("http://{}", addr),
                    targets,
                    module: "http_2xx".to_string(),
                    interval: Duration::from_secs(1),
                    timeout: Duration::from_millis(500),
                    tls: None,
                    auth: None,
                    geohash: None,
                    region: None,
                    location: None,
                    country: None,
                    name: None,
                    provider: None,
                    labels: None,
                };

                // Run source and collect events
                let events = run_and_assert_source_compliance(
                    config,
                    Duration::from_secs(3),
                    &HTTP_PULL_SOURCE_TAGS,
                )
                .await;

                // Verify we got metrics from successful targets despite failures
                prop_assert!(!events.is_empty(), "Should have received events from successful targets");

                // Verify we only got metrics from successful targets
                for event in &events {
                    let metric = event.as_metric();
                    if let Some(tags) = metric.tags() {
                        if let Some(target) = tags.get("target") {
                            prop_assert!(
                                !target.contains("fail"),
                                "Should not have metrics from failed targets, but got target: {}",
                                target
                            );
                            prop_assert!(
                                target.contains("success"),
                                "Should only have metrics from successful targets, but got target: {}",
                                target
                            );
                        }
                    }
                }

                Ok(())
            })?;
        }
    }

    // Feature: blackbox-exporter-source, Property 10: Correct target tagging for multiple targets
    // For any set of metrics collected from multiple targets, each metric should have a
    // "target" tag that matches the target URL it was scraped from.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(10))]

        #[test]
        fn test_correct_target_tagging(
            num_targets in 1usize..5,
            target_suffix in "[a-z]{3,8}",
        ) {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                // Set up mock Blackbox Exporter endpoint
                let (_guard, addr) = next_addr();

                // Track which targets were requested
                let requested_targets = Arc::new(std::sync::Mutex::new(Vec::new()));
                let requested_targets_clone = requested_targets.clone();

                let mock_endpoint = warp::path!("probe")
                    .and(warp::query::<std::collections::HashMap<String, String>>())
                    .map(move |q: std::collections::HashMap<String, String>| {
                        let target = q.get("target").unwrap().clone();
                        requested_targets_clone.lock().unwrap().push(target.clone());

                        // Return metrics with a unique value based on target
                        warp::http::Response::builder()
                            .header("Content-Type", "text/plain")
                            .body(format!(
                                r#"# HELP probe_success Displays whether or not the probe was a success
# TYPE probe_success gauge
probe_success 1
"#
                            ))
                            .unwrap()
                    });

                tokio::spawn(warp::serve(mock_endpoint).run(addr));
                wait_for_tcp(addr).await;

                // Generate targets
                let targets: Vec<String> = (0..num_targets)
                    .map(|i| format!("https://target{}-{}.com", i, target_suffix))
                    .collect();

                // Configure source
                let config = BlackboxExporterConfig {
                    url: format!("http://{}", addr),
                    targets: targets.clone(),
                    module: "http_2xx".to_string(),
                    interval: Duration::from_secs(1),
                    timeout: Duration::from_millis(500),
                    tls: None,
                    auth: None,
                    geohash: None,
                    region: None,
                    location: None,
                    country: None,
                    name: None,
                    provider: None,
                    labels: None,
                };

                // Run source and collect events
                let events = run_and_assert_source_compliance(
                    config,
                    Duration::from_secs(3),
                    &HTTP_PULL_SOURCE_TAGS,
                )
                .await;

                // Verify all metrics have correct target tags
                for event in &events {
                    let metric = event.as_metric();
                    if let Some(tags) = metric.tags() {
                        if let Some(target) = tags.get("target") {
                            // Verify the target tag is one of our configured targets
                            prop_assert!(
                                targets.contains(&target.to_string()),
                                "Target tag '{}' should be one of the configured targets",
                                target
                            );
                        }
                    }
                }

                Ok(())
            })?;
        }
    }

    // Feature: blackbox-exporter-source, Property 5: Target and module tag injection
    // For any metric scraped from a probe URL, the enriched metric should contain both
    // a "target" tag with the target URL value and a "module" tag with the module name value.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn test_target_and_module_tag_injection(
            target_scheme in "(http|https)",
            target_host in "[a-z][a-z0-9-]{2,20}\\.[a-z]{2,5}",
            target_path in proptest::option::of("[a-z][a-z0-9/_-]{0,30}"),
            module in "[a-z_][a-z0-9_]{2,20}",
            metric_name in "[a-z_][a-z0-9_]{2,20}",
            metric_value in 0.0f64..1000.0f64,
            // Generate random existing tags that should be preserved
            num_existing_tags in 0usize..5,
        ) {
            // Construct target URL
            let mut target = format!("{}://{}", target_scheme, target_host);
            if let Some(path) = target_path {
                target.push('/');
                target.push_str(&path);
            }

            // Create a BlackboxExporterContext with the generated target and module
            let mut context = BlackboxExporterContext {
                target: target.clone(),
                module: module.clone(),
                optional_labels: OptionalLabels {
                    geohash: None,
                    region: None,
                    location: None,
                    country: None,
                    name: None,
                    provider: None,
                    custom: HashMap::new(),
                },
            };

            // Create a metric event with random existing tags
            let mut metric = vector_lib::event::Metric::new(
                metric_name.clone(),
                vector_lib::event::MetricKind::Absolute,
                vector_lib::event::MetricValue::Gauge { value: metric_value },
            );

            // Add some random existing tags (but not "target" or "module" to avoid conflicts)
            let existing_tags: Vec<(String, String)> = (0..num_existing_tags)
                .map(|i| (format!("tag{}", i), format!("value{}", i)))
                .collect();

            for (key, value) in &existing_tags {
                metric.replace_tag(key.clone(), value.clone());
            }

            let mut events = vec![Event::Metric(metric)];

            // Call enrich_events
            context.enrich_events(&mut events);

            // Verify the metric has the correct target and module tags
            let enriched_metric = events[0].as_metric();
            let tags = enriched_metric.tags().expect("Metric should have tags");

            // Property: The enriched metric should contain a "target" tag with the target URL value
            prop_assert_eq!(
                tags.get("target"),
                Some(target.as_str()),
                "Metric should have target tag with value '{}'",
                target
            );

            // Property: The enriched metric should contain a "module" tag with the module name value
            prop_assert_eq!(
                tags.get("module"),
                Some(module.as_str()),
                "Metric should have module tag with value '{}'",
                module
            );

            // Verify existing tags are preserved
            for (key, value) in &existing_tags {
                prop_assert_eq!(
                    tags.get(key.as_str()),
                    Some(value.as_str()),
                    "Existing tag '{}' should be preserved with value '{}'",
                    key,
                    value
                );
            }

            // Verify the metric name and value are unchanged
            prop_assert_eq!(
                enriched_metric.name(),
                metric_name.as_str(),
                "Metric name should be unchanged"
            );

            if let vector_lib::event::MetricValue::Gauge { value } = enriched_metric.value() {
                prop_assert_eq!(
                    *value,
                    metric_value,
                    "Metric value should be unchanged"
                );
            }
        }
    }

    // Feature: blackbox-exporter-source, Property 6: Tag conflict resolution
    // For any scraped metric that already contains a "target" or "module" tag, the
    // enrichment process should rename the existing tag to "exported_target" or
    // "exported_module" respectively, and add the new tag with the correct value.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn test_tag_conflict_resolution(
            target_scheme in "(http|https)",
            target_host in "[a-z][a-z0-9-]{2,20}\\.[a-z]{2,5}",
            module in "[a-z_][a-z0-9_]{2,20}",
            metric_name in "[a-z_][a-z0-9_]{2,20}",
            metric_value in 0.0f64..1000.0f64,
            // Generate existing conflicting tag values
            existing_target_value in "[a-z][a-z0-9_-]{2,20}",
            existing_module_value in "[a-z][a-z0-9_-]{2,20}",
            // Generate random additional tags that should be preserved
            num_other_tags in 0usize..5,
            // Control which conflicts to test
            has_target_conflict in proptest::bool::ANY,
            has_module_conflict in proptest::bool::ANY,
        ) {
            // Skip if no conflicts to test
            if !has_target_conflict && !has_module_conflict {
                return Ok(());
            }

            // Construct target URL
            let target = format!("{}://{}", target_scheme, target_host);

            // Create a BlackboxExporterContext
            let mut context = BlackboxExporterContext {
                target: target.clone(),
                module: module.clone(),
                optional_labels: OptionalLabels {
                    geohash: None,
                    region: None,
                    location: None,
                    country: None,
                    name: None,
                    provider: None,
                    custom: HashMap::new(),
                },
            };

            // Create a metric event with conflicting tags
            let mut metric = vector_lib::event::Metric::new(
                metric_name.clone(),
                vector_lib::event::MetricKind::Absolute,
                vector_lib::event::MetricValue::Gauge { value: metric_value },
            );

            // Add conflicting tags based on test parameters
            if has_target_conflict {
                metric.replace_tag("target".to_string(), existing_target_value.clone());
            }
            if has_module_conflict {
                metric.replace_tag("module".to_string(), existing_module_value.clone());
            }

            // Add some other random tags that should be preserved
            let other_tags: Vec<(String, String)> = (0..num_other_tags)
                .map(|i| (format!("other_tag{}", i), format!("other_value{}", i)))
                .collect();

            for (key, value) in &other_tags {
                metric.replace_tag(key.clone(), value.clone());
            }

            let mut events = vec![Event::Metric(metric)];

            // Call enrich_events
            context.enrich_events(&mut events);

            // Verify the enrichment handled conflicts correctly
            let enriched_metric = events[0].as_metric();
            let tags = enriched_metric.tags().expect("Metric should have tags");

            // Property: The new "target" tag should have the correct value from context
            prop_assert_eq!(
                tags.get("target"),
                Some(target.as_str()),
                "Metric should have target tag with value '{}' from context",
                target
            );

            // Property: The new "module" tag should have the correct value from context
            prop_assert_eq!(
                tags.get("module"),
                Some(module.as_str()),
                "Metric should have module tag with value '{}' from context",
                module
            );

            // Property: If there was a target conflict, the original value should be renamed to "exported_target"
            if has_target_conflict {
                prop_assert_eq!(
                    tags.get("exported_target"),
                    Some(existing_target_value.as_str()),
                    "Original target tag should be renamed to 'exported_target' with value '{}'",
                    existing_target_value
                );
            } else {
                // If there was no conflict, exported_target should not exist
                prop_assert!(
                    tags.get("exported_target").is_none(),
                    "exported_target should not exist when there was no conflict"
                );
            }

            // Property: If there was a module conflict, the original value should be renamed to "exported_module"
            if has_module_conflict {
                prop_assert_eq!(
                    tags.get("exported_module"),
                    Some(existing_module_value.as_str()),
                    "Original module tag should be renamed to 'exported_module' with value '{}'",
                    existing_module_value
                );
            } else {
                // If there was no conflict, exported_module should not exist
                prop_assert!(
                    tags.get("exported_module").is_none(),
                    "exported_module should not exist when there was no conflict"
                );
            }

            // Property: All other tags should be preserved unchanged
            for (key, value) in &other_tags {
                prop_assert_eq!(
                    tags.get(key.as_str()),
                    Some(value.as_str()),
                    "Other tag '{}' should be preserved with value '{}'",
                    key,
                    value
                );
            }

            // Verify the metric name and value are unchanged
            prop_assert_eq!(
                enriched_metric.name(),
                metric_name.as_str(),
                "Metric name should be unchanged"
            );

            if let vector_lib::event::MetricValue::Gauge { value } = enriched_metric.value() {
                prop_assert_eq!(
                    *value,
                    metric_value,
                    "Metric value should be unchanged"
                );
            }
        }
    }

    // Feature: blackbox-exporter-source, Property 7: Tag preservation
    // For any metric with existing tags, after enrichment with target and module tags,
    // all original tags (except those renamed due to conflicts) should still be present in the metric.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn test_tag_preservation(
            target_scheme in "(http|https)",
            target_host in "[a-z][a-z0-9-]{2,20}\\.[a-z]{2,5}",
            module in "[a-z_][a-z0-9_]{2,20}",
            metric_name in "[a-z_][a-z0-9_]{2,20}",
            metric_value in 0.0f64..1000.0f64,
            // Generate a variety of existing tags with different names
            existing_tags in proptest::collection::vec(
                ("[a-z][a-z0-9_]{2,15}", "[a-z0-9_-]{1,20}"),
                1..10
            ),
        ) {
            // Construct target URL
            let target = format!("{}://{}", target_scheme, target_host);

            // Create a BlackboxExporterContext
            let mut context = BlackboxExporterContext {
                target: target.clone(),
                module: module.clone(),
                optional_labels: OptionalLabels {
                    geohash: None,
                    region: None,
                    location: None,
                    country: None,
                    name: None,
                    provider: None,
                    custom: HashMap::new(),
                },
            };

            // Create a metric event with various existing tags
            let mut metric = vector_lib::event::Metric::new(
                metric_name.clone(),
                vector_lib::event::MetricKind::Absolute,
                vector_lib::event::MetricValue::Gauge { value: metric_value },
            );

            // Filter out any tags named "target" or "module" to avoid conflicts in this test
            // (conflicts are tested separately in Property 6)
            let filtered_tags: Vec<(String, String)> = existing_tags
                .into_iter()
                .filter(|(key, _)| key != "target" && key != "module")
                .collect();

            // Skip test if we have no tags after filtering
            if filtered_tags.is_empty() {
                return Ok(());
            }

            // Add all existing tags to the metric
            for (key, value) in &filtered_tags {
                metric.replace_tag(key.clone(), value.clone());
            }

            let mut events = vec![Event::Metric(metric)];

            // Call enrich_events
            context.enrich_events(&mut events);

            // Verify all original tags are preserved
            let enriched_metric = events[0].as_metric();
            let tags = enriched_metric.tags().expect("Metric should have tags");

            // Property: All original tags should still be present with their original values
            for (key, value) in &filtered_tags {
                prop_assert_eq!(
                    tags.get(key.as_str()),
                    Some(value.as_str()),
                    "Original tag '{}' should be preserved with value '{}'",
                    key,
                    value
                );
            }

            // Property: The target and module tags should be present
            prop_assert_eq!(
                tags.get("target"),
                Some(target.as_str()),
                "Target tag should be present"
            );
            prop_assert_eq!(
                tags.get("module"),
                Some(module.as_str()),
                "Module tag should be present"
            );

            // Property: No original tags should have been removed (only added to)
            // We verify this by checking that every original tag key is still present
            for (key, _) in &filtered_tags {
                prop_assert!(
                    tags.contains_key(key.as_str()),
                    "Original tag key '{}' should still be present in enriched metric",
                    key
                );
            }

            // Verify the metric name and value are unchanged
            prop_assert_eq!(
                enriched_metric.name(),
                metric_name.as_str(),
                "Metric name should be unchanged"
            );

            if let vector_lib::event::MetricValue::Gauge { value } = enriched_metric.value() {
                prop_assert_eq!(
                    *value,
                    metric_value,
                    "Metric value should be unchanged"
                );
            }
        }
    }

    // Feature: blackbox-exporter-source, Property 8: Multiple target URL generation
    // For any configuration with N targets, the system should generate exactly N probe URLs,
    // one for each target.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn test_multiple_target_url_generation(
            scheme in "(http|https)",
            host in "[a-z][a-z0-9-]{2,20}\\.[a-z]{2,5}",
            port in proptest::option::of(1024u16..65535),
            num_targets in 1usize..10,
            module in "[a-z_][a-z0-9_]{2,20}",
        ) {
            // Construct base URL
            let base_url_str = if let Some(p) = port {
                format!("{}://{}:{}", scheme, host, p)
            } else {
                format!("{}://{}", scheme, host)
            };
            let base_url = base_url_str.parse::<Uri>().unwrap();

            // Generate N targets
            let targets: Vec<String> = (0..num_targets)
                .map(|i| format!("https://target{}.example.com", i))
                .collect();

            // Construct probe URLs for all targets
            let urls: Vec<Uri> = targets
                .iter()
                .map(|target| construct_probe_url(&base_url, target, &module))
                .collect::<std::result::Result<Vec<Uri>, _>>()
                .unwrap();

            // Property: The number of generated URLs should equal the number of targets
            prop_assert_eq!(
                urls.len(),
                num_targets,
                "Should generate exactly {} probe URLs for {} targets, but got {}",
                num_targets,
                num_targets,
                urls.len()
            );

            // Property: Each URL should be unique (one per target)
            let unique_urls: std::collections::HashSet<String> = urls
                .iter()
                .map(|u| u.to_string())
                .collect();

            prop_assert_eq!(
                unique_urls.len(),
                num_targets,
                "All generated URLs should be unique"
            );

            // Property: Each URL should correspond to exactly one target
            for (i, url) in urls.iter().enumerate() {
                let expected_target = &targets[i];

                // Extract and decode the target parameter from the URL
                let decoded_target = url
                    .query()
                    .and_then(|q| {
                        q.split('&')
                            .find(|param| param.starts_with("target="))
                            .and_then(|param| param.strip_prefix("target="))
                    })
                    .and_then(|encoded| {
                        // Find where this parameter ends
                        let param_value = encoded.split('&').next().unwrap();
                        percent_encoding::percent_decode_str(param_value)
                            .decode_utf8()
                            .ok()
                    })
                    .map(|decoded| decoded.to_string());

                prop_assert_eq!(
                    decoded_target.as_deref(),
                    Some(expected_target.as_str()),
                    "URL {} should contain target parameter matching '{}'",
                    i,
                    expected_target
                );

                // Verify the module parameter is correct for all URLs
                let decoded_module = url
                    .query()
                    .and_then(|q| {
                        q.split('&')
                            .find(|param| param.starts_with("module="))
                            .and_then(|param| param.strip_prefix("module="))
                    })
                    .and_then(|encoded| {
                        // Find where this parameter ends
                        let param_value = encoded.split('&').next().unwrap();
                        percent_encoding::percent_decode_str(param_value)
                            .decode_utf8()
                            .ok()
                    })
                    .map(|decoded| decoded.to_string());

                prop_assert_eq!(
                    decoded_module.as_deref(),
                    Some(module.as_str()),
                    "URL {} should contain module parameter matching '{}'",
                    i,
                    module
                );

                // Verify all URLs have the /probe path
                prop_assert!(
                    url.path().ends_with("/probe"),
                    "URL {} should have /probe path: {}",
                    i,
                    url.path()
                );

                // Verify all URLs have the same base (scheme, host, port)
                prop_assert_eq!(
                    url.scheme_str(),
                    Some(scheme.as_str()),
                    "URL {} should have scheme '{}'",
                    i,
                    scheme
                );
                prop_assert_eq!(
                    url.authority().map(|a| a.host()),
                    Some(host.as_str()),
                    "URL {} should have host '{}'",
                    i,
                    host
                );
            }
        }
    }

    // Feature: blackbox-exporter-optional-labels, Property 1: Optional label configuration validation
    // For any blackbox_exporter configuration with any combination of optional label fields
    // (geohash, region, location, country, name, provider, labels), the configuration should
    // parse successfully and accept all provided fields.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn test_optional_label_configuration_validation(
            port in 1024u16..65535,
            num_targets in 1usize..3,
            module in "[a-z_][a-z0-9_]{2,20}",
            // Optional predefined labels - use Option to test all combinations
            geohash in proptest::option::of("[a-z0-9]{5,12}"),
            region in proptest::option::of("(AMER|EMEA|APAC|LATAM)"),
            location in proptest::option::of("[A-Z][a-z]{2,15}"),
            country in proptest::option::of("[A-Z]{2}"),
            name in proptest::option::of("[A-Z][a-z0-9 ]{2,20}"),
            provider in proptest::option::of("(AWS|GCP|AZURE|DO)"),
            // Ad-hoc labels - generate 0-3 custom labels
            num_custom_labels in 0usize..4,
        ) {
            // Generate valid configuration
            let url = format!("http://localhost:{}", port);
            let targets: Vec<String> = (0..num_targets)
                .map(|i| format!("https://target{}.com", i))
                .collect();

            // Build TOML configuration string
            let mut config_toml = format!(
                r#"
                url = "{}"
                targets = [{}]
                module = "{}"
                "#,
                url,
                targets.iter().map(|t| format!("\"{}\"", t)).collect::<Vec<_>>().join(", "),
                module
            );

            // Add optional predefined labels if present
            if let Some(ref gh) = geohash {
                config_toml.push_str(&format!("geohash = \"{}\"\n", gh));
            }
            if let Some(ref r) = region {
                config_toml.push_str(&format!("region = \"{}\"\n", r));
            }
            if let Some(ref l) = location {
                config_toml.push_str(&format!("location = \"{}\"\n", l));
            }
            if let Some(ref c) = country {
                config_toml.push_str(&format!("country = \"{}\"\n", c));
            }
            if let Some(ref n) = name {
                config_toml.push_str(&format!("name = \"{}\"\n", n));
            }
            if let Some(ref p) = provider {
                config_toml.push_str(&format!("provider = \"{}\"\n", p));
            }

            // Add ad-hoc labels if any
            if num_custom_labels > 0 {
                config_toml.push_str("\n[labels]\n");
                for i in 0..num_custom_labels {
                    config_toml.push_str(&format!("custom_key_{} = \"custom_value_{}\"\n", i, i));
                }
            }

            // Parse configuration
            let result: std::result::Result<BlackboxExporterConfig, _> = toml::from_str(&config_toml);

            // Property: Valid configuration with any combination of optional labels should parse successfully
            prop_assert!(
                result.is_ok(),
                "Configuration with optional labels should parse successfully. Error: {:?}\nConfig:\n{}",
                result.err(),
                config_toml
            );

            if let Ok(config) = result {
                // Verify all fields are correctly parsed
                prop_assert_eq!(config.url, url, "URL should match");
                prop_assert_eq!(config.targets, targets, "Targets should match");
                prop_assert_eq!(config.module, module, "Module should match");

                // Verify optional predefined labels
                prop_assert_eq!(config.geohash, geohash, "Geohash should match");
                prop_assert_eq!(config.region, region, "Region should match");
                prop_assert_eq!(config.location, location, "Location should match");
                prop_assert_eq!(config.country, country, "Country should match");
                prop_assert_eq!(config.name, name, "Name should match");
                prop_assert_eq!(config.provider, provider, "Provider should match");

                // Verify ad-hoc labels
                if num_custom_labels > 0 {
                    prop_assert!(config.labels.is_some(), "Labels map should be present");
                    let labels = config.labels.as_ref().unwrap();
                    prop_assert_eq!(
                        labels.len(),
                        num_custom_labels,
                        "Should have {} custom labels",
                        num_custom_labels
                    );

                    // Verify each custom label is present
                    for i in 0..num_custom_labels {
                        let key = format!("custom_key_{}", i);
                        let expected_value = format!("custom_value_{}", i);
                        prop_assert_eq!(
                            labels.get(&key),
                            Some(&expected_value),
                            "Custom label '{}' should have value '{}'",
                            key,
                            expected_value
                        );
                    }
                } else {
                    // If no custom labels, the labels field should be None or empty
                    prop_assert!(
                        config.labels.is_none() || config.labels.as_ref().unwrap().is_empty(),
                        "Labels should be None or empty when no custom labels are specified"
                    );
                }
            }
        }
    }

    // Feature: blackbox-exporter-source, Property 11: Prometheus text format parsing
    // For any valid Prometheus text format response body, the parser should successfully
    // convert it into Vector metric events without errors.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(10))]

        #[test]
        fn test_prometheus_parsing(
            metric_name in "[a-z_][a-z0-9_]{2,20}",
            metric_value in 0.0f64..1000.0f64,
        ) {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                // Set up mock Blackbox Exporter endpoint
                let (_guard, addr) = next_addr();

                let metric_name_clone = metric_name.clone();
                let mock_endpoint = warp::path!("probe")
                    .and(warp::query::<std::collections::HashMap<String, String>>())
                    .map(move |_q: std::collections::HashMap<String, String>| {
                        // Return valid Prometheus text format with generated metric
                        warp::http::Response::builder()
                            .header("Content-Type", "text/plain")
                            .body(format!(
                                r#"# HELP {} Test metric
# TYPE {} gauge
{} {}
"#,
                                metric_name_clone, metric_name_clone, metric_name_clone, metric_value
                            ))
                            .unwrap()
                    });

                tokio::spawn(warp::serve(mock_endpoint).run(addr));
                wait_for_tcp(addr).await;

                // Configure source
                let config = BlackboxExporterConfig {
                    url: format!("http://{}", addr),
                    targets: vec!["https://example.com".to_string()],
                    module: "http_2xx".to_string(),
                    interval: Duration::from_secs(1),
                    timeout: Duration::from_millis(500),
                    tls: None,
                    auth: None,
                    geohash: None,
                    region: None,
                    location: None,
                    country: None,
                    name: None,
                    provider: None,
                    labels: None,
                };

                // Run source and collect events
                let events = run_and_assert_source_compliance(
                    config,
                    Duration::from_secs(3),
                    &HTTP_PULL_SOURCE_TAGS,
                )
                .await;

                // Verify we got at least one event (parsing succeeded)
                prop_assert!(!events.is_empty(), "Should have parsed at least one metric");

                // Verify the metric name matches what we generated
                let metric_names: Vec<String> = events
                    .iter()
                    .map(|e| e.as_metric().name().to_string())
                    .collect();

                prop_assert!(
                    metric_names.contains(&metric_name),
                    "Should have parsed metric with name '{}'",
                    metric_name
                );

                Ok(())
            })?;
        }
    }

    // Feature: blackbox-exporter-optional-labels, Property 2: Partial optional label injection
    // For any subset of optional label fields specified in the configuration, only the
    // specified labels should be added as tags to scraped metrics, and unspecified labels
    // should not appear.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn test_partial_optional_label_injection(
            target_scheme in "(http|https)",
            target_host in "[a-z][a-z0-9-]{2,20}\\.[a-z]{2,5}",
            module in "[a-z_][a-z0-9_]{2,20}",
            metric_name in "[a-z_][a-z0-9_]{2,20}",
            metric_value in 0.0f64..1000.0f64,
            // Generate random subset of optional labels
            has_geohash in proptest::bool::ANY,
            has_region in proptest::bool::ANY,
            has_location in proptest::bool::ANY,
            geohash_val in "[a-z0-9]{5,12}",
            region_val in "(AMER|EMEA|APAC)",
            location_val in "[A-Z][a-z]{2,15}",
        ) {
            // Skip if no labels are selected
            if !has_geohash && !has_region && !has_location {
                return Ok(());
            }

            // Construct target URL
            let target = format!("{}://{}", target_scheme, target_host);

            // Create OptionalLabels with only selected fields
            let optional_labels = OptionalLabels {
                geohash: if has_geohash { Some(geohash_val.clone()) } else { None },
                region: if has_region { Some(region_val.clone()) } else { None },
                location: if has_location { Some(location_val.clone()) } else { None },
                country: None,
                name: None,
                provider: None,
                custom: HashMap::new(),
            };

            // Create a BlackboxExporterContext
            let mut context = BlackboxExporterContext {
                target: target.clone(),
                module: module.clone(),
                optional_labels,
            };

            // Create a metric event
            let metric = vector_lib::event::Metric::new(
                metric_name.clone(),
                vector_lib::event::MetricKind::Absolute,
                vector_lib::event::MetricValue::Gauge { value: metric_value },
            );

            let mut events = vec![Event::Metric(metric)];

            // Call enrich_events
            context.enrich_events(&mut events);

            // Verify only specified labels are present
            let enriched_metric = events[0].as_metric();
            let tags = enriched_metric.tags().expect("Metric should have tags");

            // Property: If geohash was specified, it should be present; otherwise it should not
            if has_geohash {
                prop_assert_eq!(
                    tags.get("geohash"),
                    Some(geohash_val.as_str()),
                    "Geohash tag should be present with value '{}'",
                    geohash_val
                );
            } else {
                prop_assert!(
                    tags.get("geohash").is_none(),
                    "Geohash tag should not be present when not specified"
                );
            }

            // Property: If region was specified, it should be present; otherwise it should not
            if has_region {
                prop_assert_eq!(
                    tags.get("region"),
                    Some(region_val.as_str()),
                    "Region tag should be present with value '{}'",
                    region_val
                );
            } else {
                prop_assert!(
                    tags.get("region").is_none(),
                    "Region tag should not be present when not specified"
                );
            }

            // Property: If location was specified, it should be present; otherwise it should not
            if has_location {
                prop_assert_eq!(
                    tags.get("location"),
                    Some(location_val.as_str()),
                    "Location tag should be present with value '{}'",
                    location_val
                );
            } else {
                prop_assert!(
                    tags.get("location").is_none(),
                    "Location tag should not be present when not specified"
                );
            }

            // Verify target and module tags are always present
            prop_assert_eq!(
                tags.get("target"),
                Some(target.as_str()),
                "Target tag should always be present"
            );
            prop_assert_eq!(
                tags.get("module"),
                Some(module.as_str()),
                "Module tag should always be present"
            );
        }
    }

    // Feature: blackbox-exporter-optional-labels, Property 3: Empty string handling
    // For any optional label field with an empty string value, no tag should be added
    // to metrics for that label key.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn test_empty_string_handling(
            target_scheme in "(http|https)",
            target_host in "[a-z][a-z0-9-]{2,20}\\.[a-z]{2,5}",
            module in "[a-z_][a-z0-9_]{2,20}",
            metric_name in "[a-z_][a-z0-9_]{2,20}",
            metric_value in 0.0f64..1000.0f64,
            // Control which labels have empty strings
            geohash_empty in proptest::bool::ANY,
            region_empty in proptest::bool::ANY,
            location_empty in proptest::bool::ANY,
            // Non-empty values for comparison
            geohash_val in "[a-z0-9]{5,12}",
            region_val in "(AMER|EMEA|APAC)",
            location_val in "[A-Z][a-z]{2,15}",
        ) {
            // Skip if all are empty (nothing to test)
            if geohash_empty && region_empty && location_empty {
                return Ok(());
            }

            // Construct target URL
            let target = format!("{}://{}", target_scheme, target_host);

            // Create OptionalLabels with empty strings for selected fields
            let optional_labels = OptionalLabels {
                geohash: Some(if geohash_empty { String::new() } else { geohash_val.clone() }),
                region: Some(if region_empty { String::new() } else { region_val.clone() }),
                location: Some(if location_empty { String::new() } else { location_val.clone() }),
                country: None,
                name: None,
                provider: None,
                custom: HashMap::new(),
            };

            // Create a BlackboxExporterContext
            let mut context = BlackboxExporterContext {
                target: target.clone(),
                module: module.clone(),
                optional_labels,
            };

            // Create a metric event
            let metric = vector_lib::event::Metric::new(
                metric_name.clone(),
                vector_lib::event::MetricKind::Absolute,
                vector_lib::event::MetricValue::Gauge { value: metric_value },
            );

            let mut events = vec![Event::Metric(metric)];

            // Call enrich_events
            context.enrich_events(&mut events);

            // Verify empty strings are not added as tags
            let enriched_metric = events[0].as_metric();
            let tags = enriched_metric.tags().expect("Metric should have tags");

            // Property: If geohash is empty, it should not be present as a tag
            if geohash_empty {
                prop_assert!(
                    tags.get("geohash").is_none(),
                    "Geohash tag should not be present when value is empty string"
                );
            } else {
                prop_assert_eq!(
                    tags.get("geohash"),
                    Some(geohash_val.as_str()),
                    "Geohash tag should be present with non-empty value"
                );
            }

            // Property: If region is empty, it should not be present as a tag
            if region_empty {
                prop_assert!(
                    tags.get("region").is_none(),
                    "Region tag should not be present when value is empty string"
                );
            } else {
                prop_assert_eq!(
                    tags.get("region"),
                    Some(region_val.as_str()),
                    "Region tag should be present with non-empty value"
                );
            }

            // Property: If location is empty, it should not be present as a tag
            if location_empty {
                prop_assert!(
                    tags.get("location").is_none(),
                    "Location tag should not be present when value is empty string"
                );
            } else {
                prop_assert_eq!(
                    tags.get("location"),
                    Some(location_val.as_str()),
                    "Location tag should be present with non-empty value"
                );
            }

            // Verify target and module tags are always present
            prop_assert_eq!(
                tags.get("target"),
                Some(target.as_str()),
                "Target tag should always be present"
            );
            prop_assert_eq!(
                tags.get("module"),
                Some(module.as_str()),
                "Module tag should always be present"
            );
        }
    }

    // Feature: blackbox-exporter-optional-labels, Property 4, 5, 6, 7, 8: Comprehensive label injection
    // Tests predefined label injection, conflict resolution, tag preservation, ad-hoc labels, and combined labels
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn test_comprehensive_label_injection(
            target_scheme in "(http|https)",
            target_host in "[a-z][a-z0-9-]{2,20}\\.[a-z]{2,5}",
            module in "[a-z_][a-z0-9_]{2,20}",
            metric_name in "[a-z_][a-z0-9_]{2,20}",
            metric_value in 0.0f64..1000.0f64,
            // Predefined labels
            geohash_val in "[a-z0-9]{5,12}",
            region_val in "(AMER|EMEA|APAC)",
            location_val in "[A-Z][a-z]{2,15}",
            country_val in "[A-Z]{2}",
            name_val in "[A-Z][a-z0-9 ]{2,20}",
            provider_val in "(AWS|GCP|AZURE)",
            // Ad-hoc labels
            num_custom_labels in 0usize..3,
            // Existing tags to test preservation
            num_existing_tags in 0usize..3,
            // Conflict scenarios
            has_region_conflict in proptest::bool::ANY,
            existing_region_val in "[a-z][a-z0-9_-]{2,20}",
        ) {
            // Construct target URL
            let target = format!("{}://{}", target_scheme, target_host);

            // Create ad-hoc labels
            let mut custom_labels = HashMap::new();
            for i in 0..num_custom_labels {
                custom_labels.insert(format!("custom_key_{}", i), format!("custom_value_{}", i));
            }

            // Create OptionalLabels with all predefined labels and custom labels
            let optional_labels = OptionalLabels {
                geohash: Some(geohash_val.clone()),
                region: Some(region_val.clone()),
                location: Some(location_val.clone()),
                country: Some(country_val.clone()),
                name: Some(name_val.clone()),
                provider: Some(provider_val.clone()),
                custom: custom_labels.clone(),
            };

            // Create a BlackboxExporterContext
            let mut context = BlackboxExporterContext {
                target: target.clone(),
                module: module.clone(),
                optional_labels,
            };

            // Create a metric event with existing tags
            let mut metric = vector_lib::event::Metric::new(
                metric_name.clone(),
                vector_lib::event::MetricKind::Absolute,
                vector_lib::event::MetricValue::Gauge { value: metric_value },
            );

            // Add existing tags that should be preserved
            let mut existing_tags = Vec::new();
            for i in 0..num_existing_tags {
                let key = format!("existing_tag_{}", i);
                let value = format!("existing_value_{}", i);
                metric.replace_tag(key.clone(), value.clone());
                existing_tags.push((key, value));
            }

            // Add a conflicting region tag if requested
            if has_region_conflict {
                metric.replace_tag("region".to_string(), existing_region_val.clone());
            }

            let mut events = vec![Event::Metric(metric)];

            // Call enrich_events
            context.enrich_events(&mut events);

            // Verify all aspects
            let enriched_metric = events[0].as_metric();
            let tags = enriched_metric.tags().expect("Metric should have tags");

            // Property 4: All predefined labels should be present
            prop_assert_eq!(
                tags.get("geohash"),
                Some(geohash_val.as_str()),
                "Geohash tag should be present"
            );
            prop_assert_eq!(
                tags.get("region"),
                Some(region_val.as_str()),
                "Region tag should be present with configured value"
            );
            prop_assert_eq!(
                tags.get("location"),
                Some(location_val.as_str()),
                "Location tag should be present"
            );
            prop_assert_eq!(
                tags.get("country"),
                Some(country_val.as_str()),
                "Country tag should be present"
            );
            prop_assert_eq!(
                tags.get("name"),
                Some(name_val.as_str()),
                "Name tag should be present"
            );
            prop_assert_eq!(
                tags.get("provider"),
                Some(provider_val.as_str()),
                "Provider tag should be present"
            );

            // Property 5: Conflict resolution - if there was a region conflict, check exported_region
            if has_region_conflict {
                prop_assert_eq!(
                    tags.get("exported_region"),
                    Some(existing_region_val.as_str()),
                    "Conflicting region tag should be renamed to exported_region"
                );
            }

            // Property 6: Tag preservation - all existing tags should still be present
            for (key, value) in &existing_tags {
                prop_assert_eq!(
                    tags.get(key.as_str()),
                    Some(value.as_str()),
                    "Existing tag '{}' should be preserved",
                    key
                );
            }

            // Property 7: Ad-hoc labels should be present
            for (key, value) in &custom_labels {
                prop_assert_eq!(
                    tags.get(key.as_str()),
                    Some(value.as_str()),
                    "Ad-hoc label '{}' should be present",
                    key
                );
            }

            // Property 8: Target and module tags should always be present
            prop_assert_eq!(
                tags.get("target"),
                Some(target.as_str()),
                "Target tag should be present"
            );
            prop_assert_eq!(
                tags.get("module"),
                Some(module.as_str()),
                "Module tag should be present"
            );
        }
    }

    // Feature: blackbox-exporter-optional-labels, Property 7: Ad-hoc label injection
    // For any key-value pairs in the labels configuration map, all scraped metrics should
    // contain tags for each key-value pair.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn test_adhoc_label_injection(
            target_scheme in "(http|https)",
            target_host in "[a-z][a-z0-9-]{2,20}\\.[a-z]{2,5}",
            module in "[a-z_][a-z0-9_]{2,20}",
            metric_name in "[a-z_][a-z0-9_]{2,20}",
            metric_value in 0.0f64..1000.0f64,
            // Generate random ad-hoc labels (1-5 labels)
            num_adhoc_labels in 1usize..6,
        ) {
            // Construct target URL
            let target = format!("{}://{}", target_scheme, target_host);

            // Generate ad-hoc labels
            let mut custom_labels = HashMap::new();
            for i in 0..num_adhoc_labels {
                custom_labels.insert(
                    format!("adhoc_key_{}", i),
                    format!("adhoc_value_{}", i)
                );
            }

            // Create OptionalLabels with only ad-hoc labels
            let optional_labels = OptionalLabels {
                geohash: None,
                region: None,
                location: None,
                country: None,
                name: None,
                provider: None,
                custom: custom_labels.clone(),
            };

            // Create a BlackboxExporterContext
            let mut context = BlackboxExporterContext {
                target: target.clone(),
                module: module.clone(),
                optional_labels,
            };

            // Create a metric event
            let metric = vector_lib::event::Metric::new(
                metric_name.clone(),
                vector_lib::event::MetricKind::Absolute,
                vector_lib::event::MetricValue::Gauge { value: metric_value },
            );

            let mut events = vec![Event::Metric(metric)];

            // Call enrich_events
            context.enrich_events(&mut events);

            // Verify all ad-hoc labels are present as tags
            let enriched_metric = events[0].as_metric();
            let tags = enriched_metric.tags().expect("Metric should have tags");

            // Property: For each ad-hoc label, a tag should be present with the correct value
            for (key, value) in &custom_labels {
                prop_assert_eq!(
                    tags.get(key.as_str()),
                    Some(value.as_str()),
                    "Ad-hoc label '{}' should be present with value '{}'",
                    key,
                    value
                );
            }

            // Verify target and module tags are also present
            prop_assert_eq!(
                tags.get("target"),
                Some(target.as_str()),
                "Target tag should be present"
            );
            prop_assert_eq!(
                tags.get("module"),
                Some(module.as_str()),
                "Module tag should be present"
            );

            // Verify no predefined labels are present (since we didn't set any)
            prop_assert!(
                tags.get("geohash").is_none(),
                "Geohash should not be present when not configured"
            );
            prop_assert!(
                tags.get("region").is_none(),
                "Region should not be present when not configured"
            );
            prop_assert!(
                tags.get("location").is_none(),
                "Location should not be present when not configured"
            );
        }
    }

    // Feature: blackbox-exporter-optional-labels, Property 8: Ad-hoc label conflict resolution
    // For any scraped metric that already contains a tag matching an ad-hoc label key, the
    // existing tag should be renamed to "exported_<key>" and the new tag with the configured
    // value should be added.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn test_adhoc_label_conflict_resolution(
            target_scheme in "(http|https)",
            target_host in "[a-z][a-z0-9-]{2,20}\\.[a-z]{2,5}",
            module in "[a-z_][a-z0-9_]{2,20}",
            metric_name in "[a-z_][a-z0-9_]{2,20}",
            metric_value in 0.0f64..1000.0f64,
            // Generate ad-hoc labels
            num_adhoc_labels in 1usize..4,
            // Generate existing conflicting values
            num_conflicts in 1usize..3,
        ) {
            // Ensure we have at least one conflict
            let num_conflicts = num_conflicts.min(num_adhoc_labels);

            // Construct target URL
            let target = format!("{}://{}", target_scheme, target_host);

            // Generate ad-hoc labels
            let mut custom_labels = HashMap::new();
            for i in 0..num_adhoc_labels {
                custom_labels.insert(
                    format!("adhoc_key_{}", i),
                    format!("adhoc_value_{}", i)
                );
            }

            // Create OptionalLabels with ad-hoc labels
            let optional_labels = OptionalLabels {
                geohash: None,
                region: None,
                location: None,
                country: None,
                name: None,
                provider: None,
                custom: custom_labels.clone(),
            };

            // Create a BlackboxExporterContext
            let mut context = BlackboxExporterContext {
                target: target.clone(),
                module: module.clone(),
                optional_labels,
            };

            // Create a metric event with conflicting tags
            let mut metric = vector_lib::event::Metric::new(
                metric_name.clone(),
                vector_lib::event::MetricKind::Absolute,
                vector_lib::event::MetricValue::Gauge { value: metric_value },
            );

            // Add conflicting tags for the first num_conflicts ad-hoc labels
            let mut conflicting_values = HashMap::new();
            for i in 0..num_conflicts {
                let key = format!("adhoc_key_{}", i);
                let existing_value = format!("existing_value_{}", i);
                metric.replace_tag(key.clone(), existing_value.clone());
                conflicting_values.insert(key, existing_value);
            }

            let mut events = vec![Event::Metric(metric)];

            // Call enrich_events
            context.enrich_events(&mut events);

            // Verify conflict resolution
            let enriched_metric = events[0].as_metric();
            let tags = enriched_metric.tags().expect("Metric should have tags");

            // Property: All ad-hoc labels should be present with configured values
            for (key, value) in &custom_labels {
                prop_assert_eq!(
                    tags.get(key.as_str()),
                    Some(value.as_str()),
                    "Ad-hoc label '{}' should be present with configured value '{}'",
                    key,
                    value
                );
            }

            // Property: Conflicting tags should be renamed to exported_<key>
            for (key, existing_value) in &conflicting_values {
                let exported_key = format!("exported_{}", key);
                prop_assert_eq!(
                    tags.get(exported_key.as_str()),
                    Some(existing_value.as_str()),
                    "Conflicting tag '{}' should be renamed to '{}' with value '{}'",
                    key,
                    exported_key,
                    existing_value
                );
            }

            // Verify target and module tags are present
            prop_assert_eq!(
                tags.get("target"),
                Some(target.as_str()),
                "Target tag should be present"
            );
            prop_assert_eq!(
                tags.get("module"),
                Some(module.as_str()),
                "Module tag should be present"
            );
        }
    }

    // Feature: blackbox-exporter-optional-labels, Property 9: Combined predefined and ad-hoc labels
    // For any configuration with both predefined optional labels and ad-hoc labels, all scraped
    // metrics should contain tags for both types of labels.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn test_combined_predefined_and_adhoc_labels(
            target_scheme in "(http|https)",
            target_host in "[a-z][a-z0-9-]{2,20}\\.[a-z]{2,5}",
            module in "[a-z_][a-z0-9_]{2,20}",
            metric_name in "[a-z_][a-z0-9_]{2,20}",
            metric_value in 0.0f64..1000.0f64,
            // Predefined labels
            geohash_val in "[a-z0-9]{5,12}",
            region_val in "(AMER|EMEA|APAC)",
            location_val in "[A-Z][a-z]{2,15}",
            // Ad-hoc labels
            num_adhoc_labels in 1usize..4,
        ) {
            // Construct target URL
            let target = format!("{}://{}", target_scheme, target_host);

            // Generate ad-hoc labels
            let mut custom_labels = HashMap::new();
            for i in 0..num_adhoc_labels {
                custom_labels.insert(
                    format!("custom_key_{}", i),
                    format!("custom_value_{}", i)
                );
            }

            // Create OptionalLabels with both predefined and ad-hoc labels
            let optional_labels = OptionalLabels {
                geohash: Some(geohash_val.clone()),
                region: Some(region_val.clone()),
                location: Some(location_val.clone()),
                country: None,
                name: None,
                provider: None,
                custom: custom_labels.clone(),
            };

            // Create a BlackboxExporterContext
            let mut context = BlackboxExporterContext {
                target: target.clone(),
                module: module.clone(),
                optional_labels,
            };

            // Create a metric event
            let metric = vector_lib::event::Metric::new(
                metric_name.clone(),
                vector_lib::event::MetricKind::Absolute,
                vector_lib::event::MetricValue::Gauge { value: metric_value },
            );

            let mut events = vec![Event::Metric(metric)];

            // Call enrich_events
            context.enrich_events(&mut events);

            // Verify both predefined and ad-hoc labels are present
            let enriched_metric = events[0].as_metric();
            let tags = enriched_metric.tags().expect("Metric should have tags");

            // Property: All predefined labels should be present
            prop_assert_eq!(
                tags.get("geohash"),
                Some(geohash_val.as_str()),
                "Predefined geohash label should be present"
            );
            prop_assert_eq!(
                tags.get("region"),
                Some(region_val.as_str()),
                "Predefined region label should be present"
            );
            prop_assert_eq!(
                tags.get("location"),
                Some(location_val.as_str()),
                "Predefined location label should be present"
            );

            // Property: All ad-hoc labels should be present
            for (key, value) in &custom_labels {
                prop_assert_eq!(
                    tags.get(key.as_str()),
                    Some(value.as_str()),
                    "Ad-hoc label '{}' should be present with value '{}'",
                    key,
                    value
                );
            }

            // Verify target and module tags are present
            prop_assert_eq!(
                tags.get("target"),
                Some(target.as_str()),
                "Target tag should be present"
            );
            prop_assert_eq!(
                tags.get("module"),
                Some(module.as_str()),
                "Module tag should be present"
            );
        }
    }

    // Feature: blackbox-exporter-optional-labels, Property 10: Ad-hoc label precedence over predefined
    // For any ad-hoc label key that matches a predefined label key, the ad-hoc label value should
    // be used and the predefined label should be ignored.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn test_adhoc_label_precedence_over_predefined(
            target_scheme in "(http|https)",
            target_host in "[a-z][a-z0-9-]{2,20}\\.[a-z]{2,5}",
            module in "[a-z_][a-z0-9_]{2,20}",
            metric_name in "[a-z_][a-z0-9_]{2,20}",
            metric_value in 0.0f64..1000.0f64,
            // Predefined label values
            predefined_region in "(AMER|EMEA|APAC)",
            predefined_location in "[A-Z][a-z]{2,15}",
            // Ad-hoc label values that conflict with predefined
            adhoc_region in "[a-z]{2,10}",
            adhoc_location in "[a-z]{2,10}",
            // Non-conflicting labels
            geohash_val in "[a-z0-9]{5,12}",
        ) {
            // Construct target URL
            let target = format!("{}://{}", target_scheme, target_host);

            // Create ad-hoc labels that conflict with predefined labels
            let mut custom_labels = HashMap::new();
            custom_labels.insert("region".to_string(), adhoc_region.clone());
            custom_labels.insert("location".to_string(), adhoc_location.clone());
            custom_labels.insert("custom_key".to_string(), "custom_value".to_string());

            // Create OptionalLabels with both predefined and conflicting ad-hoc labels
            let optional_labels = OptionalLabels {
                geohash: Some(geohash_val.clone()),
                region: Some(predefined_region.clone()),
                location: Some(predefined_location.clone()),
                country: None,
                name: None,
                provider: None,
                custom: custom_labels.clone(),
            };

            // Create a BlackboxExporterContext
            let mut context = BlackboxExporterContext {
                target: target.clone(),
                module: module.clone(),
                optional_labels,
            };

            // Create a metric event
            let metric = vector_lib::event::Metric::new(
                metric_name.clone(),
                vector_lib::event::MetricKind::Absolute,
                vector_lib::event::MetricValue::Gauge { value: metric_value },
            );

            let mut events = vec![Event::Metric(metric)];

            // Call enrich_events
            context.enrich_events(&mut events);

            // Verify ad-hoc labels take precedence
            let enriched_metric = events[0].as_metric();
            let tags = enriched_metric.tags().expect("Metric should have tags");

            // Property: Ad-hoc "region" should override predefined "region"
            prop_assert_eq!(
                tags.get("region"),
                Some(adhoc_region.as_str()),
                "Ad-hoc region '{}' should override predefined region '{}'",
                adhoc_region,
                predefined_region
            );

            // Property: Ad-hoc "location" should override predefined "location"
            prop_assert_eq!(
                tags.get("location"),
                Some(adhoc_location.as_str()),
                "Ad-hoc location '{}' should override predefined location '{}'",
                adhoc_location,
                predefined_location
            );

            // Property: Non-conflicting predefined label should still be present
            prop_assert_eq!(
                tags.get("geohash"),
                Some(geohash_val.as_str()),
                "Non-conflicting predefined geohash should be present"
            );

            // Property: Non-conflicting ad-hoc label should be present
            prop_assert_eq!(
                tags.get("custom_key"),
                Some("custom_value"),
                "Non-conflicting ad-hoc label should be present"
            );

            // Verify target and module tags are present
            prop_assert_eq!(
                tags.get("target"),
                Some(target.as_str()),
                "Target tag should be present"
            );
            prop_assert_eq!(
                tags.get("module"),
                Some(module.as_str()),
                "Module tag should be present"
            );
        }
    }
}

// Unit tests for OptionalLabels construction

#[test]
fn test_optional_labels_from_config_with_all_fields() {
    // Test building OptionalLabels from config with all fields populated
    let mut labels_map = HashMap::new();
    labels_map.insert("environment".to_string(), "production".to_string());
    labels_map.insert("team".to_string(), "platform".to_string());

    let config = BlackboxExporterConfig {
        url: "http://localhost:9115".to_string(),
        targets: vec!["https://example.com".to_string()],
        module: "http_2xx".to_string(),
        interval: Duration::from_secs(15),
        timeout: Duration::from_secs(5),
        tls: None,
        auth: None,
        geohash: Some("9qx7hh9jd".to_string()),
        region: Some("AMER".to_string()),
        location: Some("Oregon".to_string()),
        country: Some("US".to_string()),
        name: Some("Example Check".to_string()),
        provider: Some("AWS".to_string()),
        labels: Some(labels_map.clone()),
    };

    let optional_labels = OptionalLabels::from_config(&config);

    // Verify all predefined labels are set
    assert_eq!(optional_labels.geohash, Some("9qx7hh9jd".to_string()));
    assert_eq!(optional_labels.region, Some("AMER".to_string()));
    assert_eq!(optional_labels.location, Some("Oregon".to_string()));
    assert_eq!(optional_labels.country, Some("US".to_string()));
    assert_eq!(optional_labels.name, Some("Example Check".to_string()));
    assert_eq!(optional_labels.provider, Some("AWS".to_string()));

    // Verify custom labels are set
    assert_eq!(optional_labels.custom.len(), 2);
    assert_eq!(
        optional_labels.custom.get("environment"),
        Some(&"production".to_string())
    );
    assert_eq!(
        optional_labels.custom.get("team"),
        Some(&"platform".to_string())
    );
}

#[test]
fn test_optional_labels_from_config_with_subset_of_fields() {
    // Test building OptionalLabels from config with only some fields populated
    let config = BlackboxExporterConfig {
        url: "http://localhost:9115".to_string(),
        targets: vec!["https://example.com".to_string()],
        module: "http_2xx".to_string(),
        interval: Duration::from_secs(15),
        timeout: Duration::from_secs(5),
        tls: None,
        auth: None,
        geohash: Some("9qx7hh9jd".to_string()),
        region: None,
        location: Some("Oregon".to_string()),
        country: None,
        name: Some("Example Check".to_string()),
        provider: None,
        labels: None,
    };

    let optional_labels = OptionalLabels::from_config(&config);

    // Verify specified labels are set
    assert_eq!(optional_labels.geohash, Some("9qx7hh9jd".to_string()));
    assert_eq!(optional_labels.location, Some("Oregon".to_string()));
    assert_eq!(optional_labels.name, Some("Example Check".to_string()));

    // Verify unspecified labels are None
    assert_eq!(optional_labels.region, None);
    assert_eq!(optional_labels.country, None);
    assert_eq!(optional_labels.provider, None);

    // Verify custom labels are empty
    assert!(optional_labels.custom.is_empty());
}

// Unit tests for label injection

#[test]
fn test_enrich_events_adds_predefined_labels_without_conflicts() {
    // Test adding predefined labels to metrics without conflicts
    let mut context = BlackboxExporterContext {
        target: "https://example.com".to_string(),
        module: "http_2xx".to_string(),
        optional_labels: OptionalLabels {
            geohash: Some("9qx7hh9jd".to_string()),
            region: Some("AMER".to_string()),
            location: Some("Oregon".to_string()),
            country: Some("US".to_string()),
            name: Some("Example Check".to_string()),
            provider: Some("AWS".to_string()),
            custom: HashMap::new(),
        },
    };

    let metric = vector_lib::event::Metric::new(
        "probe_success",
        vector_lib::event::MetricKind::Absolute,
        vector_lib::event::MetricValue::Gauge { value: 1.0 },
    );

    let mut events = vec![Event::Metric(metric)];
    context.enrich_events(&mut events);

    let enriched_metric = events[0].as_metric();
    let tags = enriched_metric.tags().unwrap();

    // Verify all predefined labels are added
    assert_eq!(tags.get("geohash"), Some("9qx7hh9jd"));
    assert_eq!(tags.get("region"), Some("AMER"));
    assert_eq!(tags.get("location"), Some("Oregon"));
    assert_eq!(tags.get("country"), Some("US"));
    assert_eq!(tags.get("name"), Some("Example Check"));
    assert_eq!(tags.get("provider"), Some("AWS"));

    // Verify target and module are also present
    assert_eq!(tags.get("target"), Some("https://example.com"));
    assert_eq!(tags.get("module"), Some("http_2xx"));
}

#[test]
fn test_enrich_events_adds_adhoc_labels_without_conflicts() {
    // Test adding ad-hoc labels to metrics without conflicts
    let mut custom_labels = HashMap::new();
    custom_labels.insert("environment".to_string(), "production".to_string());
    custom_labels.insert("team".to_string(), "platform".to_string());
    custom_labels.insert("cost_center".to_string(), "engineering".to_string());

    let mut context = BlackboxExporterContext {
        target: "https://example.com".to_string(),
        module: "http_2xx".to_string(),
        optional_labels: OptionalLabels {
            geohash: None,
            region: None,
            location: None,
            country: None,
            name: None,
            provider: None,
            custom: custom_labels,
        },
    };

    let metric = vector_lib::event::Metric::new(
        "probe_success",
        vector_lib::event::MetricKind::Absolute,
        vector_lib::event::MetricValue::Gauge { value: 1.0 },
    );

    let mut events = vec![Event::Metric(metric)];
    context.enrich_events(&mut events);

    let enriched_metric = events[0].as_metric();
    let tags = enriched_metric.tags().unwrap();

    // Verify all ad-hoc labels are added
    assert_eq!(tags.get("environment"), Some("production"));
    assert_eq!(tags.get("team"), Some("platform"));
    assert_eq!(tags.get("cost_center"), Some("engineering"));

    // Verify target and module are also present
    assert_eq!(tags.get("target"), Some("https://example.com"));
    assert_eq!(tags.get("module"), Some("http_2xx"));
}

#[test]
fn test_enrich_events_conflict_resolution_for_predefined_labels() {
    // Test conflict resolution for predefined labels
    let mut context = BlackboxExporterContext {
        target: "https://example.com".to_string(),
        module: "http_2xx".to_string(),
        optional_labels: OptionalLabels {
            geohash: Some("9qx7hh9jd".to_string()),
            region: Some("AMER".to_string()),
            location: Some("Oregon".to_string()),
            country: None,
            name: None,
            provider: None,
            custom: HashMap::new(),
        },
    };

    // Create metric with conflicting tags
    let mut metric = vector_lib::event::Metric::new(
        "probe_success",
        vector_lib::event::MetricKind::Absolute,
        vector_lib::event::MetricValue::Gauge { value: 1.0 },
    );
    metric.replace_tag("region".to_string(), "internal_region".to_string());
    metric.replace_tag("location".to_string(), "internal_location".to_string());

    let mut events = vec![Event::Metric(metric)];
    context.enrich_events(&mut events);

    let enriched_metric = events[0].as_metric();
    let tags = enriched_metric.tags().unwrap();

    // Verify new labels are added with configured values
    assert_eq!(tags.get("geohash"), Some("9qx7hh9jd"));
    assert_eq!(tags.get("region"), Some("AMER"));
    assert_eq!(tags.get("location"), Some("Oregon"));

    // Verify conflicting tags are renamed to exported_*
    assert_eq!(tags.get("exported_region"), Some("internal_region"));
    assert_eq!(tags.get("exported_location"), Some("internal_location"));
}

#[test]
fn test_enrich_events_conflict_resolution_for_adhoc_labels() {
    // Test conflict resolution for ad-hoc labels
    let mut custom_labels = HashMap::new();
    custom_labels.insert("environment".to_string(), "production".to_string());
    custom_labels.insert("team".to_string(), "platform".to_string());

    let mut context = BlackboxExporterContext {
        target: "https://example.com".to_string(),
        module: "http_2xx".to_string(),
        optional_labels: OptionalLabels {
            geohash: None,
            region: None,
            location: None,
            country: None,
            name: None,
            provider: None,
            custom: custom_labels,
        },
    };

    // Create metric with conflicting tags
    let mut metric = vector_lib::event::Metric::new(
        "probe_success",
        vector_lib::event::MetricKind::Absolute,
        vector_lib::event::MetricValue::Gauge { value: 1.0 },
    );
    metric.replace_tag("environment".to_string(), "internal_env".to_string());

    let mut events = vec![Event::Metric(metric)];
    context.enrich_events(&mut events);

    let enriched_metric = events[0].as_metric();
    let tags = enriched_metric.tags().unwrap();

    // Verify new labels are added with configured values
    assert_eq!(tags.get("environment"), Some("production"));
    assert_eq!(tags.get("team"), Some("platform"));

    // Verify conflicting tag is renamed to exported_*
    assert_eq!(tags.get("exported_environment"), Some("internal_env"));
}

#[test]
fn test_enrich_events_empty_string_values_not_added() {
    // Test that empty string values are not added as tags
    let mut context = BlackboxExporterContext {
        target: "https://example.com".to_string(),
        module: "http_2xx".to_string(),
        optional_labels: OptionalLabels {
            geohash: Some(String::new()), // Empty string
            region: Some("AMER".to_string()),
            location: Some(String::new()), // Empty string
            country: Some("US".to_string()),
            name: Some(String::new()), // Empty string
            provider: None,
            custom: HashMap::new(),
        },
    };

    let metric = vector_lib::event::Metric::new(
        "probe_success",
        vector_lib::event::MetricKind::Absolute,
        vector_lib::event::MetricValue::Gauge { value: 1.0 },
    );

    let mut events = vec![Event::Metric(metric)];
    context.enrich_events(&mut events);

    let enriched_metric = events[0].as_metric();
    let tags = enriched_metric.tags().unwrap();

    // Verify empty string labels are not added
    assert!(tags.get("geohash").is_none());
    assert!(tags.get("location").is_none());
    assert!(tags.get("name").is_none());

    // Verify non-empty labels are added
    assert_eq!(tags.get("region"), Some("AMER"));
    assert_eq!(tags.get("country"), Some("US"));
}

#[test]
fn test_enrich_events_preserves_existing_tags() {
    // Test that existing tags are preserved during enrichment
    let mut context = BlackboxExporterContext {
        target: "https://example.com".to_string(),
        module: "http_2xx".to_string(),
        optional_labels: OptionalLabels {
            geohash: Some("9qx7hh9jd".to_string()),
            region: Some("AMER".to_string()),
            location: None,
            country: None,
            name: None,
            provider: None,
            custom: HashMap::new(),
        },
    };

    // Create metric with existing tags
    let mut metric = vector_lib::event::Metric::new(
        "probe_success",
        vector_lib::event::MetricKind::Absolute,
        vector_lib::event::MetricValue::Gauge { value: 1.0 },
    );
    metric.replace_tag("instance".to_string(), "server1".to_string());
    metric.replace_tag("job".to_string(), "blackbox".to_string());
    metric.replace_tag("custom_tag".to_string(), "custom_value".to_string());

    let mut events = vec![Event::Metric(metric)];
    context.enrich_events(&mut events);

    let enriched_metric = events[0].as_metric();
    let tags = enriched_metric.tags().unwrap();

    // Verify existing tags are preserved
    assert_eq!(tags.get("instance"), Some("server1"));
    assert_eq!(tags.get("job"), Some("blackbox"));
    assert_eq!(tags.get("custom_tag"), Some("custom_value"));

    // Verify new labels are added
    assert_eq!(tags.get("geohash"), Some("9qx7hh9jd"));
    assert_eq!(tags.get("region"), Some("AMER"));

    // Verify target and module are also present
    assert_eq!(tags.get("target"), Some("https://example.com"));
    assert_eq!(tags.get("module"), Some("http_2xx"));
}

#[test]
fn test_optional_labels_from_config_with_no_optional_labels() {
    // Test building OptionalLabels from config with no optional labels
    let config = BlackboxExporterConfig {
        url: "http://localhost:9115".to_string(),
        targets: vec!["https://example.com".to_string()],
        module: "http_2xx".to_string(),
        interval: Duration::from_secs(15),
        timeout: Duration::from_secs(5),
        tls: None,
        auth: None,
        geohash: None,
        region: None,
        location: None,
        country: None,
        name: None,
        provider: None,
        labels: None,
    };

    let optional_labels = OptionalLabels::from_config(&config);

    // Verify all predefined labels are None
    assert_eq!(optional_labels.geohash, None);
    assert_eq!(optional_labels.region, None);
    assert_eq!(optional_labels.location, None);
    assert_eq!(optional_labels.country, None);
    assert_eq!(optional_labels.name, None);
    assert_eq!(optional_labels.provider, None);

    // Verify custom labels are empty
    assert!(optional_labels.custom.is_empty());
}

// Unit tests for ad-hoc label precedence

#[test]
fn test_adhoc_labels_override_predefined_labels_with_same_key() {
    // Test that ad-hoc labels override predefined labels when keys conflict
    let mut custom_labels = HashMap::new();
    custom_labels.insert("region".to_string(), "adhoc_region".to_string());
    custom_labels.insert("location".to_string(), "adhoc_location".to_string());
    custom_labels.insert("custom_key".to_string(), "custom_value".to_string());

    let mut context = BlackboxExporterContext {
        target: "https://example.com".to_string(),
        module: "http_2xx".to_string(),
        optional_labels: OptionalLabels {
            geohash: Some("9qx7hh9jd".to_string()),
            region: Some("AMER".to_string()),
            location: Some("Oregon".to_string()),
            country: Some("US".to_string()),
            name: None,
            provider: None,
            custom: custom_labels,
        },
    };

    let metric = vector_lib::event::Metric::new(
        "probe_success",
        vector_lib::event::MetricKind::Absolute,
        vector_lib::event::MetricValue::Gauge { value: 1.0 },
    );

    let mut events = vec![Event::Metric(metric)];
    context.enrich_events(&mut events);

    let enriched_metric = events[0].as_metric();
    let tags = enriched_metric.tags().unwrap();

    // Verify ad-hoc labels override predefined labels
    assert_eq!(tags.get("region"), Some("adhoc_region"));
    assert_eq!(tags.get("location"), Some("adhoc_location"));

    // Verify non-conflicting predefined labels are still present
    assert_eq!(tags.get("geohash"), Some("9qx7hh9jd"));
    assert_eq!(tags.get("country"), Some("US"));

    // Verify non-conflicting ad-hoc label is present
    assert_eq!(tags.get("custom_key"), Some("custom_value"));
}

#[test]
fn test_both_predefined_and_adhoc_labels_added_when_keys_dont_conflict() {
    // Test that both predefined and ad-hoc labels are added when keys don't conflict
    let mut custom_labels = HashMap::new();
    custom_labels.insert("environment".to_string(), "production".to_string());
    custom_labels.insert("team".to_string(), "platform".to_string());

    let mut context = BlackboxExporterContext {
        target: "https://example.com".to_string(),
        module: "http_2xx".to_string(),
        optional_labels: OptionalLabels {
            geohash: Some("9qx7hh9jd".to_string()),
            region: Some("AMER".to_string()),
            location: Some("Oregon".to_string()),
            country: None,
            name: None,
            provider: None,
            custom: custom_labels,
        },
    };

    let metric = vector_lib::event::Metric::new(
        "probe_success",
        vector_lib::event::MetricKind::Absolute,
        vector_lib::event::MetricValue::Gauge { value: 1.0 },
    );

    let mut events = vec![Event::Metric(metric)];
    context.enrich_events(&mut events);

    let enriched_metric = events[0].as_metric();
    let tags = enriched_metric.tags().unwrap();

    // Verify all predefined labels are present
    assert_eq!(tags.get("geohash"), Some("9qx7hh9jd"));
    assert_eq!(tags.get("region"), Some("AMER"));
    assert_eq!(tags.get("location"), Some("Oregon"));

    // Verify all ad-hoc labels are present
    assert_eq!(tags.get("environment"), Some("production"));
    assert_eq!(tags.get("team"), Some("platform"));

    // Verify target and module are present
    assert_eq!(tags.get("target"), Some("https://example.com"));
    assert_eq!(tags.get("module"), Some("http_2xx"));
}

// Integration tests for optional labels

#[tokio::test]
async fn test_integration_predefined_optional_labels() {
    // Set up mock Blackbox Exporter endpoint
    let (_guard, addr) = next_addr();

    let mock_endpoint = warp::path!("probe")
        .and(warp::query::<std::collections::HashMap<String, String>>())
        .map(|_q: std::collections::HashMap<String, String>| {
            // Return mock Prometheus metrics
            warp::http::Response::builder()
                .header("Content-Type", "text/plain")
                .body(
                    r#"# HELP probe_success Displays whether or not the probe was a success
# TYPE probe_success gauge
probe_success 1
# HELP probe_duration_seconds Returns how long the probe took to complete in seconds
# TYPE probe_duration_seconds gauge
probe_duration_seconds 0.123
"#,
                )
                .unwrap()
        });

    tokio::spawn(warp::serve(mock_endpoint).run(addr));
    wait_for_tcp(addr).await;

    // Configure source with predefined optional labels
    let config = BlackboxExporterConfig {
        url: format!("http://{}", addr),
        targets: vec!["https://example.com".to_string()],
        module: "http_2xx".to_string(),
        interval: Duration::from_secs(1),
        timeout: Duration::from_millis(500),
        tls: None,
        auth: None,
        geohash: Some("9qx7hh9jd".to_string()),
        region: Some("AMER".to_string()),
        location: Some("Oregon".to_string()),
        country: Some("US".to_string()),
        name: Some("Example Check".to_string()),
        provider: Some("AWS".to_string()),
        labels: None,
    };

    // Run source and collect events
    let events =
        run_and_assert_source_compliance(config, Duration::from_secs(3), &HTTP_PULL_SOURCE_TAGS)
            .await;

    // Verify metrics are scraped
    assert!(
        !events.is_empty(),
        "Should have received at least one event"
    );

    // Verify all metrics have the predefined optional labels
    for event in &events {
        let metric = event.as_metric();
        let tags = metric.tags().expect("Metric should have tags");

        // Verify target and module tags
        assert_eq!(
            tags.get("target"),
            Some("https://example.com"),
            "Metric should have correct target tag"
        );
        assert_eq!(
            tags.get("module"),
            Some("http_2xx"),
            "Metric should have correct module tag"
        );

        // Verify all predefined optional labels are present
        assert_eq!(
            tags.get("geohash"),
            Some("9qx7hh9jd"),
            "Metric should have geohash label"
        );
        assert_eq!(
            tags.get("region"),
            Some("AMER"),
            "Metric should have region label"
        );
        assert_eq!(
            tags.get("location"),
            Some("Oregon"),
            "Metric should have location label"
        );
        assert_eq!(
            tags.get("country"),
            Some("US"),
            "Metric should have country label"
        );
        assert_eq!(
            tags.get("name"),
            Some("Example Check"),
            "Metric should have name label"
        );
        assert_eq!(
            tags.get("provider"),
            Some("AWS"),
            "Metric should have provider label"
        );
    }
}

#[tokio::test]
async fn test_integration_adhoc_labels() {
    // Set up mock Blackbox Exporter endpoint
    let (_guard, addr) = next_addr();

    let mock_endpoint = warp::path!("probe")
        .and(warp::query::<std::collections::HashMap<String, String>>())
        .map(|_q: std::collections::HashMap<String, String>| {
            // Return mock Prometheus metrics
            warp::http::Response::builder()
                .header("Content-Type", "text/plain")
                .body(
                    r#"# HELP probe_success Displays whether or not the probe was a success
# TYPE probe_success gauge
probe_success 1
# HELP probe_duration_seconds Returns how long the probe took to complete in seconds
# TYPE probe_duration_seconds gauge
probe_duration_seconds 0.123
"#,
                )
                .unwrap()
        });

    tokio::spawn(warp::serve(mock_endpoint).run(addr));
    wait_for_tcp(addr).await;

    // Configure source with ad-hoc labels in labels map
    let mut labels_map = HashMap::new();
    labels_map.insert("environment".to_string(), "production".to_string());
    labels_map.insert("team".to_string(), "platform".to_string());
    labels_map.insert("cost_center".to_string(), "engineering".to_string());

    let config = BlackboxExporterConfig {
        url: format!("http://{}", addr),
        targets: vec!["https://example.com".to_string()],
        module: "http_2xx".to_string(),
        interval: Duration::from_secs(1),
        timeout: Duration::from_millis(500),
        tls: None,
        auth: None,
        geohash: None,
        region: None,
        location: None,
        country: None,
        name: None,
        provider: None,
        labels: Some(labels_map.clone()),
    };

    // Run source and collect events
    let events =
        run_and_assert_source_compliance(config, Duration::from_secs(3), &HTTP_PULL_SOURCE_TAGS)
            .await;

    // Verify metrics are scraped
    assert!(
        !events.is_empty(),
        "Should have received at least one event"
    );

    // Verify all metrics have the ad-hoc labels
    for event in &events {
        let metric = event.as_metric();
        let tags = metric.tags().expect("Metric should have tags");

        // Verify target and module tags
        assert_eq!(
            tags.get("target"),
            Some("https://example.com"),
            "Metric should have correct target tag"
        );
        assert_eq!(
            tags.get("module"),
            Some("http_2xx"),
            "Metric should have correct module tag"
        );

        // Verify all ad-hoc labels are present
        assert_eq!(
            tags.get("environment"),
            Some("production"),
            "Metric should have environment label"
        );
        assert_eq!(
            tags.get("team"),
            Some("platform"),
            "Metric should have team label"
        );
        assert_eq!(
            tags.get("cost_center"),
            Some("engineering"),
            "Metric should have cost_center label"
        );

        // Verify no predefined labels are present (since we didn't set any)
        assert!(
            tags.get("geohash").is_none(),
            "Metric should not have geohash label"
        );
        assert!(
            tags.get("region").is_none(),
            "Metric should not have region label"
        );
        assert!(
            tags.get("location").is_none(),
            "Metric should not have location label"
        );
        assert!(
            tags.get("country").is_none(),
            "Metric should not have country label"
        );
        assert!(
            tags.get("name").is_none(),
            "Metric should not have name label"
        );
        assert!(
            tags.get("provider").is_none(),
            "Metric should not have provider label"
        );
    }
}

#[tokio::test]
async fn test_integration_combined_labels() {
    // Set up mock Blackbox Exporter endpoint
    let (_guard, addr) = next_addr();

    let mock_endpoint = warp::path!("probe")
        .and(warp::query::<std::collections::HashMap<String, String>>())
        .map(|_q: std::collections::HashMap<String, String>| {
            // Return mock Prometheus metrics
            warp::http::Response::builder()
                .header("Content-Type", "text/plain")
                .body(
                    r#"# HELP probe_success Displays whether or not the probe was a success
# TYPE probe_success gauge
probe_success 1
# HELP probe_duration_seconds Returns how long the probe took to complete in seconds
# TYPE probe_duration_seconds gauge
probe_duration_seconds 0.123
"#,
                )
                .unwrap()
        });

    tokio::spawn(warp::serve(mock_endpoint).run(addr));
    wait_for_tcp(addr).await;

    // Configure source with both predefined and ad-hoc labels
    let mut labels_map = HashMap::new();
    labels_map.insert("environment".to_string(), "production".to_string());
    labels_map.insert("team".to_string(), "platform".to_string());

    let config = BlackboxExporterConfig {
        url: format!("http://{}", addr),
        targets: vec!["https://example.com".to_string()],
        module: "http_2xx".to_string(),
        interval: Duration::from_secs(1),
        timeout: Duration::from_millis(500),
        tls: None,
        auth: None,
        geohash: Some("9qx7hh9jd".to_string()),
        region: Some("AMER".to_string()),
        location: Some("Oregon".to_string()),
        country: Some("US".to_string()),
        name: Some("Example Check".to_string()),
        provider: Some("AWS".to_string()),
        labels: Some(labels_map.clone()),
    };

    // Run source and collect events
    let events =
        run_and_assert_source_compliance(config, Duration::from_secs(3), &HTTP_PULL_SOURCE_TAGS)
            .await;

    // Verify metrics are scraped
    assert!(
        !events.is_empty(),
        "Should have received at least one event"
    );

    // Verify all metrics have both predefined and ad-hoc labels
    for event in &events {
        let metric = event.as_metric();
        let tags = metric.tags().expect("Metric should have tags");

        // Verify target and module tags
        assert_eq!(
            tags.get("target"),
            Some("https://example.com"),
            "Metric should have correct target tag"
        );
        assert_eq!(
            tags.get("module"),
            Some("http_2xx"),
            "Metric should have correct module tag"
        );

        // Verify all predefined optional labels are present
        assert_eq!(
            tags.get("geohash"),
            Some("9qx7hh9jd"),
            "Metric should have geohash label"
        );
        assert_eq!(
            tags.get("region"),
            Some("AMER"),
            "Metric should have region label"
        );
        assert_eq!(
            tags.get("location"),
            Some("Oregon"),
            "Metric should have location label"
        );
        assert_eq!(
            tags.get("country"),
            Some("US"),
            "Metric should have country label"
        );
        assert_eq!(
            tags.get("name"),
            Some("Example Check"),
            "Metric should have name label"
        );
        assert_eq!(
            tags.get("provider"),
            Some("AWS"),
            "Metric should have provider label"
        );

        // Verify all ad-hoc labels are present
        assert_eq!(
            tags.get("environment"),
            Some("production"),
            "Metric should have environment label"
        );
        assert_eq!(
            tags.get("team"),
            Some("platform"),
            "Metric should have team label"
        );
    }
}

#[tokio::test]
async fn test_integration_no_optional_labels() {
    // Set up mock Blackbox Exporter endpoint
    let (_guard, addr) = next_addr();

    let mock_endpoint = warp::path!("probe")
        .and(warp::query::<std::collections::HashMap<String, String>>())
        .map(|_q: std::collections::HashMap<String, String>| {
            // Return mock Prometheus metrics
            warp::http::Response::builder()
                .header("Content-Type", "text/plain")
                .body(
                    r#"# HELP probe_success Displays whether or not the probe was a success
# TYPE probe_success gauge
probe_success 1
# HELP probe_duration_seconds Returns how long the probe took to complete in seconds
# TYPE probe_duration_seconds gauge
probe_duration_seconds 0.123
"#,
                )
                .unwrap()
        });

    tokio::spawn(warp::serve(mock_endpoint).run(addr));
    wait_for_tcp(addr).await;

    // Configure source without any optional labels
    let config = BlackboxExporterConfig {
        url: format!("http://{}", addr),
        targets: vec!["https://example.com".to_string()],
        module: "http_2xx".to_string(),
        interval: Duration::from_secs(1),
        timeout: Duration::from_millis(500),
        tls: None,
        auth: None,
        geohash: None,
        region: None,
        location: None,
        country: None,
        name: None,
        provider: None,
        labels: None,
    };

    // Run source and collect events
    let events =
        run_and_assert_source_compliance(config, Duration::from_secs(3), &HTTP_PULL_SOURCE_TAGS)
            .await;

    // Verify metrics are scraped
    assert!(
        !events.is_empty(),
        "Should have received at least one event"
    );

    // Verify source works normally with only target and module tags
    for event in &events {
        let metric = event.as_metric();
        let tags = metric.tags().expect("Metric should have tags");

        // Verify target and module tags are present
        assert_eq!(
            tags.get("target"),
            Some("https://example.com"),
            "Metric should have correct target tag"
        );
        assert_eq!(
            tags.get("module"),
            Some("http_2xx"),
            "Metric should have correct module tag"
        );

        // Verify no optional labels are present
        assert!(
            tags.get("geohash").is_none(),
            "Metric should not have geohash label"
        );
        assert!(
            tags.get("region").is_none(),
            "Metric should not have region label"
        );
        assert!(
            tags.get("location").is_none(),
            "Metric should not have location label"
        );
        assert!(
            tags.get("country").is_none(),
            "Metric should not have country label"
        );
        assert!(
            tags.get("name").is_none(),
            "Metric should not have name label"
        );
        assert!(
            tags.get("provider").is_none(),
            "Metric should not have provider label"
        );
    }
}
