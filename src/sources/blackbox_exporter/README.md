# Blackbox Exporter Source

The `blackbox_exporter` source scrapes metrics from [Prometheus Blackbox Exporter](https://github.com/prometheus/blackbox_exporter) instances.

## Overview

The Blackbox Exporter allows blackbox probing of endpoints over HTTP, HTTPS, DNS, TCP, and ICMP. This source simplifies the configuration compared to using the generic `prometheus_scrape` source by:

1. **Automatic URL Construction**: Constructs probe URLs in the format `<url>/probe?target=<target>&module=<module>`
2. **Automatic Label Injection**: Adds `target` and `module` labels to all scraped metrics
3. **Conflict Resolution**: Renames existing `target` or `module` labels to `exported_target` and `exported_module`

## Configuration

### Required Fields

- `url`: URL of the Blackbox Exporter instance (e.g., `http://localhost:9115`)
- `targets`: List of targets to probe (URLs, hostnames, or IP addresses)
- `module`: Blackbox Exporter module to use (must be defined in Blackbox Exporter config)

### Optional Fields

- `scrape_interval_secs`: Interval between scrapes (default: 15 seconds)
- `scrape_timeout_secs`: Timeout for each scrape request (default: 10 seconds)
- `tls`: TLS configuration for connecting to the Blackbox Exporter
- `auth`: Authentication configuration (basic auth, bearer token, etc.)

## Examples

### HTTP Probing

```yaml
sources:
  blackbox_http:
    type: blackbox_exporter
    url: http://localhost:9115
    targets:
      - https://example.com
      - https://www.google.com
    module: http_2xx
    scrape_interval_secs: 15
```

### ICMP Probing

```yaml
sources:
  blackbox_icmp:
    type: blackbox_exporter
    url: http://localhost:9115
    targets:
      - 8.8.8.8
      - 1.1.1.1
      - example.com
    module: icmp
    scrape_interval_secs: 30
```

### Multiple Modules

To probe targets with different modules, create multiple source blocks:

```yaml
sources:
  blackbox_http:
    type: blackbox_exporter
    url: http://localhost:9115
    targets:
      - https://api.example.com
    module: http_2xx

  blackbox_tcp:
    type: blackbox_exporter
    url: http://localhost:9115
    targets:
      - database.example.com:5432
    module: tcp_connect
```

### Secure Connection

```yaml
sources:
  blackbox_secure:
    type: blackbox_exporter
    url: https://blackbox.example.com
    targets:
      - https://api.example.com
    module: http_2xx
    tls:
      ca_file: /etc/vector/ca.crt
      verify_certificate: true
    auth:
      strategy: basic
      user: vector
      password: ${BLACKBOX_PASSWORD}
```

## Label Injection

The source automatically adds the following labels to all scraped metrics:

- `target`: The target URL being probed
- `module`: The Blackbox Exporter module used

If a metric already contains a `target` or `module` label, the existing label is renamed to `exported_target` or `exported_module` respectively.

### Example

Original metric from Blackbox Exporter:
```
probe_success{} 1
```

After enrichment:
```
probe_success{target="https://example.com", module="http_2xx"} 1
```

## Comparison with prometheus_scrape

The `blackbox_exporter` source is a specialized version of `prometheus_scrape` designed specifically for Blackbox Exporter. The main differences are:

| Feature | blackbox_exporter | prometheus_scrape |
|---------|-------------------|-------------------|
| URL Construction | Automatic | Manual |
| Label Injection | Automatic (target, module) | Manual (requires transforms) |
| Configuration | Simplified | Generic |
| Use Case | Blackbox Exporter only | Any Prometheus endpoint |

## Performance Considerations

- All targets are scraped concurrently at the configured interval
- If a scrape takes longer than the interval, a new scrape will be started (can consume extra resources)
- Set `scrape_timeout_secs` lower than `scrape_interval_secs` to prevent resource issues
- The source will emit a warning if timeout >= interval

## Error Handling

- Individual target failures do not affect other targets
- Parse errors are logged but do not stop the scraping process
- HTTP errors (non-200 responses) are logged with details
- Network errors are retried on the next scrape cycle

## Internal Events

The source emits the following internal events for monitoring:

- `EndpointBytesReceived`: Bytes received from successful scrapes
- `HttpClientEventsReceived`: Number of events parsed from responses
- `HttpClientHttpError`: Network/connection errors
- `HttpClientHttpResponseError`: Non-200 HTTP responses
- `PrometheusParseError`: Prometheus text format parsing errors
