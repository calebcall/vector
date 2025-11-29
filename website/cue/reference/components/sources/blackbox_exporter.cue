package metadata

components: sources: blackbox_exporter: {
	title: "Blackbox Exporter"

	classes: {
		commonly_used: false
		delivery:      "at_least_once"
		deployment_roles: ["daemon", "sidecar", "aggregator"]
		development:   "beta"
		egress_method: "batch"
		stateful:      false
	}

	features: {
		auto_generated:   true
		acknowledgements: false
		collect: {
			checkpoint: enabled: false
			from: {
				service: services.prometheus

				interface: socket: {
					api: {
						title: "Prometheus Blackbox Exporter"
						url:   urls.prometheus_blackbox_exporter
					}
					direction: "outgoing"
					protocols: ["http"]
					ssl: "optional"
				}
			}
			proxy: enabled: true
			tls: {
				enabled:                true
				can_verify_certificate: true
				can_verify_hostname:    true
				enabled_default:        false
				enabled_by_scheme:      true
			}
		}
		multiline: enabled: false
	}

	support: {
		requirements: []
		warnings: []
		notices: []
	}

	installation: {
		platform_name: null
	}

	configuration: generated.components.sources.blackbox_exporter.configuration

	how_it_works: {
		url_construction: {
			title: "Automatic URL construction"
			body: """
				The `blackbox_exporter` source automatically constructs probe URLs from the
				configured base URL, targets, and module. For each target, it creates a URL
				in the format:

				```
				<url>/probe?target=<encoded_target>&module=<encoded_module>
				```

				The target and module parameters are properly URL-encoded. If the base URL
				contains a path or existing query parameters, they are preserved in the
				constructed probe URLs.
				"""
		}

		label_injection: {
			title: "Automatic label injection"
			body: """
				The source automatically adds `target` and `module` labels to all scraped
				metrics. This eliminates the need for manual label remapping transforms.

				If a scraped metric already contains a `target` or `module` label, the
				existing label is renamed to `exported_target` or `exported_module`
				respectively, following Prometheus conventions for label conflicts.

				**Example:**

				Original metric from Blackbox Exporter:
				```
				probe_success{} 1
				```

				After enrichment:
				```
				probe_success{target="https://example.com", module="http_2xx"} 1
				```
				"""
		}

		multiple_targets: {
			title: "Multiple targets"
			body: """
				All configured targets are scraped concurrently at the specified interval.
				If a scrape request for one target fails, other targets continue to be
				scraped without interruption.

				Each metric is tagged with the specific target it was scraped from, allowing
				you to distinguish metrics from different targets in your monitoring system.
				"""
		}

		comparison_with_prometheus_scrape: {
			title: "Comparison with prometheus_scrape"
			body: """
				The `blackbox_exporter` source is a specialized version of `prometheus_scrape`
				designed specifically for Blackbox Exporter. The main differences are:

				- **URL Construction**: Automatic vs. manual
				- **Label Injection**: Automatic (target, module) vs. manual (requires transforms)
				- **Configuration**: Simplified vs. generic
				- **Use Case**: Blackbox Exporter only vs. any Prometheus endpoint

				Use `blackbox_exporter` when scraping Blackbox Exporter instances for a
				simpler configuration. Use `prometheus_scrape` for other Prometheus endpoints.
				"""
		}
	}

	output: metrics: {
		_extra_tags: {
			"target": {
				description: "The target URL being probed. Automatically added by the source."
				examples: ["https://example.com", "8.8.8.8"]
				required: true
			}
			"module": {
				description: "The Blackbox Exporter module used for probing. Automatically added by the source."
				examples: ["http_2xx", "icmp", "tcp_connect"]
				required: true
			}
			"exported_target": {
				description: "The original target label from the metric, if it existed. Only present if the scraped metric already had a 'target' label."
				examples: ["internal"]
				required: false
			}
			"exported_module": {
				description: "The original module label from the metric, if it existed. Only present if the scraped metric already had a 'module' label."
				examples: ["custom"]
				required: false
			}
		}

		counter: output._passthrough_counter & {
			tags: _extra_tags
		}
		gauge: output._passthrough_gauge & {
			tags: _extra_tags
		}
		histogram: output._passthrough_histogram & {
			tags: _extra_tags
		}
		summary: output._passthrough_summary & {
			tags: _extra_tags
		}
	}

	telemetry: metrics: {
		http_client_responses_total:      components.sources.internal_metrics.output.metrics.http_client_responses_total
		http_client_response_rtt_seconds: components.sources.internal_metrics.output.metrics.http_client_response_rtt_seconds
		parse_errors_total:               components.sources.internal_metrics.output.metrics.parse_errors_total
	}
}
