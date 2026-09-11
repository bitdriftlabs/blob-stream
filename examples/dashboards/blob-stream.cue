import "github.com/bitdriftlabs/dashboards/lib"

import "github.com/bitdriftlabs/dashboards/lib:k8s"

lib.#dashboard
title: "blob-stream broker"

#api_cost: {
	seconds_per_hour:            3600
	seconds_per_day:             86400
	seconds_per_month:           2_592_000
	s3_retention_days:           3
	bytes_per_gb:                1_000_000_000
	s3_put_per_request:          0.005 / 1000
	s3_get_per_request:          0.0004 / 1000
	s3_storage_per_gb_month:     0.023
	dynamo_wru_per_request_unit: 1.25 / 1_000_000
	dynamo_rru_per_request_unit: 0.25 / 1_000_000
}

#s3_put_request_rate:          "sum(loop:$environment:blob_stream_broker:blob_stream_broker:write:flush_uploaded_objects_total)"
#consumer_s3_get_request_rate: #"sum({__name__=~"loop:$environment:.*:consumer:reader:fallback_blob_range_requests"})"#
#broker_s3_get_request_rate:   "sum(loop:$environment:blob_stream_broker:blob_stream_broker:blob_cache:fetches_total)"
#s3_get_request_rate:          "\(#consumer_s3_get_request_rate) + \(#broker_s3_get_request_rate)"
#s3_upload_bytes_rate:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:write:flush_uploaded_object_bytes_total)"
#dynamo_wru_rate:              #"sum({__name__=~"loop:$environment:.*:dynamo:write_request_units_total"})"#
#dynamo_rru_rate:              #"sum({__name__=~"loop:$environment:.*:dynamo:read_request_units_total"})"#

#rows: [
	lib.#row & {
		title: "gRPC"
		panels: [
			lib.#panel & {
				title: "Requests"
				targets: [
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:grpc:requests_total)"
						legendFormat: "Requests"
					},
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:grpc:records_total)"
						legendFormat: "Records"
					},
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:grpc:batches_total)"
						legendFormat: "Batches"
					},
				]
			},
			lib.#panel & {
				title: "Responses"
				targets: [
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:grpc:responses_ok_total)"
						legendFormat: "OK"
					},
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:grpc:responses_not_lease_holder_total)"
						legendFormat: "Not lease holder"
					},
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:grpc:responses_unknown_topic_total)"
						legendFormat: "Unknown topic"
					},
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:grpc:responses_overloaded_total)"
						legendFormat: "Overloaded"
					},
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:grpc:responses_bad_request_total)"
						legendFormat: "Bad request"
					},
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:grpc:request_timeouts_total)"
						legendFormat: "Timed out"
					},
				]
			},
			lib.#panel & {
				title: "Active Batches"
				targets: [lib.#target & {
					expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:grpc:active_batches)"
					legendFormat: "Batches"
				}]
			},
			lib.#panel & {
				title: "Logical Batch Latency"
				targets: [
					for percentile in [0.99, 0.9, 0.5] {
						lib.#target & {
							expr:         "histogram_quantile(\(percentile), sum by (le) (loop:$environment:blob_stream_broker:blob_stream_broker:grpc:request_latency_seconds_bucket))"
							legendFormat: "p\(percentile)"
						}
					},
				]
				#y_format: "s"
			},
			lib.#panel & {
				title: "Grouped Request Latency"
				targets: [
					for percentile in [0.99, 0.9, 0.5] {
						lib.#target & {
							expr:         "histogram_quantile(\(percentile), sum by (le) (loop:$environment:blob_stream_broker:blob_stream_broker:grpc:grouped_request_latency_seconds_bucket))"
							legendFormat: "p\(percentile)"
						}
					},
				]
				#y_format: "s"
			},
		]
	},
	lib.#row & {
		title: "Write Path"
		panels: [
			lib.#panel & {
				title: "Successful Produce Throughput"
				targets: [
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:write:produce_requests_total)"
						legendFormat: "Attempts"
					},
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:write:produce_records_total)"
						legendFormat: "Written records"
					},
				]
			},
			lib.#panel & {
				title: "Successful Payload Throughput"
				targets: [lib.#target & {
					expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:write:produce_payload_bytes_total)"
					legendFormat: "Written payload"
				}]
				#y_format: "binBps"
			},
			lib.#panel & {
				title: "Rejected Records"
				targets: [lib.#target & {
					expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:write:produce_rejected_records_total)"
					legendFormat: "Records"
				}]
			},
			lib.#panel & {
				title: "Rejected Payload"
				targets: [lib.#target & {
					expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:write:produce_rejected_payload_bytes_total)"
					legendFormat: "Payload"
				}]
				#y_format: "binBps"
			},
			lib.#panel & {
				title: "Produce Outcomes"
				targets: [
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:write:produce_ok_total)"
						legendFormat: "OK"
					},
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:write:produce_not_lease_holder_total)"
						legendFormat: "Not lease holder"
					},
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:write:produce_unknown_topic_total)"
						legendFormat: "Unknown topic"
					},
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:write:produce_overloaded_total)"
						legendFormat: "Overloaded"
					},
				]
			},
		]
	},
	lib.#row & {
		title: "Flush"
		panels: [
			lib.#panel & {
				title: "Work"
				targets: [
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:write:flush_uploaded_objects_total)"
						legendFormat: "Uploaded objects (S3 PUTs)"
					},
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:write:flush_max_segment_size_splits_total)"
						legendFormat: "Max segment size splits"
					},
				]
			},
			lib.#panel & {
				title: "Adaptive Delay"
				targets: [
					lib.#target & {
						expr:         "max(loop:$environment:blob_stream_broker:blob_stream_broker:write:adaptive_flush_max_delay_ms)"
						legendFormat: "Maximum"
					},
					lib.#target & {
						expr:         "avg(loop:$environment:blob_stream_broker:blob_stream_broker:write:adaptive_flush_max_delay_ms)"
						legendFormat: "Average"
					},
					lib.#target & {
						expr:         "min(loop:$environment:blob_stream_broker:blob_stream_broker:write:adaptive_flush_max_delay_ms)"
						legendFormat: "Minimum"
					},
				]
				#y_format: "ms"
			},
			lib.#panel & {
				title: "Active Plans"
				targets: [
					lib.#target & {
						expr:         "max(loop:$environment:blob_stream_broker:blob_stream_broker:write:active_flush_plans)"
						legendFormat: "Active plans"
					},
					lib.#target & {
						expr:         "4"
						legendFormat: "Current limit"
					},
				]
			},
			lib.#panel & {
				title: "Trigger"
				targets: [
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:write:flush_batches_max_bytes_total)"
						legendFormat: "Max buffer bytes"
					},
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:write:flush_batches_max_delay_total)"
						legendFormat: "Max delay"
					},
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:write:flush_batches_lease_drain_total)"
						legendFormat: "Lease drain"
					},
				]
			},
			lib.#panel & {
				title: "S3 Upload Throughput"
				targets: [lib.#target & {
					expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:write:flush_uploaded_object_bytes_total)"
					legendFormat: "Uploaded object bytes"
				}]
				#y_format: "binBps"
			},
			lib.#panel & {
				title: "S3 Upload Ratio"
				targets: [lib.#target & {
					expr:         "100 * sum(loop:$environment:blob_stream_broker:blob_stream_broker:write:flush_uploaded_object_bytes_total) / clamp_min(sum(loop:$environment:blob_stream_broker:blob_stream_broker:write:produce_payload_bytes_total), 1)"
					legendFormat: "Uploaded / producer input"
				}]
				#y_format: "percent"
			},
			lib.#panel & {
				title: "S3 Object Size"
				targets: [
					for percentile in [0.99, 0.9, 0.5] {
						lib.#target & {
							expr:         "histogram_quantile(\(percentile), sum by (le) (loop:$environment:blob_stream_broker:blob_stream_broker:write:flush_uploaded_object_bytes_bucket))"
							legendFormat: "p\(percentile)"
						}
					},
				]
				#y_format: "bytes"
			},
			lib.#panel & {
				title: "Failures"
				targets: [
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:write:flush_failures_total)"
						legendFormat: "Flush failures"
					},
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:write:metadata_publication_deadline_exhausted_before_persistence_total)"
						legendFormat: "Deadline before persistence"
					},
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:write:metadata_publication_deadline_exhausted_while_persisting_total)"
						legendFormat: "Deadline while persisting"
					},
				]
			},
			lib.#panel & {
				title: "Latency"
				targets: [
					for percentile in [0.99, 0.9, 0.5] {
						lib.#target & {
							expr:         "histogram_quantile(\(percentile), sum by (le) (loop:$environment:blob_stream_broker:blob_stream_broker:write:flush_latency_seconds_bucket))"
							legendFormat: "p\(percentile)"
						}
					},
				]
				#y_format: "s"
			},
		]
	},
	lib.#row & {
		title: "Sequence Reservation"
		panels: [
			lib.#panel & {
				title: "Work"
				targets: [
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:write:sequence_reservations_total)"
						legendFormat: "Reservations"
					},
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:write:sequence_reservation_records_total)"
						legendFormat: "Records reserved"
					},
				]
			},
			lib.#panel & {
				title: "Failures"
				targets: [lib.#target & {
					expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:write:sequence_reservation_failures_total)"
					legendFormat: "Failures"
				}]
			},
			lib.#panel & {
				title: "Latency"
				targets: [
					for percentile in [0.99, 0.9, 0.5] {
						lib.#target & {
							expr:         "histogram_quantile(\(percentile), sum by (le) (loop:$environment:blob_stream_broker:blob_stream_broker:write:sequence_reservation_latency_seconds_bucket))"
							legendFormat: "p\(percentile)"
						}
					},
				]
				#y_format: "s"
			},
		]
	},
	lib.#row & {
		title: "Lease Draining"
		panels: [lib.#panel & {
			title: "Transitions"
			targets: [
				lib.#target & {
					expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:write:lease_drain_starts_total)"
					legendFormat: "Drain starts"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:write:lease_drain_completions_total)"
					legendFormat: "Drain completions"
				},
			]
		}]
	},
	lib.#row & {
		title: "Global Consumer Delivery"
		panels: [lib.#panel & {
			title: "Delivery Gaps"
			targets: [lib.#target & {
				expr:         #"sum({__name__=~"loop:$environment:.*:consumer:iterator:delivery_gap_events"})"#
				legendFormat: "Delivery gaps"
			}]
		}]
	},
	lib.#row & {
		title: "Memory Pressure Admission"
		panels: [
			lib.#panel & {
				title: "Utilization"
				targets: [lib.#target & {
					expr:         "max(loop:$environment:blob_stream_broker:blob_stream_broker:memory_pressure:utilization_percent)"
					legendFormat: "Cgroup allocation"
				}]
				#y_format: "percent"
			},
			lib.#panel & {
				title: "Enforcement"
				targets: [lib.#target & {
					expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:memory_pressure:overloaded)"
					legendFormat: "Overloaded"
				}]
			},
			lib.#panel & {
				title: "Admission Rejections"
				targets: [lib.#target & {
					expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:write:admission_rejections_total)"
					legendFormat: "Rejected"
				}]
			},
			lib.#panel & {
				title: "Transitions"
				targets: [lib.#target & {
					expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:memory_pressure:transitions_total)"
					legendFormat: "State changes"
				}]
			},
			lib.#panel & {
				title: "Sampling Failures"
				targets: [lib.#target & {
					expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:memory_pressure:sampling_failures_total)"
					legendFormat: "Failed samples"
				}]
			},
		]
	},
	lib.#row & {
		title: "Metadata Cache"
		panels: [
			lib.#panel & {
				title: "Requests and Retained Hits"
				targets: [
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:metadata_cache:requests_total)"
						legendFormat: "Requests"
					},
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:metadata_cache:storage_queries_total)"
						legendFormat: "Storage queries"
					},
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:metadata_cache:tail_hits_total)"
						legendFormat: "Tail hits"
					},
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:metadata_cache:recovery_hits_total)"
						legendFormat: "Recovery hits"
					},
				]
			},
			lib.#panel & {
				title: "Refills and Recovery Snapshots"
				targets: [
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:metadata_cache:tail_refills_total)"
						legendFormat: "Tail refills"
					},
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:metadata_cache:recovery_baselines_total)"
						legendFormat: "Recovery baselines"
					},
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:metadata_cache:recovery_seals_total)"
						legendFormat: "Recovery seals"
					},
				]
			},
			lib.#panel & {
				title: "Failures and Maintenance"
				targets: [
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:metadata_cache:failures_total)"
						legendFormat: "Failures"
					},
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:metadata_cache:overloads_total)"
						legendFormat: "Overloads"
					},
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:metadata_cache:invalidations_total)"
						legendFormat: "Invalidations"
					},
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:metadata_cache:evictions_total)"
						legendFormat: "Evictions"
					},
				]
			},
			lib.#panel & {
				title: "Response Throughput"
				targets: [
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:metadata_cache:response_items_total)"
						legendFormat: "Metadata items"
					},
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:metadata_cache:response_bytes_total)"
						legendFormat: "Response bytes"
					},
				]
				#y_format: "binBps"
			},
			lib.#panel & {
				title: "Active Coalescing"
				targets: [
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:metadata_cache:active_waiters)"
						legendFormat: "Active waiters"
					},
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:metadata_cache:active_refills)"
						legendFormat: "Active refills"
					},
				]
			},
			lib.#panel & {
				title: "Coalescing Volume"
				targets: [
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:metadata_cache:storage_queries_total)"
						legendFormat: "Storage queries"
					},
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:metadata_cache:coalescing_window_requests_total)"
						legendFormat: "Requests in query groups"
					},
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:metadata_cache:coalescing_window_requests_total) - sum(loop:$environment:blob_stream_broker:blob_stream_broker:metadata_cache:storage_queries_total)"
						legendFormat: "Collapsed requests"
					},
				]
			},
			lib.#panel & {
				title: "Coalescing Fan-in"
				targets: [lib.#target & {
					expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:metadata_cache:coalescing_window_requests_total) / clamp_min(sum(loop:$environment:blob_stream_broker:blob_stream_broker:metadata_cache:storage_queries_total), 1)"
					legendFormat: "Requests per storage query"
				}]
			},
			lib.#panel & {
				title: "Coalescing Collapse"
				targets: [lib.#target & {
					expr:         "100 * (1 - sum(loop:$environment:blob_stream_broker:blob_stream_broker:metadata_cache:storage_queries_total) / clamp_min(sum(loop:$environment:blob_stream_broker:blob_stream_broker:metadata_cache:coalescing_window_requests_total), 1))"
					legendFormat: "Collapsed"
				}]
				#y_format: "percent"
			},
			lib.#panel & {
				title: "Retained Entries"
				targets: [
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:metadata_cache:tail_entries)"
						legendFormat: "Tail"
					},
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:metadata_cache:recovery_entries)"
						legendFormat: "Recovery"
					},
				]
			},
			lib.#panel & {
				title: "Retained Bytes"
				targets: [
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:metadata_cache:tail_retained_bytes)"
						legendFormat: "Tail"
					},
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:metadata_cache:recovery_retained_bytes)"
						legendFormat: "Recovery"
					},
				]
				#y_format: "bytes"
			},
			lib.#panel & {
				title: "Observation Age"
				targets: [
					for percentile in [0.99, 0.9, 0.5] {
						lib.#target & {
							expr:         "histogram_quantile(\(percentile), sum by (le) (loop:$environment:blob_stream_broker:blob_stream_broker:metadata_cache:observation_age_seconds_bucket))"
							legendFormat: "p\(percentile)"
						}
					},
				]
				#y_format: "s"
			},
		]
	},
	lib.#row & {
		title: "Blob Cache"
		panels: [
			lib.#panel & {
				title: "Requests and Delivery"
				targets: [
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:blob_cache:requests_total)"
						legendFormat: "Broker requests"
					},
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:blob_cache:response_items_total)"
						legendFormat: "Delivered ranges"
					},
				]
			},
			lib.#panel & {
				title: "Whole-Object Cache Activity"
				targets: [
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:blob_cache:hits_total)"
						legendFormat: "Retained hits"
					},
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:blob_cache:fetches_total)"
						legendFormat: "Storage fetches"
					},
				]
			},
			lib.#panel & {
				title: "Fetch and Response Throughput"
				targets: [
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:blob_cache:fetch_bytes_total)"
						legendFormat: "Whole-object fetch bytes"
					},
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:blob_cache:response_bytes_total)"
						legendFormat: "Range response bytes"
					},
				]
				#y_format: "binBps"
			},
			lib.#panel & {
				title:       "Collapse Rate"
				description: "Percentage of successfully delivered ranges beyond one storage fetch per range. Higher is better: retained whole-object cache hits and concurrent requests that share an in-flight fetch increase the rate. Storage fetch failures can make it negative; inspect Failures and Pressure alongside this panel."
				targets: [lib.#target & {
					expr:         "100 * (1 - sum(loop:$environment:blob_stream_broker:blob_stream_broker:blob_cache:fetches_total) / clamp_min(sum(loop:$environment:blob_stream_broker:blob_stream_broker:blob_cache:response_items_total), 1))"
					legendFormat: "Collapsed ranges"
				}]
				#y_format: "percent"
			},
			lib.#panel & {
				title: "Failures and Pressure"
				targets: [
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:blob_cache:failures_total)"
						legendFormat: "Failures"
					},
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:blob_cache:overloads_total)"
						legendFormat: "Overloads"
					},
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:blob_cache:pressure_flushes_total)"
						legendFormat: "Pressure flushes"
					},
				]
			},
			lib.#panel & {
				title: "Retained Entries and Fetches"
				targets: [
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:blob_cache:entries)"
						legendFormat: "Entries"
					},
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:blob_cache:active_fetches)"
						legendFormat: "Active fetches"
					},
				]
			},
			lib.#panel & {
				title: "Retained Bytes"
				targets: [lib.#target & {
					expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:blob_cache:retained_bytes)"
					legendFormat: "Cached bytes"
				}]
				#y_format: "bytes"
			},
		]
	},
	lib.#row & {
		title: "API Cost Inputs"
		panels: [
			lib.#panel & {
				title: "S3 API Requests"
				targets: [
					lib.#target & {
						expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:write:flush_uploaded_objects_total)"
						legendFormat: "PUT requests (broker)"
					},
					lib.#target & {
						expr:         #consumer_s3_get_request_rate
						legendFormat: "GET requests (consumers)"
					},
					lib.#target & {
						expr:         #broker_s3_get_request_rate
						legendFormat: "GET requests (broker blob cache)"
					},
				]
			},
			lib.#panel & {
				title: "DynamoDB Request Units"
				targets: [
					lib.#target & {
						expr:         #"sum({__name__=~"loop:$environment:.*:dynamo:read_request_units_total"})"#
						legendFormat: "RRU (broker metadata cache and consumers)"
					},
					lib.#target & {
						expr:         #"sum({__name__=~"loop:$environment:.*:dynamo:write_request_units_total"})"#
						legendFormat: "WRU (broker and consumers)"
					},
				]
			},
		]
	},
	lib.#row & {
		title: "Estimated Cost"
		panels: [
			lib.#panel & {
				title: "Estimated Cost per Hour"
				targets: [
					lib.#target & {
						expr:         "\(#s3_put_request_rate) * \(#api_cost.seconds_per_hour*#api_cost.s3_put_per_request)"
						legendFormat: "S3 PUT ($/hour)"
					},
					lib.#target & {
						expr:         "(\(#s3_get_request_rate)) * \(#api_cost.seconds_per_hour*#api_cost.s3_get_per_request)"
						legendFormat: "S3 GET ($/hour)"
					},
					lib.#target & {
						expr:         "\(#s3_upload_bytes_rate) * \(#api_cost.s3_retention_days*#api_cost.seconds_per_day/#api_cost.bytes_per_gb*#api_cost.s3_storage_per_gb_month) * \(#api_cost.seconds_per_hour/#api_cost.seconds_per_month)"
						legendFormat: "S3 storage ($/hour)"
					},
					lib.#target & {
						expr:         "\(#dynamo_wru_rate) * \(#api_cost.seconds_per_hour*#api_cost.dynamo_wru_per_request_unit)"
						legendFormat: "DynamoDB WRU ($/hour)"
					},
					lib.#target & {
						expr:         "\(#dynamo_rru_rate) * \(#api_cost.seconds_per_hour*#api_cost.dynamo_rru_per_request_unit)"
						legendFormat: "DynamoDB RRU ($/hour)"
					},
					lib.#target & {
						expr:         "(\(#s3_put_request_rate) * \(#api_cost.s3_put_per_request) + (\(#s3_get_request_rate)) * \(#api_cost.s3_get_per_request) + \(#dynamo_wru_rate) * \(#api_cost.dynamo_wru_per_request_unit) + \(#dynamo_rru_rate) * \(#api_cost.dynamo_rru_per_request_unit)) * \(#api_cost.seconds_per_hour) + \(#s3_upload_bytes_rate) * \(#api_cost.s3_retention_days*#api_cost.seconds_per_day/#api_cost.bytes_per_gb*#api_cost.s3_storage_per_gb_month) * \(#api_cost.seconds_per_hour/#api_cost.seconds_per_month)"
						legendFormat: "Total ($/hour)"
					},
				]
			},
			lib.#panel & {
				title: "Estimated Cost per Day"
				targets: [
					lib.#target & {
						expr:         "\(#s3_put_request_rate) * \(#api_cost.seconds_per_day*#api_cost.s3_put_per_request)"
						legendFormat: "S3 PUT ($/day)"
					},
					lib.#target & {
						expr:         "(\(#s3_get_request_rate)) * \(#api_cost.seconds_per_day*#api_cost.s3_get_per_request)"
						legendFormat: "S3 GET ($/day)"
					},
					lib.#target & {
						expr:         "\(#s3_upload_bytes_rate) * \(#api_cost.s3_retention_days*#api_cost.seconds_per_day/#api_cost.bytes_per_gb*#api_cost.s3_storage_per_gb_month) * \(#api_cost.seconds_per_day/#api_cost.seconds_per_month)"
						legendFormat: "S3 storage ($/day)"
					},
					lib.#target & {
						expr:         "\(#dynamo_wru_rate) * \(#api_cost.seconds_per_day*#api_cost.dynamo_wru_per_request_unit)"
						legendFormat: "DynamoDB WRU ($/day)"
					},
					lib.#target & {
						expr:         "\(#dynamo_rru_rate) * \(#api_cost.seconds_per_day*#api_cost.dynamo_rru_per_request_unit)"
						legendFormat: "DynamoDB RRU ($/day)"
					},
					lib.#target & {
						expr:         "(\(#s3_put_request_rate) * \(#api_cost.s3_put_per_request) + (\(#s3_get_request_rate)) * \(#api_cost.s3_get_per_request) + \(#dynamo_wru_rate) * \(#api_cost.dynamo_wru_per_request_unit) + \(#dynamo_rru_rate) * \(#api_cost.dynamo_rru_per_request_unit)) * \(#api_cost.seconds_per_day) + \(#s3_upload_bytes_rate) * \(#api_cost.s3_retention_days*#api_cost.seconds_per_day/#api_cost.bytes_per_gb*#api_cost.s3_storage_per_gb_month) * \(#api_cost.seconds_per_day/#api_cost.seconds_per_month)"
						legendFormat: "Total ($/day)"
					},
				]
			},
		]
	},
	lib.#row & {
		title: "DynamoDB Capacity"
		panels: [lib.#panel & {
			title: "Request Units"
			targets: [
				lib.#target & {
					expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:dynamo:read_request_units_total)"
					legendFormat: "Read"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:blob_stream_broker:blob_stream_broker:dynamo:write_request_units_total)"
					legendFormat: "Write"
				},
			]
		}]
	},
	k8s.#k8s_row & {
		title:       "blob-stream Kubernetes"
		#namespace:  "blob-stream"
		#container:  "blob-stream-broker"
		#pod_filter: ".*"
		collapse:    false
	},
]
