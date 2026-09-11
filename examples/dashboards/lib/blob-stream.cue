package blob_stream

import "github.com/bitdriftlabs/dashboards/lib"

#producer_row: lib.#row & {
	title:         *"Blob Stream Producer" | string
	#metric_scope: string

	panels: [
		lib.#panel & {
			title: "Throughput"
			targets: [
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):producer:records_enqueued)"
					legendFormat: "Records enqueued"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):producer:records_sent)"
					legendFormat: "Records sent"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):producer:batches_sent)"
					legendFormat: "Batches sent"
				},
			]
		},
		lib.#panel & {
			title: "Outcomes"
			targets: [
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):producer:retries)"
					legendFormat: "Retries"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):producer:failures)"
					legendFormat: "Failures"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):producer:no_brokers)"
					legendFormat: "No brokers"
				},
			]
		},
		lib.#panel & {
			title: "Flush Triggers"
			targets: [
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):producer:flushes_max_size)"
					legendFormat: "Max size"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):producer:flushes_max_delay)"
					legendFormat: "Max delay"
				},
			]
		},
		lib.#panel & {
			title: "Not Lease Holder Retries"
			targets: [
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):producer:not_lease_holder_retry_timers)"
					legendFormat: "Timer fallback"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):producer:not_lease_holder_retry_membership_updates)"
					legendFormat: "Membership update"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):producer:not_lease_holder_retry_same_owner)"
					legendFormat: "Same owner"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):producer:not_lease_holder_retry_changed_owner)"
					legendFormat: "Changed owner"
				},
			]
		},
		lib.#panel & {
			title: "Active Requests"
			targets: [lib.#target & {
				expr:         "avg(loop:$environment:\(#metric_scope):producer:active_requests)"
				legendFormat: "Requests (avg)"
			},
				lib.#target & {
					expr:         "max(loop:$environment:\(#metric_scope):producer:active_requests)"
					legendFormat: "Requests (max)"
				}]
		},
		lib.#panel & {
			title: "Send Latency"
			targets: [
				for percentile in [0.99, 0.9, 0.5] {
					lib.#target & {
						expr:         "histogram_quantile(\(percentile), sum by (le) (loop:$environment:\(#metric_scope):producer:send_latency_seconds_bucket))"
						legendFormat: "p\(percentile)"
					}
				},
			]
			#y_format: "s"
		},
	]
}

#consumer_row: lib.#row & {
	title:         *"Blob Stream Consumer" | string
	#metric_scope: string

	panels: [
		lib.#panel & {
			title: "Throughput"
			targets: [
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:iterator:batches_delivered)"
					legendFormat: "Batches delivered"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:iterator:records_delivered)"
					legendFormat: "Records delivered"
				},
			]
		},
		lib.#panel & {
			title: "Outcomes"
			targets: [
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:iterator:retries)"
					legendFormat: "Retries"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:iterator:failures)"
					legendFormat: "Failures"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:iterator:delivery_gap_events)"
					legendFormat: "Delivery gaps"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:iterator:revocations)"
					legendFormat: "Revocations"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:iterator:seeks)"
					legendFormat: "Seeks"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:iterator:rebalance_failures_total)"
					legendFormat: "Rebalance failures"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:iterator:assignment_plan_rejections_total)"
					legendFormat: "Plan rejections"
				},
			]
		},
		lib.#panel & {
			title: "DynamoDB Capacity"
			targets: [
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):dynamo:read_request_units_total)"
					legendFormat: "Read"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):dynamo:write_request_units_total)"
					legendFormat: "Write"
				},
			]
		},
		lib.#panel & {
			title: "Lease Claims"
			targets: [
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:iterator:lease_claims_initial)"
					legendFormat: "Initial"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:iterator:lease_claims_retained)"
					legendFormat: "Generation retained"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:iterator:lease_claims_graceful_handoff)"
					legendFormat: "Graceful handoff"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:iterator:lease_claims_expiry_takeover)"
					legendFormat: "Expiry takeover"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:iterator:assignment_plans_applied_total)"
					legendFormat: "Plans applied"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:iterator:assignment_applications_total)"
					legendFormat: "Assignments applied"
				},
			]
		},
		lib.#panel & {
			title: "Lease Maintenance"
			targets: [
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:iterator:lease_renewed_partitions)"
					legendFormat: "Scheduled renewals"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:iterator:heartbeat_renewed_partitions)"
					legendFormat: "All renewals"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:iterator:cursor_commit_partitions)"
					legendFormat: "Cursor commits"
				},
			]
		},
		lib.#panel & {
			title: "Partition Ownership"
			targets: [
				lib.#target & {
					expr:         "max(loop:$environment:\(#metric_scope):consumer:iterator:desired_partitions)"
					legendFormat: "Desired"
				},
				lib.#target & {
					expr:         "max(loop:$environment:\(#metric_scope):consumer:iterator:owned_partitions)"
					legendFormat: "Owned"
				},
				lib.#target & {
					expr:         "max(loop:$environment:\(#metric_scope):consumer:iterator:active_partitions)"
					legendFormat: "Active"
				},
			]
		},
		lib.#panel & {
			title: "Retry Backoff"
			targets: [
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:iterator:membership_heartbeat_failures)"
					legendFormat: "Membership failures"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:iterator:lease_heartbeat_failures)"
					legendFormat: "Lease failures"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:iterator:heartbeat_retry_attempts)"
					legendFormat: "Heartbeat retries"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:iterator:rebalance_retry_attempts)"
					legendFormat: "Rebalance retries"
				},
			]
		},
		lib.#panel & {
			title: "Heartbeat Commits"
			targets: [
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:iterator:heartbeat_calls)"
					legendFormat: "Heartbeats"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:iterator:heartbeat_scheduled_calls)"
					legendFormat: "Scheduled"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:iterator:heartbeat_commit_calls)"
					legendFormat: "Commit-triggered"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:iterator:heartbeat_fenced_partitions)"
					legendFormat: "Fenced partitions"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:iterator:heartbeat_committed_offsets)"
					legendFormat: "Committed cursors"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:iterator:heartbeat_failures)"
					legendFormat: "Failures"
				},
			]
		},
		lib.#panel & {
			title: "Heartbeat Latency"
			targets: [
				for percentile in [0.99, 0.9, 0.5] {
					lib.#target & {
						expr:         "histogram_quantile(\(percentile), sum by (le) (loop:$environment:\(#metric_scope):consumer:iterator:heartbeat_latency_seconds_bucket))"
						legendFormat: "p\(percentile)"
					}
				},
			]
			#y_format: "s"
		},
		lib.#panel & {
			title: "Prefetch Buffer"
			targets: [
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:iterator:prefetch_buffered_batches)"
					legendFormat: "Buffered batches"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:iterator:prefetch_pending_batches)"
					legendFormat: "Pending batches"
				},
			]
		},
		lib.#panel & {
			title: "Prefetch Payload Bytes"
			targets: [
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:iterator:prefetch_buffered_bytes)"
					legendFormat: "Buffered bytes"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:iterator:prefetch_pending_bytes)"
					legendFormat: "Pending bytes"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:iterator:prefetch_total_bytes)"
					legendFormat: "Total bytes"
				},
			]
			#y_format: "bytes"
		},
		lib.#panel & {
			title: "Prefetch Flow Control"
			targets: [
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:iterator:prefetch_paused_budget)"
					legendFormat: "Paused by budget"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:iterator:prefetch_refill_cycles)"
					legendFormat: "Refill cycles"
				},
			]
		},
		lib.#panel & {
			title: "Prefetch Read Cycle Latency"
			targets: [
				for percentile in [0.99, 0.9, 0.5] {
					lib.#target & {
						expr:         "histogram_quantile(\(percentile), sum by (le) (loop:$environment:\(#metric_scope):consumer:iterator:next_latency_seconds_bucket))"
						legendFormat: "p\(percentile)"
					}
				},
			]
			#y_format: "s"
		},
		lib.#panel & {
			title: "Commit Latency"
			targets: [
				for percentile in [0.99, 0.9, 0.5] {
					lib.#target & {
						expr:         "histogram_quantile(\(percentile), sum by (le) (loop:$environment:\(#metric_scope):consumer:iterator:commit_latency_seconds_bucket))"
						legendFormat: "p\(percentile)"
					}
				},
			]
			#y_format: "s"
		},
	]
}

#consumer_reader_row: lib.#row & {
	title:         *"Blob Stream Read Path" | string
	#metric_scope: string

	panels: [
		lib.#panel & {
			title: "Read Cycles"
			targets: [
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:reader:read_available_calls)"
					legendFormat: "Read cycles"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:reader:read_available_empty)"
					legendFormat: "Empty cycles"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:reader:batches_read)"
					legendFormat: "Batches"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:reader:records_read)"
					legendFormat: "Records"
				},
			]
		},
		lib.#panel & {
			title: "Metadata Query Work"
			targets: [
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:reader:metadata_scan_requests)"
					legendFormat: "Metadata scans"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:reader:metadata_scan_segments)"
					legendFormat: "Metadata segments"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:reader:metadata_batches_scanned)"
					legendFormat: "Metadata batches scanned"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:reader:metadata_batches_skipped_by_cursor)"
					legendFormat: "Batches skipped by cursor"
				},
			]
		},
		lib.#panel & {
			title: "Metadata Scan Requests"
			targets: [
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:reader:metadata_fast_scan_requests)"
					legendFormat: "Fast scans"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:reader:metadata_recovery_scan_requests)"
					legendFormat: "Recovery scans"
				},
			]
		},
		lib.#panel & {
			title: "Metadata Scan Segments"
			targets: [
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:reader:metadata_fast_scan_segments)"
					legendFormat: "Fast scan segments"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:reader:metadata_recovery_scan_segments)"
					legendFormat: "Recovery scan segments"
				},
			]
		},
		lib.#panel & {
			title: "Recovery Scan Results"
			targets: [
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:reader:metadata_recovery_scan_hits)"
					legendFormat: "Scans with batches"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:reader:metadata_recovery_scan_batches_read)"
					legendFormat: "Batches read"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:reader:metadata_recovery_scan_failures)"
					legendFormat: "Failures"
				},
			]
		},
		lib.#panel & {
			title: "Broker Metadata Offload"
			targets: [
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:reader:broker_metadata_offload_requests)"
					legendFormat: "Requests"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:reader:broker_metadata_offload_deliveries)"
					legendFormat: "Broker deliveries"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:reader:broker_metadata_offload_fallbacks)"
					legendFormat: "Direct fallbacks"
				},
			]
		},
		lib.#panel & {
			title: "Blob Fetch Outcomes"
			targets: [
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:reader:broker_blob_range_requests)"
					legendFormat: "Broker requests"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:reader:broker_blob_range_successes)"
					legendFormat: "Broker successes"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:reader:broker_blob_range_fallbacks)"
					legendFormat: "Direct fallbacks"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:reader:broker_blob_range_not_found_groups)"
					legendFormat: "Authoritative missing"
				},
			]
		},
		lib.#panel & {
			title: "Mature Metadata Cache Reuses"
			targets: [
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:reader:mature_metadata_cache_reuses)"
					legendFormat: "Reuses"
				},
			]
		},
		lib.#panel & {
			title: "Recovery Metadata Cache Entries"
			targets: [lib.#target & {
				expr:         "sum(loop:$environment:\(#metric_scope):consumer:reader:recovery_metadata_cache_entries)"
				legendFormat: "Entries"
			}]
		},
		lib.#panel & {
			title: "Recovery Metadata Cache Retained Bytes"
			targets: [lib.#target & {
				expr:         "sum(loop:$environment:\(#metric_scope):consumer:reader:recovery_metadata_cache_retained_bytes)"
				legendFormat: "Metadata"
			}]
			#y_format: "bytes"
		},
		lib.#panel & {
			title: "Metadata Deferrals"
			targets: [
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:reader:metadata_segments_deferred_by_visibility_delay)"
					legendFormat: "Visibility deferred"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:reader:metadata_fast_scan_without_lower_bound)"
					legendFormat: "Unbounded fast scans"
				},
			]
		},
		lib.#panel & {
			title: "Metadata Query Latency"
			targets: [
				for percentile in [0.99, 0.9, 0.5] {
					lib.#target & {
						expr:         "histogram_quantile(\(percentile), sum by (le) (loop:$environment:\(#metric_scope):consumer:reader:metadata_scan_latency_seconds_bucket))"
						legendFormat: "p\(percentile)"
					}
				},
			]
			#y_format: "s"
		},
		lib.#panel & {
			title: "Lost Records"
			targets: [lib.#target & {
				expr:         "sum(loop:$environment:\(#metric_scope):consumer:reader:lost_records)"
				legendFormat: "Lost records"
			}]
		},
		lib.#panel & {
			title: "Blob Fetch Payload"
			targets: [
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:reader:broker_blob_range_bytes)"
					legendFormat: "Broker bytes"
				},
				lib.#target & {
					expr:         "sum(loop:$environment:\(#metric_scope):consumer:reader:fallback_blob_range_bytes)"
					legendFormat: "Direct fallback bytes"
				},
			]
			#y_format: "binBps"
		},
		lib.#panel & {
			title: "Blob Fetch Latency"
			targets: [
				for percentile in [0.99, 0.9, 0.5] {
					lib.#target & {
						expr:         "histogram_quantile(\(percentile), sum by (le) (loop:$environment:\(#metric_scope):consumer:reader:fallback_blob_range_latency_seconds_bucket))"
						legendFormat: "Fallback p\(percentile)"
					}
				},
				for percentile in [0.99, 0.9, 0.5] {
					lib.#target & {
						expr:         "histogram_quantile(\(percentile), sum by (le) (loop:$environment:\(#metric_scope):consumer:reader:broker_blob_range_latency_seconds_bucket))"
						legendFormat: "Broker p\(percentile)"
					}
				},
			]
			#y_format: "s"
		},
		lib.#panel & {
			title: "Read Cycle Latency"
			targets: [
				for percentile in [0.99, 0.9, 0.5] {
					lib.#target & {
						expr:         "histogram_quantile(\(percentile), sum by (le) (loop:$environment:\(#metric_scope):consumer:reader:read_available_latency_seconds_bucket))"
						legendFormat: "p\(percentile)"
					}
				},
			]
			#y_format: "s"
		},
	]
}
