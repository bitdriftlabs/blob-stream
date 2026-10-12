use super::aws_tracing::{instrument_operation, mark_expected_outcome};
use super::{AWS_DYNAMODB_TRACE_SAMPLE_RATE, AWS_S3_TRACE_SAMPLE_RATE, AwsTraceSampler};
use bd_log::test::TestTraceContext;
use bd_runtime_config::feature_flags::FeatureFlags;
use std::future::{Future, pending, poll_fn};
use std::sync::Arc;
use std::task::Poll;
use tokio::sync::watch;
use tracing::Instrument as _;

#[derive(Debug)]
struct Flags {
  selected: Option<&'static str>,
}

impl FeatureFlags for Flags {
  fn feature_enabled(&self, name: &str, default: bool) -> bool {
    assert!(!default);
    self.selected == Some(name)
  }

  fn get_bool(&self, _: &str, default: bool) -> bool {
    default
  }
  fn get_integer(&self, _: &str, default: u64) -> u64 {
    default
  }
  fn get_string(&self, _: &str, default: &Arc<String>) -> Arc<String> {
    default.clone()
  }
}

fn flags(selected: Option<&'static str>) -> Arc<dyn FeatureFlags> {
  Arc::new(Flags { selected })
}

#[test]
fn aws_sampling_defaults_off_and_observes_live_snapshots() {
  assert!(!AwsTraceSampler::default().sample(AWS_S3_TRACE_SAMPLE_RATE));
  let (sender, receiver) = watch::channel(None);
  let sampler = AwsTraceSampler::new(Some(receiver));
  let clone = sampler.clone();
  assert!(!sampler.sample(AWS_S3_TRACE_SAMPLE_RATE));
  sender
    .send(Some(flags(Some(AWS_S3_TRACE_SAMPLE_RATE))))
    .unwrap();
  assert!(sampler.sample(AWS_S3_TRACE_SAMPLE_RATE));
  assert!(clone.sample(AWS_S3_TRACE_SAMPLE_RATE));
  assert!(!sampler.sample(AWS_DYNAMODB_TRACE_SAMPLE_RATE));
  sender
    .send(Some(flags(Some(AWS_DYNAMODB_TRACE_SAMPLE_RATE))))
    .unwrap();
  assert!(!sampler.sample(AWS_S3_TRACE_SAMPLE_RATE));
  assert!(sampler.sample(AWS_DYNAMODB_TRACE_SAMPLE_RATE));
  sender.send(Some(flags(None))).unwrap();
  assert!(!clone.sample(AWS_DYNAMODB_TRACE_SAMPLE_RATE));
  sender.send(None).unwrap();
  assert!(!clone.sample(AWS_S3_TRACE_SAMPLE_RATE));
}

#[tokio::test]
async fn aws_operation_retains_only_final_failure_or_sample_and_preserves_results() {
  for sampled in [false, true] {
    for outcome in [Ok(7), Err("expected"), Err("failure")] {
      let (_sender, receiver) =
        watch::channel(Some(flags(sampled.then_some(AWS_S3_TRACE_SAMPLE_RATE))));
      let sampler = AwsTraceSampler::new(Some(receiver));
      let context = TestTraceContext::new("aws-operation-test");
      let dispatch = context.dispatch();
      let _guard = tracing::dispatcher::set_default(&dispatch);
      let result = instrument_operation(
        &sampler,
        AWS_S3_TRACE_SAMPLE_RATE,
        "S3",
        "outer",
        "region",
        "bucket",
        async {
          instrument_operation(
            &sampler,
            AWS_S3_TRACE_SAMPLE_RATE,
            "S3",
            "inner",
            "region",
            "bucket",
            async {
              mark_expected_outcome();
              outcome
            },
            |_| false,
          )
          .await
        },
        |error| *error == "expected",
      )
      .await;
      assert_eq!(result, outcome);
      let spans = context.exported_spans();
      assert_eq!(
        spans.len(),
        usize::from(sampled || outcome == Err("failure"))
      );
      if let Some(root) = spans.first() {
        assert_eq!(root.name, "blob_stream.aws");
        assert_eq!(root.dropped_attributes_count, 0);
        let expected = if outcome == Err("failure") {
          "failure"
        } else {
          "expected"
        };
        assert_eq!(
          root
            .attributes
            .iter()
            .find(|attribute| attribute.key.as_str() == "aws.outcome")
            .unwrap()
            .value
            .to_string(),
          expected
        );
      }
    }
  }
}

#[tokio::test]
async fn aws_operation_valid_parent_retains_completed_success_and_expected_outcomes() {
  for (service, flag) in [
    ("S3", AWS_S3_TRACE_SAMPLE_RATE),
    ("DynamoDB", AWS_DYNAMODB_TRACE_SAMPLE_RATE),
  ] {
    for outcome in [Ok(7), Err("expected")] {
      let sampler = AwsTraceSampler::default();
      let context = TestTraceContext::new("aws-parent-test");
      let dispatch = context.dispatch();
      let _guard = tracing::dispatcher::set_default(&dispatch);
      let parent = bd_log::otel_info_span!("handoff");
      let result = instrument_operation(
        &sampler,
        flag,
        service,
        "operation",
        "region",
        "resource",
        async {
          drop(bd_log::otel_info_span_if_parent!("request"));
          outcome
        },
        |error| *error == "expected",
      )
      .instrument(parent.clone())
      .await;
      assert_eq!(result, outcome);
      drop(parent);
      let spans = context.exported_spans();
      assert_eq!(spans.len(), 3);
      let root = spans
        .iter()
        .find(|span| span.name == "blob_stream.aws")
        .unwrap();
      let parent = spans.iter().find(|span| span.name == "handoff").unwrap();
      assert_eq!(root.links.len(), 1);
      assert_eq!(
        root.links[0].span_context.span_id(),
        parent.span_context.span_id()
      );
      for (key, expected) in [
        ("aws.retention_reason", "parent"),
        (
          "aws.outcome",
          if outcome.is_err() {
            "expected"
          } else {
            "success"
          },
        ),
      ] {
        assert_eq!(
          root
            .attributes
            .iter()
            .find(|attribute| attribute.key.as_str() == key)
            .unwrap()
            .value
            .to_string(),
          expected
        );
      }
      for span in &spans {
        assert_eq!(span.dropped_attributes_count, 0);
      }
    }
  }
}

#[tokio::test]
async fn aws_operation_sampled_cancellation_does_not_export() {
  let (_sender, receiver) = watch::channel(Some(flags(Some(AWS_S3_TRACE_SAMPLE_RATE))));
  let sampler = AwsTraceSampler::new(Some(receiver));
  let context = TestTraceContext::new("aws-cancellation-test");
  let dispatch = context.dispatch();
  let _guard = tracing::dispatcher::set_default(&dispatch);
  let mut operation = Box::pin(instrument_operation(
    &sampler,
    AWS_S3_TRACE_SAMPLE_RATE,
    "S3",
    "GetObject",
    "region",
    "bucket",
    pending::<Result<(), &str>>(),
    |_| false,
  ));
  assert_eq!(
    poll_fn(|task| Poll::Ready(operation.as_mut().poll(task))).await,
    Poll::Pending
  );
  drop(operation);
  assert_eq!(context.exported_spans().len(), 0);
}
