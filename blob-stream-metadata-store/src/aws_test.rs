use super::{is_dynamo_transaction_conflict, retry_dynamo_transaction_conflicts};
use aws_sdk_dynamodb::error::{ErrorMetadata, SdkError};
use aws_sdk_dynamodb::operation::update_item::UpdateItemError;
use aws_sdk_dynamodb::types::error::{
  ConditionalCheckFailedException,
  TransactionConflictException,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

fn transaction_conflict_error() -> SdkError<UpdateItemError, ()> {
  SdkError::service_error(
    UpdateItemError::TransactionConflictException(
      TransactionConflictException::builder()
        .meta(
          ErrorMetadata::builder()
            .code("TransactionConflictException")
            .build(),
        )
        .build(),
    ),
    (),
  )
}

fn conditional_check_failed_error() -> SdkError<UpdateItemError, ()> {
  SdkError::service_error(
    UpdateItemError::ConditionalCheckFailedException(
      ConditionalCheckFailedException::builder()
        .meta(
          ErrorMetadata::builder()
            .code("ConditionalCheckFailedException")
            .build(),
        )
        .build(),
    ),
    (),
  )
}

#[tokio::test]
async fn transaction_conflict_retries_until_success() {
  let attempts = Arc::new(AtomicUsize::new(0));
  let operation_attempts = Arc::clone(&attempts);

  let result = retry_dynamo_transaction_conflicts(
    "test",
    move || {
      let operation_attempts = Arc::clone(&operation_attempts);
      async move {
        let attempt = operation_attempts.fetch_add(1, Ordering::Relaxed);
        if attempt == 0 {
          Err(transaction_conflict_error())
        } else {
          Ok(())
        }
      }
    },
    is_dynamo_transaction_conflict,
  )
  .await;

  assert!(result.is_ok());
  assert_eq!(attempts.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn non_transaction_conflict_does_not_retry() {
  let attempts = Arc::new(AtomicUsize::new(0));
  let operation_attempts = Arc::clone(&attempts);

  let result = retry_dynamo_transaction_conflicts(
    "test",
    move || {
      let operation_attempts = Arc::clone(&operation_attempts);
      async move {
        operation_attempts.fetch_add(1, Ordering::Relaxed);
        Err::<(), _>(conditional_check_failed_error())
      }
    },
    is_dynamo_transaction_conflict,
  )
  .await;

  assert!(matches!(
    result,
    Err(SdkError::ServiceError(service_error))
      if service_error.err().is_conditional_check_failed_exception()
  ));
  assert_eq!(attempts.load(Ordering::Relaxed), 1);
}
