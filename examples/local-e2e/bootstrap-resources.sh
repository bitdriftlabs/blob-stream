#!/bin/sh

set -eu

# The example uses real S3 and DynamoDB adapters, so separately launched brokers and consumers
# share segment data, leases, membership, and committed offsets. All names stay local to this
# Compose project and disappear when the tutorial is cleaned up with `docker compose down -v`.
bucket=blob-stream-local-e2e
metadata_table=blob_stream_local_e2e_segments
producer_lease_table=blob_stream_local_e2e_producer_leases
consumer_lease_table=blob_stream_local_e2e_consumer_leases
consumer_membership_table=blob_stream_local_e2e_consumer_membership

aws_dynamo() {
  aws dynamodb --endpoint-url "$DYNAMODB_ENDPOINT" "$@"
}

aws_s3() {
  aws s3api --endpoint-url "$S3_ENDPOINT" "$@"
}

wait_for_dependencies() {
  until aws_dynamo list-tables >/dev/null 2>&1 && aws_s3 list-buckets >/dev/null 2>&1; do
    echo "waiting for DynamoDB Local and LocalStack"
    sleep 1
  done
}

create_pk_sk_table() {
  table_name=$1
  if aws_dynamo describe-table --table-name "$table_name" >/dev/null 2>&1; then
    return
  fi

  aws_dynamo create-table \
    --table-name "$table_name" \
    --attribute-definitions AttributeName=pk,AttributeType=S AttributeName=sk,AttributeType=S \
    --key-schema AttributeName=pk,KeyType=HASH AttributeName=sk,KeyType=RANGE \
    --billing-mode PAY_PER_REQUEST >/dev/null
}

create_pk_table() {
  table_name=$1
  if aws_dynamo describe-table --table-name "$table_name" >/dev/null 2>&1; then
    return
  fi

  aws_dynamo create-table \
    --table-name "$table_name" \
    --attribute-definitions AttributeName=pk,AttributeType=S \
    --key-schema AttributeName=pk,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST >/dev/null
}

wait_for_dependencies

# Blob Stream's storage schema requires a sort key for metadata, consumer leases, and membership.
# Producer leases are keyed only by their topic and virtual partition, so that table has no `sk`.
create_pk_sk_table "$metadata_table"
create_pk_table "$producer_lease_table"
create_pk_sk_table "$consumer_lease_table"
create_pk_sk_table "$consumer_membership_table"

if ! aws_s3 head-bucket --bucket "$bucket" >/dev/null 2>&1; then
  aws_s3 create-bucket --bucket "$bucket" >/dev/null
fi

echo "local Blob Stream resources are ready"
