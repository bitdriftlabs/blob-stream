#![allow(clippy::unwrap_used)]

use super::membership_from_endpoints;
use crate::{BrokerMembership, BrokerNode};
use k8s_openapi::api::core::v1::{
  EndpointAddress,
  EndpointPort,
  EndpointSubset,
  Endpoints,
  ObjectReference,
};

#[test]
fn membership_uses_pod_name_for_broker_identity() {
  let endpoints = Endpoints {
    subsets: Some(vec![EndpointSubset {
      addresses: Some(vec![EndpointAddress {
        ip: "10.0.0.1".to_string(),
        hostname: Some("endpoint-hostname".to_string()),
        target_ref: Some(ObjectReference {
          name: Some("blob-stream-broker-0".to_string()),
          ..Default::default()
        }),
        ..Default::default()
      }]),
      ports: Some(vec![EndpointPort {
        port: 8080,
        ..Default::default()
      }]),
      ..Default::default()
    }]),
    ..Default::default()
  };

  let membership = membership_from_endpoints(&endpoints);

  assert_eq!(
    membership,
    BrokerMembership::new(vec![BrokerNode {
      node_id: "blob-stream-broker-0".to_string(),
      address: "10.0.0.1:8080".to_string(),
    }])
  );
}
