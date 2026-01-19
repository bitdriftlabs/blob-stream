// blob-stream - kubernetes broker discovery
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::{BrokerDiscovery, BrokerMembership, BrokerNode};
use anyhow::{Context, Result};
use async_trait::async_trait;
use bd_log::warn_every;
use futures::StreamExt;
use k8s_openapi::api::core::v1::Endpoints;
use kube::runtime::WatchStreamExt;
use kube::runtime::watcher::{Config, Event, watcher};
use kube::{Api, Client};
use std::collections::HashSet;
use time::ext::NumericalDuration;
use tokio::sync::watch;

//
// K8sServiceBrokerDiscovery
//

#[derive(Clone, Debug)]
pub struct K8sServiceBrokerDiscovery {
  namespace: String,
  service_name: String,
}

impl K8sServiceBrokerDiscovery {
  #[must_use]
  pub fn new(namespace: impl Into<String>, service_name: impl Into<String>) -> Self {
    Self {
      namespace: namespace.into(),
      service_name: service_name.into(),
    }
  }
}

#[async_trait]
impl BrokerDiscovery for K8sServiceBrokerDiscovery {
  async fn watch_membership(&self) -> Result<watch::Receiver<BrokerMembership>> {
    let client = Client::try_default()
      .await
      .context("create kubernetes client")?;
    let api: Api<Endpoints> = Api::namespaced(client, &self.namespace);
    let field_selector = format!("metadata.name={}", self.service_name);
    let config = Config::default().fields(&field_selector);

    let (tx, rx) = watch::channel(BrokerMembership::default());
    let sender = tx;
    let service_name = self.service_name.clone();

    tokio::spawn(async move {
      let mut stream = watcher(api, config).default_backoff().boxed();
      let mut init_buffer = Vec::new();
      let mut init_pending = false;

      while let Some(event) = stream.next().await {
        match event {
          Ok(event) => {
            let membership = match event {
              Event::Apply(endpoints) => membership_from_endpoints(&endpoints),
              Event::Delete(_) => BrokerMembership::default(),
              Event::Init => {
                init_buffer.clear();
                init_pending = true;
                continue;
              },
              Event::InitApply(endpoints) => {
                if init_pending {
                  init_buffer.push(endpoints);
                }
                continue;
              },
              Event::InitDone => {
                if !init_pending {
                  continue;
                }

                init_pending = false;
                membership_from_items(&service_name, &init_buffer)
              },
            };

            if sender.send(membership).is_err() {
              return;
            }
          },
          Err(error) => {
            warn_every!(15.seconds(), "k8s discovery watch error: {}", error);
          },
        }
      }

      warn_every!(15.seconds(), "k8s discovery watch stream ended");
    });

    Ok(rx)
  }
}

fn membership_from_items(service_name: &str, items: &[Endpoints]) -> BrokerMembership {
  let endpoints = items
    .iter()
    .find(|item| item.metadata.name.as_deref() == Some(service_name));

  endpoints.map(membership_from_endpoints).unwrap_or_default()
}

fn membership_from_endpoints(endpoints: &Endpoints) -> BrokerMembership {
  let mut nodes = Vec::new();
  let mut seen = HashSet::new();

  let Some(subsets) = endpoints.subsets.as_ref() else {
    return BrokerMembership::default();
  };

  for subset in subsets {
    let ports = subset.ports.as_ref().and_then(|ports| ports.first());
    let Some(port) = ports.and_then(|port| u16::try_from(port.port).ok()) else {
      continue;
    };

    let Some(addresses) = subset.addresses.as_ref() else {
      continue;
    };

    for address in addresses {
      let address = format!("{}:{}", address.ip, port);
      if seen.insert(address.clone()) {
        nodes.push(BrokerNode {
          node_id: address.clone(),
          address,
        });
      }
    }
  }

  BrokerMembership::new(nodes)
}
