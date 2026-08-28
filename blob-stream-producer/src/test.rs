use crate::{ProducerAck, ProducerClient, ProducerError, ProducerRecord};
use async_trait::async_trait;

#[async_trait]
pub trait ProducerClientTestExt {
  async fn produce_one(&self, record: ProducerRecord) -> Result<ProducerAck, ProducerError>;
}

#[async_trait]
impl<T> ProducerClientTestExt for T
where
  T: ProducerClient + Send + Sync + ?Sized,
{
  async fn produce_one(&self, record: ProducerRecord) -> Result<ProducerAck, ProducerError> {
    self
      .produce(vec![record])
      .await
      .into_iter()
      .next()
      .expect("one-record producer test submission returns one result")
  }
}
