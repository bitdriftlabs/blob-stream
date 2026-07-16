use anyhow::{Result, anyhow, ensure};
use uuid::Uuid;

const HEADER_SIZE: usize = 32;
const MAGIC: [u8; 4] = *b"BSS1";

//
// StressRecordIdentity
//

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StressRecordIdentity {
  pub run_id: Uuid,
  pub producer_index: u32,
  pub sequence: u64,
}

impl StressRecordIdentity {
  #[must_use]
  pub fn encode(self, payload_size: usize) -> Vec<u8> {
    assert!(
      payload_size >= HEADER_SIZE,
      "payload size must fit the stress identity header"
    );

    let mut payload = vec![0; payload_size];
    payload[.. 4].copy_from_slice(&MAGIC);
    payload[4 .. 20].copy_from_slice(self.run_id.as_bytes());
    payload[20 .. 24].copy_from_slice(&self.producer_index.to_be_bytes());
    payload[24 .. 32].copy_from_slice(&self.sequence.to_be_bytes());
    payload
  }

  pub fn decode(payload: &[u8]) -> Result<Self> {
    ensure!(
      payload.len() >= HEADER_SIZE,
      "stress payload is too short: {} bytes",
      payload.len()
    );
    ensure!(
      payload[.. 4] == MAGIC,
      "stress payload has an invalid magic prefix"
    );

    let run_id = Uuid::from_slice(&payload[4 .. 20])
      .map_err(|error| anyhow!("stress payload has an invalid run id: {error}"))?;
    let producer_index = u32::from_be_bytes(
      payload[20 .. 24]
        .try_into()
        .map_err(|_| anyhow!("stress payload producer index is malformed"))?,
    );
    let sequence = u64::from_be_bytes(
      payload[24 .. 32]
        .try_into()
        .map_err(|_| anyhow!("stress payload sequence is malformed"))?,
    );

    Ok(Self {
      run_id,
      producer_index,
      sequence,
    })
  }
}
