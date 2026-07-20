use super::identity::StressRecordIdentity;
use anyhow::{Result, anyhow};
use uuid::Uuid;

//
// Observation
//

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Observation {
  Unique,
  Duplicate,
}

//
// StressValidationSummary
//

#[derive(Debug, Eq, PartialEq)]
pub struct StressValidationSummary {
  pub expected_records: u64,
  pub unique_records: u64,
  pub duplicate_records: u64,
  pub missing_records: u64,
  pub duplicate_samples: Vec<StressRecordIdentity>,
  pub missing_samples: Vec<StressRecordIdentity>,
}

impl StressValidationSummary {
  #[must_use]
  pub fn is_complete(&self) -> bool {
    self.missing_records == 0 && self.duplicate_records == 0
  }
}

//
// StressValidator
//

pub struct StressValidator {
  run_id: Uuid,
  expected_per_producer: Vec<u64>,
  observed: Vec<Vec<u64>>,
  expected_records: u64,
  unique_records: u64,
  duplicate_records: u64,
  duplicate_samples: Vec<StressRecordIdentity>,
  max_samples: usize,
}

impl StressValidator {
  pub fn new(run_id: Uuid, expected_per_producer: Vec<u64>, max_samples: usize) -> Result<Self> {
    let mut observed = Vec::with_capacity(expected_per_producer.len());
    let mut expected_records = 0_u64;

    for expected_count in &expected_per_producer {
      let word_count = expected_count.div_ceil(64);
      let word_count = usize::try_from(word_count)
        .map_err(|_| anyhow!("expected record count does not fit this platform"))?;
      observed.push(vec![0; word_count]);
      expected_records = expected_records
        .checked_add(*expected_count)
        .ok_or_else(|| anyhow!("expected record count overflow"))?;
    }

    Ok(Self {
      run_id,
      expected_per_producer,
      observed,
      expected_records,
      unique_records: 0,
      duplicate_records: 0,
      duplicate_samples: Vec::with_capacity(max_samples),
      max_samples,
    })
  }

  pub fn observe(&mut self, identity: StressRecordIdentity) -> Result<Observation> {
    if identity.run_id != self.run_id {
      return Err(anyhow!("received a record for a different stress run"));
    }

    let producer_index = usize::try_from(identity.producer_index)
      .map_err(|_| anyhow!("producer index does not fit this platform"))?;
    let expected_count = *self
      .expected_per_producer
      .get(producer_index)
      .ok_or_else(|| anyhow!("received a record for an unknown producer"))?;
    if identity.sequence >= expected_count {
      return Err(anyhow!(
        "received sequence {} outside producer {} expected range 0..{}",
        identity.sequence,
        identity.producer_index,
        expected_count
      ));
    }

    let word_index = usize::try_from(identity.sequence / 64)
      .map_err(|_| anyhow!("sequence index does not fit this platform"))?;
    let bit = 1_u64 << (identity.sequence % 64);
    let word = self
      .observed
      .get_mut(producer_index)
      .and_then(|producer| producer.get_mut(word_index))
      .ok_or_else(|| anyhow!("sequence bitmap is missing an expected word"))?;

    if *word & bit != 0 {
      self.duplicate_records += 1;
      if self.duplicate_samples.len() < self.max_samples {
        self.duplicate_samples.push(identity);
      }
      return Ok(Observation::Duplicate);
    }

    *word |= bit;
    self.unique_records += 1;
    Ok(Observation::Unique)
  }

  #[must_use]
  pub fn unique_records(&self) -> u64 {
    self.unique_records
  }

  #[must_use]
  pub fn summary(&self) -> StressValidationSummary {
    let mut missing_samples = Vec::with_capacity(self.max_samples);
    let mut missing_records = 0_u64;

    for (producer_index, expected_count) in self.expected_per_producer.iter().enumerate() {
      for sequence in 0 .. *expected_count {
        let word_index = usize::try_from(sequence / 64)
          .expect("sequence bitmap index was validated during construction");
        let bit = 1_u64 << (sequence % 64);
        if self.observed[producer_index][word_index] & bit == 0 {
          missing_records += 1;
          if missing_samples.len() < self.max_samples {
            missing_samples.push(StressRecordIdentity {
              run_id: self.run_id,
              producer_index: u32::try_from(producer_index)
                .expect("producer index fits the payload identity format"),
              sequence,
            });
          }
        }
      }
    }

    StressValidationSummary {
      expected_records: self.expected_records,
      unique_records: self.unique_records,
      duplicate_records: self.duplicate_records,
      missing_records,
      duplicate_samples: self.duplicate_samples.clone(),
      missing_samples,
    }
  }
}
