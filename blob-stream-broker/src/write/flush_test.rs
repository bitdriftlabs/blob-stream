use super::SnowflakeGenerator;
use anyhow::Result;
use time::OffsetDateTime;

#[test]
fn default_machine_id_generator_initializes() -> Result<()> {
  let generator = SnowflakeGenerator::new()?;

  assert!(generator.next(OffsetDateTime::now_utc())?.as_u64() > 0);
  Ok(())
}

#[test]
fn explicit_machine_ids_produce_distinct_ids() -> Result<()> {
  let first = SnowflakeGenerator::with_machine_id(1)?;
  let second = SnowflakeGenerator::with_machine_id(2)?;
  let now = OffsetDateTime::now_utc();

  assert_ne!(first.next(now)?.as_u64(), second.next(now)?.as_u64());
  Ok(())
}
