//! Shared workload configuration and correctness accounting for the local stress runner.

#[cfg(test)]
#[path = "./stress_test.rs"]
mod tests;

mod config;
mod identity;
mod runner;
mod validator;

pub use config::StressConfig;
pub use identity::StressRecordIdentity;
pub use runner::{InlineMisroutedRecord, InlineValidationSummary, StressRunSummary, run};
pub use validator::{Observation, StressValidationSummary, StressValidator};
