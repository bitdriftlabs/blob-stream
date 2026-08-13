//! `DynamoDB` attribute names shared by metadata and producer-lease persistence.

pub const ATTR_EPOCH: &str = "lease_epoch";
pub const ATTR_EXPIRES: &str = "lease_expiration_ts_ms";
pub const ATTR_HOLDER: &str = "holder_id";
pub const ATTR_PK: &str = "pk";
pub const ATTR_SESSION: &str = "lease_session_id";
pub const ATTR_SK: &str = "sk";
pub const ATTR_TTL: &str = "ttl_epoch_seconds";
