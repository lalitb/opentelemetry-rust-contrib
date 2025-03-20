use crate::auth::AuthMethod;
use std::collections::HashMap;

/// Configuration for a single GCS client instance.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub endpoint: String,
    pub environment: String,
    pub account: String,
    pub namespace: String,
    pub region: String,
    pub config_major_version: u32,
    pub agent_identity: String,
    pub agent_version: String,
    pub tag_id: Option<String>,
    pub auth_method: AuthMethod, // Authentication method for this account
}

/// Configuration for multi-tenant support, storing multiple account configurations.
#[derive(Debug, Clone)]
pub struct MultiTenantConfig {
    pub accounts: HashMap<String, ClientConfig>, // Map of account name -> ClientConfig
}
