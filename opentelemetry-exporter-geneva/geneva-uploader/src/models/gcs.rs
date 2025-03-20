use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
pub struct GcsResponse {
    #[serde(rename = "IngestionGatewayInfo")]
    pub ingestion_gateway_info: IngestionGatewayInfo,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Deserialize)]
pub struct IngestionGatewayInfo {
    #[serde(rename = "AuthToken")]
    pub auth_token: String,
    #[serde(rename = "Endpoint")]
    pub endpoint: String,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}
