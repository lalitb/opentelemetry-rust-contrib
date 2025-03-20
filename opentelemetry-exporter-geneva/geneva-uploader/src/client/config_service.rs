use crate::config::ClientConfig;
use crate::error::{Error, Result};
use crate::models::gcs::GcsResponse;
use crate::utils::jwt::decode_jwt;
use crate::utils::retry::retry_async;
use base64::{engine::general_purpose, Engine as _};
use reqwest::{header, Client};
use uuid::Uuid;

#[allow(dead_code)]
#[derive(Clone)]
pub(crate) struct ConfigServiceClient {
    client: Client,
    config: ClientConfig,
}

#[allow(dead_code)]
impl ConfigServiceClient {
    pub fn new(config: ClientConfig) -> Result<Self> {
        let client = config.auth_method.create_client()?;
        Ok(Self { client, config })
    }

    fn build_url(&self) -> String {
        let formatted_identity = self.config.agent_identity.clone();
        let encoded_identity = general_purpose::STANDARD.encode(&formatted_identity);
        let config_version_str = format!("Ver{}v0", self.config.config_major_version);

        let mut url = format!(
            "{}/api/agent/v2/{}/{}/MonitoringStorageKeys/?Namespace={}&Region={}&Identity={}&OSType=Linux&ConfigMajorVersion={}",
            self.config.endpoint.trim_end_matches('/'),
            self.config.environment,
            self.config.account,
            self.config.namespace,
            self.config.region,
            encoded_identity,
            config_version_str
        );

        if let Some(id) = &self.config.tag_id {
            url.push_str(&format!("&TagId={}", id));
        }

        url
    }

    pub async fn get_gcs(&self) -> Result<GcsResponse> {
        let request_id = Uuid::new_v4().to_string();
        let url = self.build_url();

        let request = self
            .client
            .get(&url)
            .header(
                header::USER_AGENT,
                format!("LinuxMonitoringAgent/Mdsd-{}", self.config.agent_version),
            )
            .header("x-ms-client-request-id", &request_id)
            .header(header::ACCEPT, "application/json");

        let request = self.config.auth_method.apply_auth(request);

        let response = request.send().await?;
        let status = response.status();
        let body = response.text().await?;

        if status.is_success() {
            let gcs_response: GcsResponse = serde_json::from_str(&body)?;
            decode_jwt(&gcs_response.ingestion_gateway_info.auth_token)?;
            Ok(gcs_response)
        } else {
            Err(Error::InvalidResponse(format!(
                "Request failed with status {}: {}",
                status, body
            )))
        }
    }

    pub async fn get_gcs_with_retry(
        &self,
        max_retries: u32,
        retry_delay_seconds: u64,
    ) -> Result<GcsResponse> {
        retry_async(max_retries, retry_delay_seconds, || self.get_gcs()).await
    }
}
