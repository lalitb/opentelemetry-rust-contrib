// Azure Arc Managed Identity Credential Implementation for Geneva Config Service
//
// This module provides Azure Arc MSI authentication specifically for the Geneva config service.
// It implements the TokenCredential trait from azure_core to integrate seamlessly with the
// existing Geneva config client authentication architecture without requiring changes to the
// main azure-sdk-for-rust repository.

use azure_core::{
    credentials::{AccessToken, Secret, TokenCredential, TokenRequestOptions},
    error::{Error, ErrorKind, ResultExt},
    http::{request::Request, HttpClient, Method, StatusCode, Url},
    json::from_json,
    time::OffsetDateTime,
    Result,
};
use serde::Deserialize;
use std::{fs, path::Path, sync::Arc};

const API_VERSION: &str = "2020-06-01";
const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:40342/metadata/identity/oauth2/token";
const HEADER_METADATA: &str = "metadata";
const HEADER_WWW_AUTHENTICATE: &str = "www-authenticate";
const HEADER_AUTHORIZATION: &str = "authorization";
const ARC_FILE_EXTENSION: &str = ".key";
const ARC_MAX_FILE_SIZE: u64 = 4096;
const ARC_TOKENS_DIR_LINUX: &str = "/var/opt/azcmagent/tokens";

#[cfg(target_os = "windows")]
const ARC_PROGRAM_DATA: &str = "ProgramData";
#[cfg(target_os = "windows")]
const ARC_CONNECTED_MACHINE_DIR: &str = "AzureConnectedMachineAgent";
#[cfg(target_os = "windows")]
const ARC_TOKENS_SUBDIR: &str = "Tokens";

// Test override env var (not documented / internal)
const ARC_TOKENS_DIR_OVERRIDE: &str = "AZURE_IDENTITY_ARC_TOKENS_DIR";

/// Azure Arc Managed Identity Credential for authenticating with Azure services
///
/// This credential implements the two-step challenge flow required by Azure Arc:
/// 1. Initial unauthenticated request receives 401 with www-authenticate header containing secret file path
/// 2. Second request includes secret file contents in Authorization header
///
/// # Protocol Details
/// - Only supports system-assigned managed identity (no user-assigned support in Arc)
/// - Requires access to Azure Arc agent's secret files (typically in /var/opt/azcmagent/tokens/)
/// - Secret files must have .key extension and be ≤4KB
///
/// # Example
/// ```rust,no_run
/// use azure_core::credentials::TokenCredential;
/// use geneva_uploader::AzureArcManagedIdentityCredential;
///
/// # async fn example() -> azure_core::Result<()> {
/// let http_client = std::sync::Arc::new(azure_core::http::new_http_client());
/// let credential = AzureArcManagedIdentityCredential::new(http_client)?;
///
/// let token = credential
///     .get_token(&["https://management.azure.com/.default"], None)
///     .await?;
///
/// println!("Access token: {}", token.token.secret());
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct AzureArcManagedIdentityCredential {
    http_client: Arc<dyn HttpClient>,
    endpoint: Url,
}

impl AzureArcManagedIdentityCredential {
    /// Creates a new Azure Arc Managed Identity Credential
    ///
    /// # Arguments
    /// * `http_client` - HTTP client for making requests to the Arc MSI endpoint
    ///
    /// # Returns
    /// * `Result<Self>` - New credential instance or error if endpoint URL is invalid
    ///
    /// # Environment Variables
    /// * `IDENTITY_ENDPOINT` - Optional override for Arc MSI endpoint (defaults to localhost:40342)
    /// * `AZURE_IDENTITY_ARC_TOKENS_DIR` - Optional override for tokens directory (for testing)
    pub fn new(http_client: Arc<dyn HttpClient>) -> Result<Self> {
        // Use IDENTITY_ENDPOINT if provided, else default constant (mirrors MSAL behavior)
        let endpoint =
            std::env::var("IDENTITY_ENDPOINT").unwrap_or_else(|_| DEFAULT_ENDPOINT.to_string());
        let endpoint = Url::parse(&endpoint).with_context(ErrorKind::Credential, || {
            format!("Azure Arc endpoint must be a valid URL, got '{endpoint}'")
        })?;

        Ok(Self {
            http_client,
            endpoint,
        })
    }

    /// Determines the expected tokens directory based on OS and environment
    fn tokens_dir(&self) -> Option<String> {
        if let Ok(override_dir) = std::env::var(ARC_TOKENS_DIR_OVERRIDE) {
            return Some(override_dir);
        }

        #[cfg(target_os = "windows")]
        {
            if let Ok(program_data) = std::env::var(ARC_PROGRAM_DATA) {
                let p = Path::new(&program_data)
                    .join(ARC_CONNECTED_MACHINE_DIR)
                    .join(ARC_TOKENS_SUBDIR);
                return Some(p.to_string_lossy().to_string());
            }
        }

        #[cfg(not(target_os = "windows"))]
        {
            return Some(ARC_TOKENS_DIR_LINUX.to_string());
        }

        #[allow(unreachable_code)]
        None
    }

    /// Internal implementation of token acquisition
    async fn get_token_impl(
        &self,
        scopes: &[&str],
        _options: Option<TokenRequestOptions<'_>>,
    ) -> Result<AccessToken> {
        if scopes.len() != 1 {
            return Err(Error::message(
                ErrorKind::Credential,
                "Azure Arc managed identity requires exactly one scope",
            ));
        }
        let resource = scopes[0].strip_suffix("/.default").unwrap_or(scopes[0]);

        // Phase 1: initial challenge request
        let mut url = self.endpoint.clone();
        url.query_pairs_mut()
            .append_pair("api-version", API_VERSION)
            .append_pair("resource", resource);

        let mut req = Request::new(url.clone(), Method::Get);
        req.insert_header(HEADER_METADATA, "true");

        let rsp = self.http_client.execute_request(&req).await?;
        let (status, headers, body) = rsp.deconstruct();
        let bytes = body.collect().await?;

        if status != StatusCode::Unauthorized {
            // If it's already success, treat as protocol error because Arc expects 401 first.
            if status.is_success() {
                return Err(Error::message(
                    ErrorKind::Credential,
                    format!("Azure Arc challenge expected HTTP 401, received {}", status),
                ));
            }
            return Err(Error::with_message(ErrorKind::Credential, || {
                format!(
                    "Azure Arc request failed with status {}: {}",
                    status,
                    String::from_utf8_lossy(&bytes)
                )
            }));
        }

        let www_auth = headers
            .get_str(&azure_core::http::headers::HeaderName::from_static(
                HEADER_WWW_AUTHENTICATE,
            ))
            .map_err(|_| {
                Error::message(
                    ErrorKind::Credential,
                    "Azure Arc response missing www-authenticate header",
                )
            })?;

        // Extract path after "Basic realm="
        let secret_path = parse_basic_realm_path(www_auth).ok_or_else(|| {
            Error::message(
                ErrorKind::Credential,
                "Azure Arc www-authenticate header missing expected Basic realm path",
            )
        })?;

        validate_arc_secret_path(
            &secret_path,
            self.tokens_dir()
                .as_deref()
                .unwrap_or("<unavailable default tokens dir>"),
        )?;

        let secret = fs::read(&secret_path).with_context(ErrorKind::Credential, || {
            format!("Azure Arc failed to read secret file '{}'", secret_path)
        })?;

        if secret.len() as u64 > ARC_MAX_FILE_SIZE {
            return Err(Error::message(
                ErrorKind::Credential,
                format!(
                    "Azure Arc secret file too large ({} bytes, max {})",
                    secret.len(),
                    ARC_MAX_FILE_SIZE
                ),
            ));
        }

        // Phase 2: authorized request with secret
        let url2 = url;
        let mut req2 = Request::new(url2, Method::Get);
        req2.insert_header(HEADER_METADATA, "true");
        req2.insert_header(
            HEADER_AUTHORIZATION,
            format!("Basic {}", String::from_utf8_lossy(&secret)),
        );

        let rsp2 = self.http_client.execute_request(&req2).await?;
        let (status2, _, body2) = rsp2.deconstruct();
        let body2 = body2.collect().await?;
        if !status2.is_success() {
            return Err(Error::with_message(ErrorKind::Credential, || {
                format!(
                    "Azure Arc token request failed with status {}: {}",
                    status2,
                    String::from_utf8_lossy(&body2)
                )
            }));
        }

        let token_response: ArcTokenResponse = from_json(&body2)
            .with_context(ErrorKind::Credential, || {
                "Azure Arc token response invalid JSON".to_string()
            })?;

        Ok(AccessToken::new(
            token_response.access_token,
            token_response.expires_on,
        ))
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl TokenCredential for AzureArcManagedIdentityCredential {
    async fn get_token(
        &self,
        scopes: &[&str],
        options: Option<TokenRequestOptions<'_>>,
    ) -> Result<AccessToken> {
        self.get_token_impl(scopes, options).await
    }
}

/// Parses the secret file path from the www-authenticate header
///
/// # Arguments
/// * `header` - The www-authenticate header value (e.g., "Basic realm=/path/to/secret.key")
///
/// # Returns
/// * `Option<String>` - The extracted file path, or None if parsing fails
fn parse_basic_realm_path(header: &str) -> Option<String> {
    // Expect pattern containing 'Basic realm=' then path
    let lower = header.to_lowercase();
    let idx = lower.find("basic realm=")?;
    let after = &header[idx + "Basic realm=".len()..];
    // Path ends at first whitespace or is full remainder
    let trimmed = after.trim();
    // Remove any surrounding quotes if present
    let path = trimmed
        .trim_matches('"')
        .trim_matches('\'')
        .trim()
        .to_string();
    Some(path)
}

/// Validates the Arc secret file path for security and correctness
///
/// # Arguments
/// * `path_str` - The secret file path to validate
/// * `expected_dir` - The expected directory for security validation
///
/// # Returns
/// * `Result<()>` - Ok if valid, error with details if invalid
fn validate_arc_secret_path(path_str: &str, expected_dir: &str) -> Result<()> {
    let path = Path::new(path_str);

    if path.extension().and_then(|e| e.to_str()) != Some(&ARC_FILE_EXTENSION[1..]) {
        return Err(Error::message(
            ErrorKind::Credential,
            format!(
                "Azure Arc secret file must have {} extension",
                ARC_FILE_EXTENSION
            ),
        ));
    }

    if !path.exists() {
        return Err(Error::message(
            ErrorKind::Credential,
            "Azure Arc secret file does not exist",
        ));
    }

    // Directory validation (best-effort)
    if let Some(parent) = path.parent() {
        if parent.to_string_lossy() != expected_dir {
            // Soft validation: don't fail outright if override var provided
            if std::env::var(ARC_TOKENS_DIR_OVERRIDE).is_err() {
                return Err(Error::message(
                    ErrorKind::Credential,
                    format!(
                        "Azure Arc secret file directory mismatch (expected {}, got {})",
                        expected_dir,
                        parent.to_string_lossy()
                    ),
                ));
            }
        }
    }

    Ok(())
}

/// Token response from Azure Arc MSI endpoint
#[derive(Debug, Clone, Deserialize)]
#[allow(unused)]
struct ArcTokenResponse {
    pub access_token: Secret,
    #[serde(deserialize_with = "expires_on_string")]
    pub expires_on: OffsetDateTime,
    pub token_type: String,
    pub resource: String,
}

/// Deserializes expires_on field from string unix timestamp to OffsetDateTime
fn expires_on_string<'de, D>(deserializer: D) -> std::result::Result<OffsetDateTime, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let v = String::deserialize(deserializer)?;
    let as_i64 = v.parse::<i64>().map_err(serde::de::Error::custom)?;
    OffsetDateTime::from_unix_timestamp(as_i64).map_err(serde::de::Error::custom)
}

#[cfg(test)]
mod tests {
    use super::*;
    use azure_core::{
        http::{
            headers::{HeaderName, Headers},
            RawResponse,
        },
        Bytes,
    };
    use std::{
        io::Write,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Mutex,
        },
        time::{SystemTime, UNIX_EPOCH},
    };

    // Mock HTTP client for testing
    #[derive(Debug)]
    struct MockHttpClient {
        responses: Mutex<Vec<RawResponse>>,
        call_count: AtomicUsize,
    }

    impl MockHttpClient {
        fn new(responses: Vec<RawResponse>) -> Arc<Self> {
            Arc::new(Self {
                responses: Mutex::new(responses),
                call_count: AtomicUsize::new(0),
            })
        }

        fn call_count(&self) -> usize {
            self.call_count.load(Ordering::SeqCst)
        }
    }

    use async_trait::async_trait;

    #[async_trait]
    impl HttpClient for MockHttpClient {
        async fn execute_request<'a>(&self, _request: &'a Request) -> Result<RawResponse> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            let mut responses = self.responses.lock().unwrap();
            if responses.is_empty() {
                Err(Error::message(ErrorKind::Other, "No more mock responses"))
            } else {
                Ok(responses.remove(0))
            }
        }
    }

    #[tokio::test]
    async fn test_arc_challenge_flow() {
        // Create temp directory and secret file
        let base_dir = {
            let p = std::env::temp_dir().join(format!(
                "arc_test_{}",
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            std::fs::create_dir(&p).unwrap();
            p
        };
        let secret_path = base_dir.join("secret.key");
        let mut f = std::fs::File::create(&secret_path).unwrap();
        write!(f, "arc-secret").unwrap();

        // Set environment variable for token directory override
        unsafe { std::env::set_var(ARC_TOKENS_DIR_OVERRIDE, base_dir.to_string_lossy().as_ref()) };

        let expires_on = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600;

        // First response: 401 with challenge
        let mut h1 = Headers::default();
        let header_value = format!("Basic realm={}", secret_path.to_string_lossy());
        let header_value_static: &'static str = Box::leak(header_value.into_boxed_str());
        h1.insert(
            HeaderName::from_static(HEADER_WWW_AUTHENTICATE),
            header_value_static,
        );
        let resp1 = RawResponse::from_bytes(StatusCode::Unauthorized, h1, Bytes::from_static(b""));

        // Second response: 200 with token
        let body2 = format!(
            r#"{{"access_token":"test_token","expires_on":"{}","token_type":"Bearer","resource":"https://management.azure.com"}}"#,
            expires_on
        );
        let resp2 = RawResponse::from_bytes(StatusCode::Ok, Headers::default(), Bytes::from(body2));

        let mock = MockHttpClient::new(vec![resp1, resp2]);

        // Test the credential
        let credential = AzureArcManagedIdentityCredential::new(mock.clone()).unwrap();
        let token = credential
            .get_token(&["https://management.azure.com/.default"], None)
            .await
            .unwrap();

        assert_eq!(token.token.secret(), "test_token");
        assert_eq!(mock.call_count(), 2);

        // Cleanup
        unsafe { std::env::remove_var(ARC_TOKENS_DIR_OVERRIDE) };
    }

    #[test]
    fn test_parse_basic_realm_path() {
        let header = r#"Basic realm="/var/opt/azcmagent/tokens/secret.key""#;
        let path = parse_basic_realm_path(header).unwrap();
        assert_eq!(path, "/var/opt/azcmagent/tokens/secret.key");

        let header2 = "Basic realm=/path/without/quotes.key";
        let path2 = parse_basic_realm_path(header2).unwrap();
        assert_eq!(path2, "/path/without/quotes.key");
    }

    #[test]
    fn test_validate_arc_secret_path() {
        // Create temp file for testing
        let base_dir = std::env::temp_dir().join("arc_test_validation");
        std::fs::create_dir_all(&base_dir).unwrap();
        let secret_path = base_dir.join("test.key");
        std::fs::File::create(&secret_path).unwrap();

        // Set override to avoid directory mismatch
        unsafe { std::env::set_var(ARC_TOKENS_DIR_OVERRIDE, base_dir.to_string_lossy().as_ref()) };

        // Valid path should succeed
        let result =
            validate_arc_secret_path(&secret_path.to_string_lossy(), &base_dir.to_string_lossy());
        assert!(result.is_ok());

        // Invalid extension should fail
        let bad_path = base_dir.join("test.txt");
        std::fs::File::create(&bad_path).unwrap();
        let result =
            validate_arc_secret_path(&bad_path.to_string_lossy(), &base_dir.to_string_lossy());
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("must have .key extension"));

        // Cleanup
        unsafe { std::env::remove_var(ARC_TOKENS_DIR_OVERRIDE) };
    }
}
