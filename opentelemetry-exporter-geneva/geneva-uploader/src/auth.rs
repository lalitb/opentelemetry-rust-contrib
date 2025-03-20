use crate::error::Result;
use reqwest::{ClientBuilder, Identity, RequestBuilder};
use std::fs;

/// Supported authentication mechanisms
#[derive(Debug, Clone)]
pub enum AuthMethod {
    Pkcs8 { cert_path: String, key_path: String }, // PEM format with separate key
    Pkcs12 { cert_path: String, password: String }, // PFX/P12 format    Token { token: String },
    Token { token: String },
    Basic { username: String, password: String },
    CustomHeader { key: String, value: String },
}

impl AuthMethod {
    /// Apply authentication settings to a client request
    pub fn apply_auth(&self, request: RequestBuilder) -> RequestBuilder {
        match self {
            AuthMethod::Pkcs8 { .. } | AuthMethod::Pkcs12 { .. } => request, // No extra headers needed
            AuthMethod::Token { token } => {
                request.header("Authorization", format!("Bearer {}", token))
            }
            AuthMethod::Basic { username, password } => {
                request.basic_auth(username, Some(password))
            }
            AuthMethod::CustomHeader { key, value } => request.header(key, value),
        }
    }

    pub fn create_client(&self) -> Result<reqwest::Client> {
        let mut builder = ClientBuilder::new();

        match self {
            AuthMethod::Pkcs8 {
                cert_path,
                key_path,
            } => {
                let cert_pem = fs::read(cert_path)?;
                let key_pem = fs::read(key_path)?;
                let identity = Identity::from_pkcs8_pem(&cert_pem, &key_pem)?;
                builder = builder.identity(identity);
            }
            AuthMethod::Pkcs12 {
                cert_path,
                password,
            } => {
                let cert_der = fs::read(cert_path)?;
                let identity = Identity::from_pkcs12_der(&cert_der, password)?;
                builder = builder.identity(identity);
            }
            _ => {} // Other authentication methods don't modify the client
        }
        Ok(builder
            .timeout(std::time::Duration::from_secs(30))
            .build()?)
    }
}
