use reqwest::{Client, StatusCode};
use serde_json::Value;
use std::{future::Future, sync::Arc};

pub enum TransportMechanism {
    HttpJsonPost,
}

pub struct TransportClient {
    client: Arc<Client>,
    endpoint: String,
    mechanism: TransportMechanism,
}

impl TransportClient {
    pub fn new(endpoint: &str, mechanism: TransportMechanism) -> Self {
        Self {
            client: Arc::new(Client::new()),
            endpoint: endpoint.to_string(),
            mechanism,
        }
    }

    pub fn send(&self, payload: Value) -> impl Future<Output = Result<(), String>> + Send {
        let client = self.client.clone();
        let endpoint = self.endpoint.clone();

        async move {
            let response = client.post(&endpoint).json(&payload).send().await;

            match response {
                Ok(res) if res.status() == StatusCode::OK => Ok(()),
                Ok(res) => Err(format!("HTTP error: {}", res.status())),
                Err(err) => Err(format!("Request error: {}", err)),
            }
        }
    }
}
