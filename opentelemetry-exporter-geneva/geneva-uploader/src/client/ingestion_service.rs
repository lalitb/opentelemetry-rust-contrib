#[allow(dead_code)]
struct IngestionClient {
    client: reqwest::Client,
    endpoint: String,
    token: String,
}

#[allow(dead_code)]
impl IngestionClient {
    /// Creates a new IngestionClient instance.
    ///
    /// # Arguments
    ///
    /// * `endpoint` - The endpoint URL for the GCS service.
    /// * `token` - The authentication token for the GCS service.
    pub fn new(endpoint: String, token: String) -> Self {
        let client = reqwest::Client::new();
        Self {
            client,
            endpoint,
            token,
        }
    }
}
