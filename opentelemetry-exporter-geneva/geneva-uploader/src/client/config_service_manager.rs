use crate::client::config_service::ConfigServiceClient;
use crate::config::ClientConfig;
use crate::error::{Error, Result};

const MAX_ACCOUNTS: usize = 10; // Fixed limit on the number of accounts

#[allow(dead_code)]
pub(crate) struct ConfigServiceManagerBuilder {
    configs: [Option<ClientConfig>; MAX_ACCOUNTS],
    count: usize,
}

#[allow(dead_code)]
impl ConfigServiceManagerBuilder {
    pub(crate) fn new() -> Self {
        Self {
            configs: Default::default(),
            count: 0,
        }
    }

    pub(crate) fn add_account(mut self, config: ClientConfig) -> Result<Self> {
        if self.count >= MAX_ACCOUNTS {
            return Err(Error::InvalidResponse(
                "Maximum number of accounts reached".to_string(),
            ));
        }
        self.configs[self.count] = Some(config);
        self.count += 1;
        Ok(self)
    }

    pub(crate) fn build(self) -> Result<ConfigServiceManager> {
        let mut clients: [Option<ConfigServiceClient>; MAX_ACCOUNTS] = Default::default();
        for i in 0..self.count {
            if let Some(config) = &self.configs[i] {
                clients[i] = Some(ConfigServiceClient::new(config.clone())?);
            }
        }
        Ok(ConfigServiceManager { clients })
    }
}

#[allow(dead_code)]
pub(crate) struct ConfigServiceManager {
    clients: [Option<ConfigServiceClient>; MAX_ACCOUNTS],
}

#[allow(dead_code)]
impl ConfigServiceManager {
    pub(crate) fn get_client(&self, index: usize) -> Result<ConfigServiceClient> {
        self.clients
            .get(index)
            .and_then(|client| client.clone())
            .ok_or_else(|| Error::InvalidResponse(format!("Client index {} not found", index)))
    }
}
