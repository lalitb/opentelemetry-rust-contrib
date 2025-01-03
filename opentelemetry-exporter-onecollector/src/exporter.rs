use crate::contextual_info::ContextualInfo;
use crate::serialization::{serialize_log_record, SerializationFormat};
use crate::transport::{TransportClient, TransportMechanism};
use opentelemetry_sdk::export::logs::{LogBatch, LogExporter};
use opentelemetry_sdk::logs::{LogError, LogResult};
use serde_json::Value;
use std::fmt::{self, Debug, Formatter};
use std::{
    future::Future,
    sync::{Arc, Mutex},
};

pub struct OneCollectorExporter {
    transport_client: TransportClient,
    serialization_format: SerializationFormat,
    contextual_info: Arc<Mutex<ContextualInfo>>,
}

impl Debug for OneCollectorExporter {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("OneCollectorExporter").finish()
    }
}

impl OneCollectorExporter {
    pub fn new(
        endpoint: &str,
        serialization_format: SerializationFormat,
        transport_mechanism: TransportMechanism,
        contextual_info: Arc<Mutex<ContextualInfo>>,
    ) -> Self {
        Self {
            transport_client: TransportClient::new(endpoint, transport_mechanism),
            serialization_format,
            contextual_info,
        }
    }

    fn prepare_payload(&self, record: &LogBatch<'_>) -> Vec<Value> {
        record
            .iter()
            .map(|(log_record, _)| {
                let mut payload = serialize_log_record(log_record, &self.serialization_format);
                self.contextual_info
                    .lock()
                    .unwrap()
                    .merge_with_event(&mut payload);
                payload
            })
            .collect()
    }
}

impl LogExporter for OneCollectorExporter {
    fn export(&self, batch: LogBatch<'_>) -> impl Future<Output = LogResult<()>> + Send {
        let payloads = self.prepare_payload(&batch);
        let transport_client = self.transport_client.clone();

        async move {
            for payload in payloads {
                if transport_client.send(payload).await.is_err() {
                    return Err(LogError::Other("Failed to send payload".into()));
                }
            }
            Ok(())
        }
    }

    fn shutdown(&mut self) {}
}
