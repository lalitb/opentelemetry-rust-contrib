//! OTAP Log Encoder for Geneva
//!
//! This module provides encoding support for OTAP (OpenTelemetry Arrow Protocol) log data,
//! transforming Arrow-based log records into Geneva-compliant Bond + LZ4 batches.
//!
//! # Architecture
//!
//! The encoder follows a pipeline approach:
//! 1. **Schema Detection**: Analyze log records to determine their schema structure
//! 2. **Grouping**: Group logs by event name and schema for efficient batching
//! 3. **Bond Encoding**: Transform log data into Bond binary format
//! 4. **Compression**: Apply LZ4 chunked compression
//! 5. **Batch Creation**: Package into `EncodedBatch` for upload
//!
//! # Example
//!
//! ```rust,no_run
//! use geneva_uploader::payload_encoder::otap_log_encoder::OtapLogEncoder;
//! # use geneva_uploader::client::EncodedBatch;
//!
//! let encoder = OtapLogEncoder::new();
//! let metadata = "namespace=MyNamespace/eventVersion=Ver1v0/tenant=T/role=R/roleinstance=RI";
//!
//! // Encode logs (using trait objects for flexibility)
//! // let batches: Vec<EncodedBatch> = encoder.encode_log_batch(log_views, metadata)?;
//! ```

use crate::client::EncodedBatch;
use crate::payload_encoder::bond_encoder::{BondDataType, BondEncodedSchema, BondWriter, FieldDef};
use crate::payload_encoder::central_blob::{
    BatchMetadata, CentralBlob, CentralEventEntry, CentralSchemaEntry,
};
use crate::payload_encoder::lz4_chunked_compression::lz4_chunked_compression;
use chrono::{TimeZone, Utc};
use std::borrow::Cow;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tracing::{debug, warn};

// Field name constants for Geneva OTAP log schema
const FIELD_ENV_NAME: &str = "env_name";
const FIELD_ENV_VER: &str = "env_ver";
const FIELD_TIMESTAMP: &str = "timestamp";
const FIELD_ENV_TIME: &str = "env_time";
const FIELD_TRACE_ID: &str = "env_dt_traceId";
const FIELD_SPAN_ID: &str = "env_dt_spanId";
const FIELD_TRACE_FLAGS: &str = "env_dt_traceFlags";
const FIELD_EVENT_NAME: &str = "event_name";
const FIELD_SEVERITY_NUMBER: &str = "SeverityNumber";
const FIELD_SEVERITY_TEXT: &str = "SeverityText";
const FIELD_BODY: &str = "body";

/// Represents a single log record for encoding purposes.
///
/// This struct provides a unified interface for log data that can come from various sources
/// (OTAP Arrow, OTLP protobuf, etc.). It captures all the essential fields needed for
/// Geneva log encoding.
#[derive(Debug, Clone)]
pub struct LogRecordData {
    /// Time when the event occurred (nanoseconds since Unix epoch)
    pub time_unix_nano: Option<u64>,
    /// Time when the event was observed (nanoseconds since Unix epoch)
    pub observed_time_unix_nano: Option<u64>,
    /// Numerical severity level
    pub severity_number: Option<i32>,
    /// Textual severity level (e.g., "INFO", "ERROR")
    pub severity_text: Option<String>,
    /// The body/message of the log record
    pub body: Option<LogBody>,
    /// Key-value attributes attached to this log
    pub attributes: Vec<LogAttribute>,
    /// Trace ID (16 bytes) - empty if not part of a trace
    pub trace_id: Vec<u8>,
    /// Span ID (8 bytes) - empty if not part of a span
    pub span_id: Vec<u8>,
    /// Trace flags
    pub flags: Option<u32>,
    /// Event name/category for this log
    pub event_name: Option<String>,
}

/// Represents the body of a log record
#[derive(Debug, Clone)]
pub enum LogBody {
    /// String body
    String(String),
    /// Integer body
    Int(i64),
    /// Double body
    Double(f64),
    /// Boolean body
    Bool(bool),
    // Can be extended for other types as needed
}

/// Represents a log attribute
#[derive(Debug, Clone)]
pub struct LogAttribute {
    /// Attribute key
    pub key: String,
    /// Attribute value
    pub value: AttributeValue,
}

/// Represents an attribute value
#[derive(Debug, Clone)]
pub enum AttributeValue {
    /// String value
    String(String),
    /// Integer value
    Int(i64),
    /// Double value
    Double(f64),
    /// Boolean value
    Bool(bool),
}

impl AttributeValue {
    /// Get the Bond data type for this attribute value
    fn bond_type(&self) -> BondDataType {
        match self {
            AttributeValue::String(_) => BondDataType::BT_STRING,
            AttributeValue::Int(_) => BondDataType::BT_INT64,
            AttributeValue::Double(_) => BondDataType::BT_DOUBLE,
            AttributeValue::Bool(_) => BondDataType::BT_BOOL,
        }
    }
}

/// OTAP Log Encoder for transforming Arrow-based log data into Geneva format.
///
/// This encoder handles the complete pipeline from OTAP log records to compressed
/// Geneva batches ready for upload.
#[derive(Clone)]
pub struct OtapLogEncoder {
    /// Placeholder for environment name (can be made configurable)
    env_name: String,
    /// Placeholder for environment version (can be made configurable)
    env_ver: String,
}

impl OtapLogEncoder {
    /// Create a new OTAP log encoder with default settings
    pub fn new() -> Self {
        Self {
            env_name: "TestEnv".to_string(),
            env_ver: "4.0".to_string(),
        }
    }

    /// Create a new OTAP log encoder with custom environment settings
    pub fn with_env(env_name: String, env_ver: String) -> Self {
        Self { env_name, env_ver }
    }

    /// Encode a batch of OTAP logs into Geneva-compliant batches.
    ///
    /// This method groups logs by event name and schema, then encodes each group
    /// into a separate compressed batch. Each batch contains:
    /// - Schema definitions (Bond encoded)
    /// - Log records (Bond encoded)
    /// - Metadata (timestamps, schema IDs)
    /// - LZ4 compressed payload
    ///
    /// # Arguments
    ///
    /// * `logs` - Iterator over log records to encode
    /// * `metadata` - Geneva metadata string (namespace/eventVersion/tenant/role/roleinstance)
    ///
    /// # Returns
    ///
    /// A vector of `EncodedBatch` objects ready for upload, or an error if encoding fails.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - LZ4 compression fails
    /// - Schema generation fails
    /// - Bond encoding fails
    pub fn encode_log_batch<'a, I>(
        &self,
        logs: I,
        metadata: &str,
    ) -> Result<Vec<EncodedBatch>, String>
    where
        I: IntoIterator<Item = &'a LogRecordData>,
    {
        // Internal struct to accumulate batch data before encoding
        struct BatchData {
            schemas: Vec<CentralSchemaEntry>,
            events: Vec<CentralEventEntry>,
            metadata: BatchMetadata,
        }

        impl BatchData {
            fn format_schema_ids(&self) -> String {
                use std::fmt::Write;

                if self.schemas.is_empty() {
                    return String::new();
                }

                let estimated_capacity =
                    self.schemas.len() * 32 + self.schemas.len().saturating_sub(1);

                self.schemas.iter().enumerate().fold(
                    String::with_capacity(estimated_capacity),
                    |mut acc, (i, s)| {
                        if i > 0 {
                            acc.push(';');
                        }
                        let md5_hash = md5::compute(s.id.to_le_bytes());
                        write!(&mut acc, "{md5_hash:x}").unwrap();
                        acc
                    },
                )
            }
        }

        let mut batches: HashMap<String, BatchData> = HashMap::new();
        let mut first_log_debug = true;

        for log_record in logs {
            // Determine timestamp - prefer time_unix_nano, fallback to observed_time_unix_nano
            let timestamp = log_record
                .time_unix_nano
                .or(log_record.observed_time_unix_nano)
                .unwrap_or(0);

            // Determine event name - use provided name or default to "Log"
            let event_name = log_record
                .event_name
                .as_deref()
                .filter(|s| !s.is_empty())
                .unwrap_or("Log");

            // Debug: Print first log record details
            if first_log_debug {
                eprintln!("🔍 DEBUG OTAP Encoder: First log record:");
                eprintln!("  time_unix_nano: {:?}", log_record.time_unix_nano);
                eprintln!("  observed_time_unix_nano: {:?}", log_record.observed_time_unix_nano);
                eprintln!("  severity_number: {:?}", log_record.severity_number);
                eprintln!("  severity_text: {:?}", log_record.severity_text);
                eprintln!("  body: {:?}", log_record.body);
                eprintln!("  event_name: {:?}", log_record.event_name);
                eprintln!("  attributes count: {}", log_record.attributes.len());
                eprintln!("  trace_id: {} bytes", log_record.trace_id.len());
                eprintln!("  span_id: {} bytes", log_record.span_id.len());
                first_log_debug = false;
            }

            // 1. Determine schema fields and calculate schema ID
            let (field_defs, schema_id) = self.determine_fields_and_schema_id(log_record, event_name);

            // Debug: Print field definitions for first log
            if batches.is_empty() {
                eprintln!("  Schema fields ({} total):", field_defs.len());
                for field_def in &field_defs {
                    eprintln!("    - {}: {:?}", field_def.name, field_def.type_id);
                }
            }

            // 2. Encode row data
            let row_buffer = self.write_row_data(log_record, &field_defs);

            // 3. Determine severity level (default to INFO if not provided)
            let level = log_record.severity_number.unwrap_or(9) as u8;

            // 4. Get or create batch entry
            let entry = batches.entry(event_name.to_string()).or_insert_with(|| {
                BatchData {
                    schemas: Vec::new(),
                    events: Vec::new(),
                    metadata: BatchMetadata {
                        start_time: timestamp,
                        end_time: timestamp,
                        schema_ids: String::new(),
                    },
                }
            });

            // 5. Update timestamp range
            if timestamp != 0 {
                entry.metadata.start_time = entry.metadata.start_time.min(timestamp);
                entry.metadata.end_time = entry.metadata.end_time.max(timestamp);
            }

            // 6. Add schema if not already present
            if !entry.schemas.iter().any(|s| s.id == schema_id) {
                let schema_entry = Self::create_schema(schema_id, field_defs);
                entry.schemas.push(schema_entry);
            }

            // 7. Create event entry
            let central_event = CentralEventEntry {
                schema_id,
                level,
                event_name: Arc::new(event_name.to_string()),
                row: row_buffer,
            };
            entry.events.push(central_event);
        }

        // 8. Encode and compress batches
        let mut result = Vec::with_capacity(batches.len());
        for (batch_event_name, mut batch_data) in batches {
            let schema_ids_string = batch_data.format_schema_ids();
            batch_data.metadata.schema_ids = schema_ids_string;

            let schemas_count = batch_data.schemas.len();
            let events_count = batch_data.events.len();

            let blob = CentralBlob {
                version: 1,
                format: 2,
                metadata: metadata.to_string(),
                schemas: batch_data.schemas,
                events: batch_data.events,
            };

            let uncompressed = blob.to_bytes();
            let compressed = lz4_chunked_compression(&uncompressed).map_err(|e| {
                debug!(
                    name: "otap_encoder.compress_error",
                    target: "geneva-uploader",
                    event_name = %batch_event_name,
                    error = %e,
                    "LZ4 compression failed"
                );
                format!("compression failed: {e}")
            })?;

            debug!(
                name: "otap_encoder.encode_batch",
                target: "geneva-uploader",
                event_name = %batch_event_name,
                schemas = schemas_count,
                events = events_count,
                uncompressed_size = uncompressed.len(),
                compressed_size = compressed.len(),
                "Encoded OTAP log batch"
            );

            result.push(EncodedBatch {
                event_name: batch_event_name,
                data: compressed,
                metadata: batch_data.metadata,
            });
        }

        Ok(result)
    }

    /// Determine schema fields and calculate schema ID in a single pass
    fn determine_fields_and_schema_id(
        &self,
        log: &LogRecordData,
        event_name: &str,
    ) -> (Vec<FieldDef>, u64) {
        // Pre-allocate with estimated capacity
        let estimated_capacity = 7 + log.attributes.len();
        let mut fields = Vec::with_capacity(estimated_capacity);

        // Initialize hasher for schema ID calculation
        let mut hasher = DefaultHasher::new();
        event_name.hash(&mut hasher);

        // Part A - Always present fields
        fields.push((Cow::Borrowed(FIELD_ENV_NAME), BondDataType::BT_STRING));
        fields.push((Cow::Borrowed(FIELD_ENV_VER), BondDataType::BT_STRING));
        fields.push((Cow::Borrowed(FIELD_TIMESTAMP), BondDataType::BT_STRING));
        fields.push((Cow::Borrowed(FIELD_ENV_TIME), BondDataType::BT_STRING));

        // Part A extension - Conditional fields based on trace context
        if !log.trace_id.is_empty() && log.trace_id.len() == 16 && !log.trace_id.iter().all(|&b| b == 0) {
            fields.push((Cow::Borrowed(FIELD_TRACE_ID), BondDataType::BT_STRING));
        }
        if !log.span_id.is_empty() && log.span_id.len() == 8 && !log.span_id.iter().all(|&b| b == 0) {
            fields.push((Cow::Borrowed(FIELD_SPAN_ID), BondDataType::BT_STRING));
        }
        if log.flags.is_some() && log.flags != Some(0) {
            fields.push((Cow::Borrowed(FIELD_TRACE_FLAGS), BondDataType::BT_UINT32));
        }

        // Part B - Core log fields
        if let Some(ref name) = log.event_name {
            if !name.is_empty() {
                fields.push((Cow::Borrowed(FIELD_EVENT_NAME), BondDataType::BT_STRING));
            }
        }
        if log.severity_number.is_some() {
            fields.push((Cow::Borrowed(FIELD_SEVERITY_NUMBER), BondDataType::BT_INT32));
        }
        if let Some(ref text) = log.severity_text {
            if !text.is_empty() {
                fields.push((Cow::Borrowed(FIELD_SEVERITY_TEXT), BondDataType::BT_STRING));
            }
        }
        if let Some(ref body) = log.body {
            let body_type = match body {
                LogBody::String(_) => BondDataType::BT_STRING,
                LogBody::Int(_) => BondDataType::BT_INT64,
                LogBody::Double(_) => BondDataType::BT_DOUBLE,
                LogBody::Bool(_) => BondDataType::BT_BOOL,
            };
            fields.push((Cow::Borrowed(FIELD_BODY), body_type));
        }

        // Part C - Dynamic attributes
        for attr in &log.attributes {
            fields.push((Cow::Owned(attr.key.clone()), attr.value.bond_type()));
        }

        // Convert to FieldDef and hash for schema ID
        let field_defs: Vec<FieldDef> = fields
            .into_iter()
            .enumerate()
            .map(|(i, (name, type_id))| {
                name.hash(&mut hasher);
                type_id.hash(&mut hasher);
                FieldDef {
                    name,
                    type_id,
                    field_id: (i + 1) as u16,
                }
            })
            .collect();

        let schema_id = hasher.finish();
        (field_defs, schema_id)
    }

    /// Create a schema entry from field definitions
    fn create_schema(schema_id: u64, field_info: Vec<FieldDef>) -> CentralSchemaEntry {
        let schema = BondEncodedSchema::from_fields("OtapLogRecord", "telemetry", field_info);
        let schema_bytes = schema.as_bytes();
        let schema_md5 = md5::compute(schema_bytes).0;

        CentralSchemaEntry {
            id: schema_id,
            md5: schema_md5,
            schema,
        }
    }

    /// Write row data in Bond format
    fn write_row_data(&self, log: &LogRecordData, fields: &[FieldDef]) -> Vec<u8> {
        let mut buffer = Vec::with_capacity(fields.len() * 50);

        // Pre-calculate timestamp
        let timestamp_nanos = log.time_unix_nano.or(log.observed_time_unix_nano).unwrap_or(0);
        let formatted_timestamp = Self::format_timestamp(timestamp_nanos);

        for field in fields {
            match field.name.as_ref() {
                FIELD_ENV_NAME => BondWriter::write_string(&mut buffer, &self.env_name),
                FIELD_ENV_VER => BondWriter::write_string(&mut buffer, &self.env_ver),
                FIELD_TIMESTAMP | FIELD_ENV_TIME => {
                    BondWriter::write_string(&mut buffer, &formatted_timestamp);
                }
                FIELD_TRACE_ID => {
                    if !log.trace_id.is_empty() {
                        let hex_str = hex::encode(&log.trace_id);
                        BondWriter::write_string(&mut buffer, &hex_str);
                    }
                }
                FIELD_SPAN_ID => {
                    if !log.span_id.is_empty() {
                        let hex_str = hex::encode(&log.span_id);
                        BondWriter::write_string(&mut buffer, &hex_str);
                    }
                }
                FIELD_TRACE_FLAGS => {
                    if let Some(flags) = log.flags {
                        BondWriter::write_numeric(&mut buffer, flags);
                    }
                }
                FIELD_EVENT_NAME => {
                    if let Some(ref name) = log.event_name {
                        BondWriter::write_string(&mut buffer, name);
                    }
                }
                FIELD_SEVERITY_NUMBER => {
                    if let Some(num) = log.severity_number {
                        BondWriter::write_numeric(&mut buffer, num);
                    }
                }
                FIELD_SEVERITY_TEXT => {
                    if let Some(ref text) = log.severity_text {
                        BondWriter::write_string(&mut buffer, text);
                    }
                }
                FIELD_BODY => {
                    if let Some(ref body) = log.body {
                        match body {
                            LogBody::String(s) => BondWriter::write_string(&mut buffer, s),
                            LogBody::Int(i) => BondWriter::write_numeric(&mut buffer, *i),
                            LogBody::Double(d) => BondWriter::write_numeric(&mut buffer, *d),
                            LogBody::Bool(b) => BondWriter::write_bool(&mut buffer, *b),
                        }
                    }
                }
                _ => {
                    // Handle dynamic attributes
                    if let Some(attr) = log.attributes.iter().find(|a| a.key == field.name) {
                        match &attr.value {
                            AttributeValue::String(s) => BondWriter::write_string(&mut buffer, s),
                            AttributeValue::Int(i) => BondWriter::write_numeric(&mut buffer, *i),
                            AttributeValue::Double(d) => BondWriter::write_numeric(&mut buffer, *d),
                            AttributeValue::Bool(b) => BondWriter::write_bool(&mut buffer, *b),
                        }
                    } else {
                        warn!(
                            name: "otap_encoder.missing_attribute",
                            target: "geneva-uploader",
                            field_name = %field.name,
                            "Field defined in schema but attribute not found in log record"
                        );
                    }
                }
            }
        }

        buffer
    }

    /// Format timestamp from nanoseconds to RFC3339
    fn format_timestamp(nanos: u64) -> String {
        let secs = (nanos / 1_000_000_000) as i64;
        let nsec = (nanos % 1_000_000_000) as u32;
        Utc.timestamp_opt(secs, nsec)
            .single()
            .unwrap_or_else(|| Utc.timestamp_opt(0, 0).single().unwrap())
            .to_rfc3339()
    }
}

impl Default for OtapLogEncoder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_encoding() {
        let encoder = OtapLogEncoder::new();

        let log = LogRecordData {
            time_unix_nano: Some(1_700_000_000_000_000_000),
            observed_time_unix_nano: None,
            severity_number: Some(9),
            severity_text: Some("INFO".to_string()),
            body: Some(LogBody::String("Test message".to_string())),
            attributes: vec![],
            trace_id: vec![],
            span_id: vec![],
            flags: None,
            event_name: Some("test_event".to_string()),
        };

        let metadata = "namespace=test/eventVersion=Ver1v0/tenant=T/role=R/roleinstance=RI";
        let result = encoder.encode_log_batch(&[log], metadata).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].event_name, "test_event");
        assert!(!result[0].data.is_empty());
    }

    #[test]
    fn test_empty_event_name_defaults_to_log() {
        let encoder = OtapLogEncoder::new();

        let log = LogRecordData {
            time_unix_nano: Some(1_700_000_000_000_000_000),
            observed_time_unix_nano: None,
            severity_number: Some(9),
            severity_text: None,
            body: None,
            attributes: vec![],
            trace_id: vec![],
            span_id: vec![],
            flags: None,
            event_name: None,
        };

        let result = encoder.encode_log_batch(&[log], "test").unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].event_name, "Log");
    }

    #[test]
    fn test_with_attributes() {
        let encoder = OtapLogEncoder::new();

        let log = LogRecordData {
            time_unix_nano: Some(1_700_000_000_000_000_000),
            observed_time_unix_nano: None,
            severity_number: Some(9),
            severity_text: Some("INFO".to_string()),
            body: Some(LogBody::String("User action".to_string())),
            attributes: vec![
                LogAttribute {
                    key: "user_id".to_string(),
                    value: AttributeValue::String("user123".to_string()),
                },
                LogAttribute {
                    key: "request_count".to_string(),
                    value: AttributeValue::Int(42),
                },
            ],
            trace_id: vec![],
            span_id: vec![],
            flags: None,
            event_name: Some("user_action".to_string()),
        };

        let result = encoder.encode_log_batch(&[log], "test").unwrap();

        assert_eq!(result.len(), 1);
        assert!(!result[0].data.is_empty());
    }

    #[test]
    fn test_with_trace_context() {
        let encoder = OtapLogEncoder::new();

        let log = LogRecordData {
            time_unix_nano: Some(1_700_000_000_000_000_000),
            observed_time_unix_nano: None,
            severity_number: Some(9),
            severity_text: Some("INFO".to_string()),
            body: Some(LogBody::String("Traced log".to_string())),
            attributes: vec![],
            trace_id: vec![1; 16],
            span_id: vec![2; 8],
            flags: Some(1),
            event_name: Some("traced_event".to_string()),
        };

        let result = encoder.encode_log_batch(&[log], "test").unwrap();

        assert_eq!(result.len(), 1);
        assert!(!result[0].data.is_empty());
    }

    #[test]
    fn test_multiple_event_names() {
        let encoder = OtapLogEncoder::new();

        let logs = vec![
            LogRecordData {
                time_unix_nano: Some(1_700_000_000_000_000_000),
                observed_time_unix_nano: None,
                severity_number: Some(9),
                severity_text: None,
                body: None,
                attributes: vec![],
                trace_id: vec![],
                span_id: vec![],
                flags: None,
                event_name: Some("login".to_string()),
            },
            LogRecordData {
                time_unix_nano: Some(1_700_000_001_000_000_000),
                observed_time_unix_nano: None,
                severity_number: Some(9),
                severity_text: None,
                body: None,
                attributes: vec![],
                trace_id: vec![],
                span_id: vec![],
                flags: None,
                event_name: Some("logout".to_string()),
            },
        ];

        let result = encoder.encode_log_batch(&logs, "test").unwrap();

        assert_eq!(result.len(), 2);
        let event_names: Vec<&String> = result.iter().map(|b| &b.event_name).collect();
        assert!(event_names.contains(&&"login".to_string()));
        assert!(event_names.contains(&&"logout".to_string()));
    }

    #[test]
    fn test_multiple_schemas_same_event() {
        let encoder = OtapLogEncoder::new();

        let logs = vec![
            LogRecordData {
                time_unix_nano: Some(1_700_000_000_000_000_000),
                observed_time_unix_nano: None,
                severity_number: Some(9),
                severity_text: None,
                body: None,
                attributes: vec![],
                trace_id: vec![],
                span_id: vec![],
                flags: None,
                event_name: Some("user_action".to_string()),
            },
            LogRecordData {
                time_unix_nano: Some(1_700_000_001_000_000_000),
                observed_time_unix_nano: None,
                severity_number: Some(9),
                severity_text: None,
                body: None,
                attributes: vec![],
                trace_id: vec![1; 16],
                span_id: vec![],
                flags: None,
                event_name: Some("user_action".to_string()),
            },
            LogRecordData {
                time_unix_nano: Some(1_700_000_002_000_000_000),
                observed_time_unix_nano: None,
                severity_number: Some(9),
                severity_text: None,
                body: None,
                attributes: vec![LogAttribute {
                    key: "user_id".to_string(),
                    value: AttributeValue::String("user123".to_string()),
                }],
                trace_id: vec![],
                span_id: vec![],
                flags: None,
                event_name: Some("user_action".to_string()),
            },
        ];

        let result = encoder.encode_log_batch(&logs, "test").unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].event_name, "user_action");
        // Should have 3 schema IDs (2 semicolons)
        assert_eq!(result[0].metadata.schema_ids.matches(';').count(), 2);
    }

    #[test]
    fn test_missing_timestamp_handling() {
        let encoder = OtapLogEncoder::new();

        let log = LogRecordData {
            time_unix_nano: None,
            observed_time_unix_nano: None,
            severity_number: Some(9),
            severity_text: None,
            body: Some(LogBody::String("No timestamp".to_string())),
            attributes: vec![],
            trace_id: vec![],
            span_id: vec![],
            flags: None,
            event_name: Some("test_event".to_string()),
        };

        let result = encoder.encode_log_batch(&[log], "test").unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].metadata.start_time, 0);
        assert_eq!(result[0].metadata.end_time, 0);
    }

    #[test]
    fn test_timestamp_range_tracking() {
        let encoder = OtapLogEncoder::new();

        let logs = vec![
            LogRecordData {
                time_unix_nano: Some(1_700_000_002_000_000_000),
                observed_time_unix_nano: None,
                severity_number: Some(9),
                severity_text: None,
                body: None,
                attributes: vec![],
                trace_id: vec![],
                span_id: vec![],
                flags: None,
                event_name: Some("test".to_string()),
            },
            LogRecordData {
                time_unix_nano: Some(1_700_000_000_000_000_000),
                observed_time_unix_nano: None,
                severity_number: Some(9),
                severity_text: None,
                body: None,
                attributes: vec![],
                trace_id: vec![],
                span_id: vec![],
                flags: None,
                event_name: Some("test".to_string()),
            },
            LogRecordData {
                time_unix_nano: Some(1_700_000_003_000_000_000),
                observed_time_unix_nano: None,
                severity_number: Some(9),
                severity_text: None,
                body: None,
                attributes: vec![],
                trace_id: vec![],
                span_id: vec![],
                flags: None,
                event_name: Some("test".to_string()),
            },
        ];

        let result = encoder.encode_log_batch(&logs, "test").unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].metadata.start_time, 1_700_000_000_000_000_000);
        assert_eq!(result[0].metadata.end_time, 1_700_000_003_000_000_000);
    }
}
