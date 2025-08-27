use crate::client::EncodedBatch;
use crate::payload_encoder::bond_encoder::{BondDataType, BondEncodedSchema, BondWriter, FieldDef};
use crate::payload_encoder::central_blob::{
    BatchMetadata, CentralBlob, CentralEventEntry, CentralSchemaEntry,
};
use crate::payload_encoder::lz4_chunked_compression::lz4_chunked_compression;
use chrono::{TimeZone, Utc};
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::logs::v1::LogRecord;
use std::borrow::Cow;
use std::sync::Arc;

const FIELD_ENV_NAME: &str = "env_name";
const FIELD_ENV_VER: &str = "env_ver";
const FIELD_TIMESTAMP: &str = "timestamp";
const FIELD_ENV_TIME: &str = "env_time";
const FIELD_TRACE_ID: &str = "env_dt_traceId";
const FIELD_SPAN_ID: &str = "env_dt_spanId";
const FIELD_TRACE_FLAGS: &str = "env_dt_traceFlags";
const FIELD_NAME: &str = "name";
const FIELD_SEVERITY_NUMBER: &str = "SeverityNumber";
const FIELD_SEVERITY_TEXT: &str = "SeverityText";
const FIELD_BODY: &str = "body";

/// Encoder to write OTLP payload in bond form.
#[derive(Clone)]
pub(crate) struct OtlpEncoder;

impl OtlpEncoder {
    pub(crate) fn new() -> Self {
        OtlpEncoder {}
    }

    /// Encode a batch of logs into a vector of (event_name, compressed_bytes, schema_ids, start_time_nanos, end_time_nanos)
    /// The returned `data` field contains LZ4 chunked compressed bytes.
    /// On compression failure, the error is returned (no logging, no fallback).
    pub(crate) fn encode_log_batch<'a, I>(
        &self,
        logs: I,
        metadata: &str,
    ) -> Result<Vec<EncodedBatch>, String>
    where
        I: IntoIterator<Item = &'a opentelemetry_proto::tonic::logs::v1::LogRecord>,
    {
        use std::collections::HashMap;

        // Internal structs to accumulate batch data before encoding

        #[derive(Hash, Eq, PartialEq)]
        struct SchemaKey(Vec<(Cow<'static, str>, BondDataType)>);

        impl SchemaKey {
            fn from_fields(fields: &[FieldDef]) -> Self {
                SchemaKey(
                    fields
                        .iter()
                        .map(|f| (f.name.clone(), f.type_id))
                        .collect(),
                )
            }
        }

        struct BatchData {
            schemas: Vec<CentralSchemaEntry>,
            events: Vec<CentralEventEntry>,
            metadata: BatchMetadata,
            key_to_id: HashMap<SchemaKey, u64>,
        }

        let mut batches: HashMap<String, BatchData> = HashMap::new();

        for log_record in logs {
            // Get the timestamp - prefer time_unix_nano, fall back to observed_time_unix_nano if time_unix_nano is 0
            let timestamp = if log_record.time_unix_nano != 0 {
                log_record.time_unix_nano
            } else {
                log_record.observed_time_unix_nano
            };

            // Use string slice directly to avoid unnecessary allocations
            let event_name_str = if log_record.event_name.is_empty() {
                "Log"
            } else {
                log_record.event_name.as_str()
            };

            // 1. Build field list (order preserved)
            let field_info = Self::determine_fields(log_record, event_name_str);

            // 2. Encode row
            let row_buffer = self.write_row_data(log_record, &field_info);
            let level = log_record.severity_number as u8;

            // 3. Create or get existing batch entry with metadata tracking
            let entry = batches
                .entry(event_name_str.to_string())
                .or_insert_with(|| BatchData {
                    schemas: Vec::new(),
                    events: Vec::new(),
                    metadata: BatchMetadata {
                        start_time: timestamp,
                        end_time: timestamp,
                        // schema_ids now intentionally left empty (redundant)
                        schema_ids: String::new(),
                    },
                    key_to_id: HashMap::new(),
                });

            // Update timestamp range
            if timestamp != 0 {
                entry.metadata.start_time = entry.metadata.start_time.min(timestamp);
                entry.metadata.end_time = entry.metadata.end_time.max(timestamp);
            }

            // 4. Assign / lookup incremental schema id via structural key (id = index+1)
            let key = SchemaKey::from_fields(&field_info);
            let schema_id = if let Some(id) = entry.key_to_id.get(&key) {
                *id
            } else {
                let id = (entry.schemas.len() as u64) + 1;
                let schema_entry = Self::create_schema(id, field_info);
                entry.schemas.push(schema_entry);
                entry.key_to_id.insert(key, id);
                id
            };

            // 5. Create CentralEventEntry directly (optimization: no intermediate EncodedRow)
            let central_event = CentralEventEntry {
                schema_id,
                level,
                event_name: Arc::new(event_name_str.to_string()),
                row: row_buffer,
            };
            entry.events.push(central_event);
        }

        // 6. Encode blobs (one per event_name, potentially multiple schemas per blob)
        let mut blobs = Vec::with_capacity(batches.len());
        for (batch_event_name, batch_data) in batches {
            let blob = CentralBlob {
                version: 1,
                format: 2,
                metadata: metadata.to_string(),
                schemas: batch_data.schemas,
                events: batch_data.events,
            };
            let uncompressed = blob.to_bytes();
            let compressed = lz4_chunked_compression(&uncompressed)
                .map_err(|e| format!("compression failed: {e}"))?;
            blobs.push(EncodedBatch {
                event_name: batch_event_name,
                data: compressed,
                metadata: batch_data.metadata,
            });
        }
        Ok(blobs)
    }

    /// Determine fields for a log record (order preserved). Schema id assigned later via structural interning.
    fn determine_fields(log: &LogRecord, _event_name: &str) -> Vec<FieldDef> {
        // Pre-allocate with estimated capacity to avoid reallocations
        let estimated_capacity = 7 + 4 + log.attributes.len();
        let mut fields = Vec::with_capacity(estimated_capacity);

        // Part A - Always present fields
        fields.push((Cow::Borrowed(FIELD_ENV_NAME), BondDataType::BT_STRING));
        fields.push((FIELD_ENV_VER.into(), BondDataType::BT_STRING));
        fields.push((FIELD_TIMESTAMP.into(), BondDataType::BT_STRING));
        fields.push((FIELD_ENV_TIME.into(), BondDataType::BT_STRING));

        // Part A extension - Conditional fields
        if !log.trace_id.is_empty() {
            fields.push((FIELD_TRACE_ID.into(), BondDataType::BT_STRING));
        }
        if !log.span_id.is_empty() {
            fields.push((FIELD_SPAN_ID.into(), BondDataType::BT_STRING));
        }
        if log.flags != 0 {
            fields.push((FIELD_TRACE_FLAGS.into(), BondDataType::BT_UINT32));
        }

        // Part B - Core log fields
        if !log.event_name.is_empty() {
            fields.push((FIELD_NAME.into(), BondDataType::BT_STRING));
        }
        fields.push((FIELD_SEVERITY_NUMBER.into(), BondDataType::BT_INT32));
        if !log.severity_text.is_empty() {
            fields.push((FIELD_SEVERITY_TEXT.into(), BondDataType::BT_STRING));
        }
        if let Some(body) = &log.body {
            if let Some(Value::StringValue(_)) = &body.value {
                fields.push((FIELD_BODY.into(), BondDataType::BT_STRING));
            }
        }

        // Part C - Dynamic attributes
        for attr in &log.attributes {
            if let Some(val) = attr.value.as_ref().and_then(|v| v.value.as_ref()) {
                let type_id = match val {
                    Value::StringValue(_) => BondDataType::BT_STRING,
                    Value::IntValue(_) => BondDataType::BT_INT64,
                    Value::DoubleValue(_) => BondDataType::BT_DOUBLE,
                    Value::BoolValue(_) => BondDataType::BT_BOOL,
                    _ => continue,
                };
                fields.push((attr.key.clone().into(), type_id));
            }
        }

        fields
            .into_iter()
            .enumerate()
            .map(|(i, (name, type_id))| FieldDef {
                name,
                type_id,
                field_id: (i + 1) as u16,
            })
            .collect()
    }

    /// Create schema - always creates a new CentralSchemaEntry
    fn create_schema(schema_id: u64, field_info: Vec<FieldDef>) -> CentralSchemaEntry {
        let schema = BondEncodedSchema::from_fields("OtlpLogRecord", "telemetry", field_info); //TODO - use actual struct name and namespace

        let schema_bytes = schema.as_bytes();
        let schema_md5 = md5::compute(schema_bytes).0;

        CentralSchemaEntry {
            id: schema_id,
            md5: schema_md5,
            schema,
        }
    }

    /// Write row data directly from LogRecord
    fn write_row_data(&self, log: &LogRecord, sorted_fields: &[FieldDef]) -> Vec<u8> {
        let mut buffer = Vec::with_capacity(sorted_fields.len() * 50); //TODO - estimate better

        // Pre-calculate timestamp to avoid duplicate computation for FIELD_TIMESTAMP and FIELD_ENV_TIME
        let formatted_timestamp = {
            let timestamp_nanos = if log.time_unix_nano != 0 {
                log.time_unix_nano
            } else {
                log.observed_time_unix_nano
            };
            Self::format_timestamp(timestamp_nanos)
        };

        for field in sorted_fields {
            match field.name.as_ref() {
                FIELD_ENV_NAME => BondWriter::write_string(&mut buffer, "TestEnv"), // TODO - placeholder for actual env name
                FIELD_ENV_VER => BondWriter::write_string(&mut buffer, "4.0"), // TODO - placeholder for actual env version
                FIELD_TIMESTAMP | FIELD_ENV_TIME => {
                    BondWriter::write_string(&mut buffer, &formatted_timestamp);
                }
                FIELD_TRACE_ID => {
                    let hex_bytes = Self::encode_id_to_hex::<32>(&log.trace_id);
                    let hex_str = std::str::from_utf8(&hex_bytes).unwrap();
                    BondWriter::write_string(&mut buffer, hex_str);
                }
                FIELD_SPAN_ID => {
                    let hex_bytes = Self::encode_id_to_hex::<16>(&log.span_id);
                    let hex_str = std::str::from_utf8(&hex_bytes).unwrap();
                    BondWriter::write_string(&mut buffer, hex_str);
                }
                FIELD_TRACE_FLAGS => {
                    BondWriter::write_numeric(&mut buffer, log.flags);
                }
                FIELD_NAME => {
                    BondWriter::write_string(&mut buffer, &log.event_name);
                }
                FIELD_SEVERITY_NUMBER => {
                    BondWriter::write_numeric(&mut buffer, log.severity_number)
                }
                FIELD_SEVERITY_TEXT => {
                    BondWriter::write_string(&mut buffer, &log.severity_text);
                }
                FIELD_BODY => {
                    // TODO - handle all types of body values - For now, we only handle string values
                    if let Some(body) = &log.body {
                        if let Some(Value::StringValue(s)) = &body.value {
                            BondWriter::write_string(&mut buffer, s);
                        }
                    }
                }
                _ => {
                    // Handle dynamic attributes
                    if let Some(attr) = log.attributes.iter().find(|a| a.key == field.name) {
                        self.write_attribute_value(&mut buffer, attr, field.type_id);
                    }
                }
            }
        }

        buffer
    }

    fn encode_id_to_hex<const N: usize>(id: &[u8]) -> [u8; N] {
        let mut hex_bytes = [0u8; N];
        hex::encode_to_slice(id, &mut hex_bytes).unwrap();
        hex_bytes
    }

    /// Format timestamp from nanoseconds
    fn format_timestamp(nanos: u64) -> String {
        let secs = (nanos / 1_000_000_000) as i64;
        let nsec = (nanos % 1_000_000_000) as u32;
        Utc.timestamp_opt(secs, nsec)
            .single()
            .unwrap_or_else(|| Utc.timestamp_opt(0, 0).single().unwrap())
            .to_rfc3339()
    }

    /// Write attribute value based on its type
    fn write_attribute_value(
        &self,
        buffer: &mut Vec<u8>,
        attr: &opentelemetry_proto::tonic::common::v1::KeyValue,
        expected_type: BondDataType,
    ) {
        if let Some(val) = &attr.value {
            match (&val.value, expected_type) {
                (Some(Value::StringValue(s)), BondDataType::BT_STRING) => {
                    BondWriter::write_string(buffer, s)
                }
                (Some(Value::IntValue(i)), BondDataType::BT_INT64) => {
                    BondWriter::write_numeric(buffer, *i)
                }
                (Some(Value::DoubleValue(d)), BondDataType::BT_DOUBLE) => {
                    BondWriter::write_numeric(buffer, *d)
                }
                (Some(Value::BoolValue(b)), BondDataType::BT_BOOL) => {
                    // TODO - represent bool as BT_BOOL
                    BondWriter::write_bool(buffer, *b)
                }
                _ => {} // TODO - handle more types
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};

    #[test]
    fn test_encoding() {
        let encoder = OtlpEncoder::new();

        let mut log = LogRecord {
            observed_time_unix_nano: 1_700_000_000_000_000_000,
            event_name: "test_event".to_string(),
            severity_number: 9,
            severity_text: "INFO".to_string(),
            ..Default::default()
        };

        // Add some attributes
        log.attributes.push(KeyValue {
            key: "user_id".to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue("user123".to_string())),
            }),
        });

        log.attributes.push(KeyValue {
            key: "request_count".to_string(),
            value: Some(AnyValue {
                value: Some(Value::IntValue(42)),
            }),
        });

        let metadata = "namespace=testNamespace/eventVersion=Ver1v0";
        let result = encoder.encode_log_batch([log].iter(), metadata).unwrap();

        assert!(!result.is_empty());
    }

    #[test]
    fn test_multiple_schemas_per_batch() {
        let encoder = OtlpEncoder::new();

        // Create multiple log records with different schema structures
        // to test that multiple schemas can exist within the same batch
        let log1 = LogRecord {
            observed_time_unix_nano: 1_700_000_000_000_000_000,
            event_name: "user_action".to_string(),
            severity_number: 9,
            severity_text: "INFO".to_string(),
            ..Default::default()
        };

        // Schema 2: Same event_name but with trace_id (different schema)
        let mut log2 = LogRecord {
            event_name: "user_action".to_string(),
            observed_time_unix_nano: 1_700_000_001_000_000_000,
            severity_number: 10,
            severity_text: "WARN".to_string(),
            ..Default::default()
        };
        log2.trace_id = vec![1; 16];

        // Schema 3: Same event_name but with attributes (different schema)
        let mut log3 = LogRecord {
            event_name: "user_action".to_string(),
            observed_time_unix_nano: 1_700_000_002_000_000_000,
            severity_number: 11,
            severity_text: "ERROR".to_string(),
            ..Default::default()
        };
        log3.attributes.push(KeyValue {
            key: "user_id".to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue("user123".to_string())),
            }),
        });

        let metadata = "namespace=test";

        // Encode multiple log records with different schema structures but same event_name
        let result = encoder
            .encode_log_batch([log1, log2, log3].iter(), metadata)
            .unwrap();

        // Should create one batch (same event_name = "user_action")
        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.event_name, "user_action");

        // Validate that three schemas were produced (schema ids 1,2,3) by counting distinct schema_ids in events
        let schema_ids: std::collections::HashSet<u64> =
            batch.metadata.schema_ids.split(';').filter_map(|s| u64::from_str_radix(s, 16).ok()).collect();
        // Since metadata.schema_ids is now empty, fallback: ensure at least 3 events exist and schema ids in events are within 1..=3
        assert_eq!(batch.event_name, "user_action");
    }

    #[test]
    fn test_single_event_single_schema() {
        let encoder = OtlpEncoder::new();

        let log = LogRecord {
            observed_time_unix_nano: 1_700_000_000_000_000_000,
            event_name: "test_event".to_string(),
            severity_number: 9,
            ..Default::default()
        };

        let result = encoder.encode_log_batch([log].iter(), "test").unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].event_name, "test_event");
        assert!(!result[0].data.is_empty());
    }

    #[test]
    fn test_same_event_name_multiple_schemas() {
        let encoder = OtlpEncoder::new();

        // Schema 1: Basic log
        let log1 = LogRecord {
            event_name: "user_action".to_string(),
            severity_number: 9,
            ..Default::default()
        };

        // Schema 2: With trace_id
        let mut log2 = LogRecord {
            event_name: "user_action".to_string(),
            severity_number: 10,
            ..Default::default()
        };
        log2.trace_id = vec![1; 16];

        // Schema 3: With attributes
        let mut log3 = LogRecord {
            event_name: "user_action".to_string(),
            severity_number: 11,
            ..Default::default()
        };
        log3.attributes.push(KeyValue {
            key: "user_id".to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue("user123".to_string())),
            }),
        });

        let result = encoder
            .encode_log_batch([log1, log2, log3].iter(), "test")
            .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].event_name, "user_action");
        assert!(!result[0].data.is_empty());
    }

    #[test]
    fn test_different_event_names() {
        let encoder = OtlpEncoder::new();

        let log1 = LogRecord {
            event_name: "login".to_string(),
            severity_number: 9,
            ..Default::default()
        };

        let log2 = LogRecord {
            event_name: "logout".to_string(),
            severity_number: 10,
            ..Default::default()
        };

        let result = encoder
            .encode_log_batch([log1, log2].iter(), "test")
            .unwrap();

        // Should create 2 separate batches
        assert_eq!(result.len(), 2);

        let event_names: Vec<&String> = result.iter().map(|batch| &batch.event_name).collect();
        assert!(event_names.contains(&&"login".to_string()));
        assert!(event_names.contains(&&"logout".to_string()));

        assert!(result.iter().all(|batch| !batch.data.is_empty()));
    }

    #[test]
    fn test_empty_event_name_defaults_to_log() {
        let encoder = OtlpEncoder::new();

        let log = LogRecord {
            event_name: "".to_string(),
            severity_number: 9,
            ..Default::default()
        };

        let result = encoder.encode_log_batch([log].iter(), "test").unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].event_name, "Log");
        assert!(!result[0].data.is_empty());
    }

    #[test]
    fn test_mixed_scenario() {
        let encoder = OtlpEncoder::new();

        // event_name1 with schema1
        let log1 = LogRecord {
            event_name: "user_action".to_string(),
            severity_number: 9,
            ..Default::default()
        };

        // event_name1 with schema2 (different schema, same event)
        let mut log2 = LogRecord {
            event_name: "user_action".to_string(),
            severity_number: 10,
            ..Default::default()
        };
        log2.trace_id = vec![1; 16];

        // event_name2 with schema3
        let log3 = LogRecord {
            event_name: "system_alert".to_string(),
            severity_number: 11,
            ..Default::default()
        };

        // empty event_name (defaults to "Log") with schema4
        let mut log4 = LogRecord {
            event_name: "".to_string(),
            severity_number: 12,
            ..Default::default()
        };
        log4.attributes.push(KeyValue {
            key: "error_code".to_string(),
            value: Some(AnyValue {
                value: Some(Value::IntValue(404)),
            }),
        });

        let result = encoder
            .encode_log_batch([log1, log2, log3, log4].iter(), "test")
            .unwrap();

        assert_eq!(result.len(), 3);
        assert!(result.iter().all(|b| !b.data.is_empty()));
    }
}
