use crate::payload_encoder::bond_encoder::{BondDataType, BondEncodedSchema, BondWriter, FieldDef};
use crate::payload_encoder::central_blob::{CentralBlob, CentralEventEntry, CentralSchemaEntry};
use chrono::{TimeZone, Utc};
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::logs::v1::LogRecord;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

type SchemaCache = Arc<RwLock<HashMap<u64, Arc<CentralSchemaEntry>>>>;

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

/// Event group for flattened structure - contains events for a single event_name
struct EventGroup {
    events: Vec<CentralEventEntry>,
}

/// Encoder to write OTLP payload in bond form.
#[derive(Clone)]
pub struct OtlpEncoder {
    // TODO - limit cache size or use LRU eviction, and/or add feature flag for caching
    schema_cache: SchemaCache,
}

impl OtlpEncoder {
    pub fn new() -> Self {
        OtlpEncoder {
            schema_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn encode_log_batch<'a, I>(
        &self,
        logs: I,
        metadata: &str,
    ) -> Vec<(u64, String, Vec<u8>, usize)>
    where
        I: Iterator<Item = &'a opentelemetry_proto::tonic::logs::v1::LogRecord>,
    {
        // Group events by event_name
        let mut event_groups: HashMap<String, EventGroup> = HashMap::new();

        for log_record in logs {
            // 1. Get schema and encode row
            let (schema_id, row_buffer) = self.process_log_record(log_record);

            let event_name = if log_record.event_name.is_empty() {
                "Log".to_string()
            } else {
                log_record.event_name.clone()
            };

            // 2. Add to appropriate event group
            let group = event_groups
                .entry(event_name.clone())
                .or_insert_with(|| EventGroup { events: Vec::new() });

            // Add event
            group.events.push(CentralEventEntry {
                schema_id,
                level: log_record.severity_number as u8,
                event_name,
                row: row_buffer,
            });
        }

        // 3. Create one blob per event_name
        event_groups
            .into_iter()
            .map(|(event_name, group)| {
                let events_len = group.events.len();

                // Collect unique schemas for this event group
                let unique_schema_ids: Vec<u64> = {
                    let mut ids: Vec<_> = group.events.iter().map(|e| e.schema_id).collect();
                    ids.sort_unstable();
                    ids.dedup();
                    ids
                };

                // Get schemas from cache (already Arc'd, so cheap to clone)
                let schemas: Vec<CentralSchemaEntry> = {
                    let cache = self.schema_cache.read().unwrap();
                    unique_schema_ids
                        .iter()
                        .filter_map(|&id| cache.get(&id).map(|arc| (**arc).clone()))
                        .collect()
                };

                let blob = CentralBlob {
                    version: 1,
                    format: 2,
                    metadata: metadata.to_string(),
                    schemas,
                    events: group.events,
                };

                (0, event_name, blob.to_bytes(), events_len)
            })
            .collect()
    }

    /// Process a log record - determine schema and encode row data
    fn process_log_record(&self, log: &LogRecord) -> (u64, Vec<u8>) {
        // Pre-calculate all field information once
        let fields = self.build_field_definitions(log);
        let schema_id = Self::calculate_schema_id(&fields);

        // Get or create schema (using Arc for cheap cloning)
        self.ensure_schema_cached(schema_id, &fields);

        // Encode row data
        let row_buffer = self.encode_row_data(log, &fields);

        (schema_id, row_buffer)
    }

    /// Build field definitions for a log record
    fn build_field_definitions(&self, log: &LogRecord) -> Vec<FieldDef> {
        // Pre-allocate with estimated capacity to avoid reallocations
        let estimated_capacity = 7 + 4 + log.attributes.len();
        let mut fields = Vec::with_capacity(estimated_capacity);
        let mut field_id = 1u16;
        // Helper to add field
        let mut add_field = |name: &'static str, type_id: u8| {
            fields.push(FieldDef {
                name: Cow::Borrowed(name),
                type_id,
                field_id,
            });
            field_id += 1;
        };

        add_field(FIELD_ENV_NAME, BondDataType::BT_STRING as u8);
        add_field(FIELD_ENV_VER, BondDataType::BT_STRING as u8);
        add_field(FIELD_TIMESTAMP, BondDataType::BT_STRING as u8);
        add_field(FIELD_ENV_TIME, BondDataType::BT_STRING as u8);

        // Part A extension - Conditional fields
        if !log.trace_id.is_empty() {
            add_field(FIELD_TRACE_ID, BondDataType::BT_STRING as u8);
        }
        if !log.span_id.is_empty() {
            add_field(FIELD_SPAN_ID, BondDataType::BT_STRING as u8);
        }
        if log.flags != 0 {
            add_field(FIELD_TRACE_FLAGS, BondDataType::BT_INT32 as u8);
        }

        // Part B - Core log fields
        if !log.event_name.is_empty() {
            add_field(FIELD_NAME, BondDataType::BT_STRING as u8);
        }
        add_field(FIELD_SEVERITY_NUMBER, BondDataType::BT_INT32 as u8);
        if !log.severity_text.is_empty() {
            add_field(FIELD_SEVERITY_TEXT, BondDataType::BT_STRING as u8);
        }
        if let Some(body) = &log.body {
            if let Some(Value::StringValue(_)) = &body.value {
                // Only included in schema when body is a string value
                add_field(FIELD_BODY, BondDataType::BT_STRING as u8);
            }
            //TODO - handle other body types
        }

        // Part C - Dynamic attributes
        // Dynamic attributes - collect and sort
        let mut attr_fields: Vec<_> = log
            .attributes
            .iter()
            .filter_map(|attr| {
                attr.value.as_ref()?.value.as_ref().map(|val| {
                    let type_id = match val {
                        Value::StringValue(_) => BondDataType::BT_STRING as u8,
                        Value::IntValue(_) => BondDataType::BT_INT32 as u8,
                        Value::DoubleValue(_) => BondDataType::BT_FLOAT as u8, // TODO - using float for now
                        Value::BoolValue(_) => BondDataType::BT_INT32 as u8, // representing bool as int
                        _ => return None,
                    };
                    Some((attr.key.clone(), type_id))
                })
            })
            .flatten()
            .collect();
        attr_fields.sort_by(|a, b| a.0.cmp(&b.0));

        // Add sorted attributes
        for (name, type_id) in attr_fields {
            fields.push(FieldDef {
                name: Cow::Owned(name),
                type_id,
                field_id,
            });
            field_id += 1;
        }

        fields
    }

    /// Calculate schema ID from field specifications
    fn calculate_schema_id(fields: &[FieldDef]) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();

        for field in fields {
            field.name.hash(&mut hasher);
            field.type_id.hash(&mut hasher);
        }

        hasher.finish()
    }

    /// Ensure schema is in cache
    fn ensure_schema_cached(&self, schema_id: u64, fields: &[FieldDef]) {
        let cache = self.schema_cache.read().unwrap();
        if cache.contains_key(&schema_id) {
            return;
        }
        drop(cache);

        // Create schema
        let schema = BondEncodedSchema::from_fields("OtlpLogRecord", "telemetry", fields.to_vec());

        let schema_bytes = schema.as_bytes();
        let schema_md5 = md5::compute(schema_bytes).0;

        let schema_entry = Arc::new(CentralSchemaEntry {
            id: schema_id,
            md5: schema_md5,
            schema,
        });

        self.schema_cache
            .write()
            .unwrap()
            .insert(schema_id, schema_entry);
    }

    /// Encode row data directly from LogRecord and field definitions
    fn encode_row_data(&self, log: &LogRecord, sorted_fields: &[FieldDef]) -> Vec<u8> {
        let mut buffer = Vec::with_capacity(sorted_fields.len() * 50); //TODO - estimate better

        for field in sorted_fields {
            match field.name.as_ref() {
                FIELD_ENV_NAME => BondWriter::write_string(&mut buffer, "TestEnv"), // TODO - placeholder for actual env name
                FIELD_ENV_VER => BondWriter::write_string(&mut buffer, "4.0"), // TODO - placeholder for actual env version
                FIELD_TIMESTAMP | FIELD_ENV_TIME => {
                    let dt = Self::format_timestamp(log.observed_time_unix_nano);
                    BondWriter::write_string(&mut buffer, &dt);
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
                    BondWriter::write_int32(&mut buffer, log.flags as i32);
                }
                FIELD_NAME => {
                    BondWriter::write_string(&mut buffer, &log.event_name);
                }
                FIELD_SEVERITY_NUMBER => BondWriter::write_int32(&mut buffer, log.severity_number),
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
                    // TODO - optimize better - we could update determine_fields to also return a vec of bytes which has bond serialized attributes
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
        expected_type: u8,
    ) {
        const BT_STRING: u8 = BondDataType::BT_STRING as u8;
        const BT_FLOAT: u8 = BondDataType::BT_FLOAT as u8;
        const BT_DOUBLE: u8 = BondDataType::BT_DOUBLE as u8;
        const BT_INT32: u8 = BondDataType::BT_INT32 as u8;
        const BT_WSTRING: u8 = BondDataType::BT_WSTRING as u8;

        if let Some(val) = &attr.value {
            match (&val.value, expected_type) {
                (Some(Value::StringValue(s)), BT_STRING) => BondWriter::write_string(buffer, s),
                (Some(Value::StringValue(s)), BT_WSTRING) => BondWriter::write_wstring(buffer, s),
                (Some(Value::IntValue(i)), BT_INT32) => {
                    // TODO - handle i64 properly, for now we cast to i32
                    BondWriter::write_int32(buffer, *i as i32)
                }
                (Some(Value::DoubleValue(d)), BT_FLOAT) => {
                    // TODO - handle double values properly
                    BondWriter::write_float(buffer, *d as f32)
                }
                (Some(Value::DoubleValue(d)), BT_DOUBLE) => BondWriter::write_double(buffer, *d),
                (Some(Value::BoolValue(b)), BT_INT32) => {
                    // TODO - represent bool as BT_BOOL
                    BondWriter::write_bool_as_int32(buffer, *b)
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
        let result = encoder.encode_log_batch([log].iter(), metadata);

        assert!(!result.is_empty());
    }

    #[test]
    fn test_schema_caching() {
        let encoder = OtlpEncoder::new();

        let log1 = LogRecord {
            observed_time_unix_nano: 1_700_000_000_000_000_000,
            severity_number: 9,
            ..Default::default()
        };

        let mut log2 = LogRecord {
            observed_time_unix_nano: 1_700_000_001_000_000_000,
            severity_number: 10,
            ..Default::default()
        };

        let metadata = "namespace=test";

        // First encoding creates schema
        let _result1 = encoder.encode_log_batch([log1].iter(), metadata);
        assert_eq!(encoder.schema_cache.read().unwrap().len(), 1);

        // Second encoding with same schema structure reuses schema
        let _result2 = encoder.encode_log_batch([log2.clone()].iter(), metadata);
        assert_eq!(encoder.schema_cache.read().unwrap().len(), 1);

        // Add trace_id to create different schema
        log2.trace_id = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let _result3 = encoder.encode_log_batch([log2].iter(), metadata);
        assert_eq!(encoder.schema_cache.read().unwrap().len(), 2);
    }

    #[test]
    fn test_group_by_event_name() {
        let encoder = OtlpEncoder::new();

        let mut log1 = LogRecord {
            observed_time_unix_nano: 1_700_000_000_000_000_000,
            event_name: "login".to_string(),
            severity_number: 9,
            ..Default::default()
        };
        log1.attributes.push(KeyValue {
            key: "user".to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue("alice".to_string())),
            }),
        });

        let mut log2 = LogRecord {
            observed_time_unix_nano: 1_700_000_001_000_000_000,
            event_name: "logout".to_string(),
            severity_number: 9,
            ..Default::default()
        };
        log2.attributes.push(KeyValue {
            key: "user".to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue("bob".to_string())),
            }),
        });
        log2.attributes.push(KeyValue {
            key: "duration".to_string(),
            value: Some(AnyValue {
                value: Some(Value::IntValue(3600)),
            }),
        });

        // Add another login event with different schema
        let mut log3 = LogRecord {
            observed_time_unix_nano: 1_700_000_002_000_000_000,
            event_name: "login".to_string(),
            severity_number: 10,
            ..Default::default()
        };
        log3.attributes.push(KeyValue {
            key: "user".to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue("charlie".to_string())),
            }),
        });
        log3.attributes.push(KeyValue {
            key: "ip_address".to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue("192.168.1.1".to_string())),
            }),
        });

        let metadata = "namespace=test";
        let logs = vec![log1, log2, log3];
        let result = encoder.encode_log_batch(logs.iter(), metadata);

        // Should return two blobs: one for "login" and one for "logout"
        assert_eq!(result.len(), 2);

        // Find the login and logout blobs
        let login_blob = result
            .iter()
            .find(|(_, name, _, _)| name == "login")
            .unwrap();
        let logout_blob = result
            .iter()
            .find(|(_, name, _, _)| name == "logout")
            .unwrap();

        // Login blob should have 2 events (log1 and log3)
        assert_eq!(login_blob.3, 2);
        // Logout blob should have 1 event (log2)
        assert_eq!(logout_blob.3, 1);

        // Verify we have 3 schemas cached (different attributes combinations)
        assert_eq!(encoder.schema_cache.read().unwrap().len(), 3);
    }
}
