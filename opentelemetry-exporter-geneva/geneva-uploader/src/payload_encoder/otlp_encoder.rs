use crate::payload_encoder::bond_encoder::{
    BondDataType, BondEncodedRow, BondEncodedSchema, BondWriter,
};
use crate::payload_encoder::central_blob::{CentralBlob, CentralEventEntry, CentralSchemaEntry};
use chrono::{TimeZone, Utc};
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::logs::v1::LogRecord;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

type SchemaCache = Arc<RwLock<HashMap<u64, (BondEncodedSchema, Vec<FieldInfo>)>>>;
type BatchKey = (u64, String);
type EncodedRow = (String, u8, Vec<u8>); // (event_name, level, row_buffer)
type BatchValue = (CentralSchemaEntry, Vec<EncodedRow>);
type LogBatches = HashMap<BatchKey, BatchValue>;

/// Encoder to write OTLP payload in bond form.
#[derive(Clone, Debug)]
struct FieldInfo {
    name: String,
    type_id: u8,
    order_id: u16,
}

pub struct OtlpEncoder {
    // TODO - limit cache size or use LRU eviction, and/or add feature flag for caching
    schema_cache: SchemaCache,
}

impl OtlpEncoder {
    pub fn new() -> Self {
        OtlpEncoder {
            schema_cache: Arc::new(RwLock::new(HashMap::with_capacity(32))),
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
        use std::collections::HashMap;

        let mut batches: LogBatches = HashMap::with_capacity(8);

        // TODO: Integrate compression at the batch level after blobs are created

        for log_record in logs {
            // 1. Get schema
            let field_specs = self.determine_fields(log_record);
            let schema_id = self.calculate_schema_id(&field_specs);
            let (schema_entry, field_info) = self.get_or_create_schema(schema_id, field_specs);

            // 2. Encode row
            let row_buffer = self.write_row_data(log_record, &field_info);
            let event_name: &str = if log_record.event_name.is_empty() {
                "Log"
            } else {
                &log_record.event_name
            };
            let level = log_record.severity_number as u8;

            // 3. Insert into batches - Key is (schema_id, event_name)
            batches
                .entry((schema_id, event_name.to_owned()))
                .or_insert_with(|| (schema_entry, Vec::new()))
                .1
                .push((event_name.to_owned(), level, row_buffer));
        }

        // 4. Encode blobs (one per schema AND event_name combination)
        let mut blobs = Vec::new();
        for ((schema_id, batch_event_name), (schema_entry, records)) in batches {
            let events: Vec<CentralEventEntry> = records
                .into_iter()
                .map(|(event_name, level, row_buffer)| CentralEventEntry {
                    schema_id,
                    level,
                    event_name,
                    row: BondEncodedRow::from_schema_and_row(&schema_entry.schema, &row_buffer),
                })
                .collect();
            let events_len = events.len();

            let blob = CentralBlob {
                version: 1,
                format: 2,
                metadata: metadata.to_string(),
                schemas: vec![schema_entry],
                events,
            };
            let bytes = blob.to_bytes();
            blobs.push((schema_id, batch_event_name, bytes, events_len));
        }
        blobs
    }

    /// Determine which fields are present in the LogRecord
    fn determine_fields(&self, log: &LogRecord) -> Vec<(String, u8)> {
        let mut fields = vec![
            ("env_name".to_string(), BondDataType::BT_STRING as u8),
            ("env_ver".to_string(), BondDataType::BT_STRING as u8),
            ("timestamp".to_string(), BondDataType::BT_STRING as u8),
            ("env_time".to_string(), BondDataType::BT_STRING as u8),
        ];

        // Part A extension - Conditional fields
        if !log.trace_id.is_empty() {
            fields.push(("env_dt_traceId".to_string(), BondDataType::BT_STRING as u8));
        }
        if !log.span_id.is_empty() {
            fields.push(("env_dt_spanId".to_string(), BondDataType::BT_STRING as u8));
        }
        if log.flags != 0 {
            fields.push((
                "env_dt_traceFlags".to_string(),
                BondDataType::BT_INT32 as u8,
            ));
        }

        // Part B - Core log fields
        if !log.event_name.is_empty() {
            fields.push(("name".to_string(), BondDataType::BT_STRING as u8));
        }
        fields.push(("SeverityNumber".to_string(), BondDataType::BT_INT32 as u8));
        if !log.severity_text.is_empty() {
            fields.push(("SeverityText".to_string(), BondDataType::BT_STRING as u8));
        }
        if let Some(body) = &log.body {
            if let Some(Value::StringValue(_)) = &body.value {
                fields.push(("body".to_string(), BondDataType::BT_STRING as u8));
            }
        }

        // Part C - Dynamic attributes
        for attr in &log.attributes {
            if let Some(val) = attr.value.as_ref().and_then(|v| v.value.as_ref()) {
                let type_id = match val {
                    Value::StringValue(_) => BondDataType::BT_STRING as u8,
                    Value::IntValue(_) => BondDataType::BT_INT32 as u8,
                    Value::DoubleValue(_) => BondDataType::BT_FLOAT as u8, // using float for now
                    Value::BoolValue(_) => BondDataType::BT_INT32 as u8, // representing bool as int
                    _ => continue,
                };
                fields.push((attr.key.clone(), type_id));
            }
        }

        fields
    }

    /// Calculate schema ID from field specifications
    fn calculate_schema_id(&self, fields: &[(String, u8)]) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();

        // Sort fields by name for consistent schema ID
        // Avoid clone: sort a slice reference
        let mut sorted_fields: Vec<_> = fields.iter().collect();
        sorted_fields.sort_by(|a, b| a.0.cmp(&b.0));

        for (name, type_id) in sorted_fields {
            name.hash(&mut hasher);
            type_id.hash(&mut hasher);
        }

        hasher.finish()
    }

    /// Get or create schema with field ordering information
    fn get_or_create_schema(
        &self,
        schema_id: u64,
        field_specs: Vec<(String, u8)>,
    ) -> (CentralSchemaEntry, Vec<FieldInfo>) {
        // Check cache first
        if let Some((schema, field_info)) = self.schema_cache.read().unwrap().get(&schema_id) {
            let schema_bytes = schema.as_bytes();
            let schema_md5 = md5::compute(schema_bytes).0;

            return (
                CentralSchemaEntry {
                    id: schema_id,
                    md5: schema_md5,
                    schema: schema.clone(),
                },
                field_info.clone(),
            );
        }

        // Create new schema
        // Avoid clone: sort a slice reference
        let mut sorted_specs: Vec<_> = field_specs.iter().collect();
        sorted_specs.sort_by(|a, b| a.0.cmp(&b.0));

        let field_defs: Vec<(&str, u8, u16)> = sorted_specs
            .iter()
            .enumerate()
            .map(|(i, (name, type_id))| (name.as_str(), *type_id, (i + 1) as u16))
            .collect();

        let schema = BondEncodedSchema::from_fields(&field_defs, "OtlpLogRecord", "telemetry"); //TODO - use actual struct name and namespace

        // Create field info for ordered writing
        let field_info: Vec<FieldInfo> = field_defs
            .iter()
            .map(|(name, type_id, order_id)| FieldInfo {
                name: (*name).to_string(),
                type_id: *type_id,
                order_id: *order_id,
            })
            .collect();

        // TODO: Use LRU cache for schema_cache to limit memory usage

        {
            let mut cache = self.schema_cache.write().unwrap();
            cache.insert(schema_id, (schema.clone(), field_info.clone()));
        }

        let schema_bytes = schema.as_bytes();
        let schema_md5 = md5::compute(schema_bytes).0;

        (
            CentralSchemaEntry {
                id: schema_id,
                md5: schema_md5,
                schema,
            },
            field_info,
        )
    }

    /// Write row data directly from LogRecord
    fn write_row_data(&self, log: &LogRecord, field_info: &[FieldInfo]) -> Vec<u8> {
        // Better capacity estimation based on typical log size
        let mut buffer = Vec::with_capacity(2048);

        // Write fields in schema order without clone
        let mut sorted_indices: Vec<_> = (0..field_info.len()).collect();
        sorted_indices.sort_by_key(|&i| field_info[i].order_id);

        for &i in &sorted_indices {
            let field = &field_info[i];
            match field.name.as_str() {
                "env_name" => BondWriter::write_string(&mut buffer, "TestEnv"), // TODO - placeholder for actual env name
                "env_ver" => BondWriter::write_string(&mut buffer, "4.0"), // TODO - placeholder for actual env name
                "timestamp" | "env_time" => {
                    let dt = Self::format_timestamp(log.observed_time_unix_nano);
                    BondWriter::write_string(&mut buffer, &dt);
                }
                "env_dt_traceId" => {
                    if !log.trace_id.is_empty() {
                        let hex = hex::encode(&log.trace_id);
                        BondWriter::write_string(&mut buffer, &hex);
                    }
                }
                "env_dt_spanId" => {
                    if !log.span_id.is_empty() {
                        let hex = hex::encode(&log.span_id);
                        BondWriter::write_string(&mut buffer, &hex);
                    }
                }
                "env_dt_traceFlags" => {
                    if log.flags != 0 {
                        BondWriter::write_int32(&mut buffer, log.flags as i32);
                    }
                }
                "name" => {
                    if !log.event_name.is_empty() {
                        BondWriter::write_string(&mut buffer, &log.event_name);
                    }
                }
                "SeverityNumber" => BondWriter::write_int32(&mut buffer, log.severity_number),
                "SeverityText" => {
                    if !log.severity_text.is_empty() {
                        BondWriter::write_string(&mut buffer, &log.severity_text);
                    }
                }
                "body" => {
                    // TODO - handle all types of body values
                    // For now, we only handle string values
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

        // TODO: Reuse buffer for multiple rows to reduce allocations

        buffer
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
        if let Some(val) = &attr.value {
            match (&val.value, expected_type) {
                (Some(Value::StringValue(s)), t) if t == BondDataType::BT_STRING as u8 => {
                    BondWriter::write_string(buffer, s)
                }
                (Some(Value::StringValue(s)), t) if t == BondDataType::BT_WSTRING as u8 => {
                    BondWriter::write_wstring(buffer, s)
                }
                (Some(Value::IntValue(i)), t) if t == BondDataType::BT_INT32 as u8 => {
                    BondWriter::write_int32(buffer, *i as i32)
                }
                (Some(Value::DoubleValue(d)), t) if t == BondDataType::BT_FLOAT as u8 => {
                    BondWriter::write_float(buffer, *d as f32)
                }
                (Some(Value::DoubleValue(d)), t) if t == BondDataType::BT_DOUBLE as u8 => {
                    BondWriter::write_double(buffer, *d)
                }
                (Some(Value::BoolValue(b)), t) if t == BondDataType::BT_INT32 as u8 => {
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
        log2.trace_id = vec![1, 2, 3, 4];
        let _result3 = encoder.encode_log_batch([log2].iter(), metadata);
        assert_eq!(encoder.schema_cache.read().unwrap().len(), 2);
    }
}
