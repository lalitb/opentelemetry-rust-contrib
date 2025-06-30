use crate::payload_encoder::bond_encoder::{BondDataType, BondEncodedSchema, BondWriter, FieldDef};
use crate::payload_encoder::central_blob::{CentralBlob, CentralEventEntry, CentralSchemaEntry};
use chrono::{TimeZone, Utc};
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::logs::v1::LogRecord;
use std::borrow::Cow;
use std::collections::HashMap;

type EncodedRow = (String, u8, Vec<u8>); // (event_name, level, row_buffer)

// Field name constants
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

/// Simplified encoder that only groups by event_name, without schema tracking
#[derive(Clone)]
pub struct SimplifiedOtlpEncoder {
    // Using a fixed schema ID for all logs
    fixed_schema_id: u64,
}

impl SimplifiedOtlpEncoder {
    pub fn new() -> Self {
        SimplifiedOtlpEncoder {
            // Use a constant schema ID since we're not tracking schemas
            fixed_schema_id: 0xDEADBEEF,
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
        // Group logs by event_name only
        let mut batches: HashMap<String, Vec<&LogRecord>> = HashMap::new();

        for log_record in logs {
            let event_name = if log_record.event_name.is_empty() {
                "Log".to_string()
            } else {
                log_record.event_name.clone()
            };
            
            batches.entry(event_name).or_default().push(log_record);
        }

        // Create one blob per event_name
        let mut blobs = Vec::new();
        for (event_name, logs) in batches {
            // Create a universal schema that can handle all possible fields
            let universal_schema = self.create_universal_schema();
            
            // Encode all logs for this event_name
            let mut events = Vec::new();
            for log in logs {
                let row_buffer = self.write_log_data(log);
                events.push(CentralEventEntry {
                    schema_id: self.fixed_schema_id,
                    level: log.severity_number as u8,
                    event_name: event_name.clone(),
                    row: row_buffer,
                });
            }
            
            let events_len = events.len();
            
            // Create the blob with the universal schema
            let blob = CentralBlob {
                version: 1,
                format: 2,
                metadata: metadata.to_string(),
                schemas: vec![universal_schema],
                events,
            };
            
            let bytes = blob.to_bytes();
            blobs.push((self.fixed_schema_id, event_name, bytes, events_len));
        }
        
        blobs
    }

    /// Create a universal schema that includes all possible fields
    fn create_universal_schema(&self) -> CentralSchemaEntry {
        // Define all possible fields that might appear in logs
        let mut fields = vec![
            FieldDef {
                name: Cow::Borrowed(FIELD_ENV_NAME),
                type_id: BondDataType::BT_STRING as u8,
                field_id: 1,
            },
            FieldDef {
                name: Cow::Borrowed(FIELD_ENV_VER),
                type_id: BondDataType::BT_STRING as u8,
                field_id: 2,
            },
            FieldDef {
                name: Cow::Borrowed(FIELD_TIMESTAMP),
                type_id: BondDataType::BT_STRING as u8,
                field_id: 3,
            },
            FieldDef {
                name: Cow::Borrowed(FIELD_ENV_TIME),
                type_id: BondDataType::BT_STRING as u8,
                field_id: 4,
            },
            FieldDef {
                name: Cow::Borrowed(FIELD_TRACE_ID),
                type_id: BondDataType::BT_STRING as u8,
                field_id: 5,
            },
            FieldDef {
                name: Cow::Borrowed(FIELD_SPAN_ID),
                type_id: BondDataType::BT_STRING as u8,
                field_id: 6,
            },
            FieldDef {
                name: Cow::Borrowed(FIELD_TRACE_FLAGS),
                type_id: BondDataType::BT_INT32 as u8,
                field_id: 7,
            },
            FieldDef {
                name: Cow::Borrowed(FIELD_NAME),
                type_id: BondDataType::BT_STRING as u8,
                field_id: 8,
            },
            FieldDef {
                name: Cow::Borrowed(FIELD_SEVERITY_NUMBER),
                type_id: BondDataType::BT_INT32 as u8,
                field_id: 9,
            },
            FieldDef {
                name: Cow::Borrowed(FIELD_SEVERITY_TEXT),
                type_id: BondDataType::BT_STRING as u8,
                field_id: 10,
            },
            FieldDef {
                name: Cow::Borrowed(FIELD_BODY),
                type_id: BondDataType::BT_STRING as u8,
                field_id: 11,
            },
        ];
        
        // Reserve space for dynamic attributes (field IDs 12-100)
        let mut next_field_id = 12u16;
        for i in 0..20 {  // Reserve 20 slots for dynamic string attributes
            fields.push(FieldDef {
                name: Cow::Owned(format!("attr_string_{}", i)),
                type_id: BondDataType::BT_STRING as u8,
                field_id: next_field_id,
            });
            next_field_id += 1;
        }
        
        for i in 0..10 {  // Reserve 10 slots for dynamic int attributes
            fields.push(FieldDef {
                name: Cow::Owned(format!("attr_int_{}", i)),
                type_id: BondDataType::BT_INT32 as u8,
                field_id: next_field_id,
            });
            next_field_id += 1;
        }
        
        for i in 0..10 {  // Reserve 10 slots for dynamic float attributes
            fields.push(FieldDef {
                name: Cow::Owned(format!("attr_float_{}", i)),
                type_id: BondDataType::BT_FLOAT as u8,
                field_id: next_field_id,
            });
            next_field_id += 1;
        }

        let schema = BondEncodedSchema::from_fields("UniversalLogRecord", "telemetry", fields);
        let schema_bytes = schema.as_bytes();
        let schema_md5 = md5::compute(schema_bytes).0;

        CentralSchemaEntry {
            id: self.fixed_schema_id,
            md5: schema_md5,
            schema,
        }
    }

    /// Write log data with a simplified approach - serialize all present fields
    fn write_log_data(&self, log: &LogRecord) -> Vec<u8> {
        let mut buffer = Vec::with_capacity(512); // Pre-allocate reasonable size

        // Always write the mandatory fields
        BondWriter::write_string(&mut buffer, "TestEnv"); // env_name
        BondWriter::write_string(&mut buffer, "4.0"); // env_ver
        
        let dt = Self::format_timestamp(log.observed_time_unix_nano);
        BondWriter::write_string(&mut buffer, &dt); // timestamp
        BondWriter::write_string(&mut buffer, &dt); // env_time

        // Write optional fields (using empty string for missing values)
        if !log.trace_id.is_empty() {
            let hex_bytes = Self::encode_id_to_hex::<32>(&log.trace_id);
            let hex_str = std::str::from_utf8(&hex_bytes).unwrap();
            BondWriter::write_string(&mut buffer, hex_str);
        } else {
            BondWriter::write_string(&mut buffer, "");
        }

        if !log.span_id.is_empty() {
            let hex_bytes = Self::encode_id_to_hex::<16>(&log.span_id);
            let hex_str = std::str::from_utf8(&hex_bytes).unwrap();
            BondWriter::write_string(&mut buffer, hex_str);
        } else {
            BondWriter::write_string(&mut buffer, "");
        }

        // trace_flags
        BondWriter::write_int32(&mut buffer, log.flags as i32);

        // event_name
        BondWriter::write_string(&mut buffer, &log.event_name);

        // severity
        BondWriter::write_int32(&mut buffer, log.severity_number);
        BondWriter::write_string(&mut buffer, &log.severity_text);

        // body
        if let Some(body) = &log.body {
            if let Some(Value::StringValue(s)) = &body.value {
                BondWriter::write_string(&mut buffer, s);
            } else {
                BondWriter::write_string(&mut buffer, "");
            }
        } else {
            BondWriter::write_string(&mut buffer, "");
        }

        // Write dynamic attributes in reserved slots
        let mut string_attr_count = 0;
        let mut int_attr_count = 0;
        let mut float_attr_count = 0;

        // First, write actual attributes
        for attr in &log.attributes {
            if let Some(val) = attr.value.as_ref().and_then(|v| v.value.as_ref()) {
                match val {
                    Value::StringValue(s) if string_attr_count < 20 => {
                        BondWriter::write_string(&mut buffer, s);
                        string_attr_count += 1;
                    }
                    Value::IntValue(i) if int_attr_count < 10 => {
                        BondWriter::write_int32(&mut buffer, *i as i32);
                        int_attr_count += 1;
                    }
                    Value::DoubleValue(d) if float_attr_count < 10 => {
                        BondWriter::write_float(&mut buffer, *d as f32);
                        float_attr_count += 1;
                    }
                    _ => {} // Skip if we've exceeded reserved slots or unsupported type
                }
            }
        }

        // Fill remaining reserved slots with defaults
        for _ in string_attr_count..20 {
            BondWriter::write_string(&mut buffer, "");
        }
        for _ in int_attr_count..10 {
            BondWriter::write_int32(&mut buffer, 0);
        }
        for _ in float_attr_count..10 {
            BondWriter::write_float(&mut buffer, 0.0);
        }

        buffer
    }

    fn encode_id_to_hex<const N: usize>(id: &[u8]) -> [u8; N] {
        let mut hex_bytes = [0u8; N];
        hex::encode_to_slice(id, &mut hex_bytes).unwrap();
        hex_bytes
    }

    fn format_timestamp(nanos: u64) -> String {
        let secs = (nanos / 1_000_000_000) as i64;
        let nsec = (nanos % 1_000_000_000) as u32;
        Utc.timestamp_opt(secs, nsec)
            .single()
            .unwrap_or_else(|| Utc.timestamp_opt(0, 0).single().unwrap())
            .to_rfc3339()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};

    #[test]
    fn test_simplified_encoding() {
        let encoder = SimplifiedOtlpEncoder::new();

        let mut log1 = LogRecord {
            observed_time_unix_nano: 1_700_000_000_000_000_000,
            event_name: "test_event".to_string(),
            severity_number: 9,
            severity_text: "INFO".to_string(),
            ..Default::default()
        };

        log1.attributes.push(KeyValue {
            key: "user_id".to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue("user123".to_string())),
            }),
        });

        let mut log2 = LogRecord {
            observed_time_unix_nano: 1_700_000_001_000_000_000,
            event_name: "test_event".to_string(), // Same event name
            severity_number: 10,
            severity_text: "WARN".to_string(),
            trace_id: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            ..Default::default()
        };

        log2.attributes.push(KeyValue {
            key: "request_count".to_string(),
            value: Some(AnyValue {
                value: Some(Value::IntValue(42)),
            }),
        });

        let metadata = "namespace=testNamespace/eventVersion=Ver1v0";
        let result = encoder.encode_log_batch([log1, log2].iter(), metadata);

        // Should have only one blob since both logs have the same event_name
        assert_eq!(result.len(), 1);
        let (schema_id, event_name, _bytes, event_count) = &result[0];
        assert_eq!(*schema_id, encoder.fixed_schema_id);
        assert_eq!(event_name, "test_event");
        assert_eq!(*event_count, 2);
    }

    #[test]
    fn test_grouping_by_event_name_only() {
        let encoder = SimplifiedOtlpEncoder::new();

        let log1 = LogRecord {
            observed_time_unix_nano: 1_700_000_000_000_000_000,
            event_name: "event_a".to_string(),
            severity_number: 9,
            ..Default::default()
        };

        let log2 = LogRecord {
            observed_time_unix_nano: 1_700_000_001_000_000_000,
            event_name: "event_b".to_string(),
            severity_number: 10,
            ..Default::default()
        };

        let log3 = LogRecord {
            observed_time_unix_nano: 1_700_000_002_000_000_000,
            event_name: "event_a".to_string(), // Same as log1
            severity_number: 11,
            trace_id: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            ..Default::default()
        };

        let metadata = "namespace=test";
        let result = encoder.encode_log_batch([log1, log2, log3].iter(), metadata);

        // Should have 2 blobs: one for event_a (2 logs) and one for event_b (1 log)
        assert_eq!(result.len(), 2);
        
        // Find the blob for event_a
        let event_a_blob = result.iter().find(|(_, name, _, _)| name == "event_a").unwrap();
        assert_eq!(event_a_blob.3, 2); // Should contain 2 events
        
        // Find the blob for event_b
        let event_b_blob = result.iter().find(|(_, name, _, _)| name == "event_b").unwrap();
        assert_eq!(event_b_blob.3, 1); // Should contain 1 event
    }
}
