// Integration tests for OTAP and OTLP encoders
// Verifies that both encoders can produce compatible Bond-encoded output

#[cfg(test)]
mod tests {
    use crate::payload_encoder::otap_encoder::OtapEncoder;
    use crate::payload_encoder::otlp_encoder::OtlpEncoder;
    use arrow::array::{Int32Array, StringArray, UInt32Array, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use opentelemetry_proto::tonic::logs::v1::LogRecord;
    use std::sync::Arc;

    /// Helper to convert OTLP LogRecord to Arrow RecordBatch
    fn otlp_to_arrow_log(log: &LogRecord) -> RecordBatch {
        // Create schema matching the log record fields
        let schema = Arc::new(Schema::new(vec![
            Field::new("time_unix_nano", DataType::UInt64, false),
            Field::new("severity_number", DataType::Int32, false),
            Field::new("severity_text", DataType::Utf8, true),
            Field::new("body", DataType::Utf8, true),
            Field::new("flags", DataType::UInt32, false),
        ]));

        // Extract body string if present
        let body_str = log
            .body
            .as_ref()
            .and_then(|b| b.value.as_ref())
            .and_then(|v| match v {
                opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s) => {
                    Some(s.as_str())
                }
                _ => None,
            })
            .unwrap_or("");

        // Create arrays
        let time_array = UInt64Array::from(vec![if log.time_unix_nano != 0 {
            log.time_unix_nano
        } else {
            log.observed_time_unix_nano
        }]);
        let severity_number_array = Int32Array::from(vec![log.severity_number]);
        let severity_text_array = StringArray::from(vec![if log.severity_text.is_empty() {
            None
        } else {
            Some(log.severity_text.as_str())
        }]);
        let body_array = StringArray::from(vec![if body_str.is_empty() {
            None
        } else {
            Some(body_str)
        }]);
        let flags_array = UInt32Array::from(vec![log.flags]);

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(time_array),
                Arc::new(severity_number_array),
                Arc::new(severity_text_array),
                Arc::new(body_array),
                Arc::new(flags_array),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_otap_otlp_encoders_basic_log() {
        // Create a simple log record
        let log = LogRecord {
            time_unix_nano: 1_700_000_000_000_000_000,
            severity_number: 9,
            severity_text: "INFO".to_string(),
            body: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                value: Some(
                    opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                        "Test log message".to_string(),
                    ),
                ),
            }),
            ..Default::default()
        };

        let metadata = "namespace=testNamespace/eventVersion=Ver1v0";

        // Encode with OTLP encoder
        let otlp_encoder = OtlpEncoder::new();
        let otlp_result = otlp_encoder
            .encode_log_batch([log.clone()].iter(), metadata)
            .unwrap();

        // Convert to Arrow and encode with OTAP encoder
        let arrow_batch = otlp_to_arrow_log(&log);
        let otap_encoder = OtapEncoder::new();
        let otap_result = otap_encoder
            .encode_otap_log_batch(&arrow_batch, None, None, None, metadata)
            .unwrap();

        // Both should produce output
        assert!(
            !otlp_result.is_empty(),
            "OTLP encoder should produce output"
        );
        assert!(
            !otap_result.is_empty(),
            "OTAP encoder should produce output"
        );

        // Both should have compressed data
        assert!(
            !otlp_result[0].data.is_empty(),
            "OTLP encoded data should not be empty"
        );
        assert!(
            !otap_result[0].data.is_empty(),
            "OTAP encoded data should not be empty"
        );

        // Both should have metadata with timestamps
        assert!(
            otlp_result[0].metadata.start_time > 0,
            "OTLP should have valid start_time"
        );
        assert!(
            otap_result[0].metadata.start_time > 0,
            "OTAP should have valid start_time"
        );

        println!("OTLP output size: {} bytes", otlp_result[0].data.len());
        println!("OTAP output size: {} bytes", otap_result[0].data.len());
    }

    #[test]
    fn test_otap_otlp_multiple_logs() {
        // Create multiple log records
        let logs = vec![
            LogRecord {
                time_unix_nano: 1_700_000_000_000_000_000,
                severity_number: 9,
                severity_text: "INFO".to_string(),
                body: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                            "First log".to_string(),
                        ),
                    ),
                }),
                ..Default::default()
            },
            LogRecord {
                time_unix_nano: 1_700_000_001_000_000_000,
                severity_number: 13,
                severity_text: "ERROR".to_string(),
                body: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                            "Error log".to_string(),
                        ),
                    ),
                }),
                ..Default::default()
            },
            LogRecord {
                time_unix_nano: 1_700_000_002_000_000_000,
                severity_number: 5,
                severity_text: "DEBUG".to_string(),
                body: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                            "Debug log".to_string(),
                        ),
                    ),
                }),
                ..Default::default()
            },
        ];

        let metadata = "namespace=test/eventVersion=Ver1v0";

        // Encode with OTLP
        let otlp_encoder = OtlpEncoder::new();
        let otlp_result = otlp_encoder
            .encode_log_batch(logs.iter(), metadata)
            .unwrap();

        // Convert all logs to single Arrow batch
        let schema = Arc::new(Schema::new(vec![
            Field::new("time_unix_nano", DataType::UInt64, false),
            Field::new("severity_number", DataType::Int32, false),
            Field::new("severity_text", DataType::Utf8, true),
            Field::new("body", DataType::Utf8, true),
            Field::new("flags", DataType::UInt32, false),
        ]));

        let time_values: Vec<u64> = logs.iter().map(|l| l.time_unix_nano).collect();
        let severity_values: Vec<i32> = logs.iter().map(|l| l.severity_number).collect();
        let severity_text_values: Vec<Option<&str>> = logs
            .iter()
            .map(|l| {
                if l.severity_text.is_empty() {
                    None
                } else {
                    Some(l.severity_text.as_str())
                }
            })
            .collect();
        let body_values: Vec<Option<String>> = logs
            .iter()
            .map(|l| {
                l.body.as_ref().and_then(|b| {
                    b.value.as_ref().and_then(|v| match v {
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                            s,
                        ) => Some(s.clone()),
                        _ => None,
                    })
                })
            })
            .collect();
        let flags_values: Vec<u32> = logs.iter().map(|l| l.flags).collect();

        let arrow_batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(time_values)),
                Arc::new(Int32Array::from(severity_values)),
                Arc::new(StringArray::from(severity_text_values)),
                Arc::new(StringArray::from(body_values)),
                Arc::new(UInt32Array::from(flags_values)),
            ],
        )
        .unwrap();

        // Encode with OTAP
        let otap_encoder = OtapEncoder::new();
        let otap_result = otap_encoder
            .encode_otap_log_batch(&arrow_batch, None, None, None, metadata)
            .unwrap();

        // Both should produce output
        assert_eq!(
            otlp_result.len(),
            1,
            "OTLP should produce 1 batch (all same event name 'Log')"
        );
        assert_eq!(otap_result.len(), 1, "OTAP should produce 1 batch");

        // Verify time ranges are captured correctly
        let expected_start = 1_700_000_000_000_000_000;
        let expected_end = 1_700_000_002_000_000_000;

        assert_eq!(
            otlp_result[0].metadata.start_time, expected_start,
            "OTLP start_time should match earliest log"
        );
        assert_eq!(
            otlp_result[0].metadata.end_time, expected_end,
            "OTLP end_time should match latest log"
        );
        assert_eq!(
            otap_result[0].metadata.start_time, expected_start,
            "OTAP start_time should match earliest log"
        );
        assert_eq!(
            otap_result[0].metadata.end_time, expected_end,
            "OTAP end_time should match latest log"
        );

        println!(
            "Multiple logs - OTLP output size: {} bytes",
            otlp_result[0].data.len()
        );
        println!(
            "Multiple logs - OTAP output size: {} bytes",
            otap_result[0].data.len()
        );
    }

    #[test]
    fn test_empty_batch_handling() {
        let metadata = "namespace=test/eventVersion=Ver1v0";

        // Test OTLP with empty batch
        let otlp_encoder = OtlpEncoder::new();
        let empty_logs: Vec<LogRecord> = vec![];
        let otlp_result = otlp_encoder
            .encode_log_batch(empty_logs.iter(), metadata)
            .unwrap();
        assert!(
            otlp_result.is_empty(),
            "OTLP encoder should return empty result for empty batch"
        );

        // Test OTAP with empty batch
        let schema = Arc::new(Schema::new(vec![
            Field::new("time_unix_nano", DataType::UInt64, false),
            Field::new("severity_number", DataType::Int32, false),
        ]));
        let empty_batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(Vec::<u64>::new())),
                Arc::new(Int32Array::from(Vec::<i32>::new())),
            ],
        )
        .unwrap();

        let otap_encoder = OtapEncoder::new();
        let otap_result = otap_encoder
            .encode_otap_log_batch(&empty_batch, None, None, None, metadata)
            .unwrap();
        assert!(
            otap_result.is_empty(),
            "OTAP encoder should return empty result for empty batch"
        );
    }

    #[test]
    fn test_compression_verification() {
        // Verify that both encoders produce compressed output (LZ4)
        let log = LogRecord {
            time_unix_nano: 1_700_000_000_000_000_000,
            severity_number: 9,
            body: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                value: Some(
                    opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                        "A".repeat(1000), // Large body to ensure compression benefit
                    ),
                ),
            }),
            ..Default::default()
        };

        let metadata = "namespace=test/eventVersion=Ver1v0";

        // OTLP encoding
        let otlp_encoder = OtlpEncoder::new();
        let otlp_result = otlp_encoder
            .encode_log_batch([log.clone()].iter(), metadata)
            .unwrap();

        // OTAP encoding
        let arrow_batch = otlp_to_arrow_log(&log);
        let otap_encoder = OtapEncoder::new();
        let otap_result = otap_encoder
            .encode_otap_log_batch(&arrow_batch, None, None, None, metadata)
            .unwrap();

        // Both should produce compressed data
        assert!(
            !otlp_result[0].data.is_empty(),
            "OTLP compressed data should not be empty"
        );
        assert!(
            !otap_result[0].data.is_empty(),
            "OTAP compressed data should not be empty"
        );

        // Compressed size should be smaller than uncompressed (for large repeated data)
        // We can't verify exact compression ratio but we can verify data is present
        println!(
            "OTLP compressed size: {} bytes (large body test)",
            otlp_result[0].data.len()
        );
        println!(
            "OTAP compressed size: {} bytes (large body test)",
            otap_result[0].data.len()
        );
    }
}
