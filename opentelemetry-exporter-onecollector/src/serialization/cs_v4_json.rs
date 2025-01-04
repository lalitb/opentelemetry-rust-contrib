use opentelemetry::logs::AnyValue;
use opentelemetry_sdk::export::logs::LogBatch;
use opentelemetry_sdk::logs::LogRecord;
use serde_json::{json, Value};
use std::collections::HashMap;

pub enum SerializationFormat {
    CommonSchemaV4Json,
}

/// Serializes a `LogBatch` to a JSON value.
pub fn serialize_log_batch(batch: &LogBatch<'_>, format: &SerializationFormat) -> Vec<Value> {
    batch
        .iter()
        .map(|(record, _scope)| serialize_log_record(record, format))
        .collect()
}

/// Serializes a `LogRecord` to a JSON value based on the specified format.
fn serialize_log_record(record: &LogRecord, format: &SerializationFormat) -> Value {
    match format {
        SerializationFormat::CommonSchemaV4Json => serialize_to_common_schema_v4(record),
    }
}

/// Converts a `LogRecord` to Common Schema v4 JSON format.
fn serialize_to_common_schema_v4(record: &LogRecord) -> Value {
    let mut attributes = HashMap::new();
    for (key, value) in record.attributes_iter() {
        attributes.insert(key.as_str().to_string(), value_to_json(value));
    }

    let trace_context = record.trace_context().map(|ctx| {
        json!({
            "traceId": ctx.trace_id.to_string(),
            "spanId": ctx.span_id.to_string(),
            "traceFlags": ctx.trace_flags.map_or_else(|| 0, |f| f.to_u8()),
        })
    });

    json!({
        "name": record.event_name().unwrap_or("Log"),
        "time": record.timestamp().map_or_else(|| "unknown".to_string(), |t| format!("{:?}", t)),
        "data": {
            "severityText": record.severity_text().unwrap_or("Unknown"),
            "severityNumber": record.severity_number().map_or(0, |s| s as u8),
            "body": record.body().map_or(Value::Null, value_to_json),
            "attributes": attributes,
        },
        "ext": {
            "trace": trace_context,
        }
    })
}

/// Converts `AnyValue` to a `serde_json::Value`.
fn value_to_json(value: &AnyValue) -> Value {
    match value {
        AnyValue::String(s) => Value::String(s.to_string()),
        AnyValue::Boolean(b) => Value::Bool(*b),
        //AnyValue::Bytes(b) => Value::String(base64::encode(b)),
        AnyValue::Int(i) => Value::Number((*i).into()),
        AnyValue::Double(d) => serde_json::Number::from_f64(*d).map_or(Value::Null, Value::Number),
        _ => Value::Null,
    }
}
