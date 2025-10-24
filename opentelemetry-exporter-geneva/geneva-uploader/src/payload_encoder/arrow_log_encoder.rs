//! Arrow Log Encoder for Geneva - Direct Arrow → Bond encoding
//!
//! This encoder uses otel-arrow-rust components for zero-copy traversal of Arrow RecordBatches.
//! It leverages LogsArrays, Attribute16Arrays, BatchSorter, and ChildIndexIter from otel-arrow
//! to ensure consistency with the OTLP encoder and eliminate code duplication.

use crate::client::EncodedBatch;
use crate::payload_encoder::bond_encoder::{BondDataType, BondEncodedSchema, BondWriter, FieldDef};
use crate::payload_encoder::central_blob::{
    BatchMetadata, CentralBlob, CentralEventEntry, CentralSchemaEntry,
};
use crate::payload_encoder::lz4_chunked_compression::lz4_chunked_compression;
use arrow::array::{Array, RecordBatch};
use chrono::{TimeZone, Utc};
use std::borrow::Cow;
use std::sync::Arc;
use tracing::debug;

// ✅ Import otel-arrow components for zero-copy traversal
use otel_arrow_rust::otlp::{
    BatchSorter, ChildIndexIter, NullableArrayAccessor,
    SortedBatchCursor,
};
use otel_arrow_rust::otlp::attributes::Attribute16Arrays;
use otel_arrow_rust::otlp::logs::LogsArrays;

// Field name constants (same as otlp_encoder.rs)
const FIELD_ENV_NAME: &str = "env_name";
const FIELD_ENV_VER: &str = "env_ver";
const FIELD_TIMESTAMP: &str = "timestamp";
const FIELD_ENV_TIME: &str = "env_time";
const FIELD_TRACE_ID: &str = "env_dt_traceId";
const FIELD_SPAN_ID: &str = "env_dt_spanId";
const FIELD_TRACE_FLAGS: &str = "env_dt_traceFlags";
const FIELD_SEVERITY_NUMBER: &str = "SeverityNumber";
const FIELD_SEVERITY_TEXT: &str = "SeverityText";
const FIELD_BODY: &str = "body";

/// Encoder to write Arrow RecordBatch logs in Bond format (zero-copy)
///
/// Uses otel-arrow components (LogsArrays, Attribute16Arrays, BatchSorter, ChildIndexIter)
/// for efficient traversal and to share logic with the OTLP encoder.
pub struct ArrowLogEncoder {
    env_name: String,
    env_ver: String,
    // ✅ Cursor state for efficient sorted traversal (reused across batches)
    batch_sorter: BatchSorter,
    log_attrs_cursor: SortedBatchCursor,
    resource_attrs_cursor: SortedBatchCursor,
}

impl ArrowLogEncoder {
    pub fn new() -> Self {
        Self {
            env_name: "TestEnv".to_string(),
            env_ver: "4.0".to_string(),
            batch_sorter: BatchSorter::new(),
            log_attrs_cursor: SortedBatchCursor::new(),
            resource_attrs_cursor: SortedBatchCursor::new(),
        }
    }

    pub fn with_env(env_name: String, env_ver: String) -> Self {
        Self {
            env_name,
            env_ver,
            batch_sorter: BatchSorter::new(),
            log_attrs_cursor: SortedBatchCursor::new(),
            resource_attrs_cursor: SortedBatchCursor::new(),
        }
    }

    /// Encode Arrow RecordBatches directly to Bond format using otel-arrow components
    ///
    /// Uses LogsArrays, Attribute16Arrays, and cursor-based traversal for zero-copy processing.
    /// This ensures consistency with the OTLP encoder and eliminates code duplication.
    pub fn encode_arrow_batch(
        &mut self,  // ✅ Need mut for cursor updates
        logs_batch: &RecordBatch,
        log_attrs_batch: Option<&RecordBatch>,
        resource_attrs_batch: Option<&RecordBatch>,
        metadata: &str,
    ) -> Result<Vec<EncodedBatch>, String> {
        use std::collections::HashMap;

        // Internal struct to accumulate batch data before encoding (same as otlp_encoder.rs)
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

        let mut batches: HashMap<&str, BatchData> = HashMap::new();
        let num_rows = logs_batch.num_rows();

        // Debug: Print Arrow batch schema
        eprintln!("\n🔍 DEBUG: Arrow Batch Schemas");
        eprintln!("  Logs batch columns: {:?}", logs_batch.schema().fields().iter().map(|f| f.name()).collect::<Vec<_>>());

        // Debug body column type
        if let Some(body_col) = logs_batch.column_by_name("body") {
            eprintln!("  Body column type: {:?}", body_col.data_type());
            eprintln!("  Body column null_count: {}", body_col.null_count());
            if num_rows > 0 {
                eprintln!("  Body column row 0: {:?}", body_col.slice(0, 1));
            }
        }

        if let Some(attrs) = log_attrs_batch {
            eprintln!("  LogAttrs batch columns: {:?}", attrs.schema().fields().iter().map(|f| f.name()).collect::<Vec<_>>());
            eprintln!("  LogAttrs batch rows: {}", attrs.num_rows());
        }
        if let Some(res_attrs) = resource_attrs_batch {
            eprintln!("  ResourceAttrs batch columns: {:?}", res_attrs.schema().fields().iter().map(|f| f.name()).collect::<Vec<_>>());
        }
        eprintln!();

        // ✅ Use otel-arrow LogsArrays for zero-copy access to log fields
        let logs_arrays = LogsArrays::try_from(logs_batch)
            .map_err(|e| format!("Failed to parse logs batch: {}", e))?;

        // ✅ Use otel-arrow Attribute16Arrays for zero-copy attribute access
        let log_attrs_arrays = log_attrs_batch
            .map(Attribute16Arrays::try_from)
            .transpose()
            .map_err(|e| format!("Failed to parse log attributes: {}", e))?;

        let resource_attrs_arrays = resource_attrs_batch
            .map(Attribute16Arrays::try_from)
            .transpose()
            .map_err(|e| format!("Failed to parse resource attributes: {}", e))?;

        // ✅ Initialize cursors for sorted traversal (done once per batch)
        if let Some(ref attrs) = log_attrs_arrays {
            self.log_attrs_cursor.reset();
            self.batch_sorter.init_cursor_for_u16_id_column(
                &attrs.parent_id,
                &mut self.log_attrs_cursor
            );
            eprintln!("🔍 DEBUG: Initialized log attributes cursor");
        }

        if let Some(ref attrs) = resource_attrs_arrays {
            self.resource_attrs_cursor.reset();
            self.batch_sorter.init_cursor_for_u16_id_column(
                &attrs.parent_id,
                &mut self.resource_attrs_cursor
            );
        }

        // Iterate over each log record row
        for row_idx in 0..num_rows {
            // ✅ Use LogsArrays accessors instead of custom ArrowColumns
            let timestamp = logs_arrays.time_unix_nano
                .and_then(|arr| {
                    if arr.is_null(row_idx) {
                        None
                    } else {
                        Some(arr.value(row_idx) as u64)
                    }
                })
                .unwrap_or(0);

            let event_name_str = "Log";

            // Get the log's ID from the logs batch
            let log_id = if logs_arrays.id.is_null(row_idx) {
                None
            } else {
                Some(logs_arrays.id.value(row_idx))
            };

            if row_idx < 3 {
                eprintln!("🔍 Row {}: log_id = {:?}", row_idx, log_id);
            }

            // 1. Determine fields and schema ID using otel-arrow components
            let (field_defs, schema_id) = self.determine_arrow_fields_and_schema_id(
                log_id,
                row_idx,
                &logs_arrays,
                log_attrs_arrays.as_ref(),
                resource_attrs_arrays.as_ref(),
                event_name_str,
            );

            // 2. Encode row directly to Bond using otel-arrow components
            let row_buffer = self.write_arrow_row_data(
                log_id,
                row_idx,
                &logs_arrays,
                log_attrs_arrays.as_ref(),
                resource_attrs_arrays.as_ref(),
                &field_defs,
            );

            // ✅ Use LogsArrays accessor
            let level = logs_arrays.severity_number.as_ref()
                .and_then(|arr| arr.value_at(row_idx))
                .unwrap_or(9) as u8;

            // 3. Create or get existing batch entry (same logic as otlp_encoder)
            let entry = batches.entry(event_name_str).or_insert_with(|| BatchData {
                schemas: Vec::new(),
                events: Vec::new(),
                metadata: BatchMetadata {
                    start_time: timestamp,
                    end_time: timestamp,
                    schema_ids: String::new(),
                },
            });

            // Update timestamp range
            if timestamp != 0 {
                entry.metadata.start_time = entry.metadata.start_time.min(timestamp);
                entry.metadata.end_time = entry.metadata.end_time.max(timestamp);
            }

            // 4. Add schema if not present (same logic as otlp_encoder)
            if !entry.schemas.iter().any(|s| s.id == schema_id) {
                let schema_entry = Self::create_schema(schema_id, field_defs);
                entry.schemas.push(schema_entry);
            }

            // 5. Create CentralEventEntry (same logic as otlp_encoder)
            let central_event = CentralEventEntry {
                schema_id,
                level,
                event_name: Arc::new(event_name_str.to_string()),
                row: row_buffer,
            };
            entry.events.push(central_event);
        }

        // 6. Encode and compress batches (exact same logic as otlp_encoder)
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
                    name: "arrow_encoder.compress_error",
                    target: "geneva-uploader",
                    event_name = %batch_event_name,
                    error = %e,
                    "LZ4 compression failed"
                );
                format!("compression failed: {e}")
            })?;

            debug!(
                name: "arrow_encoder.encode_arrow_batch",
                target: "geneva-uploader",
                event_name = %batch_event_name,
                schemas = schemas_count,
                events = events_count,
                uncompressed_size = uncompressed.len(),
                compressed_size = compressed.len(),
                "Encoded Arrow log batch"
            );

            result.push(EncodedBatch {
                event_name: batch_event_name.to_string(),
                data: compressed,
                metadata: batch_data.metadata,
            });
        }

        Ok(result)
    }

    /// Determine fields and schema ID using otel-arrow components (zero-copy)
    ///
    /// Uses LogsArrays and ChildIndexIter for efficient traversal without allocations.
    fn determine_arrow_fields_and_schema_id(
        &mut self,  // ✅ Need mut for cursor
        log_id: Option<u16>,
        row_idx: usize,
        logs_arrays: &LogsArrays,  // ✅ otel-arrow LogsArrays
        log_attrs_arrays: Option<&Attribute16Arrays>,  // ✅ otel-arrow Attribute16Arrays
        _resource_attrs_arrays: Option<&Attribute16Arrays>,
        event_name: &str,
    ) -> (Vec<FieldDef>, u64) {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut fields = Vec::with_capacity(20);  // Estimate
        let mut hasher = DefaultHasher::new();
        event_name.hash(&mut hasher);

        // Part A - Always present fields
        fields.push((Cow::Borrowed(FIELD_ENV_NAME), BondDataType::BT_STRING));
        fields.push((Cow::Borrowed(FIELD_ENV_VER), BondDataType::BT_STRING));
        fields.push((Cow::Borrowed(FIELD_TIMESTAMP), BondDataType::BT_STRING));
        fields.push((Cow::Borrowed(FIELD_ENV_TIME), BondDataType::BT_STRING));

        // Part A extension - Conditional fields (use LogsArrays)
        if let Some(ref trace_id) = logs_arrays.trace_id {
            if let Some(bytes) = trace_id.slice_at(row_idx) {
                if !bytes.is_empty() {
                    fields.push((Cow::Borrowed(FIELD_TRACE_ID), BondDataType::BT_STRING));
                }
            }
        }
        if let Some(ref span_id) = logs_arrays.span_id {
            if let Some(bytes) = span_id.slice_at(row_idx) {
                if !bytes.is_empty() {
                    fields.push((Cow::Borrowed(FIELD_SPAN_ID), BondDataType::BT_STRING));
                }
            }
        }
        if logs_arrays.flags.is_some() {
            if let Some(flags) = logs_arrays.flags {
                if !flags.is_null(row_idx) && flags.value(row_idx) != 0 {
                    fields.push((Cow::Borrowed(FIELD_TRACE_FLAGS), BondDataType::BT_UINT32));
                }
            }
        }

        // Part B - Core log fields (use LogsArrays)
        fields.push((Cow::Borrowed(FIELD_SEVERITY_NUMBER), BondDataType::BT_INT32));
        if logs_arrays.severity_text.as_ref().map_or(false, |arr| arr.is_valid(row_idx)) {
            fields.push((Cow::Borrowed(FIELD_SEVERITY_TEXT), BondDataType::BT_STRING));
        }
        if logs_arrays.body.is_some() {
            fields.push((Cow::Borrowed(FIELD_BODY), BondDataType::BT_STRING));
        }

        // Part C - Dynamic log attributes
        // ✅ Use ChildIndexIter for zero-copy traversal (otel-arrow pattern)
        if let (Some(id), Some(attrs)) = (log_id, log_attrs_arrays) {
            // Use a TEMPORARY cursor so we do NOT consume self.log_attrs_cursor here.
            let mut local_cursor = SortedBatchCursor::new();
            self.batch_sorter
                .init_cursor_for_u16_id_column(&attrs.parent_id, &mut local_cursor);

            for attr_index in ChildIndexIter::new(id, &attrs.parent_id, &mut local_cursor) {
                if let Some(key) = attrs.attr_key.str_at(attr_index) {
                    let attr_type_code = attrs
                        .anyval_arrays
                        .attr_type
                        .value_at(attr_index)
                        .unwrap_or(1);

                    let bond_type = match attr_type_code {
                        1 => BondDataType::BT_STRING,
                        2 => BondDataType::BT_INT64,
                        3 => BondDataType::BT_DOUBLE,
                        4 => BondDataType::BT_BOOL,
                        _ => {
                            eprintln!("  ⚠️  Skipping unsupported attribute type code {} for key {}", attr_type_code, key);
                            continue;
                        }
                    };
                    fields.push((Cow::Owned(key.to_string()), bond_type));
                }
            }
        }

        // Resource attributes are NOT included in individual log record schemas
        // (matches OTLP encoder - resource attributes are at ResourceLogs level, not LogRecord level)

        // Convert to FieldDef and hash
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

        (field_defs, hasher.finish())
    }

    /// Write Arrow row data directly to Bond using otel-arrow components (zero-copy)
    ///
    /// Uses LogsArrays and ChildIndexIter for efficient attribute access.
    fn write_arrow_row_data(
        &mut self,  // ✅ Need mut for cursor
        log_id: Option<u16>,
        row_idx: usize,
        logs_arrays: &LogsArrays,  // ✅ otel-arrow LogsArrays
        log_attrs_arrays: Option<&Attribute16Arrays>,  // ✅ otel-arrow Attribute16Arrays
        _resource_attrs_arrays: Option<&Attribute16Arrays>,
        fields: &[FieldDef],
    ) -> Vec<u8> {
        let mut buffer = Vec::with_capacity(fields.len() * 50);

        eprintln!("🔍 DEBUG [Row {}] Starting Bond encoding", row_idx);

        // ✅ Use LogsArrays accessor
        let timestamp = logs_arrays.time_unix_nano
            .and_then(|arr| arr.value_at(row_idx))
            .map(|t| t as u64)
            .unwrap_or(0);
        let formatted_timestamp = Self::format_timestamp(timestamp);

        // Precompute attribute indices & types for this row (avoid consuming shared cursor multiple times)
        let mut row_attr_indices: Vec<(String, u8, usize)> = Vec::new();
        if let (Some(id), Some(attrs)) = (log_id, log_attrs_arrays) {
            let mut local_cursor = SortedBatchCursor::new();
            self.batch_sorter
                .init_cursor_for_u16_id_column(&attrs.parent_id, &mut local_cursor);
            for attr_index in ChildIndexIter::new(id, &attrs.parent_id, &mut local_cursor) {
                if let Some(key) = attrs.attr_key.str_at(attr_index) {
                    let attr_type = attrs
                        .anyval_arrays
                        .attr_type
                        .value_at(attr_index)
                        .unwrap_or(1);
                    row_attr_indices.push((key.to_string(), attr_type, attr_index));
                }
            }
        }

        for field in fields {
            match field.name.as_ref() {
                FIELD_ENV_NAME => {
                    eprintln!("  📝 {} = {}", FIELD_ENV_NAME, self.env_name);
                    BondWriter::write_string(&mut buffer, &self.env_name);
                }
                FIELD_ENV_VER => {
                    eprintln!("  📝 {} = {}", FIELD_ENV_VER, self.env_ver);
                    BondWriter::write_string(&mut buffer, &self.env_ver);
                }
                FIELD_TIMESTAMP | FIELD_ENV_TIME => {
                    eprintln!("  📝 {} = {}", field.name, formatted_timestamp);
                    BondWriter::write_string(&mut buffer, &formatted_timestamp);
                }
                FIELD_TRACE_ID => {
                    // ✅ Use LogsArrays accessor
                    if let Some(ref trace_id) = logs_arrays.trace_id {
                        if let Some(bytes) = trace_id.slice_at(row_idx) {
                            let trace_id_hex = hex::encode(bytes);
                            eprintln!("  📝 {} = {}", FIELD_TRACE_ID, trace_id_hex);
                            BondWriter::write_string(&mut buffer, &trace_id_hex);
                        }
                    }
                }
                FIELD_SPAN_ID => {
                    // ✅ Use LogsArrays accessor
                    if let Some(ref span_id) = logs_arrays.span_id {
                        if let Some(bytes) = span_id.slice_at(row_idx) {
                            let span_id_hex = hex::encode(bytes);
                            eprintln!("  📝 {} = {}", FIELD_SPAN_ID, span_id_hex);
                            BondWriter::write_string(&mut buffer, &span_id_hex);
                        }
                    }
                }
                FIELD_TRACE_FLAGS => {
                    // ✅ Use LogsArrays accessor
                    if let Some(flags) = logs_arrays.flags {
                        if !flags.is_null(row_idx) {
                            let flags_val = flags.value(row_idx);
                            eprintln!("  📝 {} = {}", FIELD_TRACE_FLAGS, flags_val);
                            BondWriter::write_numeric(&mut buffer, flags_val);
                        }
                    }
                }
                FIELD_SEVERITY_NUMBER => {
                    // ✅ Use LogsArrays accessor
                    let num = logs_arrays.severity_number.as_ref()
                        .and_then(|arr| arr.value_at(row_idx))
                        .unwrap_or(0);
                    eprintln!("  📝 {} = {}", FIELD_SEVERITY_NUMBER, num);
                    BondWriter::write_numeric(&mut buffer, num);
                }
                FIELD_SEVERITY_TEXT => {
                    // ✅ Use LogsArrays accessor
                    if let Some(text) = logs_arrays.severity_text.as_ref().and_then(|arr| arr.str_at(row_idx)) {
                        eprintln!("  📝 {} = {}", FIELD_SEVERITY_TEXT, text);
                        BondWriter::write_string(&mut buffer, text);
                    }
                }
                FIELD_BODY => {
                    // ✅ Use LogsArrays accessor - access body from AnyValue
                    if let Some(body_arrays) = &logs_arrays.body {
                        if let Some(body_str) = body_arrays.anyval_arrays.attr_str.as_ref().and_then(|arr| arr.str_at(row_idx)) {
                            eprintln!("  📝 {} = {}", FIELD_BODY, body_str);
                            BondWriter::write_string(&mut buffer, body_str);
                        }
                    }
                }
                _ => {
                    if row_idx < 2 {
                        eprintln!("  🔍 Looking for field '{}' in attributes", field.name);
                    }
                    if let (Some(_id), Some(attrs)) = (log_id, log_attrs_arrays) {
                        if let Some((_, attr_type, attr_index)) = row_attr_indices
                            .iter()
                            .find(|(k, _, _)| k == field.name.as_ref())
                        {
                            match *attr_type {
                                1 => {
                                    if let Some(val) = attrs
                                        .anyval_arrays
                                        .attr_str
                                        .as_ref()
                                        .and_then(|arr| arr.str_at(*attr_index))
                                    {
                                        eprintln!("  📝 [LOG_ATTR:str] {} = {}", field.name, val);
                                        BondWriter::write_string(&mut buffer, val);
                                    }
                                }
                                2 => {
                                    if let Some(val) = attrs
                                        .anyval_arrays
                                        .attr_int
                                        .as_ref()
                                        .and_then(|arr| arr.value_at(*attr_index))
                                    {
                                        eprintln!("  📝 [LOG_ATTR:int] {} = {}", field.name, val);
                                        BondWriter::write_numeric(&mut buffer, val);
                                    }
                                }
                                3 => {
                                    if let Some(double_arr) = attrs.anyval_arrays.attr_double {
                                        if !double_arr.is_null(*attr_index) {
                                            let val = double_arr.value(*attr_index);
                                            eprintln!("  📝 [LOG_ATTR:double] {} = {}", field.name, val);
                                            BondWriter::write_numeric(&mut buffer, val);
                                        }
                                    }
                                }
                                4 => {
                                    if let Some(bool_arr) = attrs.anyval_arrays.attr_bool {
                                        if !bool_arr.is_null(*attr_index) {
                                            let val = bool_arr.value(*attr_index);
                                            eprintln!("  📝 [LOG_ATTR:bool] {} = {}", field.name, val);
                                            BondWriter::write_bool(&mut buffer, val);
                                        }
                                    }
                                }
                                other => {
                                    eprintln!(
                                        "  ⚠️  Unsupported attribute type code {} for key {}",
                                        other, field.name
                                    );
                                }
                            }
                        } else if row_idx < 2 {
                            eprintln!(
                                "    ❌ Attribute '{}' not found for this log row",
                                field.name
                            );
                        }
                    } else if row_idx < 2 {
                        eprintln!("    ❌ No attributes for log");
                    }
                }
            }
        }

        eprintln!("  ✅ Row {} encoded ({} bytes)", row_idx, buffer.len());
        buffer
    }

    fn create_schema(schema_id: u64, field_info: Vec<FieldDef>) -> CentralSchemaEntry {
        // Use "OtlpLogRecord" to match otlp_encoder.rs:506 for Geneva backend compatibility
        let schema = BondEncodedSchema::from_fields("OtlpLogRecord", "telemetry", field_info);
        let schema_bytes = schema.as_bytes();
        let schema_md5 = md5::compute(schema_bytes).0;

        CentralSchemaEntry {
            id: schema_id,
            md5: schema_md5,
            schema,
        }
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

impl Default for ArrowLogEncoder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::payload_encoder::otlp_encoder::OtlpEncoder;
    use arrow::array::{
        ArrayRef, DictionaryArray, Int32Array, RecordBatch, StringArray, StructArray,
        TimestampNanosecondArray, UInt8Array, UInt16Array,
    };
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    use opentelemetry_proto::tonic::logs::v1::LogRecord;
    use std::sync::Arc;

    /// Create an OTLP LogRecord with specific test data
    fn create_test_otlp_log() -> LogRecord {
        LogRecord {
            time_unix_nano: 1700000000000000000,
            observed_time_unix_nano: 1700000000000000000,
            severity_number: 9,
            severity_text: "INFO".to_string(),
            body: Some(AnyValue {
                value: Some(Value::StringValue("Test log message".to_string())),
            }),
            attributes: vec![
                KeyValue {
                    key: "code.file.path".to_string(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue("/test/file.py".to_string())),
                    }),
                },
                KeyValue {
                    key: "code.function.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue("test_function".to_string())),
                    }),
                },
                KeyValue {
                    key: "code.line.number".to_string(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue("42".to_string())),
                    }),
                },
            ],
            event_name: "".to_string(), // Empty to match OTAP (no name field)
            trace_id: vec![],
            span_id: vec![],
            flags: 0,
            dropped_attributes_count: 0,
        }
    }

    /// Create Arrow RecordBatch matching otel-arrow `LogsArrays` expectations
    ///
    /// Required columns (see `otel-arrow-rust/src/otlp/logs.rs`):
    ///   - id: UInt16
    ///   - time_unix_nano: Timestamp(Nanosecond)
    ///   - severity_number: Int32
    ///   - severity_text: Utf8
    ///   - body: Struct {
    ///         attribute_type: UInt8,
    ///         attribute_str: Utf8 (optional)
    ///     }
    /// We only populate the minimal set used by ArrowLogEncoder parity test.
    fn create_test_arrow_batch() -> (RecordBatch, RecordBatch) {
        use otel_arrow_rust::schema::{consts, FieldExt};
        use arrow::datatypes::{Fields};

        // Single row id
        let id_array = Arc::new(UInt16Array::from(vec![0u16]));

        // Timestamp
        let time_array =
            Arc::new(TimestampNanosecondArray::from(vec![1700000000000000000i64]));

        // Severity number (plain Int32, NOT dictionary: LogsArrays expects Int32)
        let severity_num_array = Arc::new(Int32Array::from(vec![9i32]));

        // Severity text (plain Utf8)
        let severity_text_array = Arc::new(StringArray::from(vec!["INFO"]));

        // Body struct: attribute_type (1 = string), attribute_str = "Test log message"
        let body_struct_fields = Fields::from(vec![
            Field::new(consts::ATTRIBUTE_TYPE, DataType::UInt8, false),
            Field::new(consts::ATTRIBUTE_STR, DataType::Utf8, true),
        ]);
        let body_attr_type = Arc::new(UInt8Array::from(vec![1u8])); // 1 => string
        let body_attr_str = Arc::new(StringArray::from(vec!["Test log message"]));
        let body_struct = Arc::new(StructArray::new(
            body_struct_fields.clone(),
            vec![body_attr_type, body_attr_str],
            None,
        ));

        // Build schema (order not critical but keep logical grouping)
        let schema = Arc::new(Schema::new(vec![
            Field::new(consts::ID, DataType::UInt16, true).with_plain_encoding(),
            Field::new(
                consts::TIME_UNIX_NANO,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new(consts::SEVERITY_NUMBER, DataType::Int32, true),
            Field::new(consts::SEVERITY_TEXT, DataType::Utf8, true),
            Field::new(consts::BODY, body_struct.data_type().clone(), true),
        ]));

        let logs_batch = RecordBatch::try_new(
            schema,
            vec![
                id_array,
                time_array,
                severity_num_array,
                severity_text_array,
                body_struct,
            ],
        )
        .expect("create logs batch");

        // Create log attributes batch (parent_id=0 for all three attributes)
        let parent_id = Arc::new(UInt16Array::from(vec![0u16, 0u16, 0u16]));

        // Key dictionary
        let key_keys = UInt8Array::from(vec![0u8, 1u8, 2u8]);
        let key_values =
            StringArray::from(vec!["code.file.path", "code.function.name", "code.line.number"]);
        let key_dict: ArrayRef =
            Arc::new(DictionaryArray::try_new(key_keys, Arc::new(key_values)).unwrap());

        // Types (all string = 1)
        let types = Arc::new(UInt8Array::from(vec![1u8, 1u8, 1u8]));

        // String values dictionary
        let str_keys = UInt16Array::from(vec![0u16, 1u16, 2u16]);
        let str_values =
            StringArray::from(vec!["/test/file.py", "test_function", "42"]);
        let str_dict: ArrayRef =
            Arc::new(DictionaryArray::try_new(str_keys, Arc::new(str_values)).unwrap());

        let attrs_schema = Arc::new(Schema::new(vec![
            Field::new("parent_id", DataType::UInt16, true),
            Field::new(
                "key",
                DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
                true,
            ),
            Field::new("type", DataType::UInt8, true),
            Field::new(
                "str",
                DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8)),
                true,
            ),
        ]));

        let attrs_batch = RecordBatch::try_new(
            attrs_schema,
            vec![parent_id, key_dict, types, str_dict],
        )
        .expect("create attrs batch");

        (logs_batch, attrs_batch)
    }

    #[test]
    fn test_compare_otlp_and_arrow_encoding() {
        let metadata = "namespace=testNamespace/eventVersion=Ver1v0/tenant=T/role=R/roleinstance=RI";

        // Encode with OTLP encoder
        let otlp_encoder = OtlpEncoder::new();
        let otlp_log = create_test_otlp_log();
        let otlp_result = otlp_encoder
            .encode_log_batch([otlp_log].iter(), metadata)
            .unwrap();

        // Encode with Arrow encoder
        let mut arrow_encoder = ArrowLogEncoder::new();
        let (logs_batch, attrs_batch) = create_test_arrow_batch();
        let arrow_result = arrow_encoder
            .encode_arrow_batch(&logs_batch, Some(&attrs_batch), None, metadata)
            .unwrap();

        // Both should produce exactly 1 batch
        assert_eq!(otlp_result.len(), 1, "OTLP should produce 1 batch");
        assert_eq!(arrow_result.len(), 1, "Arrow should produce 1 batch");

        let otlp_batch = &otlp_result[0];
        let arrow_batch = &arrow_result[0];

        // Compare event names
        assert_eq!(
            otlp_batch.event_name, arrow_batch.event_name,
            "Event names should match"
        );

        // Compare schema IDs
        println!("\n=== METADATA COMPARISON ===");
        println!("OTLP schema_ids: {}", otlp_batch.metadata.schema_ids);
        println!("Arrow schema_ids: {}", arrow_batch.metadata.schema_ids);
        println!("OTLP start_time: {}", otlp_batch.metadata.start_time);
        println!("Arrow start_time: {}", arrow_batch.metadata.start_time);
        println!("OTLP end_time: {}", otlp_batch.metadata.end_time);
        println!("Arrow end_time: {}", arrow_batch.metadata.end_time);

        assert_eq!(
            otlp_batch.metadata.schema_ids, arrow_batch.metadata.schema_ids,
            "Schema IDs should match"
        );

        // Compare compressed data lengths (should be identical)
        println!("\n=== COMPRESSED PAYLOAD COMPARISON ===");
        println!("OTLP compressed size: {}", otlp_batch.data.len());
        println!("Arrow compressed size: {}", arrow_batch.data.len());

        // Always print compressed hex dumps for comparison
        println!("\n--- OTLP Compressed Hex (first 200 bytes) ---");
        for (i, chunk) in otlp_batch.data.chunks(16).take(13).enumerate() {
            print!("{:04x}: ", i * 16);
            for byte in chunk {
                print!("{:02x} ", byte);
            }
            println!();
        }

        println!("\n--- Arrow Compressed Hex (first 200 bytes) ---");
        for (i, chunk) in arrow_batch.data.chunks(16).take(13).enumerate() {
            print!("{:04x}: ", i * 16);
            for byte in chunk {
                print!("{:02x} ", byte);
            }
            println!();
        }

        // Compare compressed bytes directly
        if otlp_batch.data != arrow_batch.data {
            println!("\n❌ Compressed bytes DIFFER!");
            // Find first difference
            for (i, (o, a)) in otlp_batch.data.iter().zip(arrow_batch.data.iter()).enumerate() {
                if o != a {
                    println!("First difference at byte {}: OTLP={:02x} Arrow={:02x}", i, o, a);
                    break;
                }
            }
        } else {
            println!("\n✅ Compressed bytes match perfectly!");
        }

        // Decompress and compare
        use crate::payload_encoder::lz4_chunked_compression::lz4_chunked_decompression;

        let otlp_decompressed = lz4_chunked_decompression(&otlp_batch.data).unwrap();
        let arrow_decompressed = lz4_chunked_decompression(&arrow_batch.data).unwrap();

        println!("\nOTLP decompressed size: {}", otlp_decompressed.len());
        println!("Arrow decompressed size: {}", arrow_decompressed.len());

        // Print hex comparison for debugging
        if otlp_decompressed != arrow_decompressed {
            println!("\n❌ MISMATCH DETECTED!");
            println!("\nOTLP bytes (first 200):");
            for (i, chunk) in otlp_decompressed.chunks(16).take(13).enumerate() {
                print!("{:04x}: ", i * 16);
                for byte in chunk {
                    print!("{:02x} ", byte);
                }
                println!();
            }

            println!("\nArrow bytes (first 200):");
            for (i, chunk) in arrow_decompressed.chunks(16).take(13).enumerate() {
                print!("{:04x}: ", i * 16);
                for byte in chunk {
                    print!("{:02x} ", byte);
                }
                println!();
            }

            // Find first difference
            for (i, (o, a)) in otlp_decompressed.iter().zip(arrow_decompressed.iter()).enumerate() {
                if o != a {
                    println!("\nFirst difference at byte {}: OTLP={:02x} Arrow={:02x}", i, o, a);
                    break;
                }
            }
        } else {
            println!("\n✅ Encodings match perfectly!");
        }

        // Final assertions
        assert_eq!(
            otlp_batch.data, arrow_batch.data,
            "Compressed payloads should be byte-for-byte identical"
        );

        assert_eq!(
            otlp_decompressed, arrow_decompressed,
            "Decompressed Bond payloads should be identical"
        );
    }
}
