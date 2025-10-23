//! Arrow Log Encoder for Geneva - Direct Arrow → Bond encoding
//!
//! This encoder mirrors the otlp_encoder.rs pattern but works directly with Arrow RecordBatches
//! instead of protobuf LogRecord structs. It iterates row-by-row over Arrow columns and encodes
//! each row directly to Bond format without intermediate allocations.

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
#[derive(Clone)]
pub struct ArrowLogEncoder {
    env_name: String,
    env_ver: String,
}

impl ArrowLogEncoder {
    pub fn new() -> Self {
        Self {
            env_name: "TestEnv".to_string(),
            env_ver: "4.0".to_string(),
        }
    }

    pub fn with_env(env_name: String, env_ver: String) -> Self {
        Self { env_name, env_ver }
    }

    /// Encode Arrow RecordBatches directly to Bond format (mirroring otlp_encoder.rs pattern)
    ///
    /// This function works like encode_log_batch() in otlp_encoder.rs but iterates over
    /// Arrow RecordBatch rows instead of protobuf LogRecords.
    pub fn encode_arrow_batch(
        &self,
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

        // Get Arrow column references (zero-copy) - analogous to log_record fields in otlp_encoder
        let arrow_columns = ArrowColumns::extract(logs_batch)?;

        // Build attribute lookup maps (only once, not per-row)
        let log_attrs_map = if let Some(attrs_batch) = log_attrs_batch {
            let map = Self::build_log_attrs_map(attrs_batch)?;
            eprintln!("🔍 DEBUG: Log attributes map keys: {:?}", map.keys().collect::<Vec<_>>());
            for (key, attrs) in &map {
                eprintln!("  parent_id {}: {} attributes", key, attrs.len());
                for (attr_key, _, _) in attrs {
                    eprintln!("    - {}", attr_key);
                }
            }
            map
        } else {
            HashMap::new()
        };

        let (resource_attrs_map, resource_ids) = if let Some(res_attrs_batch) = resource_attrs_batch {
            let map = Self::build_resource_attrs_map(res_attrs_batch)?;
            let ids = arrow_columns.extract_resource_ids(num_rows);
            (map, ids)
        } else {
            (HashMap::new(), vec![None; num_rows])
        };

        // Iterate over each log record row (analogous to `for log_record in logs` in otlp_encoder)
        for row_idx in 0..num_rows {
            // Get timestamp (analogous to log_record.time_unix_nano in otlp_encoder)
            let timestamp = arrow_columns.get_timestamp(row_idx);

            // Use default event name (analogous to log_record.event_name in otlp_encoder)
            let event_name_str = "Log";

            // Get the log's ID from the logs batch - this is what maps to parent_id in attributes
            let log_id = arrow_columns.get_id(row_idx);
            
            if row_idx < 3 {
                eprintln!("🔍 Row {}: log_id = {:?}", row_idx, log_id);
                if let Some(id) = log_id {
                    if let Some(attrs) = log_attrs_map.get(&id) {
                        eprintln!("  ✅ Found {} attributes for log_id {}", attrs.len(), id);
                    } else {
                        eprintln!("  ❌ No attributes found for log_id {}", id);
                    }
                }
            }

            // 1. Determine fields and schema ID (analogous to determine_fields_and_schema_id in otlp_encoder)
            let (field_defs, schema_id) = self.determine_arrow_fields_and_schema_id(
                log_id,
                row_idx,
                &arrow_columns,
                &log_attrs_map,
                &resource_ids,
                &resource_attrs_map,
                event_name_str,
            );

            // 2. Encode row directly to Bond (analogous to write_row_data in otlp_encoder)
            let row_buffer = self.write_arrow_row_data(
                log_id,
                row_idx,
                &arrow_columns,
                &log_attrs_map,
                log_attrs_batch,
                &resource_ids,
                &resource_attrs_map,
                resource_attrs_batch,
                &field_defs,
            );

            let level = arrow_columns.get_severity_number(row_idx).unwrap_or(9) as u8;

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

    /// Determine fields and schema ID from Arrow row (analogous to determine_fields_and_schema_id in otlp_encoder)
    fn determine_arrow_fields_and_schema_id(
        &self,
        log_id: Option<u16>,
        row_idx: usize,
        columns: &ArrowColumns,
        log_attrs_map: &std::collections::HashMap<u16, Vec<(String, u8, usize)>>,
        _resource_ids: &[Option<u16>],
        _resource_attrs_map: &std::collections::HashMap<u16, Vec<(String, String)>>,
        event_name: &str,
    ) -> (Vec<FieldDef>, u64) {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut fields = Vec::with_capacity(10 + log_attrs_map.get(&(row_idx as u16)).map(|v| v.len()).unwrap_or(0));
        let mut hasher = DefaultHasher::new();
        event_name.hash(&mut hasher);

        // Part A - Always present fields
        fields.push((Cow::Borrowed(FIELD_ENV_NAME), BondDataType::BT_STRING));
        fields.push((Cow::Borrowed(FIELD_ENV_VER), BondDataType::BT_STRING));
        fields.push((Cow::Borrowed(FIELD_TIMESTAMP), BondDataType::BT_STRING));
        fields.push((Cow::Borrowed(FIELD_ENV_TIME), BondDataType::BT_STRING));

        // Part A extension - Conditional fields
        if columns.has_trace_id(row_idx) {
            fields.push((Cow::Borrowed(FIELD_TRACE_ID), BondDataType::BT_STRING));
        }
        if columns.has_span_id(row_idx) {
            fields.push((Cow::Borrowed(FIELD_SPAN_ID), BondDataType::BT_STRING));
        }
        if columns.has_flags(row_idx) {
            fields.push((Cow::Borrowed(FIELD_TRACE_FLAGS), BondDataType::BT_UINT32));
        }

        // Part B - Core log fields
        // Note: FIELD_SEVERITY_NUMBER is ALWAYS included (matches otlp_encoder.rs:367)
        fields.push((Cow::Borrowed(FIELD_SEVERITY_NUMBER), BondDataType::BT_INT32));
        if columns.has_severity_text(row_idx) {
            fields.push((Cow::Borrowed(FIELD_SEVERITY_TEXT), BondDataType::BT_STRING));
        }
        if columns.has_body(row_idx) {
            fields.push((Cow::Borrowed(FIELD_BODY), BondDataType::BT_STRING));
        }

        // Part C - Dynamic log attributes
        // Note: Only log-level attributes are included, NOT resource attributes
        // This matches otlp_encoder.rs behavior (line 380-391)
        if let Some(id) = log_id {
            if let Some(attrs) = log_attrs_map.get(&id) {
                for (key, _, _) in attrs {
                    fields.push((Cow::Owned(key.clone()), BondDataType::BT_STRING)); // Simplified: assume string type
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

    /// Write Arrow row data directly to Bond (analogous to write_row_data in otlp_encoder)
    fn write_arrow_row_data(
        &self,
        log_id: Option<u16>,
        row_idx: usize,
        columns: &ArrowColumns,
        log_attrs_map: &std::collections::HashMap<u16, Vec<(String, u8, usize)>>,
        log_attrs_batch: Option<&RecordBatch>,
        _resource_ids: &[Option<u16>],
        _resource_attrs_map: &std::collections::HashMap<u16, Vec<(String, String)>>,
        _resource_attrs_batch: Option<&RecordBatch>,
        fields: &[FieldDef],
    ) -> Vec<u8> {
        let mut buffer = Vec::with_capacity(fields.len() * 50);

        eprintln!("🔍 DEBUG [Row {}] Starting Bond encoding", row_idx);

        let formatted_timestamp = Self::format_timestamp(columns.get_timestamp(row_idx));

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
                    if let Some(trace_id) = columns.get_trace_id(row_idx) {
                        eprintln!("  📝 {} = {}", FIELD_TRACE_ID, trace_id);
                        BondWriter::write_string(&mut buffer, &trace_id);
                    }
                }
                FIELD_SPAN_ID => {
                    if let Some(span_id) = columns.get_span_id(row_idx) {
                        eprintln!("  📝 {} = {}", FIELD_SPAN_ID, span_id);
                        BondWriter::write_string(&mut buffer, &span_id);
                    }
                }
                FIELD_TRACE_FLAGS => {
                    if let Some(flags) = columns.get_flags(row_idx) {
                        eprintln!("  📝 {} = {}", FIELD_TRACE_FLAGS, flags);
                        BondWriter::write_numeric(&mut buffer, flags);
                    }
                }
                FIELD_SEVERITY_NUMBER => {
                    let num = columns.get_severity_number(row_idx).unwrap_or(0);
                    eprintln!("  📝 {} = {}", FIELD_SEVERITY_NUMBER, num);
                    BondWriter::write_numeric(&mut buffer, num);
                }
                FIELD_SEVERITY_TEXT => {
                    if let Some(text) = columns.get_severity_text(row_idx) {
                        eprintln!("  📝 {} = {}", FIELD_SEVERITY_TEXT, text);
                        BondWriter::write_string(&mut buffer, text);
                    }
                }
                FIELD_BODY => {
                    if let Some(body) = columns.get_body(row_idx) {
                        eprintln!("  📝 {} = {}", FIELD_BODY, body);
                        BondWriter::write_string(&mut buffer, body);
                    }
                }
                _ => {
                    // Handle log attributes only (NOT resource attributes)
                    // Use log_id to look up attributes, not row_idx
                    if row_idx < 2 {
                        eprintln!("  🔍 Looking for field '{}' in attributes", field.name);
                    }
                    if let Some(id) = log_id {
                        if let Some(attrs) = log_attrs_map.get(&id) {
                            if row_idx < 2 {
                                eprintln!("    Found {} attributes for id {}", attrs.len(), id);
                            }
                            for (key, attr_type, attr_idx) in attrs {
                                if key == field.name.as_ref() {
                                    if let Some(value) = Self::extract_attribute_value(
                                        log_attrs_batch.unwrap(),
                                        *attr_type,
                                        *attr_idx,
                                    ) {
                                        eprintln!("  📝 [LOG_ATTR] {} = {}", key, value);
                                        BondWriter::write_string(&mut buffer, &value);
                                    }
                                    break;
                                }
                            }
                        } else if row_idx < 2 {
                            eprintln!("    ❌ No attributes found for id {}", id);
                        }
                    }
                    // Resource attributes are NOT written to individual log records
                }
            }
        }

        eprintln!("  ✅ Row {} encoded ({} bytes)", row_idx, buffer.len());
        buffer
    }

    fn build_log_attrs_map(
        attrs_batch: &RecordBatch,
    ) -> Result<std::collections::HashMap<u16, Vec<(String, u8, usize)>>, String> {
        use arrow::array::{DictionaryArray, StringArray, UInt16Array, UInt8Array};
        use std::collections::HashMap;

        let mut map: HashMap<u16, Vec<(String, u8, usize)>> = HashMap::new();

        let parent_id_col = attrs_batch
            .column_by_name("parent_id")
            .ok_or("parent_id not found")?
            .as_any()
            .downcast_ref::<UInt16Array>()
            .ok_or("parent_id not UInt16Array")?;

        let key_col = attrs_batch.column_by_name("key").ok_or("key not found")?;
        let type_col = attrs_batch
            .column_by_name("type")
            .ok_or("type not found")?
            .as_any()
            .downcast_ref::<UInt8Array>()
            .ok_or("type not UInt8Array")?;

        for attr_idx in 0..attrs_batch.num_rows() {
            if parent_id_col.is_null(attr_idx) {
                continue;
            }

            let parent_id = parent_id_col.value(attr_idx);
            let key = key_col
                .as_any()
                .downcast_ref::<DictionaryArray<arrow::datatypes::UInt8Type>>()
                .and_then(|dict| {
                    if dict.is_null(attr_idx) {
                        None
                    } else {
                        let key_idx = dict.keys().value(attr_idx);
                        dict.values()
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .map(|vals| vals.value(key_idx as usize).to_string())
                    }
                });

            if let Some(key) = key {
                let attr_type = type_col.value(attr_idx);
                map.entry(parent_id)
                    .or_insert_with(Vec::new)
                    .push((key, attr_type, attr_idx));
            }
        }

        Ok(map)
    }

    fn build_resource_attrs_map(
        res_attrs_batch: &RecordBatch,
    ) -> Result<std::collections::HashMap<u16, Vec<(String, String)>>, String> {
        use arrow::array::{DictionaryArray, StringArray, UInt16Array};
        use std::collections::HashMap;

        let mut map: HashMap<u16, Vec<(String, String)>> = HashMap::new();

        let parent_id_col = res_attrs_batch
            .column_by_name("parent_id")
            .ok_or("parent_id not found")?
            .as_any()
            .downcast_ref::<UInt16Array>()
            .ok_or("parent_id not UInt16Array")?;

        let key_col = res_attrs_batch.column_by_name("key").ok_or("key not found")?;
        let str_col = res_attrs_batch.column_by_name("str");

        for attr_idx in 0..res_attrs_batch.num_rows() {
            if parent_id_col.is_null(attr_idx) {
                continue;
            }

            let resource_id = parent_id_col.value(attr_idx);

            let key = key_col
                .as_any()
                .downcast_ref::<DictionaryArray<arrow::datatypes::UInt8Type>>()
                .and_then(|dict| {
                    if dict.is_null(attr_idx) {
                        None
                    } else {
                        let key_idx = dict.keys().value(attr_idx);
                        dict.values()
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .map(|vals| vals.value(key_idx as usize).to_string())
                    }
                });

            let value = str_col.and_then(|col| {
                col.as_any()
                    .downcast_ref::<DictionaryArray<arrow::datatypes::UInt16Type>>()
                    .and_then(|dict| {
                        if dict.is_null(attr_idx) {
                            None
                        } else {
                            let val_idx = dict.keys().value(attr_idx);
                            dict.values()
                                .as_any()
                                .downcast_ref::<StringArray>()
                                .map(|vals| vals.value(val_idx as usize).to_string())
                        }
                    })
            });

            if let (Some(key), Some(value)) = (key, value) {
                map.entry(resource_id)
                    .or_insert_with(Vec::new)
                    .push((key, value));
            }
        }

        Ok(map)
    }

    fn extract_attribute_value(
        attrs_batch: &RecordBatch,
        attr_type: u8,
        attr_idx: usize,
    ) -> Option<String> {
        use arrow::array::{DictionaryArray, Int64Array, StringArray};

        match attr_type {
            1 => {
                // String type - try both UInt8 and UInt16 dictionary keys
                attrs_batch.column_by_name("str").and_then(|col| {
                    // Try UInt8 keys first (most common in OTAP)
                    col.as_any()
                        .downcast_ref::<DictionaryArray<arrow::datatypes::UInt8Type>>()
                        .and_then(|dict| {
                            if dict.is_null(attr_idx) {
                                None
                            } else {
                                let val_idx = dict.keys().value(attr_idx);
                                dict.values()
                                    .as_any()
                                    .downcast_ref::<StringArray>()
                                    .map(|vals| vals.value(val_idx as usize).to_string())
                            }
                        })
                        .or_else(|| {
                            // Fallback to UInt16 keys
                            col.as_any()
                                .downcast_ref::<DictionaryArray<arrow::datatypes::UInt16Type>>()
                                .and_then(|dict| {
                                    if dict.is_null(attr_idx) {
                                        None
                                    } else {
                                        let val_idx = dict.keys().value(attr_idx);
                                        dict.values()
                                            .as_any()
                                            .downcast_ref::<StringArray>()
                                            .map(|vals| vals.value(val_idx as usize).to_string())
                                    }
                                })
                        })
                })
            }
            2 => {
                // Int type - try both UInt8 and UInt16 dictionary keys
                attrs_batch.column_by_name("int").and_then(|col| {
                    // Try UInt8 keys first
                    col.as_any()
                        .downcast_ref::<DictionaryArray<arrow::datatypes::UInt8Type>>()
                        .and_then(|dict| {
                            if dict.is_null(attr_idx) {
                                None
                            } else {
                                let val_idx = dict.keys().value(attr_idx);
                                dict.values()
                                    .as_any()
                                    .downcast_ref::<Int64Array>()
                                    .map(|vals| vals.value(val_idx as usize).to_string())
                            }
                        })
                        .or_else(|| {
                            // Fallback to UInt16 keys
                            col.as_any()
                                .downcast_ref::<DictionaryArray<arrow::datatypes::UInt16Type>>()
                                .and_then(|dict| {
                                    if dict.is_null(attr_idx) {
                                        None
                                    } else {
                                        let val_idx = dict.keys().value(attr_idx);
                                        dict.values()
                                            .as_any()
                                            .downcast_ref::<Int64Array>()
                                            .map(|vals| vals.value(val_idx as usize).to_string())
                                    }
                                })
                        })
                })
            }
            _ => None,
        }
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

/// Helper struct to hold Arrow column references (zero-copy)
struct ArrowColumns<'a> {
    id: Option<&'a arrow::array::UInt16Array>,
    time_unix_nano: Option<&'a arrow::array::TimestampNanosecondArray>,
    observed_time_unix_nano: Option<&'a arrow::array::TimestampNanosecondArray>,
    severity_number: Option<&'a dyn Array>,
    severity_text: Option<&'a dyn Array>,
    body: Option<&'a dyn Array>,
    trace_id: Option<&'a arrow::array::BinaryArray>,
    span_id: Option<&'a arrow::array::BinaryArray>,
    flags: Option<&'a arrow::array::UInt32Array>,
    resource: Option<&'a arrow::array::StructArray>,
}

impl<'a> ArrowColumns<'a> {
    fn extract(batch: &'a RecordBatch) -> Result<Self, String> {
        use arrow::array::{BinaryArray, StructArray, TimestampNanosecondArray, UInt16Array, UInt32Array};

        Ok(Self {
            id: batch
                .column_by_name("id")
                .and_then(|col| col.as_any().downcast_ref::<UInt16Array>()),
            time_unix_nano: batch
                .column_by_name("time_unix_nano")
                .and_then(|col| col.as_any().downcast_ref::<TimestampNanosecondArray>()),
            observed_time_unix_nano: batch
                .column_by_name("observed_time_unix_nano")
                .and_then(|col| col.as_any().downcast_ref::<TimestampNanosecondArray>()),
            severity_number: batch.column_by_name("severity_number").map(|col| col.as_ref()),
            severity_text: batch.column_by_name("severity_text").map(|col| col.as_ref()),
            body: batch.column_by_name("body").map(|col| col.as_ref()),
            trace_id: batch
                .column_by_name("trace_id")
                .and_then(|col| col.as_any().downcast_ref::<BinaryArray>()),
            span_id: batch
                .column_by_name("span_id")
                .and_then(|col| col.as_any().downcast_ref::<BinaryArray>()),
            flags: batch
                .column_by_name("flags")
                .and_then(|col| col.as_any().downcast_ref::<UInt32Array>()),
            resource: batch
                .column_by_name("resource")
                .and_then(|col| col.as_any().downcast_ref::<StructArray>()),
        })
    }

    fn get_id(&self, row_idx: usize) -> Option<u16> {
        self.id.and_then(|arr| {
            if arr.is_null(row_idx) {
                None
            } else {
                Some(arr.value(row_idx))
            }
        })
    }

    fn get_timestamp(&self, row_idx: usize) -> u64 {
        self.time_unix_nano
            .and_then(|arr| {
                if arr.is_null(row_idx) {
                    None
                } else {
                    Some(arr.value(row_idx) as u64)
                }
            })
            .or_else(|| {
                self.observed_time_unix_nano.and_then(|arr| {
                    if arr.is_null(row_idx) {
                        None
                    } else {
                        Some(arr.value(row_idx) as u64)
                    }
                })
            })
            .unwrap_or(0)
    }

    fn has_trace_id(&self, row_idx: usize) -> bool {
        self.trace_id
            .map(|arr| !arr.is_null(row_idx) && !arr.value(row_idx).is_empty())
            .unwrap_or(false)
    }

    fn get_trace_id(&self, row_idx: usize) -> Option<String> {
        self.trace_id.and_then(|arr| {
            if arr.is_null(row_idx) {
                None
            } else {
                Some(hex::encode(arr.value(row_idx)))
            }
        })
    }

    fn has_span_id(&self, row_idx: usize) -> bool {
        self.span_id
            .map(|arr| !arr.is_null(row_idx) && !arr.value(row_idx).is_empty())
            .unwrap_or(false)
    }

    fn get_span_id(&self, row_idx: usize) -> Option<String> {
        self.span_id.and_then(|arr| {
            if arr.is_null(row_idx) {
                None
            } else {
                Some(hex::encode(arr.value(row_idx)))
            }
        })
    }

    fn has_flags(&self, row_idx: usize) -> bool {
        self.flags
            .map(|arr| !arr.is_null(row_idx) && arr.value(row_idx) != 0)
            .unwrap_or(false)
    }

    fn get_flags(&self, row_idx: usize) -> Option<u32> {
        self.flags.and_then(|arr| {
            if arr.is_null(row_idx) {
                None
            } else {
                Some(arr.value(row_idx))
            }
        })
    }

    fn has_severity_number(&self, row_idx: usize) -> bool {
        self.severity_number
            .map(|arr| !arr.is_null(row_idx))
            .unwrap_or(false)
    }

    fn get_severity_number(&self, row_idx: usize) -> Option<i32> {
        use arrow::array::DictionaryArray;

        self.severity_number.and_then(|arr| {
            arr.as_any()
                .downcast_ref::<DictionaryArray<arrow::datatypes::UInt8Type>>()
                .and_then(|dict| {
                    if dict.is_null(row_idx) {
                        None
                    } else {
                        let key = dict.keys().value(row_idx);
                        dict.values()
                            .as_any()
                            .downcast_ref::<arrow::array::Int32Array>()
                            .map(|vals| vals.value(key as usize))
                    }
                })
        })
    }

    fn has_severity_text(&self, row_idx: usize) -> bool {
        self.severity_text
            .map(|arr| !arr.is_null(row_idx))
            .unwrap_or(false)
    }

    fn get_severity_text(&self, row_idx: usize) -> Option<&str> {
        use arrow::array::DictionaryArray;

        self.severity_text.and_then(|arr| {
            arr.as_any()
                .downcast_ref::<DictionaryArray<arrow::datatypes::UInt8Type>>()
                .and_then(|dict| {
                    if dict.is_null(row_idx) {
                        None
                    } else {
                        let key = dict.keys().value(row_idx);
                        dict.values()
                            .as_any()
                            .downcast_ref::<arrow::array::StringArray>()
                            .map(|vals| vals.value(key as usize))
                    }
                })
        })
    }

    fn has_body(&self, row_idx: usize) -> bool {
        self.body.map(|arr| !arr.is_null(row_idx)).unwrap_or(false)
    }

    fn get_body(&self, row_idx: usize) -> Option<&str> {
        use arrow::array::{DictionaryArray, StringArray, StructArray, UInt8Array};

        self.body.and_then(|arr| {
            arr.as_any().downcast_ref::<StructArray>().and_then(|st| {
                if st.is_null(row_idx) {
                    None
                } else {
                    // First get the type field to determine which field to read
                    let type_field = st.column_by_name("type")
                        .and_then(|col| col.as_any().downcast_ref::<UInt8Array>())
                        .and_then(|arr| {
                            if arr.is_null(row_idx) {
                                None
                            } else {
                                Some(arr.value(row_idx))
                            }
                        });
                    
                    // Type 1 = string
                    if type_field == Some(1) {
                        // Try UInt8 dictionary keys first (most common)
                        st.column_by_name("str")
                            .and_then(|col| {
                                col.as_any()
                                    .downcast_ref::<DictionaryArray<arrow::datatypes::UInt8Type>>()
                                    .and_then(|dict| {
                                        if dict.is_null(row_idx) {
                                            None
                                        } else {
                                            let key = dict.keys().value(row_idx);
                                            dict.values()
                                                .as_any()
                                                .downcast_ref::<StringArray>()
                                                .map(|vals| vals.value(key as usize))
                                        }
                                    })
                            })
                            .or_else(|| {
                                // Fallback to UInt16 dictionary keys
                                st.column_by_name("str")
                                    .and_then(|col| {
                                        col.as_any()
                                            .downcast_ref::<DictionaryArray<arrow::datatypes::UInt16Type>>()
                                            .and_then(|dict| {
                                                if dict.is_null(row_idx) {
                                                    None
                                                } else {
                                                    let key = dict.keys().value(row_idx);
                                                    dict.values()
                                                        .as_any()
                                                        .downcast_ref::<StringArray>()
                                                        .map(|vals| vals.value(key as usize))
                                                }
                                            })
                                    })
                            })
                    } else {
                        None
                    }
                }
            })
        })
    }

    fn extract_resource_ids(&self, num_rows: usize) -> Vec<Option<u16>> {
        if let Some(res_col) = self.resource {
            // Get the "id" column once, outside the loop
            if let Some(id_col) = res_col.column_by_name("id") {
                if let Some(id_arr) = id_col.as_any().downcast_ref::<arrow::array::UInt16Array>() {
                    // Fast path: we have the id column
                    return (0..num_rows)
                        .map(|row_idx| {
                            if res_col.is_null(row_idx) || id_arr.is_null(row_idx) {
                                None
                            } else {
                                Some(id_arr.value(row_idx))
                            }
                        })
                        .collect();
                }
            }
            
            // If we couldn't find the id column, return all None
            eprintln!("⚠️  WARNING: resource.id column not found. Available columns: {:?}", 
                res_col.column_names());
            vec![None; num_rows]
        } else {
            vec![None; num_rows]
        }
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

    /// Create Arrow RecordBatch with the same test data
    fn create_test_arrow_batch() -> (RecordBatch, RecordBatch) {
        // Create main logs batch
        let time_array = Arc::new(TimestampNanosecondArray::from(vec![1700000000000000000i64]));

        // Severity number: Dictionary(UInt8, Int32)
        let severity_keys = UInt8Array::from(vec![0u8]);
        let severity_values = Int32Array::from(vec![9i32]);
        let severity_array: ArrayRef = Arc::new(
            DictionaryArray::try_new(severity_keys, Arc::new(severity_values)).unwrap()
        );

        // Severity text: Dictionary(UInt8, Utf8)
        let severity_text_keys = UInt8Array::from(vec![0u8]);
        let severity_text_values = StringArray::from(vec!["INFO"]);
        let severity_text_array: ArrayRef = Arc::new(
            DictionaryArray::try_new(severity_text_keys, Arc::new(severity_text_values)).unwrap()
        );

        // Body: Struct with str field
        let body_str_keys = UInt16Array::from(vec![0u16]);
        let body_str_values = StringArray::from(vec!["Test log message"]);
        let body_str_dict = Arc::new(
            DictionaryArray::try_new(body_str_keys, Arc::new(body_str_values)).unwrap()
        );

        let body_struct = StructArray::from(vec![(
            Arc::new(Field::new("str", DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8)), true)),
            body_str_dict as ArrayRef,
        )]);

        let schema = Arc::new(Schema::new(vec![
            Field::new("time_unix_nano", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            Field::new("severity_number", DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Int32)), true),
            Field::new("severity_text", DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)), true),
            Field::new("body", body_struct.data_type().clone(), true),
        ]));

        let logs_batch = RecordBatch::try_new(
            schema,
            vec![
                time_array,
                severity_array,
                severity_text_array,
                Arc::new(body_struct),
            ],
        )
        .unwrap();

        // Create log attributes batch
        let parent_id = UInt16Array::from(vec![0u16, 0u16, 0u16]);

        let key_keys = UInt8Array::from(vec![0u8, 1u8, 2u8]);
        let key_values = StringArray::from(vec!["code.file.path", "code.function.name", "code.line.number"]);
        let key_dict = Arc::new(
            DictionaryArray::try_new(key_keys, Arc::new(key_values)).unwrap()
        );

        let types = UInt8Array::from(vec![1u8, 1u8, 1u8]); // All strings (type 1)

        let str_keys = UInt16Array::from(vec![0u16, 1u16, 2u16]);
        let str_values = StringArray::from(vec!["/test/file.py", "test_function", "42"]);
        let str_dict = Arc::new(
            DictionaryArray::try_new(str_keys, Arc::new(str_values)).unwrap()
        );

        let attrs_schema = Arc::new(Schema::new(vec![
            Field::new("parent_id", DataType::UInt16, true),
            Field::new("key", DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)), true),
            Field::new("type", DataType::UInt8, true),
            Field::new("str", DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8)), true),
        ]));

        let attrs_batch = RecordBatch::try_new(
            attrs_schema,
            vec![
                Arc::new(parent_id),
                key_dict,
                Arc::new(types),
                str_dict,
            ],
        )
        .unwrap();

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
        let arrow_encoder = ArrowLogEncoder::new();
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
