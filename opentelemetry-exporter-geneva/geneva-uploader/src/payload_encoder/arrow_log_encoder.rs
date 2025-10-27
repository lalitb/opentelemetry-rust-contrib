//! Arrow Log Encoder for Geneva - Arrow → Bond encoding using ArrowLogsData views
//!
//! This encoder uses ArrowLogsData view-based API for clean, idiomatic traversal of Arrow RecordBatches.
//! It provides a cleaner interface than direct cursor management while maintaining zero-copy semantics.

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

// ✅ Import ArrowLogsData view-based API for clean zero-copy traversal
use otap_df_pdata::views::otap::arrow::logs::ArrowLogsData;
use otap_df_pdata::views::logs::{LogRecordView, LogsDataView, ResourceLogsView, ScopeLogsView};
use otap_df_pdata::views::common::{AttributeView, AnyValueView};

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
/// Uses ArrowLogsData view-based API for clean, idiomatic traversal.
/// Cursors are hidden within the ArrowLogsData implementation.
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
        Self {
            env_name,
            env_ver,
        }
    }

    /// Encode Arrow RecordBatches directly to Bond format using ArrowLogsData views
    ///
    /// Uses ArrowLogsData view-based API for clean, zero-copy traversal.
    /// Cursors are managed internally by the view implementation.
    pub fn encode_arrow_batch(
        &self,  // ✅ Now &self instead of &mut self - cursors hidden in views!
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

        // ✅ Use ArrowLogsData view for clean, zero-copy traversal
        let arrow_logs = ArrowLogsData::new(logs_batch, log_attrs_batch, resource_attrs_batch)
            .map_err(|e| format!("Failed to create ArrowLogsData: {}", e))?;

        // Iterate over each resource (though Geneva flattens this)
        let mut row_idx = 0;
        for _resource_logs in arrow_logs.resources() {
            for _scope_logs in _resource_logs.scopes() {
                for log in _scope_logs.log_records() {
                    // ✅ Use view API for clean access
                    let timestamp = log.time_unix_nano().unwrap_or(0);
                    let event_name_str = "Log";

                    if row_idx < 3 {
                        eprintln!("🔍 Row {}: timestamp = {}", row_idx, timestamp);
                    }

                    // 1. Determine fields and schema ID using view API
                    let (field_defs, schema_id) = self.determine_arrow_fields_and_schema_id_from_view(
                        &log,
                        event_name_str,
                    );

                    // 2. Encode row directly to Bond using view API
                    let row_buffer = self.write_arrow_row_data_from_view(
                        &log,
                        &field_defs,
                    );

                    // ✅ Use view API accessor
                    let level = log.severity_number().unwrap_or(9) as u8;

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

                    row_idx += 1;
                }
            }
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

    /// Determine fields and schema ID using ArrowLogsData view API (zero-copy)
    ///
    /// Uses LogRecordView for clean, idiomatic traversal.
    fn determine_arrow_fields_and_schema_id_from_view(
        &self,  // ✅ No longer need mut - cursors hidden in view!
        log: &impl LogRecordView,
        event_name: &str,
    ) -> (Vec<FieldDef>, u64) {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut fields = Vec::with_capacity(20);  // Estimate
        let mut hasher = DefaultHasher::new();
        event_name.hash(&mut hasher);

        // Detect presence of synthetic "event_name" attribute (promote to FIELD_NAME like OTLP encoder)
        let has_event_name_attr = log
            .attributes()
            .any(|attr| std::str::from_utf8(attr.key()).ok() == Some("event_name") && attr.value().is_some());

        // Part A - Always present fields
        fields.push((Cow::Borrowed(FIELD_ENV_NAME), BondDataType::BT_STRING));
        fields.push((Cow::Borrowed(FIELD_ENV_VER), BondDataType::BT_STRING));
        fields.push((Cow::Borrowed(FIELD_TIMESTAMP), BondDataType::BT_STRING));
        fields.push((Cow::Borrowed(FIELD_ENV_TIME), BondDataType::BT_STRING));

        // Part A extension - Conditional fields (use view API)
        if let Some(trace_id) = log.trace_id() {
            if trace_id.len() > 0 {
                fields.push((Cow::Borrowed(FIELD_TRACE_ID), BondDataType::BT_STRING));
            }
        }
        if let Some(span_id) = log.span_id() {
            if span_id.len() > 0 {
                fields.push((Cow::Borrowed(FIELD_SPAN_ID), BondDataType::BT_STRING));
            }
        }
        if let Some(flags) = log.flags() {
            if flags != 0 {
                fields.push((Cow::Borrowed(FIELD_TRACE_FLAGS), BondDataType::BT_UINT32));
            }
        }

        // Part B - Core log fields (use view API)
        // Optional promotion of event_name attribute to Bond 'name' field controlled by env:
        //   GENEVA_PROMOTE_EVENT_NAME=1 (or 'true') to enable (default disabled to mirror OTLP encoder semantics when event_name is empty).
        let promote_event_name = std::env::var("GENEVA_PROMOTE_EVENT_NAME")
            .ok()
            .map(|v| {
                let v = v.to_ascii_lowercase();
                v == "1" || v == "true" || v == "yes"
            })
            .unwrap_or(false);

        if has_event_name_attr && promote_event_name {
            fields.push((Cow::Borrowed("name"), BondDataType::BT_STRING)); // Promote attribute to canonical FIELD_NAME
        }
        fields.push((Cow::Borrowed(FIELD_SEVERITY_NUMBER), BondDataType::BT_INT32));
        if log.severity_text().is_some() {
            fields.push((Cow::Borrowed(FIELD_SEVERITY_TEXT), BondDataType::BT_STRING));
        }
        if log.body().is_some() {
            fields.push((Cow::Borrowed(FIELD_BODY), BondDataType::BT_STRING));
        }

        // Part C - Dynamic log attributes
        // ✅ Use view's attribute iterator for clean traversal
        for attr in log.attributes() {
            let key = attr.key();
            let key_str = std::str::from_utf8(key).unwrap_or("");
            if key_str == "event_name" {
                eprintln!("  🔍 Attr raw (skipping promotion target) key='{}'", key_str);
                continue;
            }

            // Dump raw attribute info before filtering
            match attr.value() {
                Some(raw_val) => {
                    let vt = raw_val.value_type();
                    let has_str = raw_val.as_string().is_some();
                    let has_i64 = raw_val.as_int64().is_some();
                    let has_f64 = raw_val.as_double().is_some();
                    let has_bool = raw_val.as_bool().is_some();
                    eprintln!(
                        "  🔍 Attr raw key='{}' vtype={:?} probes=str:{} int:{} dbl:{} bool:{}",
                        key_str, vt, has_str, has_i64, has_f64, has_bool
                    );
                }
                None => {
                    eprintln!("  🔍 Attr raw key='{}' (EMPTY VALUE)", key_str);
                }
            }

            if let Some(val) = attr.value() {
                // First attempt concrete probes
                let (mut bond_type_opt, mut dbg_kind) = if val.as_string().is_some() {
                    (Some(BondDataType::BT_STRING), "string")
                } else if val.as_int64().is_some() {
                    (Some(BondDataType::BT_INT64), "int64")
                } else if val.as_double().is_some() {
                    (Some(BondDataType::BT_DOUBLE), "double")
                } else if val.as_bool().is_some() {
                    (Some(BondDataType::BT_BOOL), "bool")
                } else {
                    (None, "unknown")
                };

                // --- Added override logic to correct mis-probed types (e.g. status_code seen as Bool) ---
                {
                    use otap_df_pdata::views::common::ValueType as VT;
                    let declared_vt = val.value_type();

                    // If declared Int64/Double but probe gave Bool (or unknown), override to declared numeric type
                    match declared_vt {
                        VT::Int64 => {
                            if !matches!(bond_type_opt, Some(BondDataType::BT_INT64)) {
                                bond_type_opt = Some(BondDataType::BT_INT64);
                                dbg_kind = "declared_int64_override";
                            }
                        }
                        VT::Double => {
                            if !matches!(bond_type_opt, Some(BondDataType::BT_DOUBLE)) {
                                bond_type_opt = Some(BondDataType::BT_DOUBLE);
                                dbg_kind = "declared_double_override";
                            }
                        }
                        _ => {}
                    }

                    // Heuristic: numeric-looking string for status_code style keys -> INT64
                    if key_str == "status_code" || key_str.ends_with("_code") {
                        if let Some(BondDataType::BT_STRING) = bond_type_opt {
                            if let Some(sbytes) = val.as_string() {
                                if let Ok(s) = std::str::from_utf8(sbytes) {
                                    if s.chars().all(|c| c.is_ascii_digit()) {
                                        if s.parse::<i64>().is_ok() {
                                            bond_type_opt = Some(BondDataType::BT_INT64);
                                            dbg_kind = "numeric_string_coerced_to_int64";
                                        }
                                    }
                                }
                            }
                        } else if let Some(BondDataType::BT_BOOL) = bond_type_opt {
                            // If mis-probed as bool but declared/int fallback says Int64
                            if declared_vt == VT::Int64 {
                                bond_type_opt = Some(BondDataType::BT_INT64);
                                dbg_kind = "bool_misprobe_corrected_to_int64";
                            }
                        }
                    }
                }

                // Fallback to declared value_type if probes failed (covers mis-probed numeric vs bool)
                if bond_type_opt.is_none() {
                    use otap_df_pdata::views::common::ValueType as VT;
                    let vt = val.value_type();
                    bond_type_opt = match vt {
                        VT::String => Some(BondDataType::BT_STRING),
                        VT::Int64 => Some(BondDataType::BT_INT64),
                        VT::Double => Some(BondDataType::BT_DOUBLE),
                        VT::Bool => Some(BondDataType::BT_BOOL),
                        _ => None,
                    };
                    dbg_kind = "fallback_declared";
                }

                if let Some(bond_type) = bond_type_opt {
                    eprintln!("  🔧 Attr schema include: '{}' -> {} ({:?})", key_str, dbg_kind, bond_type);
                    fields.push((Cow::Owned(key_str.to_string()), bond_type));
                } else {
                    eprintln!("  ⚠️  Skipping attribute '{}' (unrecognized after fallback)", key_str);
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

        let schema_id = hasher.finish();

        // DEBUG: dump schema field ordering & types for comparison with OTLP encoder
        if std::env::var("GENEVA_DEBUG_SCHEMA").is_ok() {
            eprintln!("🔧 SCHEMA DEBUG (schema_id={} hash={:016x})", schema_id, schema_id);
            for f in &field_defs {
                eprintln!("    field_id={} name='{}' type={:?}", f.field_id, f.name, f.type_id);
            }
        }

        (field_defs, schema_id)
    }

    /// Write Arrow row data directly to Bond using ArrowLogsData view API (zero-copy)
    ///
    /// Uses LogRecordView for clean attribute access.
    fn write_arrow_row_data_from_view(
        &self,  // ✅ No longer need mut - cursors hidden in view!
        log: &impl LogRecordView,
        fields: &[FieldDef],
    ) -> Vec<u8> {
        let mut buffer = Vec::with_capacity(fields.len() * 50);

        eprintln!("🔍 DEBUG Starting Bond encoding");

        // ✅ Use view API accessor
        let timestamp = log.time_unix_nano().unwrap_or(0);
        let formatted_timestamp = Self::format_timestamp(timestamp);

        // Precompute attributes for this row to avoid multiple iterations
        // Optional raw dump (row encoding phase)
        if std::env::var("GENEVA_DEBUG_ATTRS").is_ok() {
            eprintln!("  🔎 RAW ATTR LIST (row encode) BEGIN");
            for a in log.attributes() {
                let k = std::str::from_utf8(a.key()).unwrap_or("<non-utf8>");
                match a.value() {
                    Some(v) => {
                        #[allow(unused_imports)]
                        use otap_df_pdata::views::common::ValueType as VT;
                        let vt = v.value_type();
                        let mut kinds = Vec::new();
                        if v.as_string().is_some() { kinds.push("str"); }
                        if v.as_int64().is_some() { kinds.push("i64"); }
                        if v.as_double().is_some() { kinds.push("f64"); }
                        if v.as_bool().is_some() { kinds.push("bool"); }
                        eprintln!("    • key='{}' vtype={:?} probes={}", k, vt, kinds.join(","));
                    }
                    None => eprintln!("    • key='{}' (EMPTY)", k),
                }
            }
            eprintln!("  🔎 RAW ATTR LIST (row encode) END");
        }

        // Collect per-row attributes with robust concrete type detection
        let row_attrs: Vec<(String, BondDataType)> = log
            .attributes()
            .filter_map(|attr| {
                let key = std::str::from_utf8(attr.key()).ok()?.to_string();
                if key == "event_name" {
                    return None;
                }
                let val = attr.value()?;
                // First probe concrete accessors
                let mut bond_type = if val.as_string().is_some() {
                    Some(BondDataType::BT_STRING)
                } else if val.as_int64().is_some() {
                    Some(BondDataType::BT_INT64)
                } else if val.as_double().is_some() {
                    Some(BondDataType::BT_DOUBLE)
                } else if val.as_bool().is_some() {
                    Some(BondDataType::BT_BOOL)
                } else {
                    None
                };

                // --- Added override logic mirroring schema classification to ensure consistency ---
                {
                    use otap_df_pdata::views::common::ValueType as VT;
                    let declared_vt = val.value_type();

                    // Correct mis-probed Bool/Unknown when declared numeric
                    match declared_vt {
                        VT::Int64 => {
                            if !matches!(bond_type, Some(BondDataType::BT_INT64)) {
                                bond_type = Some(BondDataType::BT_INT64);
                                eprintln!("  🔧 Row attr override -> INT64 (declared) key='{}'", key);
                            }
                        }
                        VT::Double => {
                            if !matches!(bond_type, Some(BondDataType::BT_DOUBLE)) {
                                bond_type = Some(BondDataType::BT_DOUBLE);
                                eprintln!("  🔧 Row attr override -> DOUBLE (declared) key='{}'", key);
                            }
                        }
                        _ => {}
                    }

                    // Heuristic for numeric-looking status_code style keys
                    if key == "status_code" || key.ends_with("_code") {
                        if let Some(BondDataType::BT_STRING) = bond_type {
                            if let Some(sbytes) = val.as_string() {
                                if let Ok(s) = std::str::from_utf8(sbytes) {
                                    if s.chars().all(|c| c.is_ascii_digit()) && s.parse::<i64>().is_ok() {
                                        bond_type = Some(BondDataType::BT_INT64);
                                        eprintln!("  🔧 Row attr coerced STRING->INT64 key='{}'", key);
                                    }
                                }
                            }
                        } else if let Some(BondDataType::BT_BOOL) = bond_type {
                            if declared_vt == VT::Int64 {
                                bond_type = Some(BondDataType::BT_INT64);
                                eprintln!("  🔧 Row attr coerced BOOL->INT64 (declared) key='{}'", key);
                            }
                        }
                    }
                }

                // Fallback to declared ValueType if probes failed (handles status_code mis-report)
                if bond_type.is_none() {
                    use otap_df_pdata::views::common::ValueType as VT;
                    bond_type = match val.value_type() {
                        VT::String => Some(BondDataType::BT_STRING),
                        VT::Int64 => Some(BondDataType::BT_INT64),
                        VT::Double => Some(BondDataType::BT_DOUBLE),
                        VT::Bool => Some(BondDataType::BT_BOOL),
                        _ => None,
                    };
                    if let Some(bt) = bond_type {
                        eprintln!("  🔧 Attr row include (fallback) '{}' -> {:?}", key, bt);
                    }
                } else if let Some(bt) = bond_type {
                    eprintln!("  🔧 Attr row include '{}' -> {:?}", key, bt);
                }

                bond_type.map(|bt| (key, bt))
            })
            .collect();

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
                    // ✅ Use view API accessor
                    if let Some(trace_id) = log.trace_id() {
                        let trace_id_hex = hex::encode(trace_id);
                        eprintln!("  📝 {} = {}", FIELD_TRACE_ID, trace_id_hex);
                        BondWriter::write_string(&mut buffer, &trace_id_hex);
                    }
                }
                FIELD_SPAN_ID => {
                    // ✅ Use view API accessor
                    if let Some(span_id) = log.span_id() {
                        let span_id_hex = hex::encode(span_id);
                        eprintln!("  📝 {} = {}", FIELD_SPAN_ID, span_id_hex);
                        BondWriter::write_string(&mut buffer, &span_id_hex);
                    }
                }
                FIELD_TRACE_FLAGS => {
                    // ✅ Use view API accessor
                    if let Some(flags) = log.flags() {
                        eprintln!("  📝 {} = {}", FIELD_TRACE_FLAGS, flags);
                        BondWriter::write_numeric(&mut buffer, flags);
                    }
                }
                "name" => {
                    // Retrieve promoted event_name attribute using explicit loop to avoid temporary borrow issues (fix E0515)
                    let mut event_name_owned: Option<String> = None;
                    for attr in log.attributes() {
                        if let Ok("event_name") = std::str::from_utf8(attr.key()) {
                            if let Some(val) = attr.value() {
                                if let Some(bytes) = val.as_string() {
                                    if let Ok(s) = std::str::from_utf8(bytes) {
                                        event_name_owned = Some(s.to_owned());
                                    }
                                }
                            }
                            break;
                        }
                    }
                    if let Some(s) = event_name_owned {
                        eprintln!("  📝 name (promoted event_name) = {}", s);
                        BondWriter::write_string(&mut buffer, &s);
                    }
                }
                FIELD_SEVERITY_NUMBER => {
                    // ✅ Use view API accessor
                    let num = log.severity_number().unwrap_or(0);
                    eprintln!("  📝 {} = {}", FIELD_SEVERITY_NUMBER, num);
                    BondWriter::write_numeric(&mut buffer, num);
                }
                FIELD_SEVERITY_TEXT => {
                    // ✅ Use view API accessor (returns &[u8])
                    if let Some(text_bytes) = log.severity_text() {
                        if let Ok(text) = std::str::from_utf8(text_bytes) {
                            eprintln!("  📝 {} = {}", FIELD_SEVERITY_TEXT, text);
                            BondWriter::write_string(&mut buffer, text);
                        }
                    }
                }
                FIELD_BODY => {
                    // ✅ Use view API accessor
                    if let Some(body_value) = log.body() {
                        if let Some(body_bytes) = body_value.as_string() {
                            if let Ok(body_str) = std::str::from_utf8(body_bytes) {
                                eprintln!("  📝 {} = {}", FIELD_BODY, body_str);
                                BondWriter::write_string(&mut buffer, body_str);
                            }
                        }
                    }
                }
                _ => {
                    eprintln!("  🔍 Looking for field '{}' in attributes", field.name);
                    // Find matching attribute by key and get actual value from log
                    if let Some((key, bond_type)) = row_attrs.iter().find(|(k, _)| k == field.name.as_ref()) {
                        if let Some(attr) = log.attributes().find(|a| std::str::from_utf8(a.key()).ok() == Some(key.as_str())) {
                            if let Some(value) = attr.value() {
                                match bond_type {
                                    BondDataType::BT_STRING => {
                                        if let Some(val_bytes) = value.as_string() {
                                            if let Ok(val) = std::str::from_utf8(val_bytes) {
                                                eprintln!("  📝 [LOG_ATTR:str] {} = {}", field.name, val);
                                                BondWriter::write_string(&mut buffer, val);
                                            } else {
                                                eprintln!("  ⚠️  Invalid UTF-8 for {}", field.name);
                                                BondWriter::write_string(&mut buffer, "");
                                            }
                                        } else {
                                            eprintln!("  ⚠️  Missing concrete string value for {}, writing empty", field.name);
                                            BondWriter::write_string(&mut buffer, "");
                                        }
                                    }
                                    BondDataType::BT_INT64 => {
                                        if let Some(val) = value.as_int64() {
                                            eprintln!("  📝 [LOG_ATTR:int] {} = {}", field.name, val);
                                            BondWriter::write_numeric(&mut buffer, val);
                                        } else {
                                            // Fallback: if declared type Int64 but accessor failed
                                            use otap_df_pdata::views::common::ValueType as VT;
                                            if value.value_type() == VT::Int64 {
                                                eprintln!("  ⚠️  Int64 accessor None for {}, writing 0", field.name);
                                                BondWriter::write_numeric(&mut buffer, 0i64);
                                            } else {
                                                eprintln!("  ⚠️  Int64 mismatch for {}, writing 0", field.name);
                                                BondWriter::write_numeric(&mut buffer, 0i64);
                                            }
                                        }
                                    }
                                    BondDataType::BT_DOUBLE => {
                                        if let Some(val) = value.as_double() {
                                            eprintln!("  📝 [LOG_ATTR:double] {} = {}", field.name, val);
                                            BondWriter::write_numeric(&mut buffer, val);
                                        } else {
                                            eprintln!("  ⚠️  Double accessor None for {}, writing 0.0", field.name);
                                            BondWriter::write_numeric(&mut buffer, 0f64);
                                        }
                                    }
                                    BondDataType::BT_BOOL => {
                                        if let Some(val) = value.as_bool() {
                                            eprintln!("  📝 [LOG_ATTR:bool] {} = {}", field.name, val);
                                            BondWriter::write_bool(&mut buffer, val);
                                        } else {
                                            // Handle status_code mis-typed as Bool but actually numeric: try int64/string fallback
                                            if let Some(int_candidate) = value.as_int64() {
                                                eprintln!("  ⚠️  Bool accessor None but int64 present for {}, coercing {}", field.name, int_candidate);
                                                BondWriter::write_numeric(&mut buffer, int_candidate);
                                            } else if let Some(str_bytes) = value.as_string() {
                                                if let Ok(s) = std::str::from_utf8(str_bytes) {
                                                    if let Ok(parsed) = s.parse::<i64>() {
                                                        eprintln!("  ⚠️  Bool accessor None; parsed numeric string {} for {}", parsed, field.name);
                                                        BondWriter::write_numeric(&mut buffer, parsed);
                                                    } else {
                                                        eprintln!("  ⚠️  Bool accessor None; writing false default for {}", field.name);
                                                        BondWriter::write_bool(&mut buffer, false);
                                                    }
                                                } else {
                                                    eprintln!("  ⚠️  Bool accessor None; invalid UTF8 string, writing false for {}", field.name);
                                                    BondWriter::write_bool(&mut buffer, false);
                                                }
                                            } else {
                                                eprintln!("  ⚠️  Bool accessor None; writing false default for {}", field.name);
                                                BondWriter::write_bool(&mut buffer, false);
                                            }
                                        }
                                    }
                                    _ => {
                                        eprintln!("  ⚠️  Skipping unexpected Bond type {:?} for key {}", bond_type, field.name);
                                    }
                                }
                            } else {
                                eprintln!("  ⚠️  Attribute '{}' has no value (writing default)", field.name);
                                match bond_type {
                                    BondDataType::BT_STRING => BondWriter::write_string(&mut buffer, ""),
                                    BondDataType::BT_INT64 => BondWriter::write_numeric(&mut buffer, 0i64),
                                    BondDataType::BT_DOUBLE => BondWriter::write_numeric(&mut buffer, 0f64),
                                    BondDataType::BT_BOOL => BondWriter::write_bool(&mut buffer, false),
                                    _ => {}
                                }
                            }
                        }
                    } else {
                        // Field declared in schema but not discoverable in row_attrs (e.g., probes failed). Attempt fallback by scanning attributes.
                        eprintln!("    ⚠️ Attribute '{}' not in row_attrs; attempting fallback", field.name);
                        let mut wrote = false;
                        for a in log.attributes() {
                            if let Ok(k) = std::str::from_utf8(a.key()) {
                                if k == field.name {
                                    if let Some(v) = a.value() {
                                        use otap_df_pdata::views::common::ValueType as VT;
                                        match v.value_type() {
                                            VT::String => {
                                                if let Some(bytes) = v.as_string() {
                                                    if let Ok(s) = std::str::from_utf8(bytes) {
                                                        BondWriter::write_string(&mut buffer, s);
                                                        wrote = true;
                                                    }
                                                }
                                                if !wrote {
                                                    BondWriter::write_string(&mut buffer, "");
                                                    wrote = true;
                                                }
                                            }
                                            VT::Int64 => {
                                                if let Some(i) = v.as_int64() {
                                                    BondWriter::write_numeric(&mut buffer, i);
                                                } else {
                                                    BondWriter::write_numeric(&mut buffer, 0i64);
                                                }
                                                wrote = true;
                                            }
                                            VT::Double => {
                                                if let Some(d) = v.as_double() {
                                                    BondWriter::write_numeric(&mut buffer, d);
                                                } else {
                                                    BondWriter::write_numeric(&mut buffer, 0f64);
                                                }
                                                wrote = true;
                                            }
                                            VT::Bool => {
                                                if let Some(b) = v.as_bool() {
                                                    BondWriter::write_bool(&mut buffer, b);
                                                    wrote = true;
                                                } else if let Some(i) = v.as_int64() {
                                                    eprintln!("    ⚠️ Coercing fallback Bool->Int64 for '{}' value {}", field.name, i);
                                                    BondWriter::write_numeric(&mut buffer, i);
                                                    wrote = true;
                                                } else if let Some(bytes) = v.as_string() {
                                                    if let Ok(s) = std::str::from_utf8(bytes) {
                                                        if let Ok(parsed) = s.parse::<i64>() {
                                                            eprintln!("    ⚠️ Coercing fallback Bool->parsed Int64 for '{}' value {}", field.name, parsed);
                                                            BondWriter::write_numeric(&mut buffer, parsed);
                                                            wrote = true;
                                                        }
                                                    }
                                                }
                                                if !wrote {
                                                    BondWriter::write_bool(&mut buffer, false);
                                                    wrote = true;
                                                }
                                            }
                                            _ => {}
                                        }
                                    }
                                    break;
                                }
                            }
                        }
                        if !wrote {
                            eprintln!("    ⚠️ Writing default for missing '{}'", field.name);
                            // Write default based on heuristic: treat unknown as string empty
                            BondWriter::write_string(&mut buffer, "");
                        }
                    }
                }
            }
        }

        eprintln!("  ✅ Row encoded ({} bytes)", buffer.len());
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
        let arrow_encoder = ArrowLogEncoder::new();  // ✅ No longer need mut!
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
