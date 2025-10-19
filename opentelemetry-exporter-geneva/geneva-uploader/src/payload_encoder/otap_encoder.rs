use crate::client::EncodedBatch;
use crate::payload_encoder::bond_encoder::{BondDataType, BondEncodedSchema, BondWriter, FieldDef};
use crate::payload_encoder::central_blob::{
    BatchMetadata, CentralBlob, CentralEventEntry, CentralSchemaEntry,
};
use crate::payload_encoder::lz4_chunked_compression::lz4_chunked_compression;
use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use std::borrow::Cow;
use std::sync::Arc;
use tracing::debug;

/// Encoder to write OTAP (Apache Arrow) payload in Bond format.
/// This encoder operates on Arrow RecordBatches and converts them to Bond-encoded
/// binary format for Geneva uploader with minimal memory copying.
#[derive(Clone)]
pub(crate) struct OtapEncoder;

impl OtapEncoder {
    pub(crate) fn new() -> Self {
        OtapEncoder {}
    }

    /// Encode a batch of OTAP logs into compressed Bond format
    /// Takes Arrow RecordBatches for logs and their related attributes
    /// Returns encoded batches grouped by event name with LZ4 compression
    pub(crate) fn encode_otap_log_batch(
        &self,
        logs_rb: &RecordBatch,
        log_attrs_rb: Option<&RecordBatch>,
        resource_attrs_rb: Option<&RecordBatch>,
        scope_attrs_rb: Option<&RecordBatch>,
        metadata: &str,
    ) -> Result<Vec<EncodedBatch>, String> {
        // Extract event names from logs record batch (if present)
        // For OTAP, we'll use a single batch with "Log" as the event name
        const EVENT_NAME: &str = "Log";

        let num_rows = logs_rb.num_rows();
        if num_rows == 0 {
            return Ok(Vec::new());
        }

        // Determine schema from the Arrow schema
        let (field_defs, schema_id) = Self::determine_fields_from_arrow_schema(&logs_rb.schema())?;

        // Create schema entry
        let schema_entry = Self::create_schema(schema_id, field_defs.clone());

        // Extract time range from logs
        let (start_time, end_time) = Self::extract_time_range(logs_rb)?;

        // Encode each row
        let mut events = Vec::with_capacity(num_rows);
        for row_idx in 0..num_rows {
            let row_buffer = self.encode_arrow_row(logs_rb, row_idx, &field_defs)?;
            let level = Self::extract_severity_level(logs_rb, row_idx);

            events.push(CentralEventEntry {
                schema_id,
                level,
                event_name: Arc::new(EVENT_NAME.to_string()),
                row: row_buffer,
            });
        }

        // Format schema IDs
        let schema_ids_string = {
            let md5_hash = md5::compute(schema_id.to_le_bytes());
            format!("{md5_hash:x}")
        };

        let batch_metadata = BatchMetadata {
            start_time,
            end_time,
            schema_ids: schema_ids_string,
        };

        let schemas_count = 1;
        let events_count = events.len();

        let blob = CentralBlob {
            version: 1,
            format: 2,
            metadata: metadata.to_string(),
            schemas: vec![schema_entry],
            events,
        };

        let uncompressed = blob.to_bytes();
        let compressed = lz4_chunked_compression(&uncompressed).map_err(|e| {
            debug!(
                name: "encoder.encode_otap_log_batch.compress_error",
                target: "geneva-uploader",
                event_name = %EVENT_NAME,
                error = %e,
                "LZ4 compression failed"
            );
            format!("compression failed: {e}")
        })?;

        debug!(
            name: "encoder.encode_otap_log_batch",
            target: "geneva-uploader",
            event_name = %EVENT_NAME,
            schemas = schemas_count,
            events = events_count,
            uncompressed_size = uncompressed.len(),
            compressed_size = compressed.len(),
            "Encoded OTAP log batch"
        );

        Ok(vec![EncodedBatch {
            event_name: EVENT_NAME.to_string(),
            data: compressed,
            metadata: batch_metadata,
        }])
    }

    /// Encode a batch of OTAP spans into compressed Bond format
    pub(crate) fn encode_otap_span_batch(
        &self,
        spans_rb: &RecordBatch,
        span_attrs_rb: Option<&RecordBatch>,
        resource_attrs_rb: Option<&RecordBatch>,
        scope_attrs_rb: Option<&RecordBatch>,
        metadata: &str,
    ) -> Result<Vec<EncodedBatch>, String> {
        const EVENT_NAME: &str = "Span";

        let num_rows = spans_rb.num_rows();
        if num_rows == 0 {
            return Ok(Vec::new());
        }

        // Determine schema from the Arrow schema
        let (field_defs, schema_id) =
            Self::determine_fields_from_arrow_schema(&spans_rb.schema())?;

        // Create schema entry
        let schema_entry = Self::create_span_schema(schema_id, field_defs.clone());

        // Extract time range from spans (start_time_unix_nano)
        let (start_time, end_time) = Self::extract_span_time_range(spans_rb)?;

        // Encode each row
        let mut events = Vec::with_capacity(num_rows);
        for row_idx in 0..num_rows {
            let row_buffer = self.encode_arrow_row(spans_rb, row_idx, &field_defs)?;
            let level = 5; // Default level for spans (INFO equivalent)

            events.push(CentralEventEntry {
                schema_id,
                level,
                event_name: Arc::new(EVENT_NAME.to_string()),
                row: row_buffer,
            });
        }

        // Format schema IDs
        let schema_ids_string = {
            let md5_hash = md5::compute(schema_id.to_le_bytes());
            format!("{md5_hash:x}")
        };

        let batch_metadata = BatchMetadata {
            start_time,
            end_time,
            schema_ids: schema_ids_string,
        };

        let schemas_count = 1;
        let events_count = events.len();

        let blob = CentralBlob {
            version: 1,
            format: 2,
            metadata: metadata.to_string(),
            schemas: vec![schema_entry],
            events,
        };

        let uncompressed = blob.to_bytes();
        let compressed = lz4_chunked_compression(&uncompressed).map_err(|e| {
            debug!(
                name: "encoder.encode_otap_span_batch.compress_error",
                target: "geneva-uploader",
                error = %e,
                "LZ4 compression failed for spans"
            );
            format!("compression failed: {e}")
        })?;

        debug!(
            name: "encoder.encode_otap_span_batch",
            target: "geneva-uploader",
            event_name = EVENT_NAME,
            schemas = schemas_count,
            spans = events_count,
            uncompressed_size = uncompressed.len(),
            compressed_size = compressed.len(),
            "Encoded OTAP span batch"
        );

        Ok(vec![EncodedBatch {
            event_name: EVENT_NAME.to_string(),
            data: compressed,
            metadata: batch_metadata,
        }])
    }

    /// Determine Bond field definitions from Arrow schema
    /// Maps Arrow DataTypes to Bond DataTypes
    fn determine_fields_from_arrow_schema(
        schema: &arrow::datatypes::SchemaRef,
    ) -> Result<(Vec<FieldDef>, u64), String> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        let mut field_defs = Vec::with_capacity(schema.fields().len());

        for (idx, field) in schema.fields().iter().enumerate() {
            let bond_type = Self::arrow_type_to_bond_type(field.data_type())?;

            // Hash field name and type for schema ID
            field.name().hash(&mut hasher);
            bond_type.hash(&mut hasher);

            field_defs.push(FieldDef {
                name: Cow::Owned(field.name().clone()),
                type_id: bond_type,
                field_id: (idx + 1) as u16,
            });
        }

        let schema_id = hasher.finish();
        Ok((field_defs, schema_id))
    }

    /// Map Arrow DataType to Bond DataType
    fn arrow_type_to_bond_type(arrow_type: &DataType) -> Result<BondDataType, String> {
        match arrow_type {
            DataType::Boolean => Ok(BondDataType::BT_BOOL),
            DataType::Int8 => Ok(BondDataType::BT_INT8),
            DataType::Int16 => Ok(BondDataType::BT_INT16),
            DataType::Int32 => Ok(BondDataType::BT_INT32),
            DataType::Int64 => Ok(BondDataType::BT_INT64),
            DataType::UInt8 => Ok(BondDataType::BT_UINT8),
            DataType::UInt16 => Ok(BondDataType::BT_UINT16),
            DataType::UInt32 => Ok(BondDataType::BT_UINT32),
            DataType::UInt64 => Ok(BondDataType::BT_UINT64),
            DataType::Float32 => Ok(BondDataType::BT_FLOAT),
            DataType::Float64 => Ok(BondDataType::BT_DOUBLE),
            DataType::Utf8 | DataType::LargeUtf8 => Ok(BondDataType::BT_STRING),
            DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
                Ok(BondDataType::BT_STRING) // Encode as hex string
            }
            DataType::Timestamp(_, _) => Ok(BondDataType::BT_INT64),
            DataType::Duration(_) => Ok(BondDataType::BT_INT64),
            DataType::Struct(_) => Ok(BondDataType::BT_STRUCT),
            _ => Err(format!("Unsupported Arrow type: {arrow_type:?}")),
        }
    }

    /// Encode a single Arrow row into Bond format
    /// Uses zero-copy where possible by directly accessing Arrow array data
    fn encode_arrow_row(
        &self,
        record_batch: &RecordBatch,
        row_idx: usize,
        field_defs: &[FieldDef],
    ) -> Result<Vec<u8>, String> {
        let mut buffer = Vec::with_capacity(field_defs.len() * 32);

        for (col_idx, field_def) in field_defs.iter().enumerate() {
            let column = record_batch.column(col_idx);

            if column.is_null(row_idx) {
                // Write default value for null
                Self::write_default_value(&mut buffer, field_def.type_id);
                continue;
            }

            match field_def.type_id {
                BondDataType::BT_BOOL => {
                    let array = column.as_boolean();
                    let value = array.value(row_idx);
                    BondWriter::write_bool(&mut buffer, value);
                }
                BondDataType::BT_INT8 => {
                    let array = column.as_primitive::<arrow::datatypes::Int8Type>();
                    let value = array.value(row_idx);
                    buffer.push(value as u8);
                }
                BondDataType::BT_INT16 => {
                    let array = column.as_primitive::<arrow::datatypes::Int16Type>();
                    let value = array.value(row_idx);
                    buffer.extend_from_slice(&value.to_le_bytes());
                }
                BondDataType::BT_INT32 => {
                    let array = column.as_primitive::<arrow::datatypes::Int32Type>();
                    let value = array.value(row_idx);
                    BondWriter::write_numeric(&mut buffer, value);
                }
                BondDataType::BT_INT64 => {
                    let array = column.as_primitive::<arrow::datatypes::Int64Type>();
                    let value = array.value(row_idx);
                    BondWriter::write_numeric(&mut buffer, value);
                }
                BondDataType::BT_UINT8 => {
                    let array = column.as_primitive::<arrow::datatypes::UInt8Type>();
                    let value = array.value(row_idx);
                    buffer.push(value);
                }
                BondDataType::BT_UINT16 => {
                    let array = column.as_primitive::<arrow::datatypes::UInt16Type>();
                    let value = array.value(row_idx);
                    buffer.extend_from_slice(&value.to_le_bytes());
                }
                BondDataType::BT_UINT32 => {
                    let array = column.as_primitive::<arrow::datatypes::UInt32Type>();
                    let value = array.value(row_idx);
                    BondWriter::write_numeric(&mut buffer, value);
                }
                BondDataType::BT_UINT64 => {
                    let array = column.as_primitive::<arrow::datatypes::UInt64Type>();
                    let value = array.value(row_idx);
                    buffer.extend_from_slice(&value.to_le_bytes());
                }
                BondDataType::BT_FLOAT => {
                    let array = column.as_primitive::<arrow::datatypes::Float32Type>();
                    let value = array.value(row_idx);
                    buffer.extend_from_slice(&value.to_le_bytes());
                }
                BondDataType::BT_DOUBLE => {
                    let array = column.as_primitive::<arrow::datatypes::Float64Type>();
                    let value = array.value(row_idx);
                    BondWriter::write_numeric(&mut buffer, value);
                }
                BondDataType::BT_STRING => {
                    let string_value = Self::extract_string_from_column(column, row_idx)?;
                    BondWriter::write_string(&mut buffer, &string_value);
                }
                _ => {
                    return Err(format!(
                        "Unsupported Bond type for encoding: {:?}",
                        field_def.type_id
                    ))
                }
            }
        }

        Ok(buffer)
    }

    /// Extract string value from various Arrow column types
    fn extract_string_from_column(column: &ArrayRef, row_idx: usize) -> Result<String, String> {
        match column.data_type() {
            DataType::Utf8 => {
                let array = column.as_string::<i32>();
                Ok(array.value(row_idx).to_string())
            }
            DataType::LargeUtf8 => {
                let array = column.as_string::<i64>();
                Ok(array.value(row_idx).to_string())
            }
            DataType::Binary => {
                let array = column.as_binary::<i32>();
                let bytes = array.value(row_idx);
                Ok(hex::encode(bytes))
            }
            DataType::LargeBinary => {
                let array = column.as_binary::<i64>();
                let bytes = array.value(row_idx);
                Ok(hex::encode(bytes))
            }
            DataType::FixedSizeBinary(_) => {
                let array = column.as_fixed_size_binary();
                let bytes = array.value(row_idx);
                Ok(hex::encode(bytes))
            }
            _ => Err(format!(
                "Cannot extract string from column type: {:?}",
                column.data_type()
            )),
        }
    }

    /// Write default value for a given Bond type
    fn write_default_value(buffer: &mut Vec<u8>, bond_type: BondDataType) {
        match bond_type {
            BondDataType::BT_BOOL => buffer.push(0),
            BondDataType::BT_INT8 | BondDataType::BT_UINT8 => buffer.push(0),
            BondDataType::BT_INT16 | BondDataType::BT_UINT16 => {
                buffer.extend_from_slice(&0u16.to_le_bytes())
            }
            BondDataType::BT_INT32 | BondDataType::BT_UINT32 => {
                buffer.extend_from_slice(&0u32.to_le_bytes())
            }
            BondDataType::BT_INT64 | BondDataType::BT_UINT64 => {
                buffer.extend_from_slice(&0u64.to_le_bytes())
            }
            BondDataType::BT_FLOAT => buffer.extend_from_slice(&0f32.to_le_bytes()),
            BondDataType::BT_DOUBLE => buffer.extend_from_slice(&0f64.to_le_bytes()),
            BondDataType::BT_STRING | BondDataType::BT_WSTRING => {
                buffer.extend_from_slice(&0u32.to_le_bytes())
            } // Empty string
            _ => {}
        }
    }

    /// Extract time range from logs record batch
    fn extract_time_range(logs_rb: &RecordBatch) -> Result<(u64, u64), String> {
        // Look for time_unix_nano or observed_time_unix_nano column
        let time_column = logs_rb
            .column_by_name("time_unix_nano")
            .or_else(|| logs_rb.column_by_name("observed_time_unix_nano"))
            .ok_or_else(|| "No time column found in logs".to_string())?;

        let time_array = time_column
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .ok_or_else(|| "Time column is not UInt64".to_string())?;

        let mut min_time = u64::MAX;
        let mut max_time = 0u64;

        for i in 0..time_array.len() {
            if !time_array.is_null(i) {
                let value = time_array.value(i);
                if value != 0 {
                    min_time = min_time.min(value);
                    max_time = max_time.max(value);
                }
            }
        }

        if min_time == u64::MAX {
            min_time = 0;
        }

        Ok((min_time, max_time))
    }

    /// Extract time range from spans record batch
    fn extract_span_time_range(spans_rb: &RecordBatch) -> Result<(u64, u64), String> {
        let start_time_column = spans_rb
            .column_by_name("start_time_unix_nano")
            .ok_or_else(|| "No start_time_unix_nano column found in spans".to_string())?;

        let end_time_column = spans_rb
            .column_by_name("end_time_unix_nano")
            .ok_or_else(|| "No end_time_unix_nano column found in spans".to_string())?;

        let start_time_array = start_time_column
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .ok_or_else(|| "start_time_unix_nano column is not UInt64".to_string())?;

        let end_time_array = end_time_column
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .ok_or_else(|| "end_time_unix_nano column is not UInt64".to_string())?;

        let mut min_time = u64::MAX;
        let mut max_time = 0u64;

        for i in 0..start_time_array.len() {
            if !start_time_array.is_null(i) {
                let value = start_time_array.value(i);
                if value != 0 {
                    min_time = min_time.min(value);
                }
            }
            if !end_time_array.is_null(i) {
                let value = end_time_array.value(i);
                if value != 0 {
                    max_time = max_time.max(value);
                }
            }
        }

        if min_time == u64::MAX {
            min_time = 0;
        }

        Ok((min_time, max_time))
    }

    /// Extract severity level from logs record batch
    fn extract_severity_level(logs_rb: &RecordBatch, row_idx: usize) -> u8 {
        if let Some(severity_column) = logs_rb.column_by_name("severity_number") {
            if let Some(severity_array) = severity_column
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
            {
                if !severity_array.is_null(row_idx) {
                    return severity_array.value(row_idx) as u8;
                }
            }
        }
        5 // Default to INFO level
    }

    /// Create schema entry for logs
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

    /// Create schema entry for spans
    fn create_span_schema(schema_id: u64, field_info: Vec<FieldDef>) -> CentralSchemaEntry {
        let schema = BondEncodedSchema::from_fields("OtapSpanRecord", "telemetry", field_info);
        let schema_bytes = schema.as_bytes();
        let schema_md5 = md5::compute(schema_bytes).0;

        CentralSchemaEntry {
            id: schema_id,
            md5: schema_md5,
            schema,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray, UInt64Array};
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_otap_log_encoding() {
        let encoder = OtapEncoder::new();

        // Create a simple Arrow RecordBatch for logs
        let schema = Arc::new(Schema::new(vec![
            Field::new("time_unix_nano", DataType::UInt64, false),
            Field::new("severity_number", DataType::Int32, false),
            Field::new("body", DataType::Utf8, false),
        ]));

        let time_array = UInt64Array::from(vec![1_700_000_000_000_000_000u64]);
        let severity_array = Int32Array::from(vec![9]);
        let body_array = StringArray::from(vec!["test message"]);

        let record_batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(time_array),
                Arc::new(severity_array),
                Arc::new(body_array),
            ],
        )
        .unwrap();

        let metadata = "namespace=testNamespace/eventVersion=Ver1v0";
        let result = encoder
            .encode_otap_log_batch(&record_batch, None, None, None, metadata)
            .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].event_name, "Log");
        assert!(!result[0].data.is_empty());
    }

    #[test]
    fn test_arrow_to_bond_type_mapping() {
        assert_eq!(
            OtapEncoder::arrow_type_to_bond_type(&DataType::Boolean).unwrap(),
            BondDataType::BT_BOOL
        );
        assert_eq!(
            OtapEncoder::arrow_type_to_bond_type(&DataType::Int32).unwrap(),
            BondDataType::BT_INT32
        );
        assert_eq!(
            OtapEncoder::arrow_type_to_bond_type(&DataType::Int64).unwrap(),
            BondDataType::BT_INT64
        );
        assert_eq!(
            OtapEncoder::arrow_type_to_bond_type(&DataType::UInt32).unwrap(),
            BondDataType::BT_UINT32
        );
        assert_eq!(
            OtapEncoder::arrow_type_to_bond_type(&DataType::Float64).unwrap(),
            BondDataType::BT_DOUBLE
        );
        assert_eq!(
            OtapEncoder::arrow_type_to_bond_type(&DataType::Utf8).unwrap(),
            BondDataType::BT_STRING
        );
    }
}
