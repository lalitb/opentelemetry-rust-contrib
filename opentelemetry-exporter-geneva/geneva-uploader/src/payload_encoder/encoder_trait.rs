//! Trait for telemetry encoders that can encode logs and spans into Bond format

use crate::client::EncodedBatch;
use arrow::record_batch::RecordBatch;
use opentelemetry_proto::tonic::logs::v1::LogRecord;
use opentelemetry_proto::tonic::trace::v1::Span;

/// Trait for encoding telemetry data into Bond format
pub trait TelemetryEncoder: Clone + Send + Sync {
    /// Encode log records into compressed Bond format batches
    fn encode_logs<'a>(&'a self, metadata: &'a str) -> LogEncoder<'a>;

    /// Encode spans into compressed Bond format batches
    fn encode_spans<'a>(&'a self, metadata: &'a str) -> SpanEncoder<'a>;
}

/// Helper for encoding logs
pub struct LogEncoder<'a> {
    pub(crate) encoder_type: EncoderType<'a>,
    pub(crate) metadata: &'a str,
}

/// Helper for encoding spans
pub struct SpanEncoder<'a> {
    pub(crate) encoder_type: EncoderType<'a>,
    pub(crate) metadata: &'a str,
}

pub(crate) enum EncoderType<'a> {
    Otlp(&'a super::otlp_encoder::OtlpEncoder),
    Otap(&'a super::otap_encoder::OtapEncoder),
}

impl<'a> LogEncoder<'a> {
    /// Encode OTLP-style logs (iterator of LogRecord)
    pub fn from_otlp<'b, I>(&self, logs: I) -> Result<Vec<EncodedBatch>, String>
    where
        I: IntoIterator<Item = &'b LogRecord>,
        'b: 'a,
    {
        match self.encoder_type {
            EncoderType::Otlp(encoder) => encoder.encode_log_batch(logs, self.metadata),
            EncoderType::Otap(_) => Err("Cannot encode OTLP logs with OTAP encoder".to_string()),
        }
    }

    /// Encode OTAP-style logs (Arrow RecordBatch)
    pub fn from_otap(
        &self,
        logs_rb: &RecordBatch,
        log_attrs_rb: Option<&RecordBatch>,
        resource_attrs_rb: Option<&RecordBatch>,
        scope_attrs_rb: Option<&RecordBatch>,
    ) -> Result<Vec<EncodedBatch>, String> {
        match self.encoder_type {
            EncoderType::Otap(encoder) => encoder.encode_otap_log_batch(
                logs_rb,
                log_attrs_rb,
                resource_attrs_rb,
                scope_attrs_rb,
                self.metadata,
            ),
            EncoderType::Otlp(_) => Err("Cannot encode OTAP logs with OTLP encoder".to_string()),
        }
    }
}

impl<'a> SpanEncoder<'a> {
    /// Encode OTLP-style spans (iterator of Span)
    pub fn from_otlp<'b, I>(&self, spans: I) -> Result<Vec<EncodedBatch>, String>
    where
        I: IntoIterator<Item = &'b Span>,
        'b: 'a,
    {
        match self.encoder_type {
            EncoderType::Otlp(encoder) => encoder.encode_span_batch(spans, self.metadata),
            EncoderType::Otap(_) => Err("Cannot encode OTLP spans with OTAP encoder".to_string()),
        }
    }

    /// Encode OTAP-style spans (Arrow RecordBatch)
    pub fn from_otap(
        &self,
        spans_rb: &RecordBatch,
        span_attrs_rb: Option<&RecordBatch>,
        resource_attrs_rb: Option<&RecordBatch>,
        scope_attrs_rb: Option<&RecordBatch>,
    ) -> Result<Vec<EncodedBatch>, String> {
        match self.encoder_type {
            EncoderType::Otap(encoder) => encoder.encode_otap_span_batch(
                spans_rb,
                span_attrs_rb,
                resource_attrs_rb,
                scope_attrs_rb,
                self.metadata,
            ),
            EncoderType::Otlp(_) => Err("Cannot encode OTAP spans with OTLP encoder".to_string()),
        }
    }
}

// Implement TelemetryEncoder for OtlpEncoder
impl TelemetryEncoder for super::otlp_encoder::OtlpEncoder {
    fn encode_logs<'a>(&'a self, metadata: &'a str) -> LogEncoder<'a> {
        LogEncoder {
            encoder_type: EncoderType::Otlp(self),
            metadata,
        }
    }

    fn encode_spans<'a>(&'a self, metadata: &'a str) -> SpanEncoder<'a> {
        SpanEncoder {
            encoder_type: EncoderType::Otlp(self),
            metadata,
        }
    }
}

// Implement TelemetryEncoder for OtapEncoder
impl TelemetryEncoder for super::otap_encoder::OtapEncoder {
    fn encode_logs<'a>(&'a self, metadata: &'a str) -> LogEncoder<'a> {
        LogEncoder {
            encoder_type: EncoderType::Otap(self),
            metadata,
        }
    }

    fn encode_spans<'a>(&'a self, metadata: &'a str) -> SpanEncoder<'a> {
        SpanEncoder {
            encoder_type: EncoderType::Otap(self),
            metadata,
        }
    }
}
