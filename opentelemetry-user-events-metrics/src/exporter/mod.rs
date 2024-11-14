use async_trait::async_trait;
use opentelemetry::metrics::{MetricsError, Result};
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_sdk::metrics::data;
use opentelemetry_sdk::metrics::{
    data::{
        ExponentialBucket, ExponentialHistogramDataPoint, Metric, ResourceMetrics, ScopeMetrics,
        Temporality,
    },
    exporter::PushMetricsExporter,
    reader::TemporalitySelector,
    InstrumentKind,
};

use crate::tracepoint;
use eventheader::_internal as ehi;
use prost::Message;
use std::fmt::{Debug, Formatter};
use std::pin::Pin;

const MAX_EVENT_SIZE: usize = 65360;

pub struct MetricsExporter {
    trace_point: Pin<Box<ehi::TracepointState>>,
}

impl MetricsExporter {
    pub fn new() -> MetricsExporter {
        let trace_point = Box::pin(ehi::TracepointState::new(0));
        // This is unsafe because if the code is used in a shared object,
        // the event MUST be unregistered before the shared object unloads.
        unsafe {
            let _result = tracepoint::register(trace_point.as_ref());
        }
        MetricsExporter { trace_point }
    }
}

impl Default for MetricsExporter {
    fn default() -> Self {
        Self::new()
    }
}

impl TemporalitySelector for MetricsExporter {
    // This is matching OTLP exporters delta.
    fn temporality(&self, kind: InstrumentKind) -> Temporality {
        match kind {
            InstrumentKind::Counter
            | InstrumentKind::ObservableCounter
            | InstrumentKind::ObservableGauge
            | InstrumentKind::Histogram
            | InstrumentKind::Gauge => Temporality::Delta,
            InstrumentKind::UpDownCounter | InstrumentKind::ObservableUpDownCounter => {
                Temporality::Cumulative
            }
        }
    }
}

impl Debug for MetricsExporter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("user_events metrics exporter")
    }
}

impl MetricsExporter {
    fn serialize_and_write(&self, resource_metric: &ResourceMetrics) -> Result<()> {
        // Allocate a local buffer for each write operation
        // TODO: Investigate if this can be optimized to avoid reallocation or
        // allocate a fixed buffer size for all writes
        let mut byte_array = Vec::new();

        // Convert to proto message
        let proto_message: ExportMetricsServiceRequest = resource_metric.into();

        // Encode directly into the buffer
        proto_message
            .encode(&mut byte_array)
            .map_err(|err| MetricsError::Other(err.to_string()))?;

        // Check if the encoded message exceeds the 64 KB limit
        if byte_array.len() > MAX_EVENT_SIZE {
            return Err(MetricsError::Other(
                "Event size exceeds maximum allowed limit".into(),
            ));
        }

        // Write to the tracepoint
        tracepoint::write(&self.trace_point, &byte_array);
        Ok(())
    }
}

#[async_trait]
impl PushMetricsExporter for MetricsExporter {
    async fn export(&self, metrics: &mut ResourceMetrics) -> Result<()> {
        if self.trace_point.enabled() {
            let mut errors = Vec::new();

            for scope_metric in &metrics.scope_metrics {
                for metric in &scope_metric.metrics {
                    let data = &metric.data.as_any();

                    if let Some(histogram) = data.downcast_ref::<data::Histogram<u64>>() {
                        for data_point in &histogram.data_points {
                            let resource_metric = ResourceMetrics {
                                resource: metrics.resource.clone(),
                                scope_metrics: vec![ScopeMetrics {
                                    scope: scope_metric.scope.clone(),
                                    metrics: vec![Metric {
                                        name: metric.name.clone(),
                                        description: metric.description.clone(),
                                        unit: metric.unit.clone(),
                                        data: Box::new(data::Histogram {
                                            temporality: histogram.temporality,
                                            data_points: vec![data_point.clone()],
                                        }),
                                    }],
                                }],
                            };
                            if let Err(e) = self.serialize_and_write(&resource_metric) {
                                errors.push(e);
                            }
                        }
                    } else if let Some(histogram) = data.downcast_ref::<data::Histogram<f64>>() {
                        for data_point in &histogram.data_points {
                            let resource_metric = ResourceMetrics {
                                resource: metrics.resource.clone(),
                                scope_metrics: vec![ScopeMetrics {
                                    scope: scope_metric.scope.clone(),
                                    metrics: vec![Metric {
                                        name: metric.name.clone(),
                                        description: metric.description.clone(),
                                        unit: metric.unit.clone(),
                                        data: Box::new(data::Histogram {
                                            temporality: histogram.temporality,
                                            data_points: vec![data_point.clone()],
                                        }),
                                    }],
                                }],
                            };
                            if let Err(e) = self.serialize_and_write(&resource_metric) {
                                errors.push(e);
                            }
                        }
                    } else if let Some(gauge) = data.downcast_ref::<data::Gauge<u64>>() {
                        for data_point in &gauge.data_points {
                            let resource_metric = ResourceMetrics {
                                resource: metrics.resource.clone(),
                                scope_metrics: vec![ScopeMetrics {
                                    scope: scope_metric.scope.clone(),
                                    metrics: vec![Metric {
                                        name: metric.name.clone(),
                                        description: metric.description.clone(),
                                        unit: metric.unit.clone(),
                                        data: Box::new(data::Gauge {
                                            data_points: vec![data_point.clone()],
                                        }),
                                    }],
                                }],
                            };
                            if let Err(e) = self.serialize_and_write(&resource_metric) {
                                errors.push(e);
                            }
                        }
                    } else if let Some(gauge) = data.downcast_ref::<data::Gauge<i64>>() {
                        for data_point in &gauge.data_points {
                            let resource_metric = ResourceMetrics {
                                resource: metrics.resource.clone(),
                                scope_metrics: vec![ScopeMetrics {
                                    scope: scope_metric.scope.clone(),
                                    metrics: vec![Metric {
                                        name: metric.name.clone(),
                                        description: metric.description.clone(),
                                        unit: metric.unit.clone(),
                                        data: Box::new(data::Gauge {
                                            data_points: vec![data_point.clone()],
                                        }),
                                    }],
                                }],
                            };
                            if let Err(e) = self.serialize_and_write(&resource_metric) {
                                errors.push(e);
                            }
                        }
                    } else if let Some(gauge) = data.downcast_ref::<data::Gauge<f64>>() {
                        for data_point in &gauge.data_points {
                            let resource_metric = ResourceMetrics {
                                resource: metrics.resource.clone(),
                                scope_metrics: vec![ScopeMetrics {
                                    scope: scope_metric.scope.clone(),
                                    metrics: vec![Metric {
                                        name: metric.name.clone(),
                                        description: metric.description.clone(),
                                        unit: metric.unit.clone(),
                                        data: Box::new(data::Gauge {
                                            data_points: vec![data_point.clone()],
                                        }),
                                    }],
                                }],
                            };
                            if let Err(e) = self.serialize_and_write(&resource_metric) {
                                errors.push(e);
                            }
                        }
                    } else if let Some(sum) = data.downcast_ref::<data::Sum<u64>>() {
                        for data_point in &sum.data_points {
                            let resource_metric = ResourceMetrics {
                                resource: metrics.resource.clone(),
                                scope_metrics: vec![ScopeMetrics {
                                    scope: scope_metric.scope.clone(),
                                    metrics: vec![Metric {
                                        name: metric.name.clone(),
                                        description: metric.description.clone(),
                                        unit: metric.unit.clone(),
                                        data: Box::new(data::Sum {
                                            temporality: sum.temporality,
                                            data_points: vec![data_point.clone()],
                                            is_monotonic: sum.is_monotonic,
                                        }),
                                    }],
                                }],
                            };
                            if let Err(e) = self.serialize_and_write(&resource_metric) {
                                errors.push(e);
                            }
                        }
                    } else if let Some(sum) = data.downcast_ref::<data::Sum<i64>>() {
                        for data_point in &sum.data_points {
                            let resource_metric = ResourceMetrics {
                                resource: metrics.resource.clone(),
                                scope_metrics: vec![ScopeMetrics {
                                    scope: scope_metric.scope.clone(),
                                    metrics: vec![Metric {
                                        name: metric.name.clone(),
                                        description: metric.description.clone(),
                                        unit: metric.unit.clone(),
                                        data: Box::new(data::Sum {
                                            temporality: sum.temporality,
                                            data_points: vec![data_point.clone()],
                                            is_monotonic: sum.is_monotonic,
                                        }),
                                    }],
                                }],
                            };
                            if let Err(e) = self.serialize_and_write(&resource_metric) {
                                errors.push(e);
                            }
                        }
                    } else if let Some(sum) = data.downcast_ref::<data::Sum<f64>>() {
                        for data_point in &sum.data_points {
                            let resource_metric = ResourceMetrics {
                                resource: metrics.resource.clone(),
                                scope_metrics: vec![ScopeMetrics {
                                    scope: scope_metric.scope.clone(),
                                    metrics: vec![Metric {
                                        name: metric.name.clone(),
                                        description: metric.description.clone(),
                                        unit: metric.unit.clone(),
                                        data: Box::new(data::Sum {
                                            temporality: sum.temporality,
                                            data_points: vec![data_point.clone()],
                                            is_monotonic: sum.is_monotonic,
                                        }),
                                    }],
                                }],
                            };
                            if let Err(e) = self.serialize_and_write(&resource_metric) {
                                errors.push(e);
                            }
                        }
                    } else if let Some(exp_hist) =
                        data.downcast_ref::<data::ExponentialHistogram<u64>>()
                    {
                        for data_point in &exp_hist.data_points {
                            let resource_metric = ResourceMetrics {
                                resource: metrics.resource.clone(),
                                scope_metrics: vec![ScopeMetrics {
                                    scope: scope_metric.scope.clone(),
                                    metrics: vec![Metric {
                                        name: metric.name.clone(),
                                        description: metric.description.clone(),
                                        unit: metric.unit.clone(),
                                        data: Box::new(data::ExponentialHistogram {
                                            temporality: exp_hist.temporality,
                                            data_points: vec![ExponentialHistogramDataPoint {
                                                attributes: data_point.attributes.clone(),
                                                count: data_point.count,
                                                start_time: data_point.start_time,
                                                time: data_point.time,
                                                min: data_point.min,
                                                max: data_point.max,
                                                sum: data_point.sum,
                                                scale: data_point.scale,
                                                zero_count: data_point.zero_count,
                                                zero_threshold: data_point.zero_threshold,
                                                positive_bucket: ExponentialBucket {
                                                    offset: data_point.positive_bucket.offset,
                                                    counts: data_point
                                                        .positive_bucket
                                                        .counts
                                                        .clone(),
                                                },
                                                negative_bucket: ExponentialBucket {
                                                    offset: data_point.negative_bucket.offset,
                                                    counts: data_point
                                                        .negative_bucket
                                                        .counts
                                                        .clone(),
                                                },
                                                exemplars: data_point.exemplars.clone(),
                                            }],
                                        }),
                                    }],
                                }],
                            };
                            if let Err(e) = self.serialize_and_write(&resource_metric) {
                                errors.push(e);
                            }
                        }
                    } else if let Some(exp_hist) =
                        data.downcast_ref::<data::ExponentialHistogram<f64>>()
                    {
                        for data_point in &exp_hist.data_points {
                            let resource_metric = ResourceMetrics {
                                resource: metrics.resource.clone(),
                                scope_metrics: vec![ScopeMetrics {
                                    scope: scope_metric.scope.clone(),
                                    metrics: vec![Metric {
                                        name: metric.name.clone(),
                                        description: metric.description.clone(),
                                        unit: metric.unit.clone(),
                                        data: Box::new(data::ExponentialHistogram {
                                            temporality: exp_hist.temporality,
                                            data_points: vec![ExponentialHistogramDataPoint {
                                                attributes: data_point.attributes.clone(),
                                                count: data_point.count,
                                                start_time: data_point.start_time,
                                                time: data_point.time,
                                                min: data_point.min,
                                                max: data_point.max,
                                                sum: data_point.sum,
                                                scale: data_point.scale,
                                                zero_count: data_point.zero_count,
                                                zero_threshold: data_point.zero_threshold,
                                                positive_bucket: ExponentialBucket {
                                                    offset: data_point.positive_bucket.offset,
                                                    counts: data_point
                                                        .positive_bucket
                                                        .counts
                                                        .clone(),
                                                },
                                                negative_bucket: ExponentialBucket {
                                                    offset: data_point.negative_bucket.offset,
                                                    counts: data_point
                                                        .negative_bucket
                                                        .counts
                                                        .clone(),
                                                },
                                                exemplars: data_point.exemplars.clone(),
                                            }],
                                        }),
                                    }],
                                }],
                            };
                            if let Err(e) = self.serialize_and_write(&resource_metric) {
                                errors.push(e);
                            }
                        }
                    }
                }
            }

            // Return any errors if present
            if !errors.is_empty() {
                return Err(MetricsError::Other(format!(
                    "Encountered {} errors during export",
                    errors.len()
                )));
            }
        }
        Ok(())
    }

    async fn force_flush(&self) -> Result<()> {
        Ok(()) // In this implementation, flush does nothing
    }

    fn shutdown(&self) -> Result<()> {
        // TracepointState automatically unregisters when dropped
        // https://github.com/microsoft/LinuxTracepoints-Rust/blob/main/eventheader/src/native.rs#L618
        Ok(())
    }
}
