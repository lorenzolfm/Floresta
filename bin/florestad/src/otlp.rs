// SPDX-License-Identifier: MIT OR Apache-2.0

//! Export [`tracing`] events as OpenTelemetry logs over OTLP/HTTP with JSON encoding.
//!
//! This is a deliberately small exporter that speaks the [OTLP/HTTP JSON] wire format
//! directly, without the OpenTelemetry SDK. It is a `tracing` *bridge*: events raised
//! anywhere in the node are turned into OTLP log records by [`OtlpLayer`], queued on a
//! bounded channel, and shipped in batches by a dedicated thread. The layer never
//! blocks the caller: when the queue is full, the record is dropped and counted.
//!
//! What is covered:
//! - Plain `http://` endpoints, resolved from the CLI flag or the standard
//!   `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT` / `OTEL_EXPORTER_OTLP_ENDPOINT` variables.
//! - Batching with the OpenTelemetry SDK defaults (512 records or 1 second).
//! - Resource attributes `service.name`, `service.version`, `service.instance.id`
//!   and `telemetry.sdk.*`.
//! - Event fields as typed attributes, plus `target`, `code.file.path` and
//!   `code.line.number`.
//! - Per-request timeout, partial-success reporting and a bounded flush on shutdown.
//!
//! What is *not* covered, on purpose: TLS, authentication headers, retries and
//! compression. Point the node at a local collector and let it handle those.
//!
//! [OTLP/HTTP JSON]: https://opentelemetry.io/docs/specs/otlp/#json-protobuf-encoding

use std::collections::hash_map::RandomState;
use std::fmt;
use std::hash::BuildHasher;
use std::hash::Hasher;
use std::io;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::mpsc::SyncSender;
use std::sync::mpsc::TrySendError;
use std::thread;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use serde_json::Value;
use serde_json::json;
use tracing::Event;
use tracing::Level;
use tracing::Subscriber;
use tracing::field::Field;
use tracing::field::Visit;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;

/// Environment variable holding the full logs endpoint URL, used verbatim.
pub(crate) const ENV_LOGS_ENDPOINT: &str = "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT";

/// Environment variable holding the collector base URL, to which [`LOGS_PATH`] is appended.
pub(crate) const ENV_ENDPOINT: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";

/// Environment variable holding a `RUST_LOG`-style filter that applies to this sink only.
pub(crate) const ENV_FILTER: &str = "OTLP_LOG";

/// The OTLP/HTTP path for the logs signal.
pub(crate) const LOGS_PATH: &str = "/v1/logs";

/// Records buffered between the [`OtlpLayer`] and the sender thread. Beyond this, drop.
pub(crate) const QUEUE_CAPACITY: usize = 4096;

/// Maximum records in a single export request.
pub(crate) const BATCH_SIZE: usize = 512;

/// Maximum time a record waits in the sender before being exported.
pub(crate) const BATCH_DELAY: Duration = Duration::from_secs(1);

/// Per-request timeout, covering connect, send and response.
pub(crate) const EXPORT_TIMEOUT: Duration = Duration::from_secs(10);

/// How long shutdown waits for the sender thread to flush queued records.
pub(crate) const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);

/// Report export failures on the first one and every `N`th consecutive one after it.
pub(crate) const FAILURE_REPORT_EVERY: u64 = 100;

/// Resolve the URL log batches are posted to.
///
/// Precedence, highest first:
/// 1. `flag`, the `--otlp-logs-endpoint` value: a base URL, [`LOGS_PATH`] is appended.
/// 2. [`ENV_LOGS_ENDPOINT`]: a full URL, used as-is.
/// 3. [`ENV_ENDPOINT`]: a base URL, [`LOGS_PATH`] is appended.
///
/// Returns `None` when none of the three is set.
pub(crate) fn resolve_url(flag: Option<&str>) -> Option<String> {
    let env = |key: &str| std::env::var(key).ok().filter(|v| !v.is_empty());
    resolve_url_from(
        flag,
        env(ENV_LOGS_ENDPOINT).as_deref(),
        env(ENV_ENDPOINT).as_deref(),
    )
}

/// Pure counterpart of [`resolve_url`], taking the environment values as arguments.
fn resolve_url_from(
    flag: Option<&str>,
    logs_endpoint: Option<&str>,
    base_endpoint: Option<&str>,
) -> Option<String> {
    let with_path = |base: &str| format!("{}{LOGS_PATH}", base.trim_end_matches('/'));
    flag.map(with_path)
        .or_else(|| logs_endpoint.map(str::to_owned))
        .or_else(|| base_endpoint.map(with_path))
}

/// A [`tracing_subscriber::Layer`] that turns events into OTLP log records and queues them.
pub(crate) struct OtlpLayer {
    tx: SyncSender<Value>,
    dropped: Arc<AtomicU64>,
}

/// Keeps the sender thread alive. Dropping it flushes queued records, waiting at most
/// [`SHUTDOWN_TIMEOUT`], and reports how many records were dropped for lack of queue space.
pub(crate) struct OtlpGuard {
    shutdown: Arc<AtomicBool>,
    /// A spare sender used only to wake the thread out of its batch wait at shutdown.
    tx: SyncSender<Value>,
    done: Receiver<()>,
    dropped: Arc<AtomicU64>,
}

impl OtlpLayer {
    /// Start the sender thread posting to `url` and return the layer feeding it.
    ///
    /// # Errors
    ///
    /// Returns an error when `url` is not a plain `http://` URL, since the exporter is
    /// built without a TLS provider.
    pub(crate) fn start(url: String) -> Result<(Self, OtlpGuard), io::Error> {
        if !url.starts_with("http://") {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("OTLP logs endpoint must start with http://, got {url}"),
            ));
        }

        let (tx, rx) = mpsc::sync_channel(QUEUE_CAPACITY);
        let (done_tx, done_rx) = mpsc::sync_channel(1);
        let shutdown = Arc::new(AtomicBool::new(false));
        let dropped = Arc::new(AtomicU64::new(0));

        let sender = Sender {
            rx,
            shutdown: shutdown.clone(),
            done: done_tx,
            url,
            resource: resource(),
            agent: ureq::Agent::config_builder()
                .timeout_global(Some(EXPORT_TIMEOUT))
                .build()
                .into(),
            failures: 0,
        };
        thread::Builder::new()
            .name("florestad-otlp".to_owned())
            .spawn(move || sender.run())?;

        let guard = OtlpGuard {
            shutdown,
            tx: tx.clone(),
            done: done_rx,
            dropped: dropped.clone(),
        };
        Ok((Self::with_sender(tx, dropped), guard))
    }

    fn with_sender(tx: SyncSender<Value>, dropped: Arc<AtomicU64>) -> Self {
        Self { tx, dropped }
    }
}

impl<S: Subscriber> Layer<S> for OtlpLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let record = log_record(event, SystemTime::now());
        if let Err(TrySendError::Full(_)) = self.tx.try_send(record) {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl Drop for OtlpGuard {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        // Wake the sender if it is waiting for the batch delay; a full queue means it
        // is awake already, so a failed nudge is fine.
        let _ = self.tx.try_send(Value::Null);
        let _ = self.done.recv_timeout(SHUTDOWN_TIMEOUT);

        let dropped = self.dropped.load(Ordering::Relaxed);
        if dropped > 0 {
            eprintln!("OTLP logs: dropped {dropped} records because the export queue was full");
        }
    }
}

/// State owned by the sender thread.
struct Sender {
    rx: Receiver<Value>,
    shutdown: Arc<AtomicBool>,
    done: SyncSender<()>,
    url: String,
    resource: Value,
    agent: ureq::Agent,
    /// Consecutive failed exports, reset on the first success.
    failures: u64,
}

impl Sender {
    fn run(mut self) {
        loop {
            let (batch, stop) = self.next_batch();
            for chunk in batch.chunks(BATCH_SIZE) {
                self.export(chunk);
            }
            if stop {
                let _ = self.done.try_send(());
                return;
            }
        }
    }

    /// Collect up to [`BATCH_SIZE`] records or wait at most [`BATCH_DELAY`], whichever
    /// comes first. On shutdown, drain everything still queued and report `stop = true`.
    fn next_batch(&self) -> (Vec<Value>, bool) {
        let mut batch = Vec::with_capacity(BATCH_SIZE);
        let deadline = Instant::now() + BATCH_DELAY;

        while batch.len() < BATCH_SIZE && !self.shutdown.load(Ordering::Acquire) {
            let wait = deadline.saturating_duration_since(Instant::now());
            if wait.is_zero() {
                break;
            }
            match self.rx.recv_timeout(wait) {
                Ok(record) => batch.push(record),
                Err(RecvTimeoutError::Timeout) => break,
                Err(RecvTimeoutError::Disconnected) => return (batch, true),
            }
        }

        let stop = self.shutdown.load(Ordering::Acquire);
        if stop {
            batch.extend(self.rx.try_iter());
        }
        // `Value::Null` is the shutdown nudge, never a record.
        batch.retain(|record| !record.is_null());
        (batch, stop)
    }

    fn export(&mut self, records: &[Value]) {
        let body = payload(&self.resource, records).to_string();
        let result = self
            .agent
            .post(&self.url)
            .header("Content-Type", "application/json")
            .send(body.as_bytes());

        match result {
            Ok(mut response) => {
                self.failures = 0;
                report_partial_success(&response.body_mut().read_to_string().unwrap_or_default());
            }
            Err(e) => {
                self.failures += 1;
                if self.failures == 1 || self.failures % FAILURE_REPORT_EVERY == 0 {
                    eprintln!(
                        "OTLP logs: export to {} failed ({} consecutive): {e}",
                        self.url, self.failures
                    );
                }
            }
        }
    }
}

/// A collector answers `200` even when it rejected some records; the count and reason are
/// in the body's `partialSuccess` object, so surface them.
fn report_partial_success(body: &str) {
    let Ok(response) = serde_json::from_str::<Value>(body) else {
        return;
    };
    let partial = &response["partialSuccess"];
    let rejected = partial["rejectedLogRecords"]
        .as_str()
        .and_then(|n| n.parse::<u64>().ok())
        .or_else(|| partial["rejectedLogRecords"].as_u64())
        .unwrap_or(0);
    if rejected > 0 {
        let message = partial["errorMessage"].as_str().unwrap_or("");
        eprintln!("OTLP logs: collector rejected {rejected} records: {message}");
    }
}

/// The `resource` describing this process, sent with every export.
fn resource() -> Value {
    json!({
        "attributes": [
            attribute("service.name", string("florestad")),
            attribute("service.version", string(env!("GIT_DESCRIBE"))),
            attribute("service.instance.id", string(&instance_id())),
            attribute("telemetry.sdk.name", string("florestad-otlp")),
            attribute("telemetry.sdk.language", string("rust")),
            attribute("telemetry.sdk.version", string(env!("CARGO_PKG_VERSION"))),
        ]
    })
}

/// A random identifier for this process, so several nodes sharing a collector stay apart.
///
/// Uses the randomly keyed hasher from `std` rather than pulling in a RNG crate.
fn instance_id() -> String {
    let id = RandomState::new().build_hasher().finish();
    format!("{id:016x}")
}

/// Wrap `records` in the `ExportLogsServiceRequest` envelope.
fn payload(resource: &Value, records: &[Value]) -> Value {
    json!({
        "resourceLogs": [{
            "resource": resource,
            "scopeLogs": [{
                "scope": { "name": "florestad", "version": env!("CARGO_PKG_VERSION") },
                "logRecords": records,
            }],
        }],
    })
}

/// Build one OTLP `LogRecord` from a `tracing` event observed at `now`.
fn log_record(event: &Event<'_>, now: SystemTime) -> Value {
    let metadata = event.metadata();
    let nanos = now
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    // Protobuf's JSON mapping encodes 64-bit integers as strings.
    let nanos = nanos.to_string();

    let mut fields = Fields {
        body: None,
        attributes: vec![attribute("target", string(metadata.target()))],
    };
    event.record(&mut fields);

    if let Some(file) = metadata.file() {
        fields
            .attributes
            .push(attribute("code.file.path", string(file)));
    }
    if let Some(line) = metadata.line() {
        fields
            .attributes
            .push(attribute("code.line.number", int(i64::from(line))));
    }

    let (severity_number, severity_text) = severity(*metadata.level());
    json!({
        "timeUnixNano": nanos,
        "observedTimeUnixNano": nanos,
        "severityNumber": severity_number,
        "severityText": severity_text,
        "body": fields.body.unwrap_or_else(|| string("")),
        "attributes": fields.attributes,
    })
}

/// The OpenTelemetry severity number and text for a `tracing` level.
fn severity(level: Level) -> (u8, &'static str) {
    match level {
        Level::TRACE => (1, "TRACE"),
        Level::DEBUG => (5, "DEBUG"),
        Level::INFO => (9, "INFO"),
        Level::WARN => (13, "WARN"),
        Level::ERROR => (17, "ERROR"),
    }
}

/// Collects an event's fields: `message` becomes the body, everything else an attribute.
struct Fields {
    body: Option<Value>,
    attributes: Vec<Value>,
}

impl Fields {
    fn push(&mut self, field: &Field, value: Value) {
        if field.name() == "message" {
            self.body = Some(value);
        } else {
            self.attributes.push(attribute(field.name(), value));
        }
    }
}

impl Visit for Fields {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.push(field, string(value));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.push(field, int(value));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        match i64::try_from(value) {
            Ok(value) => self.push(field, int(value)),
            Err(_) => self.push(field, string(&value.to_string())),
        }
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        self.push(field, json!({ "doubleValue": value }));
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.push(field, json!({ "boolValue": value }));
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.push(field, string(&format!("{value:?}")));
    }
}

/// An OTLP `KeyValue`.
fn attribute(key: &str, value: Value) -> Value {
    json!({ "key": key, "value": value })
}

/// An OTLP `AnyValue` holding a string.
fn string(value: &str) -> Value {
    json!({ "stringValue": value })
}

/// An OTLP `AnyValue` holding a 64-bit integer, which the JSON mapping encodes as a string.
fn int(value: i64) -> Value {
    json!({ "intValue": value.to_string() })
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;

    use tracing_subscriber::layer::SubscriberExt;

    use super::*;

    #[test]
    fn resolves_endpoint_precedence() {
        assert_eq!(resolve_url_from(None, None, None), None);
        assert_eq!(
            resolve_url_from(Some("http://a:4318/"), Some("http://b/x"), Some("http://c")),
            Some("http://a:4318/v1/logs".to_owned())
        );
        assert_eq!(
            resolve_url_from(None, Some("http://b/x"), Some("http://c")),
            Some("http://b/x".to_owned())
        );
        assert_eq!(
            resolve_url_from(None, None, Some("http://c")),
            Some("http://c/v1/logs".to_owned())
        );
    }

    #[test]
    fn rejects_non_http_endpoint() {
        assert!(OtlpLayer::start("https://collector:4318/v1/logs".to_owned()).is_err());
    }

    #[test]
    fn event_becomes_otlp_log_record() {
        let (tx, rx) = mpsc::sync_channel(8);
        let layer = OtlpLayer::with_sender(tx, Arc::new(AtomicU64::new(0)));
        let subscriber = tracing_subscriber::registry().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            tracing::warn!(height = 42u64, valid = false, "block rejected");
        });

        let record = rx.try_recv().expect("one record queued");
        assert_eq!(record["severityNumber"], 13);
        assert_eq!(record["severityText"], "WARN");
        assert_eq!(record["body"]["stringValue"], "block rejected");
        assert!(
            record["timeUnixNano"].is_string(),
            "int64 must be a JSON string"
        );
        assert_eq!(record["observedTimeUnixNano"], record["timeUnixNano"]);

        let attrs = record["attributes"].as_array().expect("attributes array");
        let find = |key: &str| {
            attrs
                .iter()
                .find(|a| a["key"] == key)
                .unwrap_or_else(|| panic!("missing attribute {key}"))["value"]
                .clone()
        };
        assert_eq!(find("height"), json!({ "intValue": "42" }));
        assert_eq!(find("valid"), json!({ "boolValue": false }));
        assert_eq!(find("target"), string(module_path!()));
        assert_eq!(find("code.file.path"), string(file!()));
        assert!(find("code.line.number")["intValue"].is_string());
    }

    #[test]
    fn payload_carries_resource_identity() {
        let payload = payload(&resource(), &[json!({})]);
        let attrs = payload["resourceLogs"][0]["resource"]["attributes"]
            .as_array()
            .expect("resource attributes");
        let has = |key: &str| attrs.iter().any(|a| a["key"] == key);
        assert!(has("service.name"));
        assert!(has("service.version"));
        assert!(has("service.instance.id"));
        assert!(has("telemetry.sdk.name"));
        assert_eq!(
            payload["resourceLogs"][0]["scopeLogs"][0]["logRecords"]
                .as_array()
                .map(Vec::len),
            Some(1)
        );
    }
}
