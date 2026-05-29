use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Instant;

// ---------------------------------------------------------------------------
// Event
// ---------------------------------------------------------------------------

/// A cross-module event on the event bus.
#[derive(Clone, Debug, PartialEq)]
pub struct Event {
    pub topic: String,
    pub payload: String,
    pub timestamp: u64,
    pub source: String,
}

// ---------------------------------------------------------------------------
// EventBus
// ---------------------------------------------------------------------------

/// Subscription identifier.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SubscriptionId(pub u64);

/// Simple publish/subscribe event bus.
pub struct EventBus {
    next_id: AtomicU64,
    subscribers: Mutex<HashMap<String, HashMap<SubscriptionId, Vec<Event>>>>,
}

impl EventBus {
    pub fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
            subscribers: Mutex::new(HashMap::new()),
        }
    }

    /// Publish an event. It is appended to every subscriber's queue for that topic.
    pub fn publish(&self, event: Event) {
        let mut subs = self.subscribers.lock().unwrap();
        if let Some(queues) = subs.get_mut(&event.topic) {
            for queue in queues.values_mut() {
                queue.push(event.clone());
            }
        }
    }

    /// Subscribe to a topic, returning a subscription id.
    pub fn subscribe(&self, topic: &str) -> SubscriptionId {
        let id = SubscriptionId(self.next_id.fetch_add(1, Ordering::SeqCst));
        let mut subs = self.subscribers.lock().unwrap();
        subs.entry(topic.to_string())
            .or_insert_with(HashMap::new)
            .insert(id, Vec::new());
        id
    }

    /// Poll all pending events for a subscription, draining the queue.
    pub fn poll(&self, subscription: SubscriptionId) -> Vec<Event> {
        let mut subs = self.subscribers.lock().unwrap();
        for queues in subs.values_mut() {
            if let Some(queue) = queues.get_mut(&subscription) {
                return std::mem::take(queue);
            }
        }
        Vec::new()
    }
}

// ---------------------------------------------------------------------------
// Metrics: Counter, Gauge, Histogram
// ---------------------------------------------------------------------------

/// Monotonically increasing counter.
#[derive(Debug)]
pub struct Counter {
    value: AtomicU64,
}

impl Counter {
    pub fn new() -> Self {
        Self {
            value: AtomicU64::new(0),
        }
    }

    pub fn inc(&self) {
        self.value.fetch_add(1, Ordering::SeqCst);
    }

    pub fn inc_by(&self, delta: u64) {
        self.value.fetch_add(delta, Ordering::SeqCst);
    }

    pub fn get(&self) -> u64 {
        self.value.load(Ordering::SeqCst)
    }
}

/// Single numeric value that can go up or down.
#[derive(Debug)]
pub struct Gauge {
    value: std::sync::atomic::AtomicU64,
}

impl Gauge {
    pub fn new() -> Self {
        Self {
            value: std::sync::atomic::AtomicU64::new(0),
        }
    }

    pub fn set(&self, value: f64) {
        self.value.store(value.to_bits(), Ordering::SeqCst);
    }

    pub fn get(&self) -> f64 {
        f64::from_bits(self.value.load(Ordering::SeqCst))
    }
}

/// Distribution of observed values, supporting mean and percentile queries.
pub struct Histogram {
    observations: Mutex<Vec<f64>>,
}

impl Histogram {
    pub fn new() -> Self {
        Self {
            observations: Mutex::new(Vec::new()),
        }
    }

    pub fn observe(&self, value: f64) {
        self.observations.lock().unwrap().push(value);
    }

    pub fn mean(&self) -> f64 {
        let obs = self.observations.lock().unwrap();
        if obs.is_empty() {
            return 0.0;
        }
        let sum: f64 = obs.iter().sum();
        sum / obs.len() as f64
    }

    pub fn percentile(&self, p: f64) -> f64 {
        let mut obs = self.observations.lock().unwrap().clone();
        if obs.is_empty() {
            return 0.0;
        }
        obs.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let idx = ((p / 100.0) * (obs.len() - 1) as f64).round() as usize;
        obs[idx.min(obs.len() - 1)]
    }

    pub fn count(&self) -> usize {
        self.observations.lock().unwrap().len()
    }
}

// ---------------------------------------------------------------------------
// MetricsRegistry
// ---------------------------------------------------------------------------

/// Register and query metrics by name.
pub struct MetricsRegistry {
    counters: Mutex<HashMap<String, Counter>>,
    gauges: Mutex<HashMap<String, Gauge>>,
    histograms: Mutex<HashMap<String, Histogram>>,
}

impl MetricsRegistry {
    pub fn new() -> Self {
        Self {
            counters: Mutex::new(HashMap::new()),
            gauges: Mutex::new(HashMap::new()),
            histograms: Mutex::new(HashMap::new()),
        }
    }

    pub fn inc_counter(&self, name: &str) {
        let mut map = self.counters.lock().unwrap();
        map.entry(name.to_string()).or_insert_with(Counter::new).inc();
    }

    pub fn inc_counter_by(&self, name: &str, delta: u64) {
        let mut map = self.counters.lock().unwrap();
        map.entry(name.to_string()).or_insert_with(Counter::new).inc_by(delta);
    }

    pub fn get_counter_value(&self, name: &str) -> u64 {
        let map = self.counters.lock().unwrap();
        map.get(name).map(|c| c.get()).unwrap_or(0)
    }

    pub fn set_gauge(&self, name: &str, value: f64) {
        let mut map = self.gauges.lock().unwrap();
        map.entry(name.to_string()).or_insert_with(Gauge::new).set(value);
    }

    pub fn get_gauge(&self, name: &str) -> f64 {
        let map = self.gauges.lock().unwrap();
        map.get(name).map(|g| g.get()).unwrap_or(0.0)
    }

    pub fn observe_histogram(&self, name: &str, value: f64) {
        let mut map = self.histograms.lock().unwrap();
        map.entry(name.to_string()).or_insert_with(Histogram::new).observe(value);
    }

    pub fn histogram_mean(&self, name: &str) -> f64 {
        let map = self.histograms.lock().unwrap();
        map.get(name).map(|h| h.mean()).unwrap_or(0.0)
    }

    pub fn histogram_percentile(&self, name: &str, p: f64) -> f64 {
        let map = self.histograms.lock().unwrap();
        map.get(name).map(|h| h.percentile(p)).unwrap_or(0.0)
    }
}

// ---------------------------------------------------------------------------
// Tracing: Span, SpanResult, Trace
// ---------------------------------------------------------------------------

/// Result of a completed span.
#[derive(Clone, Debug, PartialEq)]
pub struct SpanResult {
    pub name: String,
    pub duration_ms: f64,
    pub tags: HashMap<String, String>,
    pub children: Vec<SpanResult>,
}

/// A traced operation with start/end, tags, and child spans.
pub struct Span {
    name: String,
    start: Instant,
    tags: HashMap<String, String>,
    children: Vec<SpanResult>,
}

impl Span {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            start: Instant::now(),
            tags: HashMap::new(),
            children: Vec::new(),
        }
    }

    pub fn tag(&mut self, key: &str, value: &str) -> &mut Self {
        self.tags.insert(key.to_string(), value.to_string());
        self
    }

    /// Create a child span. When finished, record it as a child of this span.
    pub fn child(&mut self, name: &str) -> Span {
        Span::new(name)
    }

    /// Record a finished child span result.
    pub fn add_child_result(&mut self, result: SpanResult) {
        self.children.push(result);
    }

    pub fn finish(self) -> SpanResult {
        let duration = self.start.elapsed().as_secs_f64() * 1000.0;
        SpanResult {
            name: self.name,
            duration_ms: duration,
            tags: self.tags,
            children: self.children,
        }
    }
}

/// A complete trace — a tree of spans for a request flow.
#[derive(Clone, Debug)]
pub struct Trace {
    pub root: SpanResult,
}

impl Trace {
    pub fn new(root: SpanResult) -> Self {
        Self { root }
    }
}

// ---------------------------------------------------------------------------
// HealthCheck
// ---------------------------------------------------------------------------

/// Health status for a module.
#[derive(Clone, Debug, PartialEq)]
pub enum HealthState {
    Healthy,
    Degraded,
    Unhealthy,
}

#[derive(Clone, Debug, PartialEq)]
pub struct HealthStatus {
    pub module: String,
    pub status: HealthState,
    pub message: String,
    pub checked_at: u64,
}

/// Registry of per-module health checkers.
pub struct HealthCheck {
    checkers: Mutex<HashMap<String, Box<dyn Fn() -> HealthStatus + Send + Sync>>>,
}

impl HealthCheck {
    pub fn new() -> Self {
        Self {
            checkers: Mutex::new(HashMap::new()),
        }
    }

    pub fn register<F>(&self, module: &str, checker: F)
    where
        F: Fn() -> HealthStatus + Send + Sync + 'static,
    {
        self.checkers
            .lock()
            .unwrap()
            .insert(module.to_string(), Box::new(checker));
    }

    pub fn check(&self, module: &str) -> HealthStatus {
        let map = self.checkers.lock().unwrap();
        if let Some(checker) = map.get(module) {
            checker()
        } else {
            HealthStatus {
                module: module.to_string(),
                status: HealthState::Unhealthy,
                message: "No checker registered".to_string(),
                checked_at: now_ms(),
            }
        }
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn counter_increment() {
        let counter = Counter::new();
        assert_eq!(counter.get(), 0);
        counter.inc();
        assert_eq!(counter.get(), 1);
    }

    #[test]
    fn counter_inc_by() {
        let counter = Counter::new();
        counter.inc_by(5);
        assert_eq!(counter.get(), 5);
        counter.inc_by(3);
        assert_eq!(counter.get(), 8);
    }

    #[test]
    fn gauge_set_and_get() {
        let gauge = Gauge::new();
        gauge.set(42.5);
        assert!((gauge.get() - 42.5).abs() < f64::EPSILON);
        gauge.set(-10.0);
        assert!((gauge.get() - (-10.0)).abs() < f64::EPSILON);
    }

    #[test]
    fn histogram_observe_and_mean() {
        let hist = Histogram::new();
        hist.observe(10.0);
        hist.observe(20.0);
        hist.observe(30.0);
        assert!((hist.mean() - 20.0).abs() < f64::EPSILON);
    }

    #[test]
    fn histogram_percentile() {
        let hist = Histogram::new();
        for v in [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0] {
            hist.observe(v);
        }
        // p50 should be around 5-6
        let p50 = hist.percentile(50.0);
        assert!(p50 >= 5.0 && p50 <= 6.0, "p50={}", p50);
        // p100 = max
        let p100 = hist.percentile(100.0);
        assert!((p100 - 10.0).abs() < f64::EPSILON, "p100={}", p100);
    }

    #[test]
    fn span_with_tags() {
        let mut span = Span::new("request");
        span.tag("method", "GET");
        span.tag("path", "/api/v1");
        let result = span.finish();
        assert_eq!(result.tags.get("method").unwrap(), "GET");
        assert_eq!(result.tags.get("path").unwrap(), "/api/v1");
    }

    #[test]
    fn span_with_children() {
        let mut parent = Span::new("request");
        let child = parent.child("db_query");
        let child_result = child.finish();
        parent.add_child_result(child_result);
        let result = parent.finish();
        assert_eq!(result.children.len(), 1);
        assert_eq!(result.children[0].name, "db_query");
    }

    #[test]
    fn span_duration_measured() {
        let span = Span::new("slow_op");
        thread::sleep(Duration::from_millis(50));
        let result = span.finish();
        assert!(result.duration_ms >= 40.0, "duration={}ms", result.duration_ms);
    }

    #[test]
    fn event_bus_publish_subscribe() {
        let bus = EventBus::new();
        let sub = bus.subscribe("test.topic");
        bus.publish(Event {
            topic: "test.topic".to_string(),
            payload: "hello".to_string(),
            timestamp: 1000,
            source: "test".to_string(),
        });
        let events = bus.poll(sub);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].payload, "hello");
    }

    #[test]
    fn event_bus_poll_returns_events() {
        let bus = EventBus::new();
        let sub = bus.subscribe("data");
        bus.publish(Event {
            topic: "data".to_string(),
            payload: "a".to_string(),
            timestamp: 1,
            source: "src".to_string(),
        });
        bus.publish(Event {
            topic: "data".to_string(),
            payload: "b".to_string(),
            timestamp: 2,
            source: "src".to_string(),
        });
        let events = bus.poll(sub);
        assert_eq!(events.len(), 2);
        // Second poll returns empty (drained)
        assert!(bus.poll(sub).is_empty());
    }

    #[test]
    fn health_check_registration() {
        let hc = HealthCheck::new();
        hc.register("db", || HealthStatus {
            module: "db".to_string(),
            status: HealthState::Healthy,
            message: "OK".to_string(),
            checked_at: 0,
        });
        let status = hc.check("db");
        assert_eq!(status.status, HealthState::Healthy);
    }

    #[test]
    fn health_check_returns_status() {
        let hc = HealthCheck::new();
        let status = hc.check("unknown");
        assert_eq!(status.status, HealthState::Unhealthy);
        assert_eq!(status.module, "unknown");
    }

    #[test]
    fn multiple_subscribers() {
        let bus = EventBus::new();
        let sub1 = bus.subscribe("topic");
        let sub2 = bus.subscribe("topic");
        bus.publish(Event {
            topic: "topic".to_string(),
            payload: "broadcast".to_string(),
            timestamp: 1,
            source: "src".to_string(),
        });
        let e1 = bus.poll(sub1);
        let e2 = bus.poll(sub2);
        assert_eq!(e1.len(), 1);
        assert_eq!(e2.len(), 1);
        assert_eq!(e1[0].payload, "broadcast");
        assert_eq!(e2[0].payload, "broadcast");
    }

    #[test]
    fn metrics_registry_counter() {
        let reg = MetricsRegistry::new();
        reg.inc_counter("requests");
        reg.inc_counter("requests");
        reg.inc_counter_by("requests", 10);
        assert_eq!(reg.get_counter_value("requests"), 12);
    }

    #[test]
    fn metrics_registry_gauge_and_histogram() {
        let reg = MetricsRegistry::new();
        reg.set_gauge("cpu", 0.85);
        assert!((reg.get_gauge("cpu") - 0.85).abs() < f64::EPSILON);

        reg.observe_histogram("latency", 100.0);
        reg.observe_histogram("latency", 200.0);
        assert!((reg.histogram_mean("latency") - 150.0).abs() < f64::EPSILON);
    }
}
