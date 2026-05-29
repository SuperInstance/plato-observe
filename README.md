# plato-observe — Metrics, Tracing, and Event Bus

Observability infrastructure for Plato agents. Pub/sub event bus, counters, gauges, histograms, and span-based tracing — everything an agent needs to know what's happening across modules.

**Part of the [Plato](https://github.com/SuperInstance/plato-shell) ecosystem.**

## What This Gives You

- **EventBus** — topic-based pub/sub with subscription queues and polling
- **Counter** — monotonically increasing atomic counter
- **Gauge** — point-in-time value with min/max tracking
- **Histogram** — value distribution with configurable buckets
- **Span** — timing spans for latency measurement

## Quick Start

```rust
use plato_observe::*;

// Event bus
let bus = EventBus::new();
let sub = bus.subscribe("vision.events");
bus.publish(Event::new("vision.events", "motion detected", "plato-vision"));
let events = bus.poll(sub);

// Metrics
let counter = Counter::new("frames_processed");
counter.inc();
counter.inc_by(10);

let gauge = Gauge::new("temperature");
gauge.set(23.5);

let histogram = Histogram::new("latency_ms", vec![1.0, 5.0, 10.0, 50.0, 100.0]);
histogram.observe(7.3);

// Tracing
let span = Span::new("room_load");
// ... do work ...
let elapsed = span.elapsed();
```

## How It Fits

The nervous system of the Plato framework. Every module publishes events here: [plato-vision](https://github.com/SuperInstance/plato-vision) posts scene changes, [plato-correlator](https://github.com/SuperInstance/plato-correlator) posts fused events, [plato-policy](https://github.com/SuperInstance/plato-policy) posts audit decisions.

## Installation

```toml
[dependencies]
plato-observe = "0.1"
```

## License

MIT
