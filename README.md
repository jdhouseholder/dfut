# A Distributed Future System in Rust
⚠️ This is a *prototype* system for distributed futures in rust for cs244b.

## Local demo
Example usage: https://github.com/jdhouseholder/dfut-example/blob/main/src/main.rs

## Real deployment
A real deployment will consist of a `GlobalScheduler` binary, `Worker` binaries deployed to a cluster of computers, some driver client (perhaps a cli or a server), and the `dfut-ctl` cli.

## TODOs
* Refactor retries and backoff.
* Implement `DChannels` (as opposed to actors).
* Consider if we want to support driver side `d_box`.
* Metrics via `metrics` crate (https://docs.rs/metrics).
* Measure io bound vs compute bound tasks differently in the scheduler.
* Implement the default scheduler policy.
* Communicate local scheduler stats to the global scheduler in heartbeats.
* Make the global scheduler fault tolerant with the `raft` crate (https://docs.rs/raft).
* Consider using a rayon threadpool for cpu bound tasks rather than requiring `tokio::spawn_blocking`.
* Change address type.
* Rename work to calldata.
* `d_await` that returns Arc to avoid cloning + macro auto behavior when return type is `Arc<T>`.
* Extract (via a trait + derive macro) the address of all of the dfuts from args to each `d_fn` and use this info to schedule. We can't do this with serde right now due to the lack of specialization in rust.
* Support args in dstore?
