# A Distributed Future System in Rust
⚠️ This is a *prototype* system for distributed futures in rust.

## Local demo
Example usage can be found at https://github.com/jdhouseholder/dfut-example/blob/main/src/main.rs

## Real deployment
A real deployment will consist of a `GlobalScheduler` binary, `Worker` binaries deployed to a cluster of computers, some driver client (perhaps a cli or a server), and the `dfut-ctl` cli.

## TODOs
* Pool connections, timeouts, retries, backoff.
* Use performant data format like message pack.  
* DChannels
* d\_box
