use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use metrics::counter;

use crate::{ema::EMA, services::global_scheduler_service::Stats};

const STAY_LOCAL_THRESHOLD: usize = 2 << 16; // 64KiB // 2 << 32; // 1GiB
                                             //
#[derive(Debug)]
pub enum Where {
    Remote { address: String },
    Local,
}

#[derive(Debug, Default, Clone)]
struct FnStats {
    avg_arg_size: EMA,
    avg_dur: EMA,
    avg_ret_size: EMA,
}

impl FnStats {
    fn track_call(&mut self, arg_size: usize) {
        self.avg_arg_size.next(arg_size as f64);
    }
}

#[derive(Debug, Default, Clone)]
struct LocalStats {
    fn_stats: HashMap<String, FnStats>,
}

#[derive(Debug)]
pub(crate) struct DScheduler {
    runtime_info_stats: Arc<Mutex<Stats>>,
    stats: Mutex<LocalStats>,
}

impl DScheduler {
    pub fn new(runtime_info_stats: &Arc<Mutex<Stats>>) -> Self {
        Self {
            runtime_info_stats: Arc::clone(runtime_info_stats),
            stats: Mutex::default(),
        }
    }
    pub(crate) fn accept_local_work(&self, fn_name: &str, arg_size: usize) -> Where {
        // Check local stats.
        // Decide if we have enough of the DFuts locally or if they are mainly on another peer.
        let (dur, ret_size) = {
            let mut stats = self.stats.lock().unwrap();
            let fn_stats = stats.fn_stats.entry(fn_name.to_string()).or_default();
            fn_stats.track_call(arg_size);

            let dur = fn_stats.avg_dur.avg();
            let ret_size = fn_stats.avg_ret_size.avg();
            (dur, ret_size)
        };

        // 1. Increase local probability if large args or ret.
        // 2. Decrease local probability if the historical duration is large.
        let s = if arg_size as f64 + ret_size >= STAY_LOCAL_THRESHOLD as f64 {
            0.8
        } else {
            0.5
        };
        let d = if dur > 1. { 1. / (2. + dur) } else { 0. };
        let p = s - d;

        let f: f64 = rand::random();
        let local = f < p;
        counter!(
            "accept_local_work",
             "fn_name" => fn_name.to_string(),
             "choice" => if local { "local" } else { "remote" },
        )
        .increment(1);

        if local {
            Where::Local
        } else {
            Where::Remote {
                address: "TODO: decide peer here.".to_string(),
            }
        }
    }

    pub(crate) fn finish_local_work(&self, fn_name: &str, took: Duration, ret_size: usize) {
        let (avg_call_bytes, avg_dur, avg_ret_size) = {
            let mut local_stats = self.stats.lock().unwrap();
            let fn_stats = local_stats.fn_stats.entry(fn_name.to_string()).or_default();
            let avg_call_bytes = fn_stats.avg_arg_size.avg();
            let avg_dur = fn_stats.avg_dur.next(took.as_secs_f64());
            let avg_ret_size = fn_stats.avg_ret_size.next(ret_size as f64);
            (avg_call_bytes, avg_dur, avg_ret_size)
        };

        let mut stats = self.runtime_info_stats.lock().unwrap();
        let fn_stats = stats.fn_stats.entry(fn_name.to_string()).or_default();

        fn_stats.avg_call_bytes = avg_call_bytes as u64;
        fn_stats.avg_latency = 1_000 * avg_dur as u64; // ms
        fn_stats.avg_ret_bytes = avg_ret_size as u64;
    }
}
