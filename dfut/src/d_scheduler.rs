use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use metrics::counter;

const STAY_LOCAL_THRESHOLD: usize = 2 << 16; // 64KiB // 2 << 32; // 1GiB
                                             //
#[derive(Debug)]
pub enum Where {
    Remote { address: String },
    Local,
}

#[allow(unused)]
#[derive(Debug, Default, Clone)]
struct CallStats {
    arg_size: usize,
}

#[derive(Debug, Default, Clone)]
struct RetStats {
    dur: Duration,
    ret_size: usize,
}

#[derive(Debug, Default, Clone)]
struct FnStats {
    call_stats: Vec<CallStats>,
    ret_stats: Vec<RetStats>,
}

impl FnStats {
    fn track_call(&mut self, arg_size: usize) {
        self.call_stats.push(CallStats { arg_size });
    }

    fn track_ret(&mut self, dur: std::time::Duration, ret_size: usize) {
        self.ret_stats.push(RetStats { dur, ret_size });
    }
}

#[derive(Debug, Default, Clone)]
struct Stats {
    fn_stats: HashMap<String, FnStats>,
}

#[derive(Debug, Default)]
pub(crate) struct DScheduler {
    // TODO: When it is fun performance time we move this out of a mutex and
    // use a lock free data structure. We don't have any benchmarks/analysis to
    // justify not using this mutex so we'll do the simple thing first.
    stats: Mutex<Stats>,
}

impl DScheduler {
    pub(crate) fn accept_local_work(&self, fn_name: &str, arg_size: usize) -> Where {
        // Check local stats.
        // Decide if we have enough of the DFuts locally or if they are mainly on another peer.
        let (dur, ret_size) = {
            let mut stats = self.stats.lock().unwrap();
            let fn_stats = stats.fn_stats.entry(fn_name.to_string()).or_default();
            fn_stats.track_call(arg_size);
            let (dur, ret_size) = fn_stats
                .ret_stats
                .last()
                .map(|RetStats { dur, ret_size }| (dur.as_secs_f64(), *ret_size))
                .unwrap_or((0f64, 0usize));
            (dur, ret_size)
        };

        // 1. Increase local probability if large args or ret.
        // 2. Decrease local probability if the historical duration is large.
        let s = if arg_size + ret_size >= STAY_LOCAL_THRESHOLD {
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
        self.stats
            .lock()
            .unwrap()
            .fn_stats
            .entry(fn_name.to_string())
            .or_default()
            .track_ret(took, ret_size);
    }
}
