use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use metrics::counter;

const STAY_LOCAL_THRESHOLD: usize = 2 << 16; // 64KiB // 2 << 32; // 1GiB

#[derive(Debug, Default, Clone)]
struct FnStats {
    v: Vec<(Duration, usize)>,
}

impl FnStats {
    fn track(&mut self, dur: std::time::Duration, ret_size: usize) {
        self.v.push((dur, ret_size));
    }
}

#[derive(Debug, Default, Clone)]
struct Stats {
    fn_stats: HashMap<String, FnStats>,
}

#[derive(Debug)]
pub(crate) struct DScheduler {
    // TODO: When it is fun performance time we move this out of a mutex and
    // use a lock free data structure. We don't have any benchmarks/analysis to
    // justify not using this mutex so we'll do the simple thing first.
    stats: Mutex<Stats>,
}

impl DScheduler {
    pub(crate) fn new() -> Self {
        Self {
            stats: Mutex::default(),
        }
    }

    pub(crate) fn accept_local_work(&self, fn_name: &str, arg_size: usize) -> bool {
        // Check local stats.
        // Decide if we have enough of the DFuts locally or if they are mainly on another peer.
        let last_stats = {
            self.stats
                .lock()
                .unwrap()
                .fn_stats
                .get(fn_name)
                .map(|s| {
                    s.v.last()
                        .map(|(duration, ret_size)| (duration.as_secs_f64(), *ret_size))
                })
                .flatten()
        };

        // 1. Increase local probability if large args or ret.
        // 2. Decrease local probability if the historical duration is large.
        let p = if let Some((duration, ret_size)) = last_stats {
            let s = if arg_size + ret_size >= STAY_LOCAL_THRESHOLD {
                0.8
            } else {
                0.5
            };
            let d = if duration > 1. {
                1. / (2. + duration)
            } else {
                0.
            };
            s - d
        } else {
            if arg_size >= STAY_LOCAL_THRESHOLD {
                0.8
            } else {
                0.5
            }
        };

        let f: f64 = rand::random();
        let local = f < p;
        counter!(
            "accept_local_work",
             "fn_name" => fn_name.to_string(),
             "choice" => if local { "local" } else { "remote" },
        )
        .increment(1);
        local
    }

    pub(crate) fn finish_local_work(&self, fn_name: &str, took: Duration, ret_size: usize) {
        let mut stats = self.stats.lock().unwrap();

        stats
            .fn_stats
            .entry(fn_name.to_string())
            .or_default()
            .track(took, ret_size);
    }
}
