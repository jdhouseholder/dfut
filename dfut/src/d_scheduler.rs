use std::{collections::HashMap, sync::Mutex, thread::sleep, time::Duration};

use metrics::counter;
use rand::seq::SliceRandom;

use crate::{
    ema::EMA,
    services::global_scheduler_service::{FnStats, RuntimeInfo, Stats},
};

const STAY_LOCAL_THRESHOLD: usize = 2 << 16; // 64KiB // 2 << 32; // 1GiB
                                             //

#[allow(unused)]
fn f_dur(x: f64) -> f64 {
    1. / (1. + x)
}

#[allow(unused)]
fn f_bytes(x: f64) -> f64 {
    x.log2()
}

#[derive(Debug)]
pub enum Where {
    Remote { address: String },
    Local,
}

#[derive(Debug, Default, Clone)]
struct LocalFnStats {
    completed: u64,
    pending: u64,

    avg_call_bytes: EMA,
    avg_dur_ms: EMA,
    avg_ret_bytes: EMA,
}

#[derive(Debug, Default, Clone)]
struct LocalStats {
    completed: u64,
    pending: u64,

    avg_call_bytes: EMA,
    avg_dur_ms: EMA,
    avg_ret_bytes: EMA,

    fn_stats: HashMap<String, LocalFnStats>,
}

#[derive(Debug, Default)]
struct Inner {
    local_stats: LocalStats,
    index: HashMap<String, Vec<String>>,
}

#[derive(Debug, Default)]
pub(crate) struct DScheduler {
    inner: Mutex<Inner>,
}

impl DScheduler {
    pub fn new(fn_names: Vec<String>) -> Self {
        Self {
            inner: Mutex::new(Inner {
                local_stats: LocalStats {
                    fn_stats: fn_names
                        .into_iter()
                        .map(|fn_name| (fn_name, LocalFnStats::default()))
                        .collect(),
                    ..Default::default()
                },
                ..Default::default()
            }),
        }
    }

    pub fn update(
        &self,
        current_address: &str,
        address_to_runtime_info: &HashMap<String, RuntimeInfo>,
    ) {
        let mut inner = self.inner.lock().unwrap();
        for (address, runtime_info) in address_to_runtime_info {
            if address != current_address {
                if let Some(stats) = &runtime_info.stats {
                    for fn_name in stats.fn_stats.keys() {
                        inner
                            .index
                            .entry(fn_name.to_string())
                            .or_default()
                            .push(address.to_string());
                    }
                }
            }
        }
    }

    pub fn accept_local_work(&self, fn_name: &str, arg_size: usize) -> Option<Where> {
        // Check local stats.
        // Decide if we have enough of the DFuts locally or if they are mainly on another peer.
        let (dur, ret_size) = {
            let mut inner = self.inner.lock().unwrap();

            inner.local_stats.pending += 1;
            inner.local_stats.avg_call_bytes.next(arg_size as f64);

            let fn_stats = inner
                .local_stats
                .fn_stats
                .entry(fn_name.to_string())
                .or_default();
            fn_stats.avg_call_bytes.next(arg_size as f64);
            fn_stats.pending += 1;

            let dur = fn_stats.avg_dur_ms.avg();
            let ret_size = fn_stats.avg_ret_bytes.avg();
            (dur, ret_size)
        };

        // 1. Increase local probability if large args or ret.
        // 2. Decrease local probability if the avg duration is large.
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

        Some(if local {
            Where::Local
        } else {
            Where::Remote {
                address: self.schedule_fn(fn_name)?,
            }
        })
    }

    pub fn schedule_fn(&self, fn_name: &str) -> Option<String> {
        for _ in 0..50 {
            if let Some(address) = self
                .inner
                .lock()
                .unwrap()
                .index
                .get(fn_name)?
                .choose(&mut rand::thread_rng())
                .map(|s| {
                    counter!("schedule_fn", "fn_name" => fn_name.to_string(), "choice" => s.clone())
                    .increment(1);
                    s.clone()
                })
                .clone()
            {
                return Some(address);
            }

            sleep(Duration::from_millis(10));
        }
        return None;
    }

    pub(crate) fn finish_local_work(&self, fn_name: &str, dur: Duration, ret_size: usize) {
        let dur = 1_000. * dur.as_secs_f64();
        let mut inner = self.inner.lock().unwrap();

        inner.local_stats.pending -= 1;
        inner.local_stats.completed += 1;
        inner.local_stats.avg_dur_ms.next(dur);
        inner.local_stats.avg_ret_bytes.next(ret_size as f64);

        let fn_stats = inner
            .local_stats
            .fn_stats
            .entry(fn_name.to_string())
            .or_default();

        fn_stats.pending -= 1;
        fn_stats.completed += 1;

        fn_stats.avg_dur_ms.next(dur);
        fn_stats.avg_ret_bytes.next(ret_size as f64);
    }

    pub fn output_stats(&self) -> Stats {
        let inner = self.inner.lock().unwrap();

        let mut fn_stats = HashMap::new();

        for (address, fs) in &inner.local_stats.fn_stats {
            fn_stats.insert(
                address.to_string(),
                FnStats {
                    completed: fs.completed,
                    pending: fs.pending,

                    avg_dur_ms: fs.avg_dur_ms.avg() as u64,
                    avg_call_bytes: fs.avg_call_bytes.avg() as u64,
                    avg_ret_bytes: fs.avg_ret_bytes.avg() as u64,
                },
            );
        }

        Stats {
            completed: inner.local_stats.completed,
            pending: inner.local_stats.pending,

            avg_dur_ms: inner.local_stats.avg_dur_ms.avg() as u64,
            avg_call_bytes: inner.local_stats.avg_call_bytes.avg() as u64,
            avg_ret_bytes: inner.local_stats.avg_ret_bytes.avg() as u64,

            fn_stats,
        }
    }
}
