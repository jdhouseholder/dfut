use std::collections::HashMap;
use std::sync::Mutex;

use metrics::counter;
use rand::seq::SliceRandom;

use crate::{services::global_scheduler_service::RuntimeInfo, sleep::sleep_with_jitter};

#[derive(Debug, Default)]
pub struct FnIndex {
    m: Mutex<HashMap<String, Vec<String>>>,
}

impl FnIndex {
    pub fn update(
        &self,
        address_to_runtime_info: &HashMap<String, RuntimeInfo>,
        current_address: &str,
    ) {
        let mut m: HashMap<String, Vec<String>> = HashMap::new();

        for (address, runtime_info) in address_to_runtime_info {
            if address == current_address {
                continue;
            }
            if let Some(stats) = &runtime_info.stats {
                for fn_name in stats.fn_stats.keys() {
                    m.entry(fn_name.to_string())
                        .or_default()
                        .push(address.to_string());
                }
            }
        }

        *self.m.lock().unwrap() = m;
    }

    pub async fn schedule_fn(&self, fn_name: &str) -> Option<String> {
        for _ in 0..5 {
            if let Some(address) = self
                .m
                .lock()
                .unwrap()
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
            sleep_with_jitter(100).await;
        }
        return None;
    }
}
