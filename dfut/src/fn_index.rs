use std::collections::HashMap;

use metrics::counter;
use rand::seq::SliceRandom;

use crate::services::global_scheduler_service::RuntimeInfo;

#[derive(Debug, Default)]
pub struct FnIndex {
    m: HashMap<String, Vec<String>>,
}

impl FnIndex {
    pub fn compute(
        address_to_runtime_info: &HashMap<String, RuntimeInfo>,
        current_address: &str,
    ) -> Self {
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
        FnIndex { m }
    }

    pub fn schedule_fn(&self, fn_name: &str) -> Option<String> {
        self.m
            .get(fn_name)?
            .choose(&mut rand::thread_rng())
            .map(|s| {
                counter!("schedule_fn", "fn_name" => fn_name.to_string(), "choice" => s.clone())
                    .increment(1);
                s.clone()
            })
            .clone()
    }
}
