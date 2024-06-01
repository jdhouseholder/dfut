use std::collections::HashMap;

use metrics::counter;
use rand::seq::SliceRandom;

#[derive(Debug, Default)]
pub struct FnIndex {
    m: HashMap<String, Vec<String>>,
}

impl FnIndex {
    pub fn new(m: HashMap<String, Vec<String>>) -> Self {
        Self { m }
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
