use std::time::{Duration, Instant};
#[derive(Debug, Default)]
pub(crate) struct Timer {
    last_start: Option<Instant>,
    elapsed: Duration,
}

impl Timer {
    pub fn start(&mut self) {
        if self.last_start.is_none() {
            self.last_start = Some(Instant::now());
        }
    }

    pub fn stop(&mut self) {
        if let Some(t) = self.last_start.take() {
            self.elapsed += t.elapsed();
        }
    }

    pub fn elapsed(&self) -> Duration {
        self.elapsed
    }
}
