use std::time::{Duration, Instant};

#[derive(Debug, Default)]
pub(crate) struct Stopwatch {
    last_start: Option<Instant>,
    elapsed: Duration,
}

impl Stopwatch {
    pub fn start(&mut self) -> Instant {
        let now = Instant::now();
        if let Some(i) = self.last_start {
            self.elapsed += i.elapsed();
        }
        self.last_start = Some(now);
        now
    }

    pub fn stop(&mut self) {
        if let Some(t) = self.last_start.take() {
            self.elapsed += t.elapsed();
        }
    }

    pub fn elapsed(&mut self) -> Duration {
        self.stop();
        self.elapsed
    }
}
