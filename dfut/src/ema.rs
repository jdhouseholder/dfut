#[derive(Debug, Clone)]
pub struct EMA {
    b: f64,
    n: f64,
    avg: f64,
}

impl Default for EMA {
    fn default() -> Self {
        Self {
            b: 0.9,
            n: 0.,
            avg: 0.,
        }
    }
}

impl EMA {
    pub fn next(&mut self, v: f64) -> f64 {
        self.n += 1.;
        self.avg = self.b * v + (1. - self.b) * self.avg;
        self.avg
    }

    pub fn avg(&self) -> f64 {
        self.avg
    }
}
