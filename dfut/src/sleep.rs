use std::time::Duration;

use rand::Rng;
use tokio::time::sleep;

pub async fn sleep_with_jitter(sleep_for: u64) {
    if sleep_for > 0 {
        let max = u64::min(sleep_for, 100);
        let jitter = rand::thread_rng().gen_range(0..max);
        sleep(Duration::from_millis(sleep_for - jitter)).await
    }
}
