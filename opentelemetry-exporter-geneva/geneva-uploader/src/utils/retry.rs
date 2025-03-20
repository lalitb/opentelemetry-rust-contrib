use crate::error::Result;
use tokio::time::{sleep, Duration};

pub async fn retry_async<F, Fut, T>(
    max_retries: u32,
    retry_delay_seconds: u64,
    mut f: F,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut attempt = 0;

    loop {
        attempt += 1;

        match f().await {
            Ok(result) => return Ok(result),
            Err(_e) if attempt <= max_retries => {
                let delay = std::cmp::min(retry_delay_seconds * attempt as u64, 60);
                sleep(Duration::from_secs(delay)).await;
            }
            Err(e) => return Err(e),
        }
    }
}
