use anyhow::{Context, Result, bail};
use reqwest::{
    StatusCode,
    blocking::{Client, RequestBuilder, Response},
    header::RETRY_AFTER,
};
use std::time::{Duration, Instant, SystemTime};

pub struct Http {
    client: Client,
    interval: Duration,
    last: Option<Instant>,
}

impl Http {
    pub fn new(interval: Duration) -> Result<Self> {
        let client = Client::builder()
            .user_agent(concat!("Arkhiva/", env!("CARGO_PKG_VERSION")))
            .connect_timeout(Duration::from_secs(20))
            .timeout(Duration::from_secs(180))
            .redirect(if interval.is_zero() {
                reqwest::redirect::Policy::limited(5)
            } else {
                reqwest::redirect::Policy::none()
            })
            .build()?;
        Ok(Self {
            client,
            interval,
            last: Some(Instant::now()),
        })
    }

    pub fn request<T>(
        &mut self,
        request: impl Fn(&Client) -> RequestBuilder,
        read: impl Fn(Response) -> Result<T>,
    ) -> Result<T> {
        for attempt in 0..4 {
            if let Some(last) = self.last {
                std::thread::sleep(self.interval.saturating_sub(last.elapsed()));
            }
            self.last = Some(Instant::now());
            let backoff = Duration::from_secs(1 << attempt);
            let result = match request(&self.client).send() {
                Ok(response) if response.status().is_success() => read(response),
                Ok(response)
                    if response.status() == StatusCode::TOO_MANY_REQUESTS
                        || response.status().is_server_error() =>
                {
                    let status = response.status();
                    let delay = retry_after(&response).unwrap_or(backoff);
                    drop(response);
                    if attempt == 3 {
                        bail!("HTTP {status} after four attempts");
                    }
                    std::thread::sleep(delay.max(backoff));
                    continue;
                }
                Ok(response) => bail!(
                    "HTTP {} from {}",
                    response.status(),
                    response.url().host_str().unwrap_or("server")
                ),
                Err(error) => Err(error.without_url().into()),
            };
            match result {
                Ok(value) => return Ok(value),
                Err(error) if attempt == 3 => {
                    return Err(error).context("request failed after four attempts");
                }
                Err(_) => std::thread::sleep(backoff),
            }
        }
        unreachable!()
    }
}

fn retry_after(response: &Response) -> Option<Duration> {
    let raw = response.headers().get(RETRY_AFTER)?.to_str().ok()?;
    if let Ok(seconds) = raw.parse() {
        return Some(Duration::from_secs(seconds));
    }
    let until = httpdate::parse_http_date(raw).ok()?;
    Some(until.duration_since(SystemTime::now()).unwrap_or_default())
}
