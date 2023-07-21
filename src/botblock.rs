use bytes::Bytes;
use futures::{stream::FuturesUnordered, StreamExt};
use rayon::prelude::{ParallelBridge, ParallelIterator};
use reqwest::Response;
use serde::Deserialize;
use std::{
    cmp,
    collections::{HashMap, HashSet},
    env,
    sync::{atomic::AtomicU64, Arc},
    time::SystemTime,
};

use crate::discord::{send_discord_webhook, DiscordWebhookMessage, GrafanaRender};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Request Error: {0}")]
    RequestError(#[from] reqwest::Error),

    #[error("Cloudflare API Error - {0}: {1}")]
    CloudflareApiError(u16, String),

    #[error("BotBlock API Error - {0}: {1}")]
    BotBlockApiError(u16, String),

    #[error("JSON Error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("Failed to read config file: {0}")]
    ConfigFileError(#[from] std::io::Error),

    #[error("Failed to parse config file: {0}")]
    ConfigParseError(#[from] toml::de::Error),

    #[error("User activity contains invalid date: {0}")]
    InvalidDateError(#[from] chrono::ParseError),
}

pub struct ActivityScan {
    pub name: String,

    pub period: u64,
    pub precision: u64,
    pub iterations: u64,
    pub concurrency: usize,

    pub checks: Vec<ActivityScanCheck>,
}

pub struct ActivityScanCheck {
    pub active_ratio_threshold: Option<f64>,
    pub dispersion_index_threshold: Option<f64>,
    pub delta_dispersion_index_threshold: Option<f64>,
}

#[derive(Deserialize)]
pub struct BotBlockBan {
    pub user_id: String,
}

#[derive(Clone)]
pub struct BotBlock {
    config: Arc<BotBlockConfig>,
    ban_list: Arc<HashSet<String>>,

    http_client: reqwest::Client,
    discord_webhook_queue: crossbeam::channel::Sender<DiscordWebhookMessage>,
}

#[derive(Deserialize)]
pub struct BotBlockConfig {
    pub cloudflare_analytics_uri: String,
    pub cloudflare_api_token: String,

    pub grafana_uri_base: String,
    pub grafana_api_token: String,

    pub discord_webhook_uri: String,

    pub botblock_api_token: String,
}

impl BotBlock {
    pub async fn new() -> Result<Self, Error> {
        let config: Arc<BotBlockConfig> = Arc::new(toml::from_str(&std::fs::read_to_string(
            env::var("BOTBLOCK_CONFIG").unwrap_or_else(|_| "botblock.toml".to_string()),
        )?)?);

        let http_client = reqwest::ClientBuilder::new()
            .user_agent("BotBlock/1.0")
            .http1_only()
            .timeout(std::time::Duration::from_secs(10))
            .build()?;

        let discord_webhook_queue = Self::start_discord_webhook_task(
            http_client.clone(),
            config.grafana_uri_base.clone(),
            config.grafana_api_token.clone(),
            config.discord_webhook_uri.clone(),
        );

        let ban_list =
            Arc::new(Self::fetch_ban_list(http_client.clone(), &config.botblock_api_token).await?);

        Ok(BotBlock {
            http_client,
            discord_webhook_queue,

            config,
            ban_list,
        })
    }

    async fn fetch_ban_list(
        http_client: reqwest::Client,
        botblock_api_token: &str,
    ) -> Result<HashSet<String>, Error> {
        let ban_list_response = http_client
            .get("https://botblock.limbolabs.gg/api/bans")
            .header("Authorization", botblock_api_token)
            .send()
            .await?;

        if !ban_list_response.status().is_success() {
            let status = ban_list_response.status().as_u16();
            let body = ban_list_response.text().await?;

            return Err(Error::BotBlockApiError(status, body));
        }

        Ok(ban_list_response
            .json::<Vec<BotBlockBan>>()
            .await?
            .into_iter()
            .map(|ban| ban.user_id)
            .collect::<HashSet<String>>())
    }

    fn start_discord_webhook_task(
        http_client: reqwest::Client,
        grafana_uri_base: String,
        grafana_api_token: String,
        discord_webhook_uri: String,
    ) -> crossbeam::channel::Sender<DiscordWebhookMessage> {
        let (tx, rx) = crossbeam::channel::unbounded::<DiscordWebhookMessage>();

        tokio::task::spawn(async move {
            let mut last_webhook_sent_at = SystemTime::now();

            while let Ok(message) = rx.recv() {
                let elapsed = SystemTime::now()
                    .duration_since(last_webhook_sent_at)
                    .unwrap()
                    .as_millis() as u64;

                if elapsed < 1000 {
                    tokio::time::sleep(tokio::time::Duration::from_millis(1000 - elapsed)).await;
                }

                last_webhook_sent_at = SystemTime::now();

                if let Err(error) = send_discord_webhook(
                    http_client.clone(),
                    &grafana_uri_base,
                    &grafana_api_token,
                    &discord_webhook_uri,
                    message,
                )
                .await
                {
                    log::error!("Discord Webhook Error: {}", error);
                }
            }

            log::debug!("Discord Webhook Sender Task Exited");
        });

        tx
    }

    pub async fn scan_recent_player_activity(&self, scan: ActivityScan) -> Result<(), Error> {
        let current_timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // let mut user_list = self.fetch_user_list(0, current_timestamp).await?;
        // user_list.retain(|entry| !self.ban_list.contains(&entry.user_id));F

        let mut last_start_timestamp = current_timestamp;

        let (tx, rx) = crossbeam::channel::unbounded();

        let client = self.clone();
        tokio::spawn(async move {
            let mut iteration = 0;
            let mut fetch_tasks = FuturesUnordered::new();

            let mut start_fetcher = |iteration: &mut u64| {
                let end_timestamp = last_start_timestamp;
                let start_timestamp = end_timestamp - scan.period;

                last_start_timestamp = start_timestamp;

                *iteration += 1;

                return client.fetch_user_activity_raw(
                    start_timestamp,
                    end_timestamp,
                    scan.precision,
                );
            };

            for _ in 0..cmp::min(scan.iterations as usize, scan.concurrency) {
                fetch_tasks.push(start_fetcher(&mut iteration));
            }

            while let Some(response) = fetch_tasks.next().await {
                match tx.send(response) {
                    Ok(_) => {}
                    Err(err) => {
                        log::warn!("Failed to send response to processing pool: {}", err);
                        break;
                    }
                }

                if iteration >= scan.iterations {
                    drop(tx);
                    break;
                }

                fetch_tasks.push(start_fetcher(&mut iteration));
            }
        });

        let total_scan_duration = AtomicU64::new(0);

        const FLOAT_PRECISION_FACTOR: u64 = 1000000;

        let total_active_ratio = AtomicU64::new(0);
        let total_dispersion_index = AtomicU64::new(0);
        let total_standard_deviation = AtomicU64::new(0);

        let total_user_samples = AtomicU64::new(0);

        let total_interaction_delta = AtomicU64::new(0);
        let total_interaction_delta_dispersion = AtomicU64::new(0);

        let user_block_count = AtomicU64::new(0);

        rx.into_iter()
            .par_bridge()
            .map(|result| -> Result<(), Error> {
                let started_at = SystemTime::now();
                let (start_timestamp, end_timestamp, bytes) = result?;

                let mut block_activity: QueryResult<UserActivityData> = serde_json::from_slice(&bytes)?;
                let data_size_raw = bytes.len();
                drop(bytes);

                let mut data_by_user_id: HashMap<String, UserActivity> = HashMap::new();

                for entry in block_activity.data.drain(..) {
                    if !self.ban_list.contains(&entry.user_id) {
                        continue;
                    }

                    if let Some(user_activity) = data_by_user_id.get_mut(&entry.user_id) {
                        user_activity.append_sample(entry)?;
                    } else {
                        let mut user_activity = UserActivity::new(
                            start_timestamp,
                            end_timestamp,
                            scan.precision,
                        );

                        let user_id = entry.user_id.clone();
                        user_activity.append_sample(entry)?;

                        data_by_user_id.insert(user_id, user_activity);
                    }
                }

                for (user_id, activity) in data_by_user_id.drain() {
                    if activity.time_active_ratio() == 0.0 {
                        continue;
                    }

                    total_user_samples.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let active_ratio = activity.time_active_ratio();

                    let (mean, variance) = activity.activity_level_mean_and_variance();
                    let dispersion_index = variance as f64 / mean as f64;

                    let standard_deviation = variance.sqrt();

                    let (delta_mean, delta_variance) =
                        activity.interaction_delta_mean_and_variance();

                    total_active_ratio.fetch_add(
                        (active_ratio * FLOAT_PRECISION_FACTOR as f64) as u64
                            / FLOAT_PRECISION_FACTOR,
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    total_dispersion_index.fetch_add(
                        (dispersion_index * FLOAT_PRECISION_FACTOR as f64) as u64
                            / FLOAT_PRECISION_FACTOR,
                        std::sync::atomic::Ordering::Relaxed,
                    );

                    total_standard_deviation.fetch_add(
                        (standard_deviation * FLOAT_PRECISION_FACTOR as f64) as u64
                            / FLOAT_PRECISION_FACTOR,
                        std::sync::atomic::Ordering::Relaxed,
                    );

                    total_interaction_delta.fetch_add(
                        delta_mean as u64,
                        std::sync::atomic::Ordering::Relaxed,
                    );

                    total_interaction_delta_dispersion.fetch_add(
                        (delta_variance * FLOAT_PRECISION_FACTOR as f64) as u64,
                        std::sync::atomic::Ordering::Relaxed,
                    );

                    user_block_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                    for check in &scan.checks {
                        let user_id = user_id.clone();

                        let is_over_active_threshold = if check.active_ratio_threshold.is_none() {
                            true
                        } else {
                            active_ratio >= check.active_ratio_threshold.unwrap_or(f64::MIN)
                        };

                        let is_below_dispersion_threshold = if check.dispersion_index_threshold.is_none() {
                            true
                        } else {
                            dispersion_index
                                <= check.dispersion_index_threshold.unwrap_or(f64::MAX)
                        };

                        let is_below_delta_dispersion_threshold = if check.delta_dispersion_index_threshold.is_none() {
                            true
                        } else {
                            delta_variance
                                <= check.delta_dispersion_index_threshold.unwrap_or(f64::MAX)
                        };

                        if is_over_active_threshold & is_below_dispersion_threshold & is_below_delta_dispersion_threshold {
                            let grafana_url = format!("https://limbolabs.grafana.net/d/RL9sMaS4z/cloudflare-analytics?orgId=1&from={}000&to={}000&var-targetUser={}&viewPanel=5",
                                start_timestamp, end_timestamp, user_id);
                            log::info!(
                                "User {} ({}) - Active Ratio: {} - Dispersion Index: {} - Delta Dispersion Index: {}",
                                user_id,
                                hyperlink(&grafana_url, "View In Grafana"),
                                active_ratio,
                                dispersion_index,
                                delta_variance
                            );

                            let message = format!(
                                "**User {}**\n**Activity Ratio**: {}\n**Dispersion Index**: {}\n\n**[View In Grafana]({})**",
                                user_id, active_ratio, dispersion_index, grafana_url
                            );

                            self.discord_webhook_queue.send(DiscordWebhookMessage {
                                content: message,
                                graph: Some(GrafanaRender {
                                    user_id: user_id.to_string(),
                                    start_timestamp,
                                    end_timestamp,
                                })
                            }).unwrap();
                        }
                    }
                }

                // let average_active_ratio = total_active_ratio
                //     .load(std::sync::atomic::Ordering::SeqCst)
                //     / total_user_samples.load(std::sync::atomic::Ordering::SeqCst);
                // let average_dispersion_index = total_dispersion_index
                //     .load(std::sync::atomic::Ordering::SeqCst)
                //     / total_user_samples.load(std::sync::atomic::Ordering::SeqCst);

                // log::debug!(
                //     "Average Active Ratio: {} - Average Dispersion Index: {}",
                //     average_active_ratio,
                //     average_dispersion_index
                // );

                let elapsed = SystemTime::now().duration_since(started_at).unwrap();
                total_scan_duration.fetch_add(
                    elapsed.as_millis() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );

                log::debug!(
                    "Scanned {:.2}MB Data Block in {}ms",
                    data_size_raw as f64 / 1024.0 / 1024.0,
                    elapsed.as_millis()
                );

                Ok(())
            })
            .collect::<Result<(), Error>>()?;

        let average_scan_duration =
            total_scan_duration.load(std::sync::atomic::Ordering::SeqCst) / scan.iterations;

        let user_sample_count = total_user_samples.load(std::sync::atomic::Ordering::SeqCst);

        let average_active_ratio =
            total_active_ratio.load(std::sync::atomic::Ordering::SeqCst) / user_sample_count;
        let average_dispersion_index =
            total_dispersion_index.load(std::sync::atomic::Ordering::SeqCst) / user_sample_count;

        let average_standard_deviation =
            total_standard_deviation.load(std::sync::atomic::Ordering::SeqCst) / user_sample_count;

        let user_block_count = user_block_count.load(std::sync::atomic::Ordering::SeqCst);

        let average_interaction_delta_mean =
            total_interaction_delta.load(std::sync::atomic::Ordering::SeqCst) / user_block_count;

        let average_interaction_delta_dispersion =
            (total_interaction_delta_dispersion.load(std::sync::atomic::Ordering::SeqCst) as f64
                / FLOAT_PRECISION_FACTOR as f64)
                / user_block_count as f64;

        log::info!(
            "Scanned total of {} seconds, {}x {}s blocks with {} second precision. Average block processing duration: {}ms",
            scan.period * scan.iterations,
            scan.iterations,
            scan.period,
            scan.precision,
            average_scan_duration
        );

        log::info!(
            "Average Active Ratio: {} - Average Dispersion Index: {} - Average Standard Deviation: {} - Average Interaction Delta Mean: {} - Average Interaction Delta Dispersion: {}",
            average_active_ratio,
            average_dispersion_index,
            average_standard_deviation,
            average_interaction_delta_mean,
            average_interaction_delta_dispersion
        );

        Ok(())
    }

    async fn query(&self, query: String) -> Result<Response, Error> {
        // log::debug!("Querying Cloudflare API: {}", query);

        let response = self
            .http_client
            .post(&self.config.cloudflare_analytics_uri)
            .body(query)
            .header(
                "Authorization",
                format!("Bearer {}", &self.config.cloudflare_api_token),
            )
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status().as_u16();
            let body = response.text().await?;

            return Err(Error::CloudflareApiError(status, body));
        }

        Ok(response)
    }

    // async fn fetch_user_list(
    //     &self,
    //     start_timestamp: u64,
    //     end_timestamp: u64,
    // ) -> Result<RecentUserList, Error> {
    //     let query = format!(
    //         "
    //         SELECT
    //             blob4 as user_id
    //         FROM analytics_v0
    //         WHERE timestamp >= toDateTime({}) AND timestamp <= toDateTime({})
    //         GROUP BY user_id; FORMAT JSON
    //     ",
    //         start_timestamp, end_timestamp
    //     );

    //     let raw = self.query(query).await?;
    //     Ok(raw.json::<QueryResult<RecentUserList>>().await?.data)
    // }

    async fn fetch_user_activity_raw(
        &self,
        start_timestamp: u64,
        end_timestamp: u64,
        precision: u64,
    ) -> Result<(u64, u64, Bytes), Error> {
        let started_at = SystemTime::now();
        log::debug!(
            "Fetching User Activity Data: {} - {} (Precision: {})",
            start_timestamp,
            end_timestamp,
            precision
        );

        let query = format!(
            "
            SELECT
                blob4 as user_id,
                sum(_sample_interval) as interactions,
                toDateTime(intDiv(toUnixTimestamp(timestamp), ({})) * ({})) as interval_start
            FROM analytics_v0
            WHERE timestamp >= toDateTime({}) AND timestamp <= toDateTime({})
            GROUP BY interval_start, user_id
            ORDER BY interval_start desc; FORMAT JSON
        ",
            precision, precision, start_timestamp, end_timestamp
        );

        let raw = self.query(query).await?;
        log::debug!(
            "Fetched User Activity Data: {} - {} (Precision: {}) ({}ms)",
            start_timestamp,
            end_timestamp,
            precision,
            SystemTime::now()
                .duration_since(started_at)
                .unwrap()
                .as_millis()
        );

        Ok((start_timestamp, end_timestamp, raw.bytes().await?))
    }
}

#[derive(Deserialize)]
pub struct QueryResult<T> {
    data: T,
}

// pub type RecentUserList = Vec<RecentUserEntry>;

// #[derive(Deserialize, Debug)]
// pub struct RecentUserEntry {
//     user_id: String,
// }

pub type UserActivityData = Vec<UserActivityEntry>;

#[derive(Deserialize, Debug)]
pub struct UserActivityEntry {
    user_id: String,
    interactions: String,
    interval_start: String,
}

#[derive(Debug)]
struct UserActivityMeta {
    start_timestamp: u64,
    end_timestamp: u64,

    precision: u64,
}

#[derive(Debug)]
struct UserActivity {
    meta: Arc<UserActivityMeta>,

    samples: Vec<UserActivityEntry>,

    total_interactions: u64,
    total_squared_interactions: u64,

    last_interaction_at: Option<u64>,

    total_interaction_delta: u64,
    total_squared_interaction_delta: u64,
}

impl UserActivity {
    fn new(start_timestamp: u64, end_timestamp: u64, precision: u64) -> Self {
        UserActivity {
            meta: Arc::new(UserActivityMeta {
                start_timestamp,
                end_timestamp,

                precision,
            }),

            samples: Vec::new(),

            total_interactions: 0,
            total_squared_interactions: 0,

            last_interaction_at: None,

            total_interaction_delta: 0,
            total_squared_interaction_delta: 0,
        }
    }

    pub fn append_sample(&mut self, entry: UserActivityEntry) -> Result<(), Error> {
        let count = entry.interactions.parse::<u64>().unwrap();

        self.total_interactions += count;
        self.total_squared_interactions += count.pow(2);

        let timestamp =
            chrono::NaiveDateTime::parse_from_str(&entry.interval_start, "%Y-%m-%d %H:%M:%S")?
                .timestamp() as u64;

        if let Some(last_interaction_at) = self.last_interaction_at {
            let delta = (last_interaction_at - timestamp) / self.meta.precision;

            self.total_interaction_delta += delta;
            self.total_squared_interaction_delta += delta.pow(2);
        }

        self.last_interaction_at = Some(timestamp);
        self.samples.push(entry);

        Ok(())
    }

    pub fn time_active_ratio(&self) -> f64 {
        let active_samples = self.samples.len() as f64;

        let total_intervals = (self.meta.end_timestamp - self.meta.start_timestamp) as f64
            / self.meta.precision as f64;

        active_samples / total_intervals
    }

    pub fn activity_level_mean_and_variance(&self) -> (f64, f64) {
        let total_intervals = (self.meta.end_timestamp - self.meta.start_timestamp) as f64
            / self.meta.precision as f64;

        let total = self.total_interactions as f64;
        let total_squared = self.total_squared_interactions as f64;

        let mean = total / total_intervals;
        let variance = total_squared / total_intervals - mean * mean;

        (mean, variance)
    }

    pub fn interaction_delta_mean_and_variance(&self) -> (f64, f64) {
        let active_samples = self.samples.len() as f64;

        let total = self.total_interaction_delta as f64;
        let total_squared = self.total_squared_interaction_delta as f64;

        let mean = total / active_samples;
        let variance = total_squared / active_samples - mean * mean;

        (mean, variance)
    }
}

fn hyperlink(url: &str, text: &str) -> String {
    format!("\x1B]8;;{}\x1B\\{}\x1B]8;;\x1B\\", url, text)
}
