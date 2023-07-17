use bytes::Bytes;
use futures::{stream::FuturesUnordered, StreamExt};
use rayon::prelude::{ParallelBridge, ParallelIterator};
use reqwest::Response;
use serde::Deserialize;
use std::{
    cmp, env,
    process::exit,
    sync::{atomic::AtomicU64, Arc},
    time::SystemTime,
};

use crate::discord::GrafanaRender;

const BANNED_USERS: &[&str] = &[
    "304732833543487499",
    "475016236388843522",
    "854500395345641482",
    "1062408462220414997",
    "1102980635359981608", // selfbot
    "930827724942110730",  // bot dev
    "228096171497750528",  // new selfbot duo
    "424382316790546435",
    "944364342965583883", // selfbot 05/18, small server entirely wiped
    // 2 long running selfbots growing evry 10m, private serve
    "1039617200031531101",
    "359457963955453967",
    "430172752700375053",
    "344199622694273069",
    "1061687333625282620",
    "257353688165777408",
    "1041512947882655876",
    "892288625541251093",
    "259389655345135617",
    "180522503175667721",
    "542242219218698241",
    "547585330946244628",
    "398755862354853891",
    "1061336710455251027", // selfbot
    "215255612035039242",  // selfbot
    // week old selfbot pair,
    "1106809740522168443",
    "1106811554231500820",
    // knowing accomplices
    "790780144406757476",
    "802351698026692668",
    "288093658480115712",
    "794042220566675456",
    "1026944710650105896",
    "338847512255528960",
    "327652181107015682",
    "475837799216447500",
    // triple selfbot
    "1089501432606109726",
    "1089517983409393825",
    "1086770907722289213",
    // double selfbot
    "1110648273057894430",
    ////////////////////////,
    "589337371162443796",
    "145994177279295488",
    "724442177303740418",
    "574914455226286100",
    "880138615575949323",
    "1083680741122125925",
    "577012636902883339",
    "1120583959957491832",
    "984149444180213840",
    "706562774029041764",
    // probably knowing accomplices of large group
    "988433294498607115",
    "1042050814283366450",
    "785792156270460929",
    "428359446884909064",
    "949705195074494545",
    // one massive spammer
    "280898491260207107",
    // solo spambot but with only one other waterer in server
    "1118063893592408094",
    "472109093146984469",
    "444281705809838080",
    // sneaky fucker
    "874765316880744580",
    // WOOOO BOT BLOCK
    "972420711413063730",
    "280575018004381697",
    "1028192672533458985",
    "1008475788917612595",
    // NEW
    "721336100060266518",
    "551422711105323018",
    "484082692191682583",
    "1070765449471594506",
    "1042642642904829992",
    "417503711401607170",
    "484082692191682583",
    "1115590045349445652",
    "1098826595894050847",
    "895416800890716193",
    "259928907912839168",
    "517468040695382023",
    "1071495617873989725",
    "1077353683882352746",
    "1103066873395941456",
    "1009239788387319919",
    "1032796039989690380",
    // lol
    "752100724359430195",
    "550811934677663755",
    "406226514934366259",
    "445956808725757962",
    "688146006692593706",
    "367333221811355648",
    "1031579351956860959",
    "259029103716466688",
    "221310372714512384",
    "878179196290088991",
];

fn read_envvar(key: &str) -> String {
    match env::var(key) {
        Ok(value) => value,
        _ => {
            eprintln!("Please set the {} environment variable correctly.", key);
            exit(1);
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Request Error: {0}")]
    RequestError(#[from] reqwest::Error),

    #[error("Cloudflare API Error - {0}: {1}")]
    CloudflareApiError(u16, String),

    #[error("JSON Error: {0}")]
    JsonError(#[from] serde_json::Error),
}

pub struct ActivityScan {
    pub period: u64,
    pub precision: u64,
    pub iterations: u64,
    pub concurrency: usize,

    pub checks: Vec<ActivityScanCheck>,
}

pub struct ActivityScanCheck {
    pub active_ratio_threshold: Option<f64>,
    pub dispersion_index_threshold: Option<f64>,
}

#[derive(Clone)]
pub struct BotBlock {
    cloudflare_uri: String,
    cloudflare_api_token: String,

    pub grafana_uri_base: String,
    pub grafana_api_token: String,

    pub discord_webhook_url: String,

    pub http_client: reqwest::Client,
}

impl BotBlock {
    pub fn new() -> Self {
        let cloudflare_account = read_envvar("CLOUDFLARE_ACCOUNT_ID");
        let cloudflare_api_token = format!("Bearer {}", read_envvar("CLOUDFLARE_API_TOKEN"));

        let cloudflare_uri = format!(
            "https://api.cloudflare.com/client/v4/accounts/{userId}/analytics_engine/sql",
            userId = cloudflare_account
        );

        let grafana_uri_base = read_envvar("GRAFANA_URI_BASE");
        let grafana_api_token = format!("Bearer {}", read_envvar("GRAFANA_API_TOKEN"));

        let discord_webhook_url = read_envvar("DISCORD_WEBHOOK_URL");

        let http_client = reqwest::Client::new();

        BotBlock {
            cloudflare_uri,
            cloudflare_api_token,

            grafana_uri_base,
            grafana_api_token,

            discord_webhook_url,

            http_client,
        }
    }

    pub async fn scan_recent_player_activity(&self, scan: ActivityScan) -> Result<(), Error> {
        let current_timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut user_list = self.fetch_user_list(0, current_timestamp).await?;
        user_list.retain(|entry| !BANNED_USERS.contains(&entry.user_id.as_str()));

        let mut last_start_timestamp = current_timestamp;

        let (tx, rx) = crossbeam::channel::unbounded();

        let client = self.clone();
        tokio::spawn(async move {
            let tx = tx.clone();

            let mut iteration = 0;
            let mut fetch_tasks = FuturesUnordered::new();

            for _ in 0..cmp::min(scan.iterations as usize, scan.concurrency) {
                let end_timestamp = last_start_timestamp;
                let start_timestamp = end_timestamp - scan.period;

                last_start_timestamp = start_timestamp;

                let tx = tx.clone();
                let client: BotBlock = client.clone();

                let future = tokio::task::spawn(async move {
                    let response = client
                        .fetch_user_activity_raw(start_timestamp, end_timestamp, scan.precision)
                        .await;

                    tx.send(response).expect("Channel send failed");
                });

                fetch_tasks.push(future);
                iteration += 1;
            }

            while let Some(_) = fetch_tasks.next().await {
                if iteration >= scan.iterations {
                    break;
                }

                log::info!("Fetching Data Block #{}", iteration);

                let end_timestamp = last_start_timestamp;
                let start_timestamp = end_timestamp - scan.period;

                last_start_timestamp = start_timestamp;

                let tx = tx.clone();
                let client: BotBlock = client.clone();

                let future = tokio::task::spawn(async move {
                    let started_at = SystemTime::now();

                    let response = client
                        .fetch_user_activity_raw(start_timestamp, end_timestamp, scan.precision)
                        .await;

                    log::info!(
                        "Fetched Data Block #{} in {}ms",
                        iteration,
                        started_at.elapsed().unwrap().as_millis()
                    );

                    tx.send(response).expect("Channel send failed");
                });

                fetch_tasks.push(future);
                iteration += 1;
            }
        });

        let total_scan_duration = AtomicU64::new(0);

        const FLOAT_PRECISION_FACTOR: u64 = 1000000;

        let total_active_ratio = AtomicU64::new(0);
        let total_dispersion_index = AtomicU64::new(0);
        let total_standard_deviation = AtomicU64::new(0);

        let total_user_samples = AtomicU64::new(0);

        rx.into_iter()
            .par_bridge()
            .map(|result| -> Result<(), Error> {
                let started_at = SystemTime::now();

                let (start_timestamp, end_timestamp, bytes) = result?;
                let data: QueryResult<UserActivityData> = serde_json::from_slice(&bytes)?;
                let data_size_raw = bytes.len();

                drop(bytes);

                let user_activity = data.data;

                let mut data_by_user_id = user_list
                    .iter()
                    .map(|entry| {
                        (
                            entry.user_id.clone(),
                            UserActivity::new(start_timestamp, end_timestamp, scan.precision),
                        )
                    })
                    .collect::<std::collections::HashMap<String, UserActivity>>();

                for entry in &user_activity {
                    if let Some(activity) = data_by_user_id.get(&entry.user_id) {
                        activity.append_sample(&entry);
                    }
                }

                for (user_id, activity) in data_by_user_id.drain() {
                    if activity.time_active_ratio() == 0.0 {
                        continue;
                    }

                    total_user_samples.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let active_ratio = activity.time_active_ratio();

                    let (mean, variance) = activity.mean_and_variance();
                    let dispersion_index = variance as f64 / mean as f64;

                    let standard_deviation = variance.sqrt();

                    total_active_ratio.fetch_add(
                        (active_ratio * FLOAT_PRECISION_FACTOR as f64) as u64,
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    total_dispersion_index.fetch_add(
                        (dispersion_index * FLOAT_PRECISION_FACTOR as f64) as u64,
                        std::sync::atomic::Ordering::Relaxed,
                    );

                    total_standard_deviation.fetch_add(
                        (standard_deviation * FLOAT_PRECISION_FACTOR as f64) as u64,
                        std::sync::atomic::Ordering::Relaxed,
                    );

                    for check in &scan.checks {
                        let user_id = user_id.clone();

                        let is_over_active_threshold =
                            active_ratio >= check.active_ratio_threshold.unwrap_or(f64::MIN);
                        let is_below_dispersion_threshold = dispersion_index
                            <= check.dispersion_index_threshold.unwrap_or(f64::MAX);

                        if is_over_active_threshold & is_below_dispersion_threshold {
                            log::info!(
                                "User {} ({} - {}) - Active Ratio: {} - Dispersion Index: {}",
                                user_id,
                                start_timestamp,
                                end_timestamp,
                                active_ratio,
                                dispersion_index
                            );

                            let message = format!(
                                "**User {}**\n**Activity Ratio**: {}\n**Dispersion Index**: {}",
                                user_id, active_ratio, dispersion_index
                            );

                            let botblock = self.clone();
                            // tokio::spawn(async move {
                            //     botblock
                            //         .send_webhook(
                            //             &message,
                            //             Some(GrafanaRender {
                            //                 user_id: user_id.to_string(),
                            //                 start_timestamp,
                            //                 end_timestamp,
                            //             }),
                            //         )
                            //         .await
                            //         .unwrap()
                            // });
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

                log::info!(
                    "Scanned {:.2}MB Data Block in {}ms",
                    data_size_raw as f64 / 1024.0 / 1024.0,
                    elapsed.as_millis()
                );

                Ok(())
            })
            .collect::<Result<(), Error>>();

        let average_scan_duration =
            total_scan_duration.load(std::sync::atomic::Ordering::SeqCst) / scan.iterations;

        let user_sample_count = total_user_samples.load(std::sync::atomic::Ordering::SeqCst);

        let average_active_ratio =
            total_active_ratio.load(std::sync::atomic::Ordering::SeqCst) / user_sample_count;
        let average_dispersion_index =
            total_dispersion_index.load(std::sync::atomic::Ordering::SeqCst) / user_sample_count;

        let average_standard_deviation =
            total_standard_deviation.load(std::sync::atomic::Ordering::SeqCst) / user_sample_count;

        log::info!(
            "Scanned total of {} seconds, {}x {}s blocks with {} second precision. Average block processing duration: {}ms",
            scan.period * scan.iterations,
            scan.iterations,
            scan.period,
            scan.precision,
            average_scan_duration
        );

        log::info!(
            "Average Active Ratio: {} - Average Dispersion Index: {} - Average Standard Deviation: {}",
            average_active_ratio,
            average_dispersion_index,
            average_standard_deviation
        );

        Ok(())
    }

    async fn query(&self, query: String) -> Result<Response, Error> {
        log::debug!("Querying Cloudflare API: {}", query);

        let response = self
            .http_client
            .post(&self.cloudflare_uri)
            .body(query)
            .header("Authorization", &self.cloudflare_api_token)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status().as_u16();
            let body = response.text().await?;

            return Err(Error::CloudflareApiError(status, body));
        }

        Ok(response)
    }

    async fn fetch_user_list(
        &self,
        start_timestamp: u64,
        end_timestamp: u64,
    ) -> Result<RecentUserList, Error> {
        let query = format!(
            "
            SELECT
                blob4 as user_id
            FROM analytics_v0
            WHERE timestamp >= toDateTime({}) AND timestamp <= toDateTime({})
            GROUP BY user_id; FORMAT JSON
        ",
            start_timestamp, end_timestamp
        );

        let raw = self.query(query).await?;
        Ok(raw.json::<QueryResult<RecentUserList>>().await?.data)
    }

    async fn fetch_user_activity_raw(
        &self,
        start_timestamp: u64,
        end_timestamp: u64,
        precision: u64,
    ) -> Result<(u64, u64, Bytes), Error> {
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
        Ok((start_timestamp, end_timestamp, raw.bytes().await?))
    }
}

#[derive(Deserialize)]
pub struct QueryResult<T> {
    data: T,
}

pub type RecentUserList = Vec<RecentUserEntry>;

#[derive(Deserialize, Debug)]
pub struct RecentUserEntry {
    user_id: String,
}

pub type UserActivityData = Vec<UserActivityEntry>;

#[derive(Deserialize, Debug)]
pub struct UserActivityEntry {
    user_id: String,
    interactions: String,
    // interval_start: String,
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

    active_intervals: AtomicU64,

    total_interactions: AtomicU64,
    total_squared_interactions: AtomicU64,
}

impl UserActivity {
    fn new(start_timestamp: u64, end_timestamp: u64, precision: u64) -> Self {
        UserActivity {
            meta: Arc::new(UserActivityMeta {
                start_timestamp,
                end_timestamp,

                precision,
            }),

            active_intervals: AtomicU64::new(0),

            total_interactions: AtomicU64::new(0),
            total_squared_interactions: AtomicU64::new(0),
        }
    }

    pub fn append_sample(&self, entry: &UserActivityEntry) {
        let count = entry.interactions.parse::<u64>().unwrap();

        self.total_interactions
            .fetch_add(count, std::sync::atomic::Ordering::Release);
        self.total_squared_interactions
            .fetch_add(count * count, std::sync::atomic::Ordering::Release);

        self.active_intervals
            .fetch_add(1, std::sync::atomic::Ordering::Release);
    }

    pub fn time_active_ratio(&self) -> f64 {
        let active_samples = self
            .active_intervals
            .load(std::sync::atomic::Ordering::Acquire) as f64;

        let total_intervals = (self.meta.end_timestamp - self.meta.start_timestamp) as f64
            / self.meta.precision as f64;

        active_samples / total_intervals
    }

    pub fn mean_and_variance(&self) -> (f64, f64) {
        let total_intervals = (self.meta.end_timestamp - self.meta.start_timestamp) as f64
            / self.meta.precision as f64;

        let total = self
            .total_interactions
            .load(std::sync::atomic::Ordering::Acquire) as f64;
        let total_squared = self
            .total_squared_interactions
            .load(std::sync::atomic::Ordering::Acquire) as f64;

        let mean = total / total_intervals;
        let variance = total_squared / total_intervals - mean * mean;

        (mean, variance)
    }
}
