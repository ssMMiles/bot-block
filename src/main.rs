use crate::botblock::{ActivityScan, ActivityScanCheck, BotBlock};

#[cfg(feature = "mimalloc")]
use mimalloc::MiMalloc;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

mod botblock;
mod discord;

#[tokio::main]
async fn main() {
    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "h2=info,info"),
    );

    log::info!("Initializing BotBlock");

    let started_at = std::time::Instant::now();

    let client = match BotBlock::new().await {
        Ok(client) => client,
        Err(err) => {
            log::error!("Failed to initialize BotBlock: {}", err);
            return;
        }
    };

    let scans = vec![
        ActivityScan {
            name: "Moderately Clustered Daily Activity".to_owned(),

            period: 60 * 60 * 24,
            precision: 15,

            iterations: 4,
            concurrency: 4,

            checks: vec![ActivityScanCheck {
                active_ratio_threshold: Some(0.05),
                dispersion_index_threshold: Some(2.0),
                delta_dispersion_index_threshold: None,
            }],
        },
        ActivityScan {
            name: "High Precision & Highly Clustered Hourly Activity".to_owned(),

            period: 60 * 60,
            precision: 1,

            iterations: 4,
            concurrency: 4,

            checks: vec![ActivityScanCheck {
                active_ratio_threshold: Some(0.002),
                dispersion_index_threshold: Some(0.8),
                delta_dispersion_index_threshold: None,
            }],
        },
        ActivityScan {
            name: "Bi-Daily High Activity".to_owned(),

            period: 60 * 60 * 12,
            precision: 60 * 3,

            iterations: 4,
            concurrency: 4,

            checks: vec![ActivityScanCheck {
                active_ratio_threshold: Some(0.8),
                dispersion_index_threshold: None,
                delta_dispersion_index_threshold: None,
            }],
        },
        ActivityScan {
            name: "Daily High Activity".to_owned(),

            period: 60 * 60 * 24,
            precision: 60 * 20,

            iterations: 4,
            concurrency: 4,

            checks: vec![ActivityScanCheck {
                active_ratio_threshold: Some(0.9),
                dispersion_index_threshold: None,
                delta_dispersion_index_threshold: None,
            }],
        },
    ];

    for scan in scans {
        let scan_started_at = std::time::Instant::now();
        let name = scan.name.clone();

        log::info!("Scan Starting: {}", name);

        if let Err(err) = client.scan_recent_player_activity(scan).await {
            log::error!("Scan Failed: {}", err);
            continue;
        }

        log::info!(
            "Scan Complete ({:.2}s)",
            scan_started_at.elapsed().as_secs_f64()
        );
    }

    log::info!(
        "All Scans Complete ({:.2}s)",
        started_at.elapsed().as_secs_f64()
    );
}
