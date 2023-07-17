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
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
    );

    log::info!("Initializing BotBlock client...");

    let started_at = std::time::Instant::now();

    let client = BotBlock::new();

    let scans = vec![
        // ActivityScan {
        //     period: 60 * 60 * 24,
        //     precision: 15,

        //     iterations: 14,
        //     concurrency: 4,

        //     checks: vec![ActivityScanCheck {
        //         active_ratio_threshold: 0.05,
        //         dispersion_index_threshold: 2.0,
        //     }],
        // },
        // ActivityScan {
        //     period: 60 * 60,
        //     precision: 1,

        //     iterations: 24,
        //     concurrency: 2,

        //     checks: vec![ActivityScanCheck {
        //         active_ratio_threshold: Some(0.002),
        //         dispersion_index_threshold: Some(0.8),
        //     }],
        // },
        ActivityScan {
            period: 60 * 60 * 12,
            precision: 1,

            iterations: 100,
            concurrency: 8,

            checks: vec![ActivityScanCheck {
                active_ratio_threshold: Some(0.5),
                dispersion_index_threshold: None,
            }],
        },
    ];

    for scan in scans {
        client.scan_recent_player_activity(scan).await.unwrap();
    }

    log::info!(
        "Finished BotBlock scans in {} seconds",
        started_at.elapsed().as_secs_f64()
    );
}
