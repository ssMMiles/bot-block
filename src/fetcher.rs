use std::cmp;

use crossbeam::channel::Receiver;
use futures::{future::BoxFuture, stream::FuturesUnordered, StreamExt};

pub async fn start_fetcher_task<
    'a,
    T: Sized + Send + Sync + 'a,
    S: Sized + Send + Sync + 'a + Clone,
>(
    iterations: usize,
    concurrency: usize,

    mut fetcher: impl FnMut(usize, S) -> BoxFuture<'a, T> + Send + Sync + 'a,
    state: S,
) -> Receiver<T> {
    let (tx, rx) = crossbeam::channel::unbounded::<T>();

    tokio::spawn(async move {
        let mut iteration: usize = 0;
        let mut fetch_tasks = FuturesUnordered::new();

        let mut start_fetcher = |iteration: &mut usize| {
            *iteration += 1;

            return fetcher(*iteration, state.clone());
        };

        for _ in 0..cmp::min(iterations, concurrency) {
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

            if iteration >= iterations {
                drop(tx);
                break;
            }

            fetch_tasks.push(start_fetcher(&mut iteration));
        }
    });

    rx
}
