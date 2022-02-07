use clap::Parser;
use dedup::{BlockDedupError, BlockLocation};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::mpsc;
use std::thread;
use tracing::warn;

mod dedup;
mod scan;

#[derive(Parser, Debug)]
struct Args {
    /// Root directory for optimization
    root: Vec<PathBuf>,
    /// Deduplication block size
    #[clap(short, default_value_t = 4096)]
    block_size: usize,

    /// Queue size for storing dedup tasks between scan and deduplication
    #[clap(short, default_value_t = 32)]
    dedup_queue: usize,
}

fn main() {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    let mut block_locations: HashMap<_, BlockLocation> = HashMap::new();

    let (scanned_tx, scanned_rx) = mpsc::sync_channel(args.dedup_queue);

    // Crawlers in thread pool
    let crawler_handle = thread::spawn(move || {
        scan::crawl_paths(&args.root, args.block_size, scanned_tx);
    });

    // Main thread: dedup
    while let Ok(scan_result) = scanned_rx.recv() {
        for (number, block_hash) in scan_result.block_hashes.iter().enumerate() {
            let block_location = scan_result.get_block_location(number);

            match block_locations.get(block_hash) {
                Some(x) => {
                    let res = dedup::dedup(x.clone(), block_location);
                    match res {
                        Ok(_) => {}
                        Err(BlockDedupError::SameBlock { block }) => {
                            warn!("Block dedup struct points to exact same block: {block:?}");
                        }
                        Err(BlockDedupError::SameExtent { .. }) => {
                            warn!("Possible hardlinks detected: {:?}", res.err());
                        }
                        Err(BlockDedupError::DedupInternal(e)) => {
                            warn!("Dedup returned error: {e}");
                        }
                        Err(BlockDedupError::FileErrors(e1, e2)) => {
                            warn!("I/O error: {e1:?}, {e2:?}");
                        }
                    }
                }
                None => {
                    block_locations.insert(*block_hash, block_location);
                }
            };
        }
    }

    let _ = crawler_handle.join();
}
