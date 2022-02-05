use btrfs::{deduplicate_range, DedupeRange, DedupeRangeDestInfo, DedupeRangeStatus};
use clap::Parser;
use crc64fast::Digest;
use std::collections::HashMap;
use std::io::{BufReader, Error, Read};
use std::os::unix::fs::MetadataExt;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::{fs, io, thread};
use tracing::{debug, info, trace, warn};
use walkdir::WalkDir;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct BlockLocation {
    path: PathBuf,
    offset: u64,
    length: usize,
}

#[derive(Debug)]
enum BlockDedupError {
    SameBlock {
        block: BlockLocation,
    },
    SameExtent {
        path1: PathBuf,
        path2: PathBuf,
        ino: u64,
        offset: u64,
        length: usize,
    },
    DedupInternal(String),
    FileErrors(Option<io::Error>, Option<io::Error>),
}

#[tracing::instrument]
fn dedup(block1: BlockLocation, block2: BlockLocation) -> Result<(), BlockDedupError> {
    debug!(
        "Trying to DEDUP {:?}[{}..{}], {:?}[{}..{}]",
        block1.path, block1.offset, block1.length, block2.path, block2.offset, block2.length
    );
    if block1 == block2 {
        return Err(BlockDedupError::SameBlock { block: block1 });
    }

    match (
        fs::File::open(block1.path.clone()),
        fs::OpenOptions::new().write(true).open(block2.path.clone()),
    ) {
        (Ok(file1), Ok(file2)) => {
            let (ino1, ino2) = (
                file1.metadata().unwrap().ino(),
                file2.metadata().unwrap().ino(),
            );

            if (ino1, block1.offset, block1.length) == (ino2, block2.offset, block2.length) {
                return Err(BlockDedupError::SameExtent {
                    path1: block1.path,
                    path2: block2.path,
                    ino: ino1,
                    offset: block1.offset,
                    length: block1.length,
                });
            }

            let mut range = DedupeRange {
                src_offset: block1.offset,
                src_length: block1.length as u64,
                dest_infos: vec![DedupeRangeDestInfo {
                    dest_fd: file2.as_raw_fd() as i64,
                    dest_offset: block2.offset,
                    bytes_deduped: block2.length as u64,
                    status: DedupeRangeStatus::Same,
                }],
            };

            let x = deduplicate_range(file1.as_raw_fd(), &mut range);

            match x {
                Ok(_) => {
                    info!(
                        "DEDUP [{}..{}], [{}..{}]",
                        block1.offset, block1.length, block2.offset, block2.length
                    )
                }
                Err(e) => return Err(BlockDedupError::DedupInternal(e)),
            }
        }
        (e1, e2) => return Err(BlockDedupError::FileErrors(e1.err(), e2.err())),
    }

    Ok(())
}

#[derive(Parser, Debug)]
struct Args {
    /// Root directory for optimization
    root: Vec<PathBuf>,
    /// Deduplication block size
    #[clap(short)]
    block_size: usize,

    /// Queue size for storing dedup tasks between scan and deduplication
    #[clap(short, default_value_t = 32)]
    dedup_queue: usize,
}

type Hash = u64;

#[derive(Default)]
struct ScanResult {
    path: PathBuf,
    block_size: usize,
    block_hashes: Vec<Hash>,
    last_block_size: usize,
}

impl ScanResult {
    fn get_block_location(&self, index: usize) -> BlockLocation {
        BlockLocation {
            path: self.path.clone(),
            offset: (self.block_size as u64) * (index as u64),
            length: if index == self.block_hashes.len() - 1 {
                self.last_block_size
            } else {
                self.block_size
            },
        }
    }
}

enum ScanError {
    IoError(io::Error),
}

impl From<io::Error> for ScanError {
    fn from(e: Error) -> Self {
        Self::IoError(e)
    }
}

#[tracing::instrument]
fn scan_file(path: &Path, block_size: usize) -> Result<ScanResult, ScanError> {
    let absolute_path = fs::canonicalize(path)?;

    info!("Scanning {}", absolute_path.to_string_lossy());

    let file = fs::File::open(absolute_path.clone())?;
    let file_size = file.metadata()?.len();
    let blocks_total = ((file_size + (block_size as u64) - 1) / (block_size as u64)) as usize;
    let mut reader = BufReader::new(file);
    let mut buf = vec![0u8; block_size];

    let mut result = ScanResult {
        path: absolute_path,
        block_size,
        block_hashes: Vec::with_capacity(blocks_total),
        ..ScanResult::default()
    };

    loop {
        match reader.read(&mut buf)? {
            0 => break,
            chunk_size => {
                let chunk_data = &buf[0..chunk_size];

                let chunk_hash = {
                    let mut digest = Digest::new();
                    digest.write(chunk_data);
                    digest.sum64()
                };

                result.block_hashes.push(chunk_hash);
                result.last_block_size = chunk_size;
            }
        }
    }
    Ok(result)
}

fn crawl_paths(paths: &[PathBuf], block_size: usize, scanned_tx: mpsc::SyncSender<ScanResult>) {
    let walk_iter = paths.iter().flat_map(|path| {
        WalkDir::new(path)
            .same_file_system(true)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|e| e.file_type().is_file())
    });

    for entry in walk_iter {
        let path = entry.path();
        let scan_result = scan_file(path, block_size);

        if let Ok(scan_result) = scan_result {
            scanned_tx.send(scan_result).unwrap();
        }
    }
}

fn main() {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    let mut inodes: HashMap<_, BlockLocation> = HashMap::new();

    let (scanned_tx, scanned_rx) = mpsc::sync_channel(args.dedup_queue);

    let crawler_handle = thread::spawn(move || {
        crawl_paths(&args.root, args.block_size, scanned_tx);
    });

    while let Ok(scan_result) = scanned_rx.recv() {
        for (number, block_hash) in scan_result.block_hashes.iter().enumerate() {
            let block_location = scan_result.get_block_location(number);

            match inodes.get(block_hash) {
                Some(x) => {
                    let res = dedup(x.clone(), block_location);
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
                    inodes.insert(*block_hash, block_location);
                }
            };
        }
    }

    crawler_handle.join();
}
