use crate::dedup::BlockLocation;
use crc64fast::Digest;
use rayon::iter::{ParallelBridge, ParallelIterator};
use std::io::{BufReader, Error, Read};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::SystemTime;
use std::{fs, io};
use tracing::info;
use walkdir::WalkDir;

type Hash = u64;

#[derive(Clone)]
pub struct ScanResult {
    pub path: PathBuf,
    block_size: usize,
    pub block_hashes: Vec<Hash>,
    pub last_block_size: usize,
    pub mtime: SystemTime,
    pub ino: u64,
}

impl ScanResult {
    pub fn get_block_location(&self, index: usize) -> BlockLocation {
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

pub enum ScanError {
    IoError(io::Error),
}

impl From<io::Error> for ScanError {
    fn from(e: Error) -> Self {
        Self::IoError(e)
    }
}

#[tracing::instrument]
pub fn scan_file(path: &Path, block_size: usize) -> Result<ScanResult, ScanError> {
    let absolute_path = fs::canonicalize(path)?;

    info!("Scanning {}", absolute_path.to_string_lossy());

    let file = fs::File::open(absolute_path.clone())?;
    let metadata = file.metadata()?;
    let file_size = metadata.len();
    let blocks_total = ((file_size + (block_size as u64) - 1) / (block_size as u64)) as usize;

    let mut result = ScanResult {
        path: absolute_path,
        block_size,
        block_hashes: Vec::with_capacity(blocks_total),
        last_block_size: (file_size % (block_size as u64)) as usize,
        mtime: metadata.modified()?,
        ino: metadata.ino(),
    };

    let mut reader = BufReader::new(file);
    let mut buf = vec![0u8; block_size];
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
            }
        }
    }
    Ok(result)
}

pub fn crawl_paths(paths: &[PathBuf], block_size: usize, scanned_tx: mpsc::SyncSender<ScanResult>) {
    paths
        .iter()
        .flat_map(|path| {
            WalkDir::new(path)
                .same_file_system(true)
                .into_iter()
                .filter_map(Result::ok)
                .filter(|e| e.file_type().is_file())
        })
        .par_bridge()
        .for_each(|entry| {
            let scan_result = scan_file(entry.path(), block_size);

            if let Ok(scan_result) = scan_result {
                scanned_tx.send(scan_result).unwrap();
            }
        });
}
