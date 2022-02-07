use btrfs::{deduplicate_range, DedupeRange, DedupeRangeDestInfo, DedupeRangeStatus};
use std::os::unix::fs::MetadataExt;
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::{fs, io};
use tracing::{debug, info};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockLocation {
    pub path: PathBuf,
    pub offset: u64,
    pub length: usize,
}

#[derive(Debug)]
#[allow(dead_code)]
pub enum BlockDedupError {
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
pub fn dedup(block1: BlockLocation, block2: BlockLocation) -> Result<(), BlockDedupError> {
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
