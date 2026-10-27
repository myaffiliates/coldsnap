// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/*!
Block device helper functions.
*/

use snafu::Snafu;
#[cfg(unix)]
use snafu::{ensure, ResultExt};
#[cfg(unix)]
use std::convert::TryFrom;
#[cfg(unix)]
use std::fs::OpenOptions;
#[cfg(unix)]
use std::os::unix::io::AsRawFd;
use std::path::Path;

#[derive(Debug, Snafu)]
pub struct Error(error::Error);
type Result<T> = std::result::Result<T, Error>;

#[cfg(unix)]
/// Generates the blkgetsize64 function.
mod ioctl {
    use nix::ioctl_read;
    ioctl_read!(blkgetsize64, 0x12, 114, u64);
}

#[cfg(unix)]
pub(crate) fn is_block_device(file_type: &std::fs::FileType) -> bool {
    use std::os::unix::fs::FileTypeExt;
    file_type.is_block_device()
}

#[cfg(not(unix))]
pub(crate) fn is_block_device(_file_type: &std::fs::FileType) -> bool {
    false
}

/// Find the size of a block device.
#[cfg(unix)]
pub(crate) fn get_block_device_size(path: &Path) -> Result<i64> {
    let file = OpenOptions::new()
        .read(true)
        .open(path)
        .context(error::OpenFileSnafu { path })?;

    let mut block_device_size = 0;
    let result = unsafe { ioctl::blkgetsize64(file.as_raw_fd(), &mut block_device_size) }
        .context(error::GetBlockDeviceSizeSnafu { path })?;
    ensure!(result == 0, error::InvalidBlockDeviceSizeSnafu { result });

    let block_device_size =
        i64::try_from(block_device_size).with_context(|_| error::ConvertNumberSnafu {
            what: "block device size",
            number: block_device_size.to_string(),
            target: "i64",
        })?;
    Ok(block_device_size)
}

/// Block devices are not detected on platforms without Unix block-device APIs.
#[cfg(not(unix))]
pub(crate) fn get_block_device_size(path: &Path) -> Result<i64> {
    error::UnsupportedBlockDeviceSnafu { path }
        .fail()
        .map_err(Error)
}

/// Potential errors from block device helper functions.
mod error {
    use snafu::Snafu;
    use std::path::PathBuf;

    #[derive(Debug, Snafu)]
    #[snafu(visibility(pub(super)))]
    pub(super) enum Error {
        #[cfg(unix)]
        #[snafu(display("Failed to open '{}': {}", path.display(), source))]
        OpenFile {
            path: PathBuf,
            source: std::io::Error,
        },

        #[cfg(unix)]
        #[snafu(display("Failed to get block device size for '{}': {}", path.display(), source))]
        GetBlockDeviceSize { path: PathBuf, source: nix::Error },

        #[cfg(unix)]
        #[snafu(display("Invalid block device size: {}", result))]
        InvalidBlockDeviceSize { result: i32 },

        #[cfg(unix)]
        #[snafu(display("Failed to convert {} {} to {}: {}", what, number, target, source))]
        ConvertNumber {
            what: String,
            number: String,
            target: String,
            source: std::num::TryFromIntError,
        },

        #[cfg(not(unix))]
        #[snafu(display("Block devices are not supported on this platform: '{}'", path.display()))]
        UnsupportedBlockDevice { path: PathBuf },
    }
}
