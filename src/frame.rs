//! Shared frame reader with caller-owned allocation permits and deadlines.

use std::io;
use tokio::io::{AsyncRead, AsyncReadExt};

#[derive(Debug)]
pub(crate) enum ReadError {
    Io(io::Error),
    TooLarge { length: u32, max_frame_bytes: u32 },
}

impl From<io::Error> for ReadError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl ReadError {
    pub(crate) fn into_io(self) -> io::Error {
        match self {
            Self::Io(error) => error,
            Self::TooLarge {
                length,
                max_frame_bytes,
            } => io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "frame size {length} exceeds max_frame_bytes {max_frame_bytes}; refusing to allocate"
                ),
            ),
        }
    }
}

pub(crate) async fn read<R, P>(
    stream: &mut R,
    max_frame_bytes: u32,
    reserve: impl FnOnce(u32) -> io::Result<P>,
) -> Result<(Vec<u8>, P), ReadError>
where
    R: AsyncRead + Unpin,
{
    let mut prefix = [0_u8; 4];
    stream.read_exact(&mut prefix).await?;
    let length = u32::from_be_bytes(prefix);
    if length > max_frame_bytes {
        return Err(ReadError::TooLarge {
            length,
            max_frame_bytes,
        });
    }
    let permit = reserve(length)?;
    let mut bytes = vec![0_u8; length as usize];
    stream.read_exact(&mut bytes).await?;
    Ok((bytes, permit))
}
