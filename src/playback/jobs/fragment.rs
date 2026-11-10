//! Bounded fMP4 framing check for our own FFmpeg startup artifacts. This is not
//! decoder validation. Seek over media payloads; never allocate a whole segment.
use std::{io, path::Path};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

pub(super) async fn ready(path: &Path) -> io::Result<bool> {
    let mut file = tokio::fs::File::open(path).await?;
    let metadata = file.metadata().await?;
    let total = metadata.len();
    if !metadata.is_file() || total < 24 {
        return Ok(false);
    }
    let (mut offset, mut moof, mut mdat) = (0_u64, false, false);
    // Normal HLS output has only a handful of top-level boxes. Bound malformed
    // output independently of payload size, without restricting video bitrate.
    for _ in 0..1024 {
        if offset == total {
            return Ok(moof && mdat && file.metadata().await?.len() == total);
        }
        file.seek(io::SeekFrom::Start(offset)).await?;
        let mut header = [0_u8; 8];
        if file.read_exact(&mut header).await.is_err() {
            return Ok(false);
        }
        let short = u32::from_be_bytes(header[..4].try_into().unwrap());
        let (size, prefix) = if short == 1 {
            let mut extended = [0_u8; 8];
            if file.read_exact(&mut extended).await.is_err() {
                return Ok(false);
            }
            (u64::from_be_bytes(extended), 16_u64)
        } else if short == 0 {
            (total - offset, 8)
        } else {
            (u64::from(short), 8)
        };
        let Some(next) = offset.checked_add(size) else {
            return Ok(false);
        };
        if size < prefix || next > total || next <= offset {
            return Ok(false);
        }
        match &header[4..] {
            b"moof" if size > prefix => moof = true,
            b"mdat" if moof && size > prefix => mdat = true,
            _ => {}
        }
        offset = next;
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn boxed(kind: &[u8; 4], payload: &[u8]) -> Vec<u8> {
        let mut bytes = u32::try_from(payload.len() + 8)
            .unwrap()
            .to_be_bytes()
            .to_vec();
        bytes.extend_from_slice(kind);
        bytes.extend_from_slice(payload);
        bytes
    }

    #[tokio::test]
    async fn header_only_init_partial_and_overflowing_fragments_are_not_ready() -> io::Result<()> {
        let root = tempfile::tempdir()?;
        let path = root.path().join("segment.m4s");
        let moof = boxed(b"moof", &[0; 8]); // Framing only, not a decodable movie.
        let mdat = boxed(b"mdat", &[1; 12]);
        let valid = [moof.as_slice(), mdat.as_slice()].concat();
        let mut truncated = valid.clone();
        truncated.pop();
        let mut overflow = moof.clone();
        overflow.extend_from_slice(&1_u32.to_be_bytes());
        overflow.extend_from_slice(b"mdat");
        overflow.extend_from_slice(&u64::MAX.to_be_bytes());
        for (bytes, expected) in [
            (vec![], false),
            (boxed(b"styp", &[0; 16]), false), // Actual observed 24-byte failure.
            (boxed(b"ftyp", &[0; 16]), false),
            (moof.clone(), false),
            (mdat.clone(), false),
            ([moof.clone(), boxed(b"mdat", &[])].concat(), false),
            (truncated, false),
            (overflow, false),
            (valid, true),
        ] {
            tokio::fs::write(&path, bytes).await?;
            assert_eq!(ready(&path).await?, expected);
        }
        Ok(())
    }

    #[tokio::test]
    async fn extended_and_eof_data_boxes_work_without_loading_large_payloads() -> io::Result<()> {
        let root = tempfile::tempdir()?;
        let path = root.path().join("segment.m4s");
        let moof = boxed(b"moof", &[0; 8]);
        let size = 32 * 1024 * 1024_u64;
        let mut prefix = moof.clone();
        prefix.extend_from_slice(&1_u32.to_be_bytes());
        prefix.extend_from_slice(b"mdat");
        prefix.extend_from_slice(&size.to_be_bytes());
        tokio::fs::write(&path, &prefix).await?;
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .await?;
        file.set_len(moof.len() as u64 + size).await?;
        assert!(ready(&path).await?); // Sparse media bytes are skipped, not read.
        let mut eof = moof;
        eof.extend_from_slice(&0_u32.to_be_bytes());
        eof.extend_from_slice(b"mdat");
        eof.push(1);
        tokio::fs::write(&path, eof).await?;
        assert!(ready(&path).await?);
        Ok(())
    }

    #[tokio::test]
    async fn startup_does_not_promote_a_header_only_filename() -> io::Result<()> {
        let root = tempfile::tempdir()?;
        let path = root.path().join("segment_00000.m4s");
        tokio::fs::write(&path, boxed(b"styp", &[0; 16])).await?;
        assert!(!super::super::has_first_media_segment(root.path()).await);
        tokio::fs::write(
            &path,
            [boxed(b"moof", &[0; 8]), boxed(b"mdat", &[1; 12])].concat(),
        )
        .await?;
        assert!(super::super::has_first_media_segment(root.path()).await);
        Ok(())
    }
}
