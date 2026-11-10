//! Exercise the real per-entry extractor, not ZipArchive's high-level extract.
use super::*;
use std::io::Write;
use zip::{CompressionMethod, ZipWriter, write::SimpleFileOptions};

fn archive(path: &Path, name: &str, contents: &[u8], compression: CompressionMethod) -> Result<()> {
    let mut writer = ZipWriter::new(File::create(path)?);
    writer.start_file(
        name,
        SimpleFileOptions::default().compression_method(compression),
    )?;
    writer.write_all(contents)?;
    writer.finish()?;
    Ok(())
}

#[test]
fn existing_zip_formats_extract_identical_bytes() -> Result<()> {
    let directory = tempfile::tempdir()?;
    for (n, compression) in [
        CompressionMethod::Stored,
        CompressionMethod::Deflated,
        CompressionMethod::Bzip2,
        CompressionMethod::Zstd,
        CompressionMethod::Xz,
    ]
    .into_iter()
    .enumerate()
    {
        let package = directory.path().join(format!("{n}.zip"));
        let destination = directory.path().join(format!("out-{n}"));
        std::fs::create_dir(&destination)?;
        let contents = b"harmless synthetic compatibility data\n";
        archive(&package, "src/provider.mjs", contents, compression)?;
        unpack_zip(&package, &destination)?;
        assert_eq!(
            std::fs::read(destination.join("src/provider.mjs"))?,
            contents
        );
    }
    Ok(())
}

#[test]
fn traversal_is_rejected_without_writing_outside_staging() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let package = directory.path().join("traversal.zip");
    let destination = directory.path().join("stage");
    std::fs::create_dir(&destination)?;
    archive(
        &package,
        "../outside",
        b"must not escape",
        CompressionMethod::Stored,
    )?;
    assert!(unpack_zip(&package, &destination).is_err());
    assert!(!directory.path().join("outside").exists());
    assert_eq!(std::fs::read_dir(&destination)?.count(), 0);
    Ok(())
}

#[test]
fn symlink_is_rejected_without_following_or_chmodding_target() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let target = directory.path().join("outside");
    std::fs::write(&target, b"original")?;
    let package = directory.path().join("link.zip");
    let destination = directory.path().join("stage");
    std::fs::create_dir(&destination)?;
    let before = std::fs::metadata(&target)?.permissions();
    let mut writer = ZipWriter::new(File::create(&package)?);
    writer.add_symlink("link", "../outside", SimpleFileOptions::default())?;
    writer.finish()?;
    assert!(unpack_zip(&package, &destination).is_err());
    assert_eq!(std::fs::read(&target)?, b"original");
    assert_eq!(std::fs::metadata(&target)?.permissions(), before);
    assert_eq!(std::fs::read_dir(&destination)?.count(), 0);
    Ok(())
}
