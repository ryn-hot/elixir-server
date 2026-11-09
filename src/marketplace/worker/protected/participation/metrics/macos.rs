//! Conservative admission estimate, not an allocation or latency guarantee.
//! No anonymous/compressed/inactive/purgeable sums and no memory_pressure %.
use anyhow::{Context, Result, bail};

#[cfg(target_os = "macos")]
pub(super) fn pressure() -> Result<u32> {
    let mut level = 0u32;
    let mut size = std::mem::size_of_val(&level);
    // Read-only, fixed kernel key, exact-sized stack output; no input buffer.
    // This sysctl returns DISPATCH flags, not the kernel's internal enum.
    let result = unsafe {
        libc::sysctlbyname(
            c"kern.memorystatus_vm_pressure_level".as_ptr(),
            std::ptr::addr_of_mut!(level).cast(),
            &mut size,
            std::ptr::null_mut(),
            0,
        )
    };
    if result != 0 {
        return Err(std::io::Error::last_os_error()).context("host memory pressure unavailable");
    }
    if size != std::mem::size_of_val(&level) {
        bail!("host memory pressure units differ");
    }
    Ok(level)
}

pub(super) fn available(input: &str, pressure: u32) -> Result<u64> {
    if input.len() > 32 * 1024 || input.as_bytes().contains(&0) {
        bail!("host memory measurement bounds");
    }
    let size = input
        .lines()
        .next()
        .and_then(|header| {
            header
                .strip_prefix("Mach Virtual Memory Statistics: (page size of ")
                .and_then(|value| value.strip_suffix(" bytes)"))
        })
        .context("unknown VM page units")?
        .parse::<u64>()?;
    if !matches!(size, 4096 | 16384) {
        bail!("unsupported VM page size");
    }
    let pages = |label| -> Result<u64> {
        let mut values = input.lines().filter_map(|line| line.strip_prefix(label));
        let value = values.next().context("missing VM page count")?;
        if values.next().is_some() {
            bail!("duplicate VM page count");
        }
        Ok(value
            .trim()
            .strip_suffix('.')
            .context("unknown VM page count")?
            .parse()?)
    };
    let free = pages("Pages free:")?;
    let speculative = pages("Pages speculative:")?;
    let file_backed = pages("File-backed pages:")?;
    // vm_stat displays raw free (Mach free minus speculative). The kernel
    // counts speculative in BOTH Mach free and external/file-backed pages.
    // Partition these disjointly; leave HALF the other file cache untouched,
    // before the ordinary/job headroom margins. This holdback is our policy,
    // not Apple's promise that every file-backed page is immediately free.
    // Primary definitions:
    // apple-oss-distributions/system_cmds vm_stat.tproj/vm_stat.c (sspstat)
    // apple-oss-distributions/xnu osfmk/vm/vm_resident.c (vm_page_speculate)
    // apple-oss-distributions/xnu doc/vm/memorystatus.md (Memory Monitoring)
    let cache = file_backed
        .checked_sub(speculative)
        .context("inconsistent VM file/speculative counts")?;
    let bytes = free
        .checked_add(speculative)
        .and_then(|pages| pages.checked_add(cache / 2))
        .and_then(|pages| pages.checked_mul(size))
        .context("host available memory overflow")?;
    // sysctl_memorystatus_vm_pressure_level converts the internal enum to
    // NOTE_MEMORYSTATUS_PRESSURE_{NORMAL,WARN,CRITICAL}: 1,2,4, not 0,1,3.
    // Unknown/error fails closed; known pressure allows no new admission.
    match pressure {
        1 => Ok(bytes),
        2 | 4 => Ok(0),
        _ => bail!("unknown host memory pressure"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const SAMPLE: &str = "Mach Virtual Memory Statistics: (page size of 4096 bytes)\nPages free: 65536.\nPages speculative: 1024.\nFile-backed pages: 9216.\nPages inactive: 999999.\nPages purgeable: 999999.\nAnonymous pages: 999999.\n";
    #[test]
    fn cache_estimate_excludes_overlap_and_retains_half_the_other_file_cache() -> Result<()> {
        // 256 MiB rawfree +4 MiB speculative +half32 MiB remaining filecache.
        assert_eq!(available(SAMPLE, 1)?, 276 * 1024 * 1024);
        assert_eq!(
            available(&SAMPLE.replace("4096", "16384"), 1)?,
            1104 * 1024 * 1024
        );
        assert_eq!(
            available(&SAMPLE.replace("999999", "18446744073709551615"), 1)?,
            276 * 1024 * 1024
        );
        assert_eq!(
            available(&SAMPLE.replace("9216.", "1024."), 1)?,
            260 * 1024 * 1024
        );
        Ok(())
    }
    #[test]
    fn known_pressure_denies_headroom_and_unknown_flags_never_guess_normal() -> Result<()> {
        for flag in [2, 4] {
            assert_eq!(available(SAMPLE, flag)?, 0);
        }
        for flag in [0, 3, 5, 7, u32::MAX] {
            assert!(available(SAMPLE, flag).is_err());
        }
        Ok(())
    }
    #[test]
    fn malformed_missing_duplicate_overflow_and_inconsistent_counters_fail_closed() {
        for bad in [
            SAMPLE.replace("4096", "8192"),
            SAMPLE.replace("9216.", "1000."),
            SAMPLE.replace("65536.", "18446744073709551615."),
            SAMPLE.replace("9216.", "18446744073709551615."),
            SAMPLE.replace("1024.", "-1."),
            SAMPLE.replace("1024.", "1024"),
            format!("{SAMPLE}\0"),
            "x".repeat(32 * 1024 + 1),
        ] {
            assert!(available(&bad, 1).is_err());
        }
        for line in [
            "Pages free: 65536.\n",
            "Pages speculative: 1024.\n",
            "File-backed pages: 9216.\n",
        ] {
            assert!(available(&SAMPLE.replace(line, ""), 1).is_err());
            assert!(available(&format!("{SAMPLE}{line}"), 1).is_err());
        }
    }
}
