//! Fixed read-only measurements shared by an inspected owner exec and the
//! standalone fenced measurement actor. No shell/code or paths from a request.
//! Parsing observations does not establish their native ownership or admission.

#[derive(Clone, Copy)]
pub(crate) enum Disk {
    State,
    Root,
}

pub(crate) fn command(disk: Disk) -> Vec<String> {
    let path = match disk {
        Disk::State => "/state",
        Disk::Root => "/",
    };
    let script = format!(
        "printf 'ELIXIR_RESOURCE_V1\\nCPU_BEFORE\\n'\n\
         /usr/bin/head -n 1 /proc/stat\n\
         printf 'KERNEL\\n'\n\
         /bin/cat /proc/meminfo /proc/pressure/cpu /proc/pressure/memory /proc/pressure/io\n\
         printf 'DISK\\n'\n\
         /usr/bin/stat -f -c '%a %S' {path}\n\
         /bin/sleep 1\n\
         printf 'CPU_AFTER\\n'\n\
         /usr/bin/head -n 1 /proc/stat\n"
    );
    vec!["/bin/sh".into(), "-eu".into(), "-c".into(), script]
}

pub(crate) struct Snapshot<'a> {
    pub(crate) before: &'a str,
    pub(crate) kernel: &'a str,
    pub(crate) disk: &'a str,
    pub(crate) after: &'a str,
}
impl<'a> Snapshot<'a> {
    pub(crate) fn parse(input: &'a str) -> Result<Self, &'static str> {
        if input.len() > 32 * 1024 || input.contains('\0') {
            return Err("resource probe bounds");
        }
        for tag in [
            "ELIXIR_RESOURCE_V1",
            "CPU_BEFORE",
            "KERNEL",
            "DISK",
            "CPU_AFTER",
        ] {
            if input.lines().filter(|line| *line == tag).count() != 1 {
                return Err("missing/repeated resource probe section");
            }
        }
        let input = input
            .strip_prefix("ELIXIR_RESOURCE_V1\nCPU_BEFORE\n")
            .ok_or("resource probe version/order")?;
        let (before, input) = input
            .split_once("\nKERNEL\n")
            .ok_or("resource kernel section")?;
        let (kernel, input) = input
            .split_once("\nDISK\n")
            .ok_or("resource disk section")?;
        let (disk, after) = input
            .split_once("\nCPU_AFTER\n")
            .ok_or("resource final CPU section")?;
        if [before, kernel, disk, after]
            .iter()
            .any(|value| value.is_empty())
        {
            return Err("empty resource probe section");
        }
        Ok(Self {
            before,
            kernel,
            disk,
            after,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn closed_resource_frame_rejects_missing_repeated_reordered_or_unbounded_sections() {
        let valid = "ELIXIR_RESOURCE_V1\nCPU_BEFORE\ncpu 1\nKERNEL\nMemAvailable: 1 kB\nDISK\n10 4096\nCPU_AFTER\ncpu 2\n";
        let parsed = Snapshot::parse(valid).unwrap();
        assert_eq!(parsed.before, "cpu 1");
        assert_eq!(parsed.kernel, "MemAvailable: 1 kB");
        assert_eq!(parsed.disk, "10 4096");
        assert_eq!(parsed.after, "cpu 2\n");
        for bad in [
            valid.replace("_V1", "_V2"),
            valid.replace("KERNEL\n", "KERNEL\nKERNEL\n"),
            valid.replace("DISK\n", ""),
            valid.replace("cpu 1\n", ""),
            valid.replace("CPU_BEFORE", "CPU_AFTER"),
            format!("{valid}\0"),
            "x".repeat(32 * 1024 + 1),
        ] {
            assert!(Snapshot::parse(&bad).is_err());
        }
    }
    #[test]
    fn only_fixed_state_or_root_paths_can_be_selected_for_the_read_only_script() {
        for (disk, path) in [(Disk::State, "/state"), (Disk::Root, "/")] {
            let command = command(disk);
            assert_eq!(&command[..3], ["/bin/sh", "-eu", "-c"]);
            assert!(command[3].contains(&format!("'%a %S' {path}\n")));
            assert!(!command[3].contains('$'));
        }
    }
}
