//! Aggregate guest CPU capacity, not quota-induced PSI waiting time. No I/O,
//! caller-supplied health capability, or adjustable worker resource budget.

#[derive(Clone, Copy, Debug)]
pub(super) struct Sample([u64; 8]);

impl Sample {
    pub(super) fn parse(input: &str) -> Result<Self, &'static str> {
        if input.len() > 512 || !input.is_ascii() {
            return Err("CPU counter sample bounds");
        }
        let mut lines = input.lines();
        let line = lines.next().ok_or("missing CPU counters")?;
        if lines.next().is_some() {
            return Err("extra CPU counter lines");
        }
        let fields: Vec<_> = line.split_ascii_whitespace().collect();
        // Linux cgroup-v2/PSI-capable kernels expose these ten aggregate fields.
        // guest/guest_nice are already included in user/nice; validate but do
        // not add them twice. Unknown formats refuse admission.
        if fields.len() != 11 || fields[0] != "cpu" {
            return Err("unknown aggregate CPU counter format");
        }
        let mut counters = [0; 8];
        for (index, field) in fields[1..].iter().enumerate() {
            if field.is_empty() || !field.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err("invalid CPU counter");
            }
            let value = field.parse::<u64>().map_err(|_| "CPU counter overflow")?;
            if index < counters.len() {
                counters[index] = value;
            }
        }
        Ok(Self(counters))
    }

    pub(super) fn pressured_since(self, before: Self) -> Result<bool, &'static str> {
        let mut delta = [0_u64; 8];
        let mut total = 0_u64;
        for (index, (after, before)) in self.0.into_iter().zip(before.0).enumerate() {
            delta[index] = after
                .checked_sub(before)
                .ok_or("CPU counters went backwards")?;
            total = total
                .checked_add(delta[index])
                .ok_or("CPU delta overflow")?;
        }
        if total == 0 {
            return Err("CPU counters did not advance");
        }
        // iowait is idle CPU time; the independent I/O PSI gate still applies.
        // Stolen VM time is NOT available capacity, so keep it in busy time.
        let idle = delta[3].checked_add(delta[4]).ok_or("CPU idle overflow")?;
        let busy = total.checked_sub(idle).ok_or("invalid CPU idle counters")?;
        Ok(u128::from(busy) * 100 >= u128::from(total) * 80)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn row(values: [u64; 8]) -> Sample {
        Sample(values)
    }

    #[test]
    fn requires_two_advancing_monotonic_closed_samples() {
        let valid = "cpu  100 2 3 400 5 6 7 8 90 1\n";
        assert!(Sample::parse(valid).is_ok());
        for invalid in [
            "",
            "cpu0 1 2 3 4 5 6 7 8 9 10",
            "cpu 1 2",
            "cpu 1 2 3 4 5 6 7 8 9 10 11",
            "cpu +1 2 3 4 5 6 7 8 9 10",
            "cpu 1 2 3 4 5 6 7 8 NaN 10",
            "cpu 1 2 3 4 5 6 7 8 9 18446744073709551616",
            "cpu 1 2 3 4 5 6 7 8 9 10\ncpu0 1 2",
        ] {
            assert!(Sample::parse(invalid).is_err(), "{invalid}");
        }
        let start = row([100; 8]);
        assert!(start.pressured_since(start).is_err());
        assert!(
            row([99, 100, 100, 200, 100, 100, 100, 100])
                .pressured_since(start)
                .is_err()
        );
        assert!(row([u64::MAX; 8]).pressured_since(row([0; 8])).is_err());
    }

    #[test]
    fn counts_real_capacity_at_the_exact_boundary_and_keeps_steal_unavailable() {
        let start = row([0; 8]);
        assert!(
            !row([79, 0, 0, 21, 0, 0, 0, 0])
                .pressured_since(start)
                .unwrap()
        );
        assert!(
            row([80, 0, 0, 20, 0, 0, 0, 0])
                .pressured_since(start)
                .unwrap()
        );
        assert!(
            !row([10, 0, 0, 0, 90, 0, 0, 0])
                .pressured_since(start)
                .unwrap()
        );
        assert!(
            row([0, 0, 0, 20, 0, 0, 0, 80])
                .pressured_since(start)
                .unwrap()
        );
    }

    #[test]
    fn guest_ticks_are_not_double_counted_and_absolute_counters_need_not_sum() {
        let start = Sample::parse("cpu 0 0 0 0 0 0 0 0 0 0").unwrap();
        let guest = Sample::parse("cpu 70 0 0 30 0 0 0 0 70 0").unwrap();
        assert!(!guest.pressured_since(start).unwrap());
        let before = row([u64::MAX - 100; 8]);
        let after = row([u64::MAX - 99; 8]);
        assert!(!after.pressured_since(before).unwrap());
    }
}
