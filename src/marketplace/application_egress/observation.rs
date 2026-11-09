//! Read-only native observations, not a readiness/admission capability. These
//! must come from the exact owned gateway, independently of the HTTP helper.
use anyhow::{Context, Result, bail};
use elixir_marketplace::identity::Digest;
use std::net::IpAddr;

/// Intentionally not Deserialize: a peer or stored health flag is not a native
/// observation. Runtime implementations execute the fixed inspection below.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApplicationGatewayObservation {
    namespace: u64,
    addresses: Vec<IpAddr>,
    resources: String,
    policy: [Digest; 2],
}
impl ApplicationGatewayObservation {
    pub(crate) fn parse(bytes: &str) -> Result<Self> {
        if bytes.len() > 4096 || !bytes.ends_with('\n') {
            bail!("application gateway observation bounds");
        }
        let mut lines = bytes.lines();
        if lines.next() != Some("ELIXIR_APPLICATION_GUARD_V2") {
            bail!("application gateway observation version");
        }
        let namespace = lines
            .next()
            .and_then(|v| v.strip_prefix("net:["))
            .and_then(|v| v.strip_suffix(']'))
            .filter(|v| !v.is_empty() && v.bytes().all(|b| b.is_ascii_digit()))
            .context("application gateway namespace unavailable")?
            .parse::<u64>()?;
        if namespace == 0 {
            bail!("application gateway namespace is empty");
        }
        let mut addresses = Vec::new();
        loop {
            let line = lines
                .next()
                .context("application gateway observation truncated")?;
            if line == "RESOURCES" {
                break;
            }
            let (address, prefix) = line
                .strip_prefix("address=")
                .and_then(|v| v.split_once('/'))
                .context("application gateway tunnel address unavailable")?;
            let address: IpAddr = address.parse()?;
            let prefix = prefix.parse::<u8>()?;
            if addresses.len() >= 8
                || addresses.contains(&address)
                || address.is_unspecified()
                || address.is_loopback()
                || address.is_multicast()
                || prefix > if address.is_ipv4() { 32 } else { 128 }
            {
                bail!("application gateway tunnel address differs");
            }
            addresses.push(address);
        }
        let mut resources = String::new();
        for _ in 0..4 {
            resources.push_str(
                lines
                    .next()
                    .context("application gateway resources truncated")?,
            );
            resources.push('\n');
        }
        let mut policy = Vec::with_capacity(2);
        for prefix in ["POLICY4 ", "POLICY6 "] {
            policy.push(
                lines
                    .next()
                    .and_then(|line| line.strip_prefix(prefix))
                    .context("application gateway policy fingerprint missing")?
                    .parse::<Digest>()?,
            );
        }
        if addresses.is_empty()
            || lines.next() != Some("ELIXIR_APPLICATION_GUARD_END_V2")
            || lines.next().is_some()
        {
            bail!("application gateway observation incomplete or has extra output");
        }
        addresses.sort();
        Ok(Self {
            namespace,
            addresses,
            resources,
            policy: [policy[0], policy[1]],
        })
    }
    pub(in crate::marketplace) fn namespace(&self) -> u64 {
        self.namespace
    }
    pub(in crate::marketplace) fn addresses(&self) -> &[IpAddr] {
        &self.addresses
    }
    pub(in crate::marketplace) fn same_tunnel(&self, other: &Self) -> bool {
        self.namespace == other.namespace && self.addresses == other.addresses
    }
    /// Expected immutable configuration, not cached health or send authority.
    /// Later calls must read and compare the complete CURRENT kernel policy.
    pub(crate) fn policy_hashes(&self) -> [Digest; 2] {
        self.policy
    }
    pub(crate) fn same_guard(&self, other: &Self) -> bool {
        self.same_tunnel(other) && self.policy == other.policy
    }
    pub(in crate::marketplace) fn resources(&self) -> &str {
        &self.resources
    }
}

// A separate mangle-table owner-UID guard survives Gluetun's filter-table
// resets. In particular, a helper cannot use Gluetun's VPN-endpoint exception
// (including TCP/443) or Docker DNS/loopback when the tunnel disappears. No
// publisher code has NET_ADMIN or runs as this trusted root VPN process.
// Kernel WireGuard can retain the inner socket's UID on encrypted packets.
// Exempt ONLY its privileged, fixed Gluetun fwmark (51820) on UDP before the
// off-tunnel drop; otherwise the guard drops its own encrypted DNS/TLS traffic.
// The actual HTTP helper has no capabilities, so neither SO_MARK nor SCM_MARK
// can forge this exception. Its unmarked plaintext is still charged/filtered
// on tun0, before encryption. No endpoint/port-only exemption is introduced.
// https://www.wireguard.com/netns/ (Improved Rule-based Routing).
// Byte-mode hashlimit requires a burst greater than its rounded one-second
// rate. Inbound therefore uses 4 MiB, not an invalid 1-MiB burst at 2 MiB/s.
pub(crate) const INSTALL_GUARD: &str = r#"
for tool in iptables-nft ip6tables-nft; do
    "$tool" -w 2 -t mangle -N ELIXIR_APP
    "$tool" -w 2 -t mangle -A ELIXIR_APP -p udp -m mark --mark 0xca6c -j RETURN
    "$tool" -w 2 -t mangle -A ELIXIR_APP ! -o tun0 -j DROP
    "$tool" -w 2 -t mangle -A ELIXIR_APP -m hashlimit --hashlimit-above 512kb/s --hashlimit-burst 1024kb --hashlimit-name elixir_app_out --hashlimit-htable-size 16 --hashlimit-htable-max 1 --hashlimit-htable-expire 180000 -j DROP
    "$tool" -w 2 -t mangle -A ELIXIR_APP -p tcp -m multiport --dports 53,80,443 -j RETURN
    "$tool" -w 2 -t mangle -A ELIXIR_APP -p udp -m udp --dport 53 -j RETURN
    "$tool" -w 2 -t mangle -A ELIXIR_APP -j DROP
    "$tool" -w 2 -t mangle -I OUTPUT 1 -m owner --uid-owner 10005 -j ELIXIR_APP
    "$tool" -w 2 -t mangle -N ELIXIR_APP_IN
    "$tool" -w 2 -t mangle -A ELIXIR_APP_IN -m hashlimit --hashlimit-above 2048kb/s --hashlimit-burst 4096kb --hashlimit-name elixir_app_in --hashlimit-htable-size 16 --hashlimit-htable-max 1 --hashlimit-htable-expire 180000 -j DROP
    "$tool" -w 2 -t mangle -A ELIXIR_APP_IN -j RETURN
    "$tool" -w 2 -t mangle -I INPUT 1 -i tun0 -j ELIXIR_APP_IN
done
"#;

// Fixed trusted command, no secrets, user paths, destinations or interpolation.
// Check the COMPLETE hook/chain, not rule existence (which could hide an earlier
// bypass). The inner watchdog bounds a detached Docker exec if its CLI dies.
pub(crate) const OBSERVE_GUARD: &str = r#"set -eu
phase=expectations
trap 'elixir_status=$?; if [ "$elixir_status" -ne 0 ]; then printf "ELIXIR_APP_PROBE_FAILURE:%s\n" "$phase" >&2; fi; exit "$elixir_status"' EXIT
valid_policy_hash() {
    case "$1" in sha256:*) ;; *) return 1 ;; esac
    set -- "${1#sha256:}"
    [ "${#1}" -eq 64 ] || return 1
    case "$1" in *[!0123456789abcdef]*) return 1 ;; esac
}
expected4=${ELIXIR_APP_POLICY4:-}
expected6=${ELIXIR_APP_POLICY6:-}
if [ -z "$expected4" ] && [ -z "$expected6" ]; then
    bootstrap=1
else
    # Partial/malformed expectations MUST NOT silently re-bootstrap.
    valid_policy_hash "$expected4"
    valid_policy_hash "$expected6"
    bootstrap=0
fi
for tool in iptables-nft ip6tables-nft; do
    case "$tool" in iptables-nft) phase=policy4 ;; ip6tables-nft) phase=policy6 ;; esac
    # Fingerprint the WHOLE table, not filtered/normalized rows. This also
    # detects changes outside our hooks, without an awk/pipeline on later reads.
    # Here-doc expansion is data only (never reparsed as shell commands).
    policy=$("$tool" -w 2 -t mangle -S)
    digest=$(/bin/busybox sha256sum <<ELIXIR_POLICY_SNAPSHOT
$policy
ELIXIR_POLICY_SNAPSHOT
)
    actual=sha256:${digest%% *}
    if [ "$bootstrap" = 1 ]; then
        "$tool" -w 2 -t mangle -C ELIXIR_APP -m hashlimit --hashlimit-above 512kb/s --hashlimit-burst 1024kb --hashlimit-name elixir_app_out --hashlimit-htable-size 16 --hashlimit-htable-max 1 --hashlimit-htable-expire 180000 -j DROP
        "$tool" -w 2 -t mangle -C ELIXIR_APP_IN -m hashlimit --hashlimit-above 2048kb/s --hashlimit-burst 4096kb --hashlimit-name elixir_app_in --hashlimit-htable-size 16 --hashlimit-htable-max 1 --hashlimit-htable-expire 180000 -j DROP
        [ "$(printf '%s\n' "$policy" | awk '
        ($1 == "-P" || $1 == "-A") && $2 == "OUTPUT" { output = output $0 "\n" }
        ($1 == "-P" || $1 == "-A") && $2 == "INPUT" { incoming = incoming $0 "\n" }
        $1 == "-N" && $2 == "ELIXIR_APP" { app = app $0 "\n" }
        $1 == "-N" && $2 == "ELIXIR_APP_IN" { appin = appin $0 "\n" }
        $1 == "-A" && $2 == "ELIXIR_APP" {
            app_rules++
            if (app_rules != 3) app = app $0 "\n"
        }
        $1 == "-A" && $2 == "ELIXIR_APP_IN" {
            in_rules++
            if (in_rules != 1) appin = appin $0 "\n"
        }
        END { printf "%s%s%s%s", output, app, incoming, appin }
    ')" = '-P OUTPUT ACCEPT
-A OUTPUT -m owner --uid-owner 10005 -j ELIXIR_APP
-N ELIXIR_APP
-A ELIXIR_APP -p udp -m mark --mark 0xca6c -j RETURN
-A ELIXIR_APP ! -o tun0 -j DROP
-A ELIXIR_APP -p tcp -m multiport --dports 53,80,443 -j RETURN
-A ELIXIR_APP -p udp -m udp --dport 53 -j RETURN
-A ELIXIR_APP -j DROP
-P INPUT ACCEPT
-A INPUT -i tun0 -j ELIXIR_APP_IN
-N ELIXIR_APP_IN
-A ELIXIR_APP_IN -j RETURN' ]
        # Do not bind a policy which changed while semantic checks ran.
        [ "$policy" = "$("$tool" -w 2 -t mangle -S)" ]
    else
        case "$tool" in iptables-nft) expected=$expected4 ;; ip6tables-nft) expected=$expected6 ;; esac
        [ "$actual" = "$expected" ]
    fi
    case "$tool" in iptables-nft) policy4=$actual ;; ip6tables-nft) policy6=$actual ;; esac
done
phase=tunnel_link
ip link show dev tun0 | grep -Eq '<([^>]*,)?UP(,[^>]*)?>'
phase=mtu
if [ "${VPN_TYPE:-}" = wireguard ] && [ -n "${WIREGUARD_MTU:-}" ]; then
    IFS= read -r actual_mtu < /sys/class/net/tun0/mtu
    [ "$actual_mtu" = "$WIREGUARD_MTU" ]
fi
phase=resources
printf '%s\n' ELIXIR_APPLICATION_GUARD_V2
readlink /proc/self/ns/net
ip -o address show dev tun0 | awk '{print "address=" $4}'
printf '%s\n' RESOURCES
awk '/^MemAvailable:/ || /^some / {print}' /proc/meminfo /proc/pressure/cpu /proc/pressure/memory /proc/pressure/io
printf 'POLICY4 %s\nPOLICY6 %s\n' "$policy4" "$policy6"
printf '%s\n' ELIXIR_APPLICATION_GUARD_END_V2
"#;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn encrypted_wireguard_mark_precedes_plaintext_drops_and_limits() {
        let encrypted = "-A ELIXIR_APP -p udp -m mark --mark 0xca6c -j RETURN";
        let plaintext = "-A ELIXIR_APP ! -o tun0 -j DROP";
        assert!(INSTALL_GUARD.find(encrypted).unwrap() < INSTALL_GUARD.find(plaintext).unwrap());
        assert!(
            INSTALL_GUARD.find(plaintext).unwrap()
                < INSTALL_GUARD.find("--hashlimit-above").unwrap()
        );
        assert!(OBSERVE_GUARD.contains(&format!("{encrypted}\n{plaintext}")));
        assert!(OBSERVE_GUARD.contains("policy=$(\"$tool\" -w 2 -t mangle -S)"));
        assert!(OBSERVE_GUARD.contains("app_rules != 3"));
        assert!(OBSERVE_GUARD.contains("in_rules != 1"));
        assert!(OBSERVE_GUARD.contains("[ \"$policy\" = \"$(\"$tool\" -w 2 -t mangle -S)\" ]"));
        assert!(OBSERVE_GUARD.contains("[ \"$actual\" = \"$expected\" ]"));
        assert_eq!(OBSERVE_GUARD.matches(" -t mangle -S").count(), 2);
        assert!(OBSERVE_GUARD.contains("sha256sum <<ELIXIR_POLICY_SNAPSHOT\n$policy\n"));
        assert!(INSTALL_GUARD.contains("--uid-owner 10005"));
        assert!(!INSTALL_GUARD.contains("--dport 51820"));
        assert_eq!(u32::from_str_radix("ca6c", 16).unwrap(), 51820);
    }
    #[test]
    fn bounded_native_observations_require_complete_independent_context() -> Result<()> {
        let policy4 = format!("POLICY4 {}\n", Digest::of(b"v4"));
        let policy6 = format!("POLICY6 {}\n", Digest::of(b"v6"));
        let valid = format!(
            "ELIXIR_APPLICATION_GUARD_V2\nnet:[4026532000]\naddress=10.2.0.2/32\naddress=fd00::2/128\nRESOURCES\nMemAvailable: 2048000 kB\nsome avg10=0.00 avg60=0.00 avg300=0.00 total=0\nsome avg10=0.00 avg60=0.00 avg300=0.00 total=0\nsome avg10=0.00 avg60=0.00 avg300=0.00 total=0\n{policy4}{policy6}ELIXIR_APPLICATION_GUARD_END_V2\n"
        );
        let observed = ApplicationGatewayObservation::parse(&valid)?;
        assert_eq!(observed.namespace(), 4026532000);
        assert_eq!(observed.addresses().len(), 2);
        assert_eq!(
            observed.policy_hashes(),
            [Digest::of(b"v4"), Digest::of(b"v6")]
        );
        let fresh_resources =
            ApplicationGatewayObservation::parse(&valid.replace("2048000", "2047000"))?;
        assert!(observed.same_guard(&fresh_resources));
        assert_ne!(observed.resources(), fresh_resources.resources());
        for changed in [
            valid.replace("4026532000", "4026532001"),
            valid.replace("10.2.0.2/32", "10.2.0.3/32"),
            valid.replace(
                &policy4,
                &format!("POLICY4 {}\n", Digest::of(b"changed v4")),
            ),
            valid.replace(
                &policy6,
                &format!("POLICY6 {}\n", Digest::of(b"changed v6")),
            ),
        ] {
            assert!(!observed.same_guard(&ApplicationGatewayObservation::parse(&changed)?));
        }
        for invalid in [
            valid.replace("4026532000", "0"),
            valid.replace("10.2.0.2/32", "127.0.0.1/32"),
            valid.replace("10.2.0.2/32", "10.2.0.2/33"),
            valid.replace("address=fd00::2/128", "address=10.2.0.2/32"),
            valid.replace("_V2", "_V1"),
            valid.replace("ELIXIR_APPLICATION_GUARD_END_V2\n", ""),
            valid.replace(&policy4, ""),
            valid.replace(&policy6, ""),
            valid.replace(&policy6, &policy4),
            valid.replace(
                &format!("{policy4}{policy6}"),
                &format!("{policy6}{policy4}"),
            ),
            valid.replace(&policy4, &policy4.to_uppercase()),
            valid.replace(&policy4, "POLICY4 sha256:abc\n"),
            valid.replace(&policy4, "POLICY4 md5:00000000000000000000000000000000\n"),
            valid.replace(&policy6, &format!("{policy6}{policy6}")),
            format!("{valid}extra\n"),
            valid.trim_end().to_owned(),
            "x".repeat(4097),
        ] {
            assert!(ApplicationGatewayObservation::parse(&invalid).is_err());
        }
        Ok(())
    }
}
