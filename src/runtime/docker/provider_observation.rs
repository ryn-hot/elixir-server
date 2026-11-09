//! Fixed read-only commands in the exact original provider container. No shell,
//! arbitrary URL, normal host resolver, proxy, account output or new namespace.
use super::*;
use crate::{
    marketplace::provider::Profile,
    runtime::provider_observation::{self as data, ProviderTransport},
};
use anyhow::{bail, ensure};

const STATS_COMMAND: &[&str] = &[
    "/usr/bin/warp-cli",
    "--accept-tos",
    "--no-ansi",
    "--json",
    "tunnel",
    "stats",
];
type ProviderRead = (std::process::ExitStatus, String, String);

/// The pinned LC_ALL=C client returns this closed error while its tunnel is
/// briefly reconnecting. No unknown error, signal, partial data or translated
/// message may enter the grace path. This grants no health or packet lease.
fn reconnecting_stats(code: Option<i32>, output: &str, error: &str) -> bool {
    if code != Some(1) || !output.is_empty() {
        return false;
    }
    let Ok(value) = elixir_marketplace::canonical::parse(error.as_bytes(), 4096) else {
        return false;
    };
    value.as_object().is_some_and(|fields| {
        fields.len() == 2
            && fields.get("code").and_then(serde_json::Value::as_str) == Some("WarpNotConnected")
            && fields.get("error").and_then(serde_json::Value::as_str)
                == Some("WARP is not connected.")
    })
}

async fn stats_with_reconnect_grace<F, Fut>(mut read: F) -> Result<String>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<ProviderRead>>,
{
    // At most three local reads and 1.2s total backoff. The caller's existing
    // renewal/startup deadline still bounds all I/O and cancellation. Never
    // issue a heartbeat, reconnect command or credit from an error/status flag.
    for delay in [Some(400), Some(800), None] {
        let (status, output, error) = read().await?;
        if status.success() {
            return Ok(output);
        }
        if !reconnecting_stats(status.code(), &output, &error) {
            bail!("fixed provider observation failed");
        }
        let Some(delay) = delay else {
            bail!("provider tunnel remains disconnected");
        };
        tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
    }
    bail!("provider observation retry exhausted")
}

impl DockerRuntimeManager {
    async fn provider_read_capture(
        &self,
        handle: &ContainerHandle,
        uid: &str,
        command: &[&str],
    ) -> Result<ProviderRead> {
        let mut args = vec![
            "exec".into(),
            "--user".into(),
            uid.into(),
            "--env".into(),
            "LC_ALL=C".into(),
            handle.id.clone(),
        ];
        args.extend(command.iter().map(|s| (*s).into()));
        self.packet_network_capture(&args).await
    }
    async fn provider_read(
        &self,
        handle: &ContainerHandle,
        uid: &str,
        command: &[&str],
    ) -> Result<String> {
        let (status, output, _) = self.provider_read_capture(handle, uid, command).await?;
        ensure!(status.success(), "fixed provider observation failed");
        Ok(output)
    }
    pub(super) async fn observe_provider_transport(
        &self,
        handle: &ContainerHandle,
        profile: &Profile,
    ) -> Result<ProviderTransport> {
        let namespace_inode = self.provider_native_namespace(handle, profile).await?;
        let stats =
            stats_with_reconnect_grace(|| self.provider_read_capture(handle, "0:0", STATS_COMMAND))
                .await
                .context("provider tunnel stats command")?;
        let sockets = self
            .provider_read(
                handle,
                "0:0",
                &["/usr/bin/ss", "-H", "-u", "-a", "-n", "-p"],
            )
            .await
            .context("provider connected UDP sockets")?;
        let socket = data::socket(&stats, &sockets)?;
        // Process names are not ownership. Require the pinned, read-only vendor
        // executable for the PID owning this connected socket, before AND after.
        let path = format!("/proc/{}/exe", socket.pid);
        let binary = self
            .provider_read(handle, "0:0", &["/usr/bin/readlink", &path])
            .await
            .context("provider socket executable read")?;
        ensure!(
            binary.trim() == "/usr/bin/warp-svc",
            "provider socket executable differs"
        );
        let link = self
            .provider_read(
                handle,
                "0:0",
                &[
                    "/usr/sbin/ip",
                    "-j",
                    "address",
                    "show",
                    "dev",
                    "CloudflareWARP",
                ],
            )
            .await
            .context("provider TUN addresses read")?;
        let tunnel_addresses = data::addresses(&link)?;
        let after = self
            .provider_read(
                handle,
                "0:0",
                &["/usr/bin/ss", "-H", "-u", "-a", "-n", "-p"],
            )
            .await
            .context("provider UDP socket reinspection")?;
        let after = data::socket(&stats, &after)?;
        ensure!(
            after.pid == socket.pid && after.endpoint == socket.endpoint,
            "provider transport changed during observation"
        );
        ensure!(
            self.provider_read(handle, "0:0", &["/usr/bin/readlink", &path])
                .await?
                .trim()
                == "/usr/bin/warp-svc"
                && self.provider_native_namespace(handle, profile).await? == namespace_inode,
            "provider owner changed during transport read"
        );
        Ok(ProviderTransport {
            namespace_inode,
            endpoint: socket.endpoint,
            tunnel_addresses,
        })
    }
    pub(super) async fn observe_provider_exit(
        &self,
        handle: &ContainerHandle,
        profile: &Profile,
        ipv6: bool,
    ) -> Result<()> {
        let before = self.provider_native_namespace(handle, profile).await?;
        // --disable MUST be first: ignore curlrc. Explicit no proxy, HTTPS-only,
        // verified TLS 1.3 and DoH to a literal address: no ISP/Docker DNS lookup.
        // The process has the separately metered FETCH_UID, no writable state,
        // and the existing provider cgroup. Killing the parent retires it too;
        // it cannot keep a separate container/namespace alive after retirement.
        let output = self
            .provider_read(
                handle,
                "10005:10005",
                &[
                    "/usr/bin/curl",
                    "--disable",
                    if ipv6 { "--ipv6" } else { "--ipv4" },
                    "--silent",
                    "--show-error",
                    "--fail",
                    "--noproxy",
                    "*",
                    "--proxy",
                    "",
                    "--proto",
                    "=https",
                    "--proto-redir",
                    "=https",
                    "--max-redirs",
                    "0",
                    "--tlsv1.3",
                    "--tls-max",
                    "1.3",
                    "--connect-timeout",
                    "3",
                    "--max-time",
                    "8",
                    "--max-filesize",
                    "4096",
                    "--doh-url",
                    if ipv6 {
                        "https://[2606:4700:4700::1111]/dns-query"
                    } else {
                        "https://1.1.1.1/dns-query"
                    },
                    "--url",
                    "https://www.cloudflare.com/cdn-cgi/trace",
                ],
            )
            .await
            .context("unprivileged provider DoH/TLS command")?;
        data::trace(&output, ipv6)?;
        ensure!(
            self.provider_native_namespace(handle, profile).await? == before,
            "provider namespace changed during protected probe"
        );
        Ok(())
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::{cell::Cell, os::unix::process::ExitStatusExt, time::Duration};

    const DISCONNECTED: &str = r#"{"code":"WarpNotConnected","error":"WARP is not connected."}"#;
    fn captured(code: i32, output: &str, error: &str) -> Result<ProviderRead> {
        Ok((
            std::process::ExitStatus::from_raw(code << 8),
            output.into(),
            error.into(),
        ))
    }

    #[test]
    fn reconnect_grace_accepts_only_the_exact_closed_vendor_error() {
        assert!(reconnecting_stats(Some(1), "", DISCONNECTED));
        for (code, output, error) in [
            (None, "", DISCONNECTED),
            (Some(0), "", DISCONNECTED),
            (Some(2), "", DISCONNECTED),
            (Some(1), "partial stats", DISCONNECTED),
            (
                Some(1),
                "",
                r#"{"code":"Other","error":"WARP is not connected."}"#,
            ),
            (
                Some(1),
                "",
                r#"{"code":"WarpNotConnected","error":"different"}"#,
            ),
            (
                Some(1),
                "",
                r#"{"code":"WarpNotConnected","error":"WARP is not connected.","extra":true}"#,
            ),
            (
                Some(1),
                "",
                r#"{"code":"Other","code":"WarpNotConnected","error":"WARP is not connected."}"#,
            ),
            (Some(1), "", "WARP is not connected."),
        ] {
            assert!(!reconnecting_stats(code, output, error));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn healthy_stats_have_no_grace_delay_or_extra_read() -> Result<()> {
        let calls = Cell::new(0);
        let started = tokio::time::Instant::now();
        let output = stats_with_reconnect_grace(|| {
            calls.set(calls.get() + 1);
            async { captured(0, "fresh native stats", "") }
        })
        .await?;
        assert_eq!(output, "fresh native stats");
        assert_eq!(calls.get(), 1);
        assert_eq!(started.elapsed(), Duration::ZERO);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn disconnected_stats_recheck_is_bounded_and_returns_only_fresh_output() -> Result<()> {
        let calls = Cell::new(0);
        let started = tokio::time::Instant::now();
        let output = stats_with_reconnect_grace(|| {
            let attempt = calls.get();
            calls.set(attempt + 1);
            async move {
                if attempt < 2 {
                    captured(1, "", DISCONNECTED)
                } else {
                    captured(0, "fresh native stats", "")
                }
            }
        })
        .await?;
        assert_eq!(output, "fresh native stats");
        assert_eq!(calls.get(), 3);
        assert_eq!(started.elapsed(), Duration::from_millis(1200));
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn persistent_disconnect_or_unknown_failure_never_produces_a_transport() {
        let calls = Cell::new(0);
        let result = stats_with_reconnect_grace(|| {
            calls.set(calls.get() + 1);
            async { captured(1, "", DISCONNECTED) }
        })
        .await;
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("remains disconnected")
        );
        assert_eq!(calls.get(), 3);
        for (code, output, error) in [
            (1, "", "unknown secret vendor error"),
            (1, "partial", DISCONNECTED),
        ] {
            let calls = Cell::new(0);
            let result = stats_with_reconnect_grace(|| {
                calls.set(calls.get() + 1);
                async move { captured(code, output, error) }
            })
            .await;
            assert_eq!(calls.get(), 1);
            assert_eq!(
                result.unwrap_err().to_string(),
                "fixed provider observation failed"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn capture_failure_and_cancellation_do_not_start_another_stats_read() {
        let calls = Cell::new(0);
        let result = stats_with_reconnect_grace(|| {
            calls.set(calls.get() + 1);
            async { anyhow::bail!("original capture failed") }
        })
        .await;
        assert_eq!(calls.get(), 1);
        assert_eq!(result.unwrap_err().to_string(), "original capture failed");
        calls.set(0);
        let result = tokio::time::timeout(
            Duration::from_millis(200),
            stats_with_reconnect_grace(|| {
                calls.set(calls.get() + 1);
                async { captured(1, "", DISCONNECTED) }
            }),
        )
        .await;
        assert!(result.is_err());
        assert_eq!(calls.get(), 1);
    }
}
