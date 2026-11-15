//! Playback priority for managed extension containers.
//!
//! Extensions get the whole machine while nothing plays. While a stream starts
//! or plays, the governed containers share at most a quarter of the Docker
//! host's CPUs at the lowest scheduling weight, and get everything back shortly
//! after playback ends. Memory is not touched: it cannot be taken back from a
//! running container safely. The marketplace's own network worker keeps its
//! separate fixed budget and pauses for playback on its own.
//!
//! A CPU quota rather than only a lower weight, because on macOS the containers
//! run inside Docker Desktop's VM while ffmpeg runs on the host: a weight inside
//! the VM would not give playback anything.
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

use crate::playback::PlaybackJobManager;
use crate::runtime::model::{ELIXIR_EXTENSION_ID_LABEL, ELIXIR_MANAGED_LABEL};

const TICK: Duration = Duration::from_secs(2);
/// Keeps the cap through seeks and back-to-back episodes.
const RELEASE_DELAY: Duration = Duration::from_secs(20);
const CPU_PERIOD_US: u64 = 100_000;
/// Per container while capped, so downloads and health checks keep moving.
const MIN_QUOTA_US: u64 = 10_000;
const CAPPED_SHARES: &str = "2";
const NORMAL_SHARES: &str = "1024";
const DOCKER_TIMEOUT: Duration = Duration::from_secs(15);

/// Installed extension containers that yield to playback: the members of an
/// installed container stack and the built-in downloaders. The older bundled
/// Arr modules are left exactly as they are.
pub fn is_governed(extension_id: &str) -> bool {
    crate::marketplace::stack::is_stack_member_id(extension_id)
        || matches!(
            extension_id,
            "elixir.modules.qbittorrent" | "elixir.modules.nzbget"
        )
}

/// Per-container quota while playback has priority: the governed containers
/// together use at most a quarter of the host's CPUs.
fn capped_quota_us(host_cpus: u64, containers: usize) -> u64 {
    let total = host_cpus.max(1) * CPU_PERIOD_US / 4;
    (total / containers.max(1) as u64).max(MIN_QUOTA_US)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Change {
    Cap { id: String, quota_us: u64 },
    Lift { id: String },
}

#[derive(Default)]
struct Governor {
    /// Applied quota per container ID; empty while extensions run freely.
    capped: HashMap<String, u64>,
    /// Containers whose CPU cannot be changed (for example a fixed CPU limit
    /// from their manifest). Skipped instead of retried every tick.
    refused: HashSet<String>,
    last_busy: Option<Instant>,
    /// Lift everything once at startup in case a previous run stopped while
    /// containers were capped.
    settled: bool,
}

impl Governor {
    fn holding(&mut self, busy: bool, now: Instant) -> bool {
        if busy {
            self.last_busy = Some(now);
        }
        self.last_busy
            .is_some_and(|at| now.duration_since(at) < RELEASE_DELAY)
    }

    fn idle(&self, holding: bool) -> bool {
        !holding && self.settled && self.capped.is_empty()
    }

    /// `running` lists the governed running containers; it is required while
    /// holding and at startup, and ignored otherwise.
    fn plan(&mut self, holding: bool, running: Option<&[String]>, host_cpus: u64) -> Vec<Change> {
        if let Some(running) = running {
            self.capped.retain(|id, _| running.contains(id));
            self.refused.retain(|id| running.contains(id));
        }
        if holding {
            let running = running.unwrap_or_default();
            let quota_us = capped_quota_us(host_cpus, running.len());
            return running
                .iter()
                .filter(|id| !self.refused.contains(*id))
                .filter(|id| self.capped.get(*id) != Some(&quota_us))
                .map(|id| Change::Cap {
                    id: id.clone(),
                    quota_us,
                })
                .collect();
        }
        if !self.settled {
            return running
                .unwrap_or_default()
                .iter()
                .map(|id| Change::Lift { id: id.clone() })
                .collect();
        }
        self.capped
            .keys()
            .map(|id| Change::Lift { id: id.clone() })
            .collect()
    }

    fn applied(&mut self, change: &Change) {
        match change {
            Change::Cap { id, quota_us } => {
                self.capped.insert(id.clone(), *quota_us);
            }
            Change::Lift { id } => {
                self.capped.remove(id);
            }
        }
    }

    fn failed(&mut self, change: &Change, missing: bool) {
        match change {
            Change::Cap { id, .. } if !missing => {
                self.refused.insert(id.clone());
            }
            Change::Cap { id, .. } | Change::Lift { id } => {
                if missing {
                    self.capped.remove(id);
                }
            }
        }
    }
}

/// Watches playback for the life of the server and caps or frees the governed
/// containers. On shutdown everything is given back.
pub async fn run(playback: Arc<PlaybackJobManager>, shutdown: CancellationToken) {
    let mut governor = Governor::default();
    let mut host_cpus = None;
    let mut interval = tokio::time::interval(TICK);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = interval.tick() => {}
        }
        let holding = governor.holding(playback.has_latency_sensitive_work(), Instant::now());
        if governor.idle(holding) {
            continue;
        }
        if let Err(error) = step(&mut governor, holding, &mut host_cpus).await {
            tracing::debug!("playback priority check failed: {error:#}");
        }
    }
    if !governor.capped.is_empty() {
        let _ = step(&mut governor, false, &mut host_cpus).await;
    }
}

async fn step(governor: &mut Governor, holding: bool, host_cpus: &mut Option<u64>) -> Result<()> {
    let running = if holding || !governor.settled {
        Some(governed_containers().await?)
    } else {
        None
    };
    let cpus = if holding {
        match *host_cpus {
            Some(cpus) => cpus,
            None => {
                let cpus = docker_host_cpus().await?;
                *host_cpus = Some(cpus);
                cpus
            }
        }
    } else {
        1
    };
    let was_free = governor.capped.is_empty();
    let changes = governor.plan(holding, running.as_deref(), cpus);
    for change in &changes {
        match apply(change).await {
            Ok(()) => governor.applied(change),
            Err(error) => {
                let missing = format!("{error:#}").contains("No such container");
                if !missing {
                    tracing::warn!("playback priority could not update a container: {error:#}");
                }
                governor.failed(change, missing);
            }
        }
    }
    governor.settled = true;
    match (was_free, governor.capped.is_empty()) {
        (true, false) => {
            let quota_us = governor.capped.values().next().copied().unwrap_or_default();
            tracing::info!(
                containers = governor.capped.len(),
                cpus_each = quota_us as f64 / CPU_PERIOD_US as f64,
                "media is playing: extension containers yield CPU to playback"
            );
        }
        (false, true) => {
            tracing::info!("playback finished: extension containers have full CPU again");
        }
        _ => {}
    }
    Ok(())
}

async fn apply(change: &Change) -> Result<()> {
    match change {
        Change::Cap { id, quota_us } => {
            let quota = quota_us.to_string();
            let period = CPU_PERIOD_US.to_string();
            docker(&[
                "update",
                "--cpu-shares",
                CAPPED_SHARES,
                "--cpu-period",
                &period,
                "--cpu-quota",
                &quota,
                id,
            ])
            .await?;
        }
        Change::Lift { id } => {
            docker(&[
                "update",
                "--cpu-shares",
                NORMAL_SHARES,
                "--cpu-quota",
                "-1",
                id,
            ])
            .await?;
        }
    }
    Ok(())
}

async fn governed_containers() -> Result<Vec<String>> {
    let filter = format!("label={ELIXIR_MANAGED_LABEL}=true");
    let format = format!("{{{{.ID}}}}\t{{{{.Label \"{ELIXIR_EXTENSION_ID_LABEL}\"}}}}");
    let output = docker(&["ps", "--filter", &filter, "--format", &format]).await?;
    Ok(parse_governed(&output))
}

fn parse_governed(output: &str) -> Vec<String> {
    output
        .lines()
        .filter_map(|line| {
            let (id, extension_id) = line.trim().split_once('\t')?;
            let id = id.trim();
            (!id.is_empty() && is_governed(extension_id.trim())).then(|| id.to_string())
        })
        .collect()
}

pub(crate) async fn docker_host_cpus() -> Result<u64> {
    let output = docker(&["info", "--format", "{{.NCPU}}"]).await?;
    let cpus: u64 = output.trim().parse().context("docker host CPU count")?;
    if cpus == 0 {
        bail!("docker host reports no CPUs");
    }
    Ok(cpus)
}

pub(crate) async fn docker_host_memory_bytes() -> Result<u64> {
    let output = docker(&["info", "--format", "{{.MemTotal}}"]).await?;
    let bytes: u64 = output.trim().parse().context("docker host memory")?;
    if bytes == 0 {
        bail!("docker host reports no memory");
    }
    Ok(bytes)
}

async fn docker(args: &[&str]) -> Result<String> {
    let output = tokio::time::timeout(
        DOCKER_TIMEOUT,
        Command::new("docker")
            .args(args)
            .kill_on_drop(true)
            .output(),
    )
    .await
    .context("docker timed out")??;
    if !output.status.success() {
        bail!(
            "docker {} failed: {}",
            args.first().copied().unwrap_or_default(),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[test]
    fn governs_stack_members_and_built_in_downloaders_only() {
        assert!(is_governed("stack.811a0ada.sonarr"));
        assert!(is_governed("elixir.modules.qbittorrent"));
        assert!(is_governed("elixir.modules.nzbget"));
        assert!(!is_governed("elixir.modules.sonarr"));
        assert!(!is_governed("elixir.connectors.qbittorrent_defaults"));
        assert_eq!(
            parse_governed(
                "aaa\tstack.811a0ada.radarr\nbbb\telixir.modules.radarr\nccc\telixir.modules.nzbget\n\n"
            ),
            ids(&["aaa", "ccc"])
        );
    }

    #[test]
    fn playback_gets_three_quarters_of_the_cpus() {
        assert_eq!(capped_quota_us(8, 4), 50_000);
        assert_eq!(capped_quota_us(4, 5), 20_000);
        assert_eq!(capped_quota_us(2, 8), MIN_QUOTA_US);
        assert_eq!(capped_quota_us(0, 0), 25_000);
    }

    #[test]
    fn caps_during_playback_and_lifts_after_the_release_delay() {
        let mut governor = Governor {
            settled: true,
            ..Default::default()
        };
        let start = Instant::now();
        assert!(!governor.holding(false, start));
        assert!(governor.idle(false));

        let running = ids(&["a", "b"]);
        assert!(governor.holding(true, start));
        let changes = governor.plan(true, Some(&running), 8);
        assert_eq!(
            changes,
            vec![
                Change::Cap {
                    id: "a".into(),
                    quota_us: 100_000
                },
                Change::Cap {
                    id: "b".into(),
                    quota_us: 100_000
                },
            ]
        );
        changes.iter().for_each(|change| governor.applied(change));
        assert!(
            governor.plan(true, Some(&running), 8).is_empty(),
            "already capped"
        );

        // A container started mid-playback is capped and the share is resplit.
        let running = ids(&["a", "b", "c"]);
        assert_eq!(governor.plan(true, Some(&running), 8).len(), 3);

        // Still held just after playback stops, released after the delay.
        assert!(governor.holding(false, start + RELEASE_DELAY / 2));
        assert!(!governor.holding(false, start + RELEASE_DELAY));
        let lifts = governor.plan(false, None, 1);
        assert_eq!(lifts.len(), 2);
        lifts.iter().for_each(|change| governor.applied(change));
        assert!(governor.idle(false));
    }

    #[test]
    fn startup_lifts_everything_once_and_refused_containers_are_not_retried() {
        let mut governor = Governor::default();
        let running = ids(&["a", "b"]);
        assert!(!governor.idle(false));
        assert_eq!(
            governor.plan(false, Some(&running), 1),
            vec![
                Change::Lift { id: "a".into() },
                Change::Lift { id: "b".into() }
            ]
        );
        governor.settled = true;

        let cap = Change::Cap {
            id: "a".into(),
            quota_us: 25_000,
        };
        governor.failed(&cap, false);
        let changes = governor.plan(true, Some(&running), 2);
        assert_eq!(
            changes,
            vec![Change::Cap {
                id: "b".into(),
                quota_us: 25_000
            }]
        );

        // A container that disappeared is forgotten, not lifted.
        governor.applied(&changes[0]);
        governor.failed(&Change::Lift { id: "b".into() }, true);
        assert!(governor.capped.is_empty());
    }

    /// Live Docker: a governed container is capped while a stream starts and
    /// freed after the release delay. Caps every governed container on the
    /// host for about 25 s, so run it where none are serving real work.
    #[tokio::test]
    #[ignore = "live Docker: caps a real labelled container during playback, then frees it"]
    async fn live_docker_container_yields_cpu_during_playback() -> Result<()> {
        use crate::{config::DatabaseConfig, db::Database};
        let root = tempfile::tempdir()?;
        let database = Database::connect(&DatabaseConfig {
            url: format!("sqlite://{}", root.path().join("playback.sqlite").display()),
            max_connections: 2,
            connect_timeout_seconds: 5,
        })
        .await?;
        database.run_migrations().await?;
        let playback = Arc::new(PlaybackJobManager::new(database.pool.clone(), None));
        let name = format!("elx-playback-priority-{}", uuid::Uuid::new_v4().simple());
        docker(&[
            "run",
            "-d",
            "--rm",
            "--name",
            &name,
            "--label",
            "elixir.managed=true",
            "--label",
            "elixir.extension_id=elixir.modules.nzbget",
            "alpine:3.20",
            "sleep",
            "120",
        ])
        .await?;
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(run(playback.clone(), shutdown.clone()));
        let cpu = |name: String| async move {
            docker(&[
                "inspect",
                "-f",
                "{{.HostConfig.CpuQuota}} {{.HostConfig.CpuShares}}",
                &name,
            ])
            .await
            .map(|value| value.trim().to_string())
        };
        let outcome = async {
            playback.set_test_startup_queue_len(1);
            let started = Instant::now();
            let capped = loop {
                let value = cpu(name.clone()).await?;
                if value.ends_with(" 2") {
                    break value;
                }
                anyhow::ensure!(
                    started.elapsed() < Duration::from_secs(15),
                    "not capped: {value}"
                );
                tokio::time::sleep(Duration::from_millis(500)).await;
            };
            let quota: i64 = capped.split(' ').next().unwrap_or_default().parse()?;
            anyhow::ensure!(quota >= MIN_QUOTA_US as i64, "capped quota {quota}");
            eprintln!("capped during playback: quota/shares {capped}");
            playback.set_test_startup_queue_len(0);
            let stopped = Instant::now();
            loop {
                let value = cpu(name.clone()).await?;
                if value == "-1 1024" {
                    eprintln!("freed {:?} after playback stopped", stopped.elapsed());
                    anyhow::ensure!(stopped.elapsed() >= RELEASE_DELAY - TICK, "freed too early");
                    break;
                }
                anyhow::ensure!(
                    stopped.elapsed() < RELEASE_DELAY + Duration::from_secs(15),
                    "not freed: {value}"
                );
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            anyhow::Ok(())
        }
        .await;
        shutdown.cancel();
        let _ = task.await;
        let _ = docker(&["rm", "-f", &name]).await;
        outcome
    }
}
