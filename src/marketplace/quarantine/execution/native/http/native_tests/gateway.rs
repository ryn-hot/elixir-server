//! Actual matched source/inspected VM + original gateway task. Grants and
//! aggregate admission are synthetic. Internal bridge/fake keys, NO live VPN,
//! network Ready, public HTTP, installed activation or provider port claimed.
use super::*;
use crate::{
    config::Settings,
    db::{Database, models::SecretScope},
    extensions::store::{ExtensionStore, NewSecret},
    marketplace::quarantine::execution::{GatewayLaunch, HttpLaunch},
    runtime::packet_network::{PacketNetworkInspection, PacketNetworkSpec},
    secrets::SecretsManager,
};

struct NetworkEmergency {
    spec: PacketNetworkSpec,
    id: String,
}
impl Drop for NetworkEmergency {
    fn drop(&mut self) {
        if let Ok(out) = std::process::Command::new("docker")
            .args(["network", "inspect", &self.id])
            .output()
            && out.status.success()
            && serde_json::from_slice::<Vec<serde_json::Value>>(&out.stdout)
                .map_err(anyhow::Error::from)
                .and_then(|rows| {
                    rows.first()
                        .context("fixture network missing")
                        .and_then(PacketNetworkInspection::parse)
                })
                .is_ok_and(|actual| self.spec.verify(&actual).is_ok() && actual.members.is_empty())
        {
            let _ = std::process::Command::new("docker")
                .args(["network", "rm", &self.id])
                .output();
        }
    }
}
async fn selected(image: &str) -> Result<(Database, Arc<SecretsManager>)> {
    let settings = Settings {
        database: crate::config::DatabaseConfig {
            url: "sqlite::memory:?cache=shared".into(),
            max_connections: 1,
            ..Settings::default().database
        },
        ..Settings::default()
    };
    sqlx::any::install_default_drivers();
    let db = Database::connect(&settings.database).await?;
    db.run_migrations().await?;
    let secrets = Arc::new(SecretsManager::from_key_bytes([91; 32], false));
    sqlx::query("INSERT INTO download_network_profiles (id,name,kind,enabled,strict,scope,gateway_runtime,config_json,status,active) VALUES ('selected','Fixture VPN','wireguard_config',TRUE,TRUE,'marketplace_http','gluetun_wireguard',$1,'protected',FALSE)")
        .bind(serde_json::json!({"gatewayImage":image}).to_string()).execute(&db.pool).await?;
    let config = "[Interface]\nPrivateKey = yAnzdtF2rM8Nl1N8MPm+2MvmFo0xSg6u40qCMgfHdC0=\nAddress = 10.64.0.2/32\n[Peer]\nPublicKey = fNBdb9h9NP7VDaRao7IhiHBpjz2uVH54camzato3tr0=\nAllowedIPs = 0.0.0.0/0\nEndpoint = 1.1.1.1:51820\n";
    ExtensionStore::new(&db.pool)
        .upsert_secret(&NewSecret {
            secret_id: uuid::Uuid::new_v4(),
            scope: SecretScope::Global,
            scope_id: None,
            key: "config".into(),
            value_encrypted: secrets.encrypt(config)?,
            rotatable: true,
        })
        .await?;
    sqlx::query("INSERT INTO download_network_profile_secrets (profile_id,key,secret_ref) VALUES ('selected','wireguard_config','global:config')")
        .execute(&db.pool).await?;
    let identities = crate::network::vpn_identity::wireguard_keys(config)?
        .into_iter()
        .map(|key| (key, crate::network::vpn_identity::Purpose::MarketplaceHttp))
        .collect::<Vec<_>>();
    crate::network::vpn_identity::reserve(&db.pool, &identities).await?;
    Ok((db, secrets))
}
struct GatewayScope {
    native: Option<Arc<NativeInvocation>>,
    gateway: Option<GatewayLaunch>,
    http: Option<HttpLaunch>,
    image: String,
    began: bool,
    runtime: Arc<Runtime>,
    network_id: String,
    database: Database,
    secrets: Arc<SecretsManager>,
}
#[async_trait::async_trait]
impl Broker for GatewayScope {
    fn native_started(&mut self, native: NativeInvocation) -> Result<()> {
        if self.native.is_some() {
            bail!("original source scope replaced");
        }
        self.native = Some(Arc::new(native));
        Ok(())
    }
    async fn current(&mut self) -> Result<()> {
        if let Some(native) = self.native.as_ref() {
            native.current().await?;
            if !self.began {
                self.began = true;
                self.gateway = native
                    .launch_application_gateway(
                        "storage-gateway-lifetime-fixture",
                        &self.network_id,
                        self.database.clone(),
                        self.secrets.clone(),
                        Arc::new(Authority),
                    )
                    .await?;
                self.gateway
                    .as_mut()
                    .context("original gateway owner")?
                    .materialized()
                    .await?;
                if self.runtime.gate == Gate::Ready {
                    let gateway = self.gateway.as_mut().context("original gateway")?;
                    gateway.current().await?; // Identity/config, NOT Ready.
                    assert!(gateway.launch_http("mutable:tag").await.is_err());
                    assert!(!self.runtime.lane.join("http-helper.pending").exists());
                    self.http = Some(gateway.launch_http(&self.image).await?);
                    let http = self.http.as_mut().unwrap();
                    http.ready().await?;
                    assert_eq!(
                        http.profile().spec().network_mode.as_deref(),
                        Some(format!("container:{}", gateway.materialized().await?.id).as_str())
                    );
                    assert_eq!(
                        http.profile().spec().labels["elixir.marketplace.application_http.owner_deployment"],
                        "storage-gateway-lifetime-fixture"
                    );
                    // No attach/open/HTTP input. The retained original helper
                    // must still retire before its gateway and source parent.
                    self.runtime.reached.notify_one();
                }
                self.runtime.release.notified().await;
                bail!("controlled source/gateway exchange stop");
            }
        }
        Ok(())
    }
    async fn request(&mut self, _: &RuntimeJson, _: usize) -> Result<SourceSdkReply> {
        bail!("gateway lifetime fixture must never request an HTTP/SDK effect")
    }
}
async fn scenario(gate: Gate) -> Result<()> {
    let image = std::env::var("ELIXIR_MARKETPLACE_APPLICATION_HTTP_IMAGE")?;
    let gateway_image = if gate == Gate::Rejected {
        image.clone()
    } else {
        std::env::var("ELIXIR_TEST_APPLICATION_GATEWAY_IMAGE")?
    };
    if !image.strip_prefix("sha256:").is_some_and(immutable_id)
        || !gateway_image
            .strip_prefix("sha256:")
            .is_some_and(immutable_id)
    {
        bail!("immutable fixture images required");
    }
    let (database, secrets) = selected(&gateway_image).await?;
    let root = tempfile::tempdir()?;
    let lane = root.path().canonicalize()?.join("global");
    let deployment = format!("gateway-life-{}", uuid::Uuid::new_v4().simple());
    let inner = Arc::new(DockerRuntimeManager::new(None));
    let network = PacketNetworkSpec::isolated_fixture(&deployment, false)?;
    let actual = inner.ensure_packet_uplink(&network).await?;
    network.verify(&actual)?;
    let underlay = NetworkEmergency {
        spec: network,
        id: actual.id,
    };
    let labels = HashMap::from([
        (ELIXIR_MANAGED_LABEL.into(), "true".into()),
        (ELIXIR_DEPLOYMENT_ID_LABEL.into(), deployment.clone()),
        ("elixir.marketplace.volume".into(), "quarantine-v1".into()),
    ]);
    let volume = format!("elixir-marketplace-source-{deployment}");
    let owned = Arc::new(Mutex::new(Vec::new()));
    let _emergency = Emergency {
        actors: owned.clone(),
        volume: volume.clone(),
        labels: labels.clone(),
    };
    let runtime = Arc::new(Runtime {
        inner: inner.clone(),
        gate,
        lane: lane.clone(),
        journal: "gateway-helper.pending",
        reached: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
        helper: Mutex::default(),
        starts: AtomicUsize::new(0),
        owned,
    });
    let mut broker = GatewayScope {
        native: None,
        gateway: None,
        http: None,
        image: image.clone(),
        began: false,
        runtime: runtime.clone(),
        network_id: underlay.id.clone(),
        database,
        secrets,
    };
    let mut matched: Option<SourceMatchedQuarantine> = None;
    let outcome = std::panic::AssertUnwindSafe(async {
        let (release, package, proof) =
            crate::marketplace::quarantine::tests::operational_fixture().await?;
        let budget = SourceJobBudget {
            input_bytes: 8 * MIB,
            ..crate::marketplace::quarantine::tests::budget()
        };
        let plan = SourceQuarantinePlan::for_release(&release, None, &budget)?;
        let mut job =
            SourceQuarantineJob::open(runtime.clone(), &lane, &deployment, &image, &plan).await?;
        job.receive(
            runtime.as_ref(),
            QuarantinePurpose::Package,
            &mut package.as_slice(),
        )
        .await?;
        job.receive(
            runtime.as_ref(),
            QuarantinePurpose::Proof,
            &mut proof.as_slice(),
        )
        .await?;
        let source = job.finish(runtime.as_ref()).await?;
        matched = Some(source.match_source(runtime.as_ref()).await?);
        let limits = SourceExecutionLimits {
            profile: Default::default(),
            heap_bytes: 16 * MIB,
            timeout_millis: 30_000,
            max_file_bytes: 4 * MIB,
            json_bytes: MIB,
            pending_jobs: 128,
            sdk: None,
        };
        let args = RuntimeJson::parse(b"null")?;
        let mut invoke = Box::pin(matched.as_mut().unwrap().evaluate_native(
            SourceExecutionMode::Invoke,
            limits,
            &args,
            &mut broker,
        ));
        timeout(Duration::from_secs(30), async {
            tokio::select! {
                result=&mut invoke=>{result?;bail!("actual source ended before gateway boundary");},
                _=runtime.reached.notified()=>Ok::<_,anyhow::Error>(()),
            }
        })
        .await
        .context("actual source/gateway boundary deadline")??;
        let helper = runtime.helper.lock().unwrap().clone();
        if gate == Gate::Ready {
            tokio::time::sleep(Duration::from_millis(800)).await;
        }
        if gate == Gate::Rejected {
            assert!(helper.is_none());
            runtime.phase("creating")?;
        } else {
            let h = helper.as_ref().context("original gateway CID")?;
            let actual = inner
                .inspect(&ContainerHandle {
                    id: h.id.clone(),
                    name: h.id.clone(),
                })
                .await?;
            assert_eq!(actual.running, gate != Gate::Created);
            eprintln!("source-bound gateway {gate:?}: {}", h.id);
            if gate == Gate::Material {
                let names = inner
                    .exec_container_command(
                        h,
                        "0:0",
                        &[
                            "/bin/busybox".into(),
                            "ps".into(),
                            "-o".into(),
                            "comm".into(),
                        ],
                    )
                    .await?;
                assert!(
                    !names.lines().any(|name| name.trim().starts_with("gluetun")),
                    "trailer must not start Gluetun before fresh authority"
                );
            }
        }
        drop(invoke); // Drop the ACTUAL NativeScope, not a fixture flag.
        let native = broker.native.as_ref().context("actual source owner")?;
        assert!(native.check().is_err());
        let gateway = broker.gateway.as_mut().context("original gateway owner")?;
        assert!(gateway.current().await.is_err());
        assert!(gateway.launch_http(&image).await.is_err());
        if let Some(http) = broker.http.as_mut() {
            assert!(http.attach_io().await.is_err());
            timeout(Duration::from_secs(40), http.cancel()).await??;
        }
        broker.http = None;
        assert!(!lane.join("http-helper.pending").exists());
        assert!(
            Fence::load(&native.lease)?
                .unwrap()
                .clear(&native.lease)
                .is_err()
        );
        if gate != Gate::Ready {
            assert!(
                timeout(
                    Duration::from_millis(20),
                    broker.gateway.as_mut().unwrap().cancel()
                )
                .await
                .is_err()
            );
        }
        runtime.release.notify_one();
        timeout(
            Duration::from_secs(40),
            broker.gateway.as_mut().unwrap().cancel(),
        )
        .await??;
        assert!(!lane.join("gateway-helper.pending").exists());
        if let Some(h) = helper {
            assert!(inner.get_container_handle(&h.id).await?.is_none());
        }
        assert_eq!(
            runtime.starts.load(Ordering::SeqCst),
            match gate {
                Gate::Ready => 2, // Original gateway plus its derived HTTP child.
                Gate::Started | Gate::Attached | Gate::Material => 1,
                _ => 0,
            }
        );
        assert!(
            ControllerLease::acquire(&lane).is_err(),
            "original source still owns global lane"
        );
        Ok::<_, anyhow::Error>(())
    })
    .catch_unwind()
    .await;
    if let Ok(Err(error)) = &outcome {
        eprintln!("original gateway boundary {gate:?} failed before cleanup: {error:#}");
    }
    runtime.release.notify_one();
    let http_cleanup = if let Some(http) = broker.http.as_mut() {
        timeout(Duration::from_secs(40), http.cancel())
            .await
            .context("original derived HTTP fixture cleanup")?
    } else {
        Ok(())
    };
    let gateway_cleanup = if let Some(owner) = broker.gateway.as_mut() {
        timeout(Duration::from_secs(40), owner.cancel())
            .await
            .context("original gateway fixture cleanup")?
    } else {
        Ok(())
    };
    let source_cleanup = if let Some(source) = matched.as_mut() {
        source.source.discard_retained().await
    } else {
        Ok(())
    };
    drop(matched);
    drop(broker);
    let volume_cleanup = async {
        if inner.private_file_volume_owned(&volume, &labels).await? {
            inner.remove_private_file_volume(&volume, &labels).await?;
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    if http_cleanup.is_err()
        || gateway_cleanup.is_err()
        || source_cleanup.is_err()
        || volume_cleanup.is_err()
    {
        eprintln!(
            "retained original gateway fixture journals at {}",
            root.path().display()
        );
        let _ = root.keep();
    }
    http_cleanup.context("derived HTTP fixture cleanup")?;
    gateway_cleanup.context("original gateway fixture cleanup")?;
    source_cleanup.context("original source fixture cleanup after gateway")?;
    volume_cleanup.context("original fixture volume cleanup")?;
    let originals = runtime.owned.lock().unwrap().clone();
    for (handle, _) in originals {
        assert!(inner.get_container_handle(&handle.id).await?.is_none());
    }
    ControllerLease::acquire(&lane)?;
    inner
        .remove_packet_uplink(&underlay.spec, &underlay.id)
        .await?;
    match outcome {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

#[tokio::test]
#[ignore = "exclusive actual source/VM+local VPN config+original gateway/derived HTTP child lifetime;6 cancellation boundaries, synthetic grants/resources, NO working VPN or HTTP input"]
async fn native_source_gateway_retains_late_startup_and_joins_original_private_material_tasks()
-> Result<()> {
    for gate in [
        Gate::Created,
        Gate::Started,
        Gate::Attached,
        Gate::Material,
        Gate::Ready,
        Gate::Rejected,
    ] {
        scenario(gate)
            .await
            .with_context(|| format!("original gateway boundary {gate:?}"))?;
    }
    Ok(())
}
