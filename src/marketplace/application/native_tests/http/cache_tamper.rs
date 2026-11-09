//! Real cached-volume corruption, not a forged verification flag. The helper
//! has no network/SDK and mutates only one owned disposable fixture volume.
use super::*;
use crate::runtime::model::{ELIXIR_DEPLOYMENT_ID_LABEL, ELIXIR_MANAGED_LABEL, SourceCacheState};
use std::collections::HashMap;

pub(in crate::marketplace) async fn mutate(
    runtime: &dyn RuntimeManager,
    lane: &Path,
    image: &str,
    purpose: elixir_marketplace::quarantine::QuarantinePurpose,
) -> Result<()> {
    // Retain the SAME global foreground lane until the helper is actually gone.
    let lease = ControllerLease::acquire(lane)?;
    let deployment = lease.storage_namespace()?;
    let volume = format!("elixir-marketplace-source-{deployment}");
    let labels = HashMap::from([
        (ELIXIR_MANAGED_LABEL.into(), "true".into()),
        (ELIXIR_DEPLOYMENT_ID_LABEL.into(), deployment.clone()),
        ("elixir.marketplace.volume".into(), "quarantine-v1".into()),
        ("elixir.marketplace.cache_slot".into(), "source-v1".into()),
    ]);
    anyhow::ensure!(
        matches!(
            runtime.source_cache_state(&volume, &labels).await?,
            SourceCacheState::Present(Some(_))
        ),
        "no exactly owned cached workspace to corrupt"
    );
    anyhow::ensure!(
        runtime
            .get_container_handle(&format!("elixir-marketplace-source-writer-{deployment}"))
            .await?
            .is_none(),
        "cache mutation requires no retained source process"
    );
    let name = format!(
        "elixir-marketplace-cache-tamper-{}",
        Uuid::new_v4().simple()
    );
    let marker = format!("elixir.marketplace.fixture.instance={name}");
    let mount = format!("type=volume,source={volume},target=/quarantine");
    // Preserve file size, physical allocation and ownership. The labels and
    // descriptor still match; only actual native byte verification can reject.
    const SCRIPT: &str = "const fs=require('node:fs'),fd=fs.openSync('/quarantine/package.blob',fs.constants.O_RDWR|fs.constants.O_NOFOLLOW);try{const before=fs.fstatSync(fd),b=Buffer.alloc(1);if(!before.isFile()||before.nlink!==1||before.size<1||fs.readSync(fd,b,0,1,0)!==1)throw Error('shape');b[0]^=1;if(fs.writeSync(fd,b,0,1,0)!==1)throw Error('write');fs.fsyncSync(fd);const after=fs.fstatSync(fd);if(before.ino!==after.ino||before.size!==after.size||before.blocks!==after.blocks||before.mode!==after.mode)throw Error('changed shape')}finally{fs.closeSync(fd)}";
    let filename = match purpose {
        elixir_marketplace::quarantine::QuarantinePurpose::Package => "package.blob",
        elixir_marketplace::quarantine::QuarantinePurpose::Proof => "proof.blob",
        elixir_marketplace::quarantine::QuarantinePurpose::Artifact => "artifact.blob",
    };
    let script = SCRIPT.replace("package.blob", filename);
    let created = docker(&[
        "create",
        "--pull=never",
        "--name",
        &name,
        "--label",
        "elixir.marketplace.fixture=cache-tamper-v1",
        "--label",
        &marker,
        "--network=none",
        "--read-only",
        "--user=10004:10004",
        "--cap-drop=ALL",
        "--security-opt=no-new-privileges",
        "--memory=64m",
        "--memory-swap=64m",
        "--cpus=0.2",
        "--pids-limit=16",
        "--log-driver=none",
        "--shm-size=1m",
        "--ulimit",
        "nofile=64:64",
        "--ulimit",
        "core=0:0",
        "--ulimit",
        "fsize=1048576:1048576",
        "--mount",
        &mount,
        "--entrypoint=/usr/local/bin/node",
        image,
        "--max-old-space-size=16",
        "-e",
        &script,
    ])
    .await;
    // Also discover an uncertain create reply, but never start that outcome.
    let handle = runtime
        .get_container_handle(&name)
        .await?
        .context("cache helper create returned no owned identity")?;
    let inspected: Value = serde_json::from_str(&docker(&["inspect", &handle.id]).await?)?;
    let owned = &inspected[0];
    anyhow::ensure!(
        owned["Id"] == handle.id
            && owned["Name"] == format!("/{name}")
            && owned["Config"]["Labels"]["elixir.marketplace.fixture"] == "cache-tamper-v1"
            && owned["Config"]["Labels"]["elixir.marketplace.fixture.instance"] == name,
        "refusing foreign cache-helper ownership"
    );
    let outcome = async {
        anyhow::ensure!(
            created? == handle.id,
            "create reply changed the original helper identity"
        );
        let config = &owned["HostConfig"];
        anyhow::ensure!(
            owned["Image"] == image
                && owned["State"]["Running"] == false
                && config["NetworkMode"] == "none"
                && config["ReadonlyRootfs"] == true
                && config["Memory"] == 64 * MIB
                && config["MemorySwap"] == 64 * MIB
                && config["NanoCpus"] == 200_000_000
                && config["PidsLimit"] == 16
                && owned["Config"]["User"] == "10004:10004"
                && config["CapDrop"] == json!(["ALL"])
                && config["LogConfig"]["Type"] == "none",
            "cache helper physical profile changed"
        );
        let mounts = owned["Mounts"].as_array().context("cache helper mounts")?;
        anyhow::ensure!(
            mounts.len() == 1
                && mounts[0]["Type"] == "volume"
                && mounts[0]["Name"] == volume
                && mounts[0]["Destination"] == "/quarantine"
                && mounts[0]["RW"] == true,
            "cache helper mount changed"
        );
        docker(&["start", &handle.id]).await?;
        anyhow::ensure!(
            docker(&["wait", &handle.id]).await? == "0",
            "cache mutation failed"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    // Exact positively owned immutable CID only, including failed/late waits.
    let removed = docker(&["rm", "--force", &handle.id]).await;
    let gone = runtime.get_container_handle(&handle.id).await?;
    removed?;
    anyhow::ensure!(gone.is_none(), "original cache helper has not retired");
    outcome
}

pub(super) async fn qualify(
    state: &crate::state::AppState,
    database: &Database,
    runtime: &dyn RuntimeManager,
    workspace: (&Path, &str),
    home: Uuid,
    instance: Uuid,
    extension: &elixir_marketplace::identity::ExtensionId,
) -> Result<()> {
    let (lane, image) = workspace;
    let before = sdk_rows(database).await?;
    mutate(
        runtime,
        lane,
        image,
        elixir_marketplace::quarantine::QuarantinePurpose::Package,
    )
    .await?;
    // Same-shaped cache metadata still claims a hit. This must fail before SDK
    // effects, dispose the poisoned bytes, and never replay code automatically.
    let mut denied = state.marketplace.begin_installed_call(
        home,
        extension.clone(),
        instance,
        RuntimeJson::from_value(json!({}))?,
    )?;
    assert!(
        denied.finish().await.is_err(),
        "cached corruption reached executable source"
    );
    denied.cancel().await?;
    drop(denied);
    assert_eq!(
        sdk_rows(database).await?,
        before,
        "corrupted cache caused SDK effects"
    );
    require_idle_byte_workspace(runtime, lane, false).await?;
    // A NEW explicit call may restore from the unchanged authoritative archive.
    let mut restored = state.marketplace.begin_installed_call(
        home,
        extension.clone(),
        instance,
        RuntimeJson::from_value(json!({}))?,
    )?;
    let value = restored.finish().await?;
    assert_eq!(
        serde_json::from_str::<Value>(value.as_str())?,
        json!({"cursor":1,"key":"credential-private"})
    );
    restored.cancel().await?;
    drop(restored);
    assert_eq!(
        sdk_rows(database).await?,
        before,
        "cold recovery reran migration or changed SDK state"
    );
    require_idle_byte_workspace(runtime, lane, true).await?;
    Ok(())
}
