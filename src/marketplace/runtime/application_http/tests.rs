//! Desired-profile/OCI-shape fixtures only, not live VPN or native admission.
use super::*;
use crate::runtime::model::{
    CONTAINER_SPEC_HASH_LABEL, ContainerRuntimeLogConfig, ContainerRuntimeSecurityState,
    ContainerRuntimeState,
};
use std::collections::BTreeMap;

fn profile() -> ApplicationHttpProfile {
    ApplicationHttpProfile::new(
        "fixture",
        &format!("sha256:{}", "a".repeat(64)),
        &"b".repeat(64),
        Digest::of(b"installed scope fixture"),
        Digest::of(b"original invocation fixture"),
    )
    .unwrap()
}
fn inspection(p: &ApplicationHttpProfile) -> (ContainerHandle, PrivateActorInspection) {
    let spec = p.spec();
    let handle = ContainerHandle {
        id: "c".repeat(64),
        name: spec.name.clone(),
    };
    let limit = |value| {
        Some(ContainerRuntimeUlimit {
            soft: value,
            hard: value,
        })
    };
    let actual = PrivateActorInspection {
        id: handle.id.clone(),
        running: false,
        runtime: ContainerRuntimeState {
            name: spec.name.clone(),
            network_mode: spec.network_mode.clone(),
            labels: spec.labels.clone(),
            mounts: vec![],
            published_ports: vec![],
            security: ContainerRuntimeSecurityState {
                image_reference: Some(spec.image.clone()),
                image_id: Some(spec.image.clone()),
                user: Some("10005:10005".into()),
                read_only_rootfs: true,
                no_new_privileges: true,
                cap_drop: vec!["ALL".into()],
                memory_limit_bytes: Some(MEMORY_BYTES as i64),
                memory_swap_limit_bytes: Some(MEMORY_BYTES as i64),
                nano_cpus: Some(i64::from(CPU_MILLICORES) * 1_000_000),
                pids_limit: Some(64),
                shared_memory_bytes: Some(MIB as i64),
                core_dump_limit: limit(0),
                open_files_limit: limit(64),
                file_size_limit: limit(1),
                log_config: Some(ContainerRuntimeLogConfig {
                    driver: "none".into(),
                    options: BTreeMap::new(),
                }),
                stdin_open: Some(true),
                tty: Some(false),
                privileged: Some(false),
                ..Default::default()
            },
        },
        command: spec.command.clone(),
        entrypoint: vec![ENTRYPOINT.into()],
        environment: vec![
            "PATH=/usr/local/bin:/usr/bin:/bin".into(),
            "NODE_VERSION=24.16.0".into(),
        ],
        pid_mode: String::new(),
        ipc_mode: "private".into(),
        userns_mode: String::new(),
        device_count: 0,
        device_request_count: 0,
        bind_count: 0,
        sysctls: BTreeMap::new(),
        networks: vec![],
        network_ids: BTreeMap::new(),
        endpoint_ids: BTreeMap::new(),
        restart_policy: "no".into(),
    };
    (handle, actual)
}

#[test]
fn application_http_has_no_catalog_storage_or_background_allocation() -> Result<()> {
    let p = profile();
    let spec = p.spec();
    assert_eq!(spec.command, ["--sdk-http-stream-large"]);
    assert_eq!(spec.security.user.as_deref(), Some("10005:10005"));
    assert_eq!(spec.security.memory_limit_mb, Some(96));
    assert_eq!(spec.security.memory_swap_limit_mb, Some(96));
    assert_eq!(spec.security.cpu_quota.as_deref(), Some("0.090"));
    assert!(spec.env.is_empty() && spec.volumes.is_empty() && spec.security.tmpfs.is_empty());
    assert!(spec.ports.is_empty() && spec.devices.is_empty() && spec.cap_add.is_empty());
    assert_eq!(p.native_deadline(), Duration::from_secs(65));
    assert_eq!(spec.labels[OWNER_LABEL], "fixture");
    assert!(spec.labels[ELIXIR_DEPLOYMENT_ID_LABEL].starts_with("application-http-"));
    assert_ne!(spec.labels[ELIXIR_DEPLOYMENT_ID_LABEL], "fixture");
    assert_eq!(
        spec.labels[CONTAINER_SPEC_HASH_LABEL],
        crate::runtime::model::container_spec_fingerprint(spec)
    );
    let (handle, actual) = inspection(&p);
    p.verify(&handle, &actual)?;
    Ok(())
}

#[test]
fn buffered_http_uses_existing_single_exchange_and_legacy_intents_keep_stream_mode() -> Result<()> {
    let stream = profile();
    let legacy = elixir_marketplace::canonical::to_vec(&stream.intent())?;
    assert!(!String::from_utf8_lossy(&legacy).contains("\"mode\""));
    let restored: ApplicationHttpIntent = elixir_marketplace::canonical::decode(&legacy, 4096)?;
    assert_eq!(
        restored.profile()?.spec().command,
        ["--sdk-http-stream-large"]
    );
    let buffered = ApplicationHttpProfile::for_mode(
        "fixture",
        &stream.spec.image,
        &"b".repeat(64),
        stream.intent.scope,
        stream.intent.invocation,
        HttpMode::Buffered,
    )?;
    assert_eq!(buffered.spec().command, ["--sdk-http"]);
    assert_eq!(buffered.native_deadline(), Duration::from_secs(15));
    assert_ne!(buffered.intent().fingerprint, stream.intent().fingerprint);
    assert_eq!(
        buffered.spec.security.memory_limit_mb,
        stream.spec.security.memory_limit_mb
    );
    assert_eq!(
        buffered.spec.security.cpu_quota,
        stream.spec.security.cpu_quota
    );
    let encoded = elixir_marketplace::canonical::to_vec(&buffered.intent())?;
    let restored: ApplicationHttpIntent = elixir_marketplace::canonical::decode(&encoded, 4096)?;
    assert_eq!(restored.profile()?.spec().command, ["--sdk-http"]);
    let (handle, mut actual) = inspection(&buffered);
    buffered.verify(&handle, &actual)?;
    actual.command = stream.spec.command.clone();
    assert!(buffered.verify(&handle, &actual).is_err());
    let mut changed = buffered.intent();
    changed.mode = HttpMode::StreamLarge;
    assert!(
        changed.profile().is_err(),
        "recovery cannot replace the frozen helper mode"
    );
    let mut changed = stream.intent();
    changed.mode = HttpMode::Buffered;
    assert!(changed.profile().is_err());
    Ok(())
}

#[test]
fn application_http_requires_immutable_separate_gateway_and_scoped_identity() {
    let p = profile();
    let image = &p.spec.image;
    let scope = Digest::of(b"scope");
    let invocation = Digest::of(b"invocation");
    for deployment in ["", "../owner", "owner/name", &"x".repeat(65)] {
        assert!(
            ApplicationHttpProfile::new(deployment, image, &"b".repeat(64), scope, invocation)
                .is_err()
        );
    }
    for gateway in ["host", "none", "gateway-name", &"B".repeat(64)] {
        assert!(ApplicationHttpProfile::new("fixture", image, gateway, scope, invocation).is_err());
    }
    for image in [
        "worker:latest",
        &format!("worker@sha256:{}", "a".repeat(64)),
        &format!("sha256:{}", "A".repeat(64)),
    ] {
        assert!(
            ApplicationHttpProfile::new("fixture", image, &"b".repeat(64), scope, invocation)
                .is_err()
        );
    }
    let zero: Digest = format!("sha256:{}", "0".repeat(64)).parse().unwrap();
    assert!(
        ApplicationHttpProfile::new("fixture", image, &"b".repeat(64), zero, invocation).is_err()
    );
    assert!(ApplicationHttpProfile::new("fixture", image, &"b".repeat(64), scope, zero).is_err());
}

#[test]
fn application_http_rejects_changed_native_identity_resources_and_process_hooks() -> Result<()> {
    let p = profile();
    let (handle, actual) = inspection(&p);
    macro_rules! denied {
        ($change:expr) => {{
            let mut bad = actual.clone();
            $change(&mut bad);
            assert!(p.verify(&handle, &bad).is_err());
        }};
    }
    denied!(|v: &mut PrivateActorInspection| v.id = "d".repeat(64));
    denied!(|v: &mut PrivateActorInspection| v.runtime.name.push_str("-other"));
    denied!(|v: &mut PrivateActorInspection| v.runtime.network_mode = Some("host".into()));
    denied!(|v: &mut PrivateActorInspection| v.runtime.network_mode =
        Some(format!("container:{}", "d".repeat(64))));
    denied!(|v: &mut PrivateActorInspection| v.runtime.labels.remove(SCOPE_LABEL));
    denied!(|v: &mut PrivateActorInspection| v.runtime.labels.remove(CONTAINER_SPEC_HASH_LABEL));
    denied!(
        |v: &mut PrivateActorInspection| v.runtime.security.image_id =
            Some(format!("sha256:{}", "d".repeat(64)))
    );
    denied!(
        |v: &mut PrivateActorInspection| v.runtime.security.image_reference =
            Some("worker:latest".into())
    );
    denied!(|v: &mut PrivateActorInspection| v.runtime.security.user = Some("10004:10004".into()));
    denied!(|v: &mut PrivateActorInspection| v.runtime.security.read_only_rootfs = false);
    denied!(|v: &mut PrivateActorInspection| v.runtime.security.no_new_privileges = false);
    denied!(|v: &mut PrivateActorInspection| v.runtime.security.privileged = Some(true));
    denied!(|v: &mut PrivateActorInspection| v.runtime.security.cap_add.push("NET_ADMIN".into()));
    denied!(|v: &mut PrivateActorInspection| v.runtime.security.cap_drop.clear());
    denied!(|v: &mut PrivateActorInspection| v.runtime.security.memory_limit_bytes = None);
    denied!(
        |v: &mut PrivateActorInspection| v.runtime.security.memory_swap_limit_bytes =
            Some(2 * MEMORY_BYTES as i64)
    );
    denied!(|v: &mut PrivateActorInspection| v.runtime.security.nano_cpus = Some(200_000_000));
    denied!(|v: &mut PrivateActorInspection| v.runtime.security.pids_limit = None);
    denied!(
        |v: &mut PrivateActorInspection| v.runtime.security.shared_memory_bytes =
            Some(64 * MIB as i64)
    );
    denied!(|v: &mut PrivateActorInspection| v.runtime.security.core_dump_limit = None);
    denied!(|v: &mut PrivateActorInspection| v.runtime.security.open_files_limit = None);
    denied!(|v: &mut PrivateActorInspection| v.runtime.security.file_size_limit = None);
    denied!(|v: &mut PrivateActorInspection| v.runtime.security.log_config = None);
    denied!(|v: &mut PrivateActorInspection| v.runtime.security.stdin_open = Some(false));
    denied!(|v: &mut PrivateActorInspection| v.runtime.security.tty = Some(true));
    denied!(
        |v: &mut PrivateActorInspection| v.runtime.security.seccomp_profile =
            Some("unconfined".into())
    );
    denied!(|v: &mut PrivateActorInspection| v.command.push("--unsafe".into()));
    denied!(|v: &mut PrivateActorInspection| v.entrypoint = vec!["/bin/sh".into()]);
    denied!(|v: &mut PrivateActorInspection| v
        .environment
        .push("NODE_OPTIONS=--require=/evil".into()));
    denied!(|v: &mut PrivateActorInspection| v.environment.push("PATH=/evil".into()));
    denied!(|v: &mut PrivateActorInspection| v.pid_mode = "host".into());
    denied!(|v: &mut PrivateActorInspection| v.ipc_mode = "host".into());
    denied!(|v: &mut PrivateActorInspection| v.userns_mode = "host".into());
    denied!(|v: &mut PrivateActorInspection| v.device_count = 1);
    denied!(|v: &mut PrivateActorInspection| v.device_request_count = 1);
    denied!(|v: &mut PrivateActorInspection| v.bind_count = 1);
    denied!(|v: &mut PrivateActorInspection| v
        .sysctls
        .insert("net.ipv4.ip_forward".into(), "1".into()));
    denied!(|v: &mut PrivateActorInspection| v.networks.push("default".into()));
    denied!(|v: &mut PrivateActorInspection| v
        .network_ids
        .insert("default".into(), "d".repeat(64)));
    denied!(|v: &mut PrivateActorInspection| v
        .endpoint_ids
        .insert("default".into(), "e".repeat(64)));
    denied!(|v: &mut PrivateActorInspection| v.restart_policy = "always".into());
    let mut other = handle;
    other.id = "mutable-name".into();
    assert!(p.verify(&other, &actual).is_err());
    Ok(())
}

#[test]
fn original_invocation_and_owner_scope_change_the_frozen_fingerprint() -> Result<()> {
    let p = profile();
    for (deployment, gateway, scope, invocation) in [
        (
            "other",
            "b".repeat(64),
            Digest::of(b"installed scope fixture"),
            Digest::of(b"original invocation fixture"),
        ),
        (
            "fixture",
            "d".repeat(64),
            Digest::of(b"installed scope fixture"),
            Digest::of(b"original invocation fixture"),
        ),
        (
            "fixture",
            "b".repeat(64),
            Digest::of(b"different source/grant scope"),
            Digest::of(b"original invocation fixture"),
        ),
        (
            "fixture",
            "b".repeat(64),
            Digest::of(b"installed scope fixture"),
            Digest::of(b"next invocation"),
        ),
    ] {
        let other =
            ApplicationHttpProfile::new(deployment, &p.spec.image, &gateway, scope, invocation)?;
        assert_ne!(
            other.spec.labels[CONTAINER_SPEC_HASH_LABEL],
            p.spec.labels[CONTAINER_SPEC_HASH_LABEL]
        );
    }
    Ok(())
}

#[test]
fn a_gateway_change_cannot_create_a_second_name_for_the_same_original_invocation() -> Result<()> {
    let p = profile();
    let other = ApplicationHttpProfile::new(
        "fixture",
        &p.spec.image,
        &"d".repeat(64),
        Digest::of(b"installed scope fixture"),
        Digest::of(b"original invocation fixture"),
    )?;
    assert_eq!(other.spec.name, p.spec.name);
    assert_ne!(
        other.spec.labels[CONTAINER_SPEC_HASH_LABEL],
        p.spec.labels[CONTAINER_SPEC_HASH_LABEL]
    );
    let next = ApplicationHttpProfile::new(
        "fixture",
        &p.spec.image,
        &"b".repeat(64),
        Digest::of(b"installed scope fixture"),
        Digest::of(b"next invocation"),
    )?;
    assert_ne!(next.spec.name, p.spec.name);
    let foreign = ApplicationHttpProfile::new(
        "other",
        &p.spec.image,
        &"b".repeat(64),
        Digest::of(b"installed scope fixture"),
        Digest::of(b"original invocation fixture"),
    )?;
    assert_ne!(foreign.spec.name, p.spec.name);
    Ok(())
}
