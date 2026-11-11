//! The shipped SDK talks to the original holder over authenticated loopback.
//! Only the already-checked synthetic inputs ever enter this private test.
use super::*;
use std::{ffi::OsString, process::Stdio, sync::Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const OUTPUT_LIMIT: u64 = 64 * 1024;
const SDK: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../extensions/sdk/marketplace/src/publish-cli.mjs"
);
const INVOKE: &str = r#"
import {pathToFileURL} from 'node:url';
// With node -e, the first supplied argument occupies argv[1]. Remove the
// wrapper's arguments before importing the CLI so it cannot also auto-run
// with process.env (which deliberately does not contain the owner token).
const [cli,...args]=process.argv.splice(1);
const chunks=[];let size=0;
for await(const bytes of process.stdin){
  size+=bytes.length;
  if(size>8192)throw Error('test owner token exceeds bound');
  chunks.push(bytes);
}
const token=Buffer.concat(chunks,size).toString('utf8');
if(!/^[\x21-\x7e]+$/.test(token))throw Error('invalid test owner token');
const {run}=await import(pathToFileURL(cli));
await run(args,{ELIXIR_OWNER_ACCESS_TOKEN:token});
"#;

pub(super) struct PublisherSdk {
    node: PathBuf,
}

impl PublisherSdk {
    pub(super) fn configured() -> Result<Self> {
        let node = PathBuf::from(
            std::env::var_os("ELIXIR_MARKETPLACE_SDK_NODE")
                .context("explicit Node executable required")?,
        );
        ensure!(
            node.is_absolute() && node.is_file() && Path::new(SDK).is_file(),
            "explicit Node executable and repository SDK required"
        );
        Ok(Self {
            node: node.canonicalize()?,
        })
    }

    pub(super) async fn preflight(&self) -> Result<()> {
        // No socket, fixture credentials, provider, or source upload. A
        // double invocation prints help twice even if both calls exit zero.
        let output = self
            .invoke(None, "local-preflight-only", vec!["--help".into()])
            .await?;
        let usage = std::str::from_utf8(&output)?;
        ensure!(
            usage.starts_with("usage: elixir-marketplace-publish ")
                && usage.matches("usage: elixir-marketplace-publish ").count() == 1,
            "the token-pipe wrapper did not invoke the SDK CLI exactly once"
        );
        Ok(())
    }

    async fn run(&self, actor: &Participant, args: Vec<OsString>) -> Result<Value> {
        let token = actor.token()?;
        ensure!(
            !token.is_empty() && token.len() <= 8192,
            "test owner token bound"
        );
        let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
        let origin = format!("http://{}", listener.local_addr()?);
        let last = Arc::new(Mutex::new(None));
        let seen = last.clone();
        let app = actor.app()?.clone().layer(axum::middleware::from_fn(
            move |request: axum::extract::Request, next: axum::middleware::Next| {
                let seen = seen.clone();
                async move {
                    let stage = request_stage(request.uri().path());
                    let response = next.run(request).await;
                    if let Ok(mut last) = seen.lock() {
                        *last = Some((stage, response.status().as_u16()));
                    }
                    response
                }
            },
        ));
        let stop = CancellationToken::new();
        let _on_drop = stop.clone().drop_guard();
        let shutdown = stop.clone();
        let mut server = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(shutdown.cancelled_owned())
                .await
        });
        let result = self
            .invoke(Some(&origin), token, args)
            .await
            .and_then(|output| {
                canonical::parse(&output, OUTPUT_LIMIT as usize).context("invalid SDK response")
            });
        stop.cancel();
        let retirement = match timeout(Duration::from_secs(10), &mut server).await {
            Ok(result) => result
                .context("SDK holder HTTP join")
                .and_then(|served| served.context("SDK holder HTTP")),
            Err(error) => {
                server.abort();
                let _ = server.await;
                Err(error).context("SDK holder HTTP retirement")
            }
        };
        match result {
            Ok(value) => {
                retirement?;
                Ok(value)
            }
            Err(error) => {
                // Only a static route class and numeric status, not the
                // child's stderr, actual URI, token, body or a response error.
                let last = last.lock().ok().and_then(|value| *value);
                let error = error.context(format!("last SDK holder HTTP reply: {last:?}"));
                match retirement {
                    Ok(()) => Err(error),
                    Err(cleanup) => {
                        Err(error.context(format!("SDK HTTP cleanup also failed: {cleanup:#}")))
                    }
                }
            }
        }
    }

    async fn invoke(
        &self,
        origin: Option<&str>,
        token: &str,
        args: Vec<OsString>,
    ) -> Result<Vec<u8>> {
        let mut command = tokio::process::Command::new(&self.node);
        command.args(["--input-type=module", "-e", INVOKE, "--", SDK]);
        if let Some(origin) = origin {
            command.args(["--server", origin]);
        }
        let mut child = command
            .args(args)
            .env_clear()
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()?;
        let operation = timeout(Duration::from_secs(240), async {
            let mut stdin = child.stdin.take().context("SDK stdin missing")?;
            stdin.write_all(token.as_bytes()).await?;
            stdin.shutdown().await?;
            drop(stdin);
            let stdout = child.stdout.take().context("SDK stdout missing")?;
            let mut output = Vec::new();
            let mut stdout = stdout.take(OUTPUT_LIMIT + 1);
            stdout.read_to_end(&mut output).await?;
            ensure!(
                output.len() as u64 <= OUTPUT_LIMIT,
                "SDK output exceeds bound"
            );
            ensure!(
                child.wait().await?.success(),
                "original publisher SDK command failed"
            );
            Ok(output)
        })
        .await;
        match operation {
            Ok(Ok(value)) => Ok(value),
            failed => {
                let error = match failed {
                    Ok(Err(error)) => error,
                    Err(error) => error.into(),
                    Ok(Ok(_)) => unreachable!(),
                };
                let retired = async {
                    if child.try_wait()?.is_none() {
                        child.start_kill()?;
                        timeout(Duration::from_secs(10), child.wait()).await??;
                    }
                    Ok::<(), anyhow::Error>(())
                }
                .await;
                match retired {
                    Ok(()) => Err(error),
                    Err(cleanup) => {
                        Err(error.context(format!("SDK child cleanup also failed: {cleanup:#}")))
                    }
                }
            }
        }
    }

    pub(super) async fn install_and_seed(
        &self,
        actor: &Participant,
        source: &SourceFixture,
        existing: Option<Uuid>,
    ) -> Result<Uuid> {
        // Original fixture bytes were checked before enrollment. Use an
        // owned private copy to avoid rereading replaceable external inputs.
        let root = tempfile::tempdir_in(actor.rig.root.path())?;
        let publication = root.path().join("publication.json");
        let package = root.path().join("source.elx");
        let proof = root.path().join("source-proof.elxgit");
        std::fs::write(&publication, source.publication.to_bytes()?)?;
        std::fs::write(&package, &source.package)?;
        std::fs::write(&proof, &source.proof)?;
        let publication = publication.into_os_string();
        let mut args = vec![
            "--publication".into(),
            publication.clone(),
            "--package".into(),
            package.into_os_string(),
            "--proof".into(),
            proof.into_os_string(),
            "--accept-unqualified".into(),
        ];
        match existing {
            Some(id) => args.extend([OsString::from("--instance"), id.to_string().into()]),
            None => args.extend([
                OsString::from("--name"),
                "Public qualification source".into(),
            ]),
        }
        let view = self.run(actor, args).await?;
        let reference = Digest::of(source.publication.to_bytes()?);
        ensure!(
            view["phase"] == "awaiting_approval"
                && view["release_id"] == json!(source.matched.release_id)
                && view["initial_request"]["direct_reference"] == json!(reference)
                && view["initial_request"]["upload_source"] == true
                && view["installation_ready"] == false,
            "SDK did not stop at the original exact source review"
        );
        let id: Uuid = serde_json::from_value(view["operation_id"].clone())?;
        eprintln!(
            "{}",
            json!({"stage":"real_publisher_sdk_exact_source_review",
            "version":source.publication.release.payload.version,"source_uploads":2})
        );
        let instance = activate_direct_review(
            actor.app()?,
            actor.state()?,
            actor.token()?,
            id,
            &view,
            existing,
            false,
        )
        .await?;
        actor.roles(true, 2).await?;
        let seeding = self
            .run(
                actor,
                vec![
                    "--publication".into(),
                    publication,
                    "--seed-instance".into(),
                    instance.to_string().into(),
                    "--accept-unqualified-seeding".into(),
                ],
            )
            .await?;
        ensure!(
            seeding["accepted_reference"] == json!(reference)
                && seeding["enabled"] == true
                && seeding["instance_id"] == json!(instance),
            "SDK seeding receipt changed original source or instance"
        );
        actor.wait_for_sharing().await?;
        eprintln!(
            "{}",
            json!({"stage":"real_publisher_sdk_activated_and_seeded",
            "version":source.publication.release.payload.version,"source_uploads":2})
        );
        Ok(instance)
    }
}

fn request_stage(path: &str) -> &'static str {
    match path {
        "/api/v1/marketplace/installations/current" => "current",
        "/api/v1/marketplace/install/targets" => "targets",
        "/api/v1/marketplace/installations/direct" => "direct",
        "/api/v1/marketplace/publisher-seeding" => "seed_submit",
        path if path.starts_with("/api/v1/marketplace/publisher-seeding/") => "seed_lookup",
        path if path.starts_with("/api/v1/marketplace/installations/") => {
            if path.contains("/uploads/") {
                "upload"
            } else {
                "status"
            }
        }
        _ => "unexpected",
    }
}

#[tokio::test]
#[ignore = "requires an explicitly selected Node runtime and the repository SDK; no network"]
async fn publisher_sdk_pipe_invokes_cli_once_before_enrollment() -> Result<()> {
    PublisherSdk::configured()?.preflight().await
}
