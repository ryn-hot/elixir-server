//! The same owned compiled preparation used by activation and installed calls.
//! Standing authority remains the caller's restriction, never a saved record.
use super::*;

pub(super) async fn load(
    owner: &mut Owner,
    database: &Database,
    binding: &dyn Access,
    configuration: &InstalledRuntime,
    capsule: &Capsule,
    secrets: &SecretsManager,
    stop: &mut RunControl,
    expires: Deadline,
) -> Result<()> {
    let trusted = configuration
        .compiled_toolchains
        .as_deref()
        .context("compiled execution is not configured")?;
    let record = capsule
        .build
        .as_ref()
        .context("compiled installed history missing")?;
    let preparation = {
        let Stage::Matched(source) = &mut owner.0 else {
            bail!("compiled source owner missing");
        };
        CompiledPreparation::begin(
            source,
            record,
            capsule.facts()?,
            capsule.descriptor()?,
            capsule.source.summary().clone(),
            trusted,
            secrets,
        )?
    };
    owner.0 = Stage::Compiling(Box::new(preparation));
    loop {
        let (retryable, result) = {
            let Stage::Compiling(preparation) = &mut owner.0 else {
                unreachable!()
            };
            let retryable = preparation.admission_retryable();
            (
                retryable,
                guarded(
                    database,
                    binding,
                    stop,
                    expires,
                    Box::pin(preparation.advance()),
                )
                .await,
            )
        };
        match result {
            Ok(true) => break,
            Ok(false) => {}
            Err(error) if retryable => {
                resource_pause(database, binding, stop, expires, error).await?
            }
            Err(error) => return Err(error),
        }
    }
    let Stage::Compiling(preparation) = &mut owner.0 else {
        unreachable!()
    };
    let code = preparation.take_ready()?;
    owner.0 = Stage::Executing(Box::new(code));
    Ok(())
}
