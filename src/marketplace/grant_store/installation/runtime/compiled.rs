//! Retained native code materialization under the existing installed/candidate
//! owner and standing checks. No saved image ID, new compiler or permission.
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
    let facts = capsule.facts()?;
    let record = capsule
        .build
        .as_ref()
        .context("compiled installed history missing")?;
    let trusted = configuration
        .compiled_toolchains
        .as_deref()
        .context("compiled execution is not configured")?;
    let proof = {
        let Stage::Matched(slot) = &mut owner.0 else {
            bail!("compiled source owner missing");
        };
        record.bind_retained(
            slot.as_mut().as_mut().context("compiled source missing")?,
            &facts,
            trusted,
            secrets,
        )?
    };
    let filesystem = loop {
        let checked = {
            let Stage::Matched(slot) = &mut owner.0 else {
                bail!("compiled source owner changed");
            };
            guarded(
                database,
                binding,
                stop,
                expires,
                slot.as_mut()
                    .as_mut()
                    .context("compiled source missing")?
                    .check_retained_container_filesystem(&proof),
            )
            .await
        };
        match checked {
            Ok(filesystem) => break filesystem,
            Err(error) => resource_pause(database, binding, stop, expires, error).await?,
        }
    };
    let exporting = {
        let Stage::Matched(slot) = &mut owner.0 else {
            bail!("compiled export source missing");
        };
        SourceMatchedQuarantine::begin_retained_image_export(slot, &proof, &filesystem)?
    };
    owner.0 = Stage::Exporting(Box::new(exporting));
    {
        let Stage::Exporting(exporting) = &mut owner.0 else {
            unreachable!()
        };
        // These are one-use export/import attempts. Unknown or post-start
        // failures must retire this owner, not replay work or reset its budget.
        guarded(database, binding, stop, expires, exporting.run()).await?;
    }
    let (source, sealed) = {
        let Stage::Exporting(exporting) = &mut owner.0 else {
            unreachable!()
        };
        exporting.finish()?
    };
    owner.0 = Stage::Matched(Box::new(Some(source)));
    let mut sealed = Some(sealed);
    let importing = {
        let Stage::Matched(slot) = &mut owner.0 else {
            unreachable!()
        };
        SourceMatchedQuarantine::begin_image_import(slot, &mut sealed)?
    };
    owner.0 = Stage::Importing(Box::new(importing));
    {
        let Stage::Importing(importing) = &mut owner.0 else {
            unreachable!()
        };
        guarded(database, binding, stop, expires, importing.run()).await?;
    }
    // Ownership moves inside the guarded future BEFORE its post-return stop
    // check. Cancellation can reject success, but cannot discard the cleanup
    // owner returned by adopt or replace it with a serialized image reference.
    guarded(database, binding, stop, expires, async {
        let (source, image) = {
            let Stage::Importing(importing) = &mut owner.0 else {
                unreachable!()
            };
            importing.adopt().await?
        };
        owner.0 = Stage::Matched(Box::new(Some(source)));
        let mut image = Some(image);
        let code = {
            let Stage::Matched(slot) = &mut owner.0 else {
                unreachable!()
            };
            RetainedCode::take_compiled(
                slot,
                &mut image,
                &facts,
                &capsule.descriptor()?,
                capsule.source.summary(),
            )?
        };
        owner.0 = Stage::Executing(Box::new(code));
        Ok(())
    })
    .await
}
