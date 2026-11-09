//! One original native source/image preparation owner. Each borrow may be
//! cancelled; the owner retains partial work and its unchanged lane/deadline.
//! This grants no public-source freshness, SDK effects or installed authority.
use super::super::super::{
    image_export::Export,
    image_import::Import,
    local_build::{LocalBuildRecord, VerifiedLocalBuild},
};
use super::*;
use crate::{marketplace::runtime::build::ApprovedToolchains, secrets::SecretsManager};

pub(in crate::marketplace) struct CompiledPreparation {
    stage: Stage,
    authority: Box<VerifiedRetainedAuthority>,
    expected: Box<SourceMatchDescriptor>,
    summary: Box<SourceMatchSummary>,
    proof: VerifiedLocalBuild,
    filesystem: Option<super::super::super::container_filesystem::NativeContainerFilesystemMatch>,
}
enum Stage {
    Empty,
    Matched(Box<Option<SourceMatchedQuarantine>>),
    Exporting(Box<Export>),
    Importing(Box<Import>),
    Ready(Box<RetainedCode>),
}
impl CompiledPreparation {
    /// All preflight borrows the original slot. Only actual matching source,
    /// authenticated local history and current compiler trust can move it.
    pub(in crate::marketplace) fn begin(
        source: &mut Option<SourceMatchedQuarantine>,
        record: &LocalBuildRecord,
        authority: VerifiedRetainedAuthority,
        expected: SourceMatchDescriptor,
        summary: SourceMatchSummary,
        approved: &ApprovedToolchains,
        secrets: &SecretsManager,
    ) -> Result<Self> {
        let original = source.as_mut().context("compiled source owner missing")?;
        require_byte_plan_for(
            &authority,
            &expected,
            &summary,
            &original.execution_descriptor(),
            RuntimeProfile::Reproducible,
        )?;
        if canonical::to_vec(original.summary())? != canonical::to_vec(&summary)? {
            bail!("compiled source differs from enrolled native summary");
        }
        let proof = record.bind_retained(original, &authority, approved, secrets)?;
        Ok(Self {
            stage: Stage::Matched(Box::new(source.take())),
            authority: Box::new(authority),
            expected: Box::new(expected),
            summary: Box::new(summary),
            proof,
            filesystem: None,
        })
    }
    /// Only the first filesystem-admission phase may return a retryable
    /// resource wait. Export/import are single attempts, never replayed.
    pub(in crate::marketplace) fn admission_retryable(&self) -> bool {
        matches!(self.stage, Stage::Matched(_))
    }
    pub(in crate::marketplace) async fn advance(&mut self) -> Result<bool> {
        match &mut self.stage {
            Stage::Matched(source) => {
                let filesystem = source
                    .as_mut()
                    .as_mut()
                    .context("compiled source missing")?
                    .check_retained_container_filesystem(&self.proof)
                    .await?;
                let original = source
                    .as_ref()
                    .as_ref()
                    .context("compiled source missing")?;
                if let Some(borrow) = original
                    .source
                    .job
                    .admission
                    .as_ref()
                    .and_then(|a| a.cached_image())
                    && let Some(anchor) = borrow.get(original, &self.proof, &filesystem).await?
                {
                    let mut image = Some(
                        super::super::super::image_reference::RuntimeImage::capture_cached(
                            original,
                            anchor,
                            &self.proof,
                            &filesystem,
                        )?,
                    );
                    let code = RetainedCode::take_compiled(
                        source,
                        &mut image,
                        &self.authority,
                        &self.expected,
                        &self.summary,
                    )?;
                    self.stage = Stage::Ready(Box::new(code));
                    return Ok(true);
                }
                let exporting = SourceMatchedQuarantine::begin_retained_image_export(
                    source,
                    &self.proof,
                    &filesystem,
                )?;
                self.filesystem = Some(filesystem);
                self.stage = Stage::Exporting(Box::new(exporting));
                Ok(false)
            }
            Stage::Exporting(exporting) => {
                exporting.run().await?;
                let (source, sealed) = exporting.finish()?;
                self.stage = Stage::Matched(Box::new(Some(source)));
                let mut sealed = Some(sealed);
                let Stage::Matched(source) = &mut self.stage else {
                    unreachable!()
                };
                let importing = SourceMatchedQuarantine::begin_image_import(source, &mut sealed)?;
                self.stage = Stage::Importing(Box::new(importing));
                Ok(false)
            }
            Stage::Importing(importing) => {
                importing.run().await?;
                let (source, image) = importing.adopt().await?;
                // Install every returned cleanup owner before this future can
                // return Ready and its caller can perform a post-return check.
                self.stage = Stage::Matched(Box::new(Some(source)));
                let mut image = Some(image);
                let Stage::Matched(source) = &mut self.stage else {
                    unreachable!()
                };
                let original = source.as_ref().as_ref().context("cache source missing")?;
                if let Some(borrow) = original
                    .source
                    .job
                    .admission
                    .as_ref()
                    .and_then(|a| a.cached_image())
                {
                    borrow
                        .put(
                            original,
                            image.as_ref().context("cache image missing")?,
                            self.filesystem
                                .as_ref()
                                .context("cache filesystem missing")?,
                        )
                        .await?;
                }
                let code = RetainedCode::take_compiled(
                    source,
                    &mut image,
                    &self.authority,
                    &self.expected,
                    &self.summary,
                )?;
                self.stage = Stage::Ready(Box::new(code));
                Ok(true)
            }
            Stage::Ready(_) => Ok(true),
            Stage::Empty => bail!("compiled preparation already retired or handed off"),
        }
    }
    pub(in crate::marketplace) fn take_ready(&mut self) -> Result<RetainedCode> {
        if !matches!(self.stage, Stage::Ready(_)) {
            bail!("compiled preparation is not ready");
        }
        let Stage::Ready(code) = std::mem::replace(&mut self.stage, Stage::Empty) else {
            unreachable!()
        };
        Ok(*code)
    }
    pub(in crate::marketplace) async fn discard(&mut self) -> Result<()> {
        discard(&mut self.stage).await
    }
}
async fn discard(stage: &mut Stage) -> Result<()> {
    match stage {
        Stage::Empty => {}
        Stage::Matched(source) => {
            if let Some(source) = source.as_mut().as_mut() {
                source.discard_retained().await?;
            }
        }
        Stage::Exporting(exporting) => exporting.discard_source().await?,
        Stage::Importing(importing) => importing.discard_source().await?,
        Stage::Ready(code) => code.discard().await?,
    }
    *stage = Stage::Empty;
    Ok(())
}
impl Drop for CompiledPreparation {
    fn drop(&mut self) {
        let mut stage = std::mem::replace(&mut self.stage, Stage::Empty);
        if matches!(stage, Stage::Empty) {
            return;
        }
        if let Ok(executor) = tokio::runtime::Handle::try_current() {
            executor.spawn(async move {
                let mut reported=false;
                loop {
                    match discard(&mut stage).await {
                        Ok(())=>return,
                        Err(error)=>if !reported {
                            tracing::warn!(error=%error,"compiled preparation cleanup pending; retaining its original native owner");
                            reported=true;
                        },
                    }
                    tokio::time::sleep(Duration::from_secs(10)).await;
                }
            });
        }
        // Without an executor the durable original native fence remains; a
        // dropped wrapper must never manufacture disposal or release it.
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn preparation_keeps_bounded_owner_and_advance_future_sizes() {
        fn size<F: std::future::Future>(
            _: impl FnOnce(&'static mut CompiledPreparation) -> F,
        ) -> usize {
            std::mem::size_of::<F>()
        }
        // Type measurements only; no synthetic native ownership or readiness.
        assert!(std::mem::size_of::<CompiledPreparation>() <= 16 * 1024);
        let future = size(|preparation| preparation.advance());
        assert!(
            future <= 128 * 1024,
            "compiled advance future too large: {future}"
        );
    }
}
