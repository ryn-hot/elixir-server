//! Structured authorization monitoring of the ORIGINAL source owner. No task
//! is spawned or aborted: a timed-out SQL check signals failure immediately,
//! then remains owned until it actually joins alongside native cleanup.
use super::*;
use crate::{db::Database, marketplace::grant_store::source_authorization::SourceAuthorization};

const CHECK_INTERVAL: Duration = Duration::from_secs(5);
const CHECK_DEADLINE: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub(super) struct Binding {
    pub authorization: SourceAuthorization,
    pub database: Database,
}
#[async_trait]
pub(super) trait Check: Send + Sync {
    async fn check(&self) -> Result<()>;
}
#[async_trait]
impl Check for Binding {
    async fn check(&self) -> Result<()> {
        self.authorization.current(&self.database).await
    }
}
pub(super) async fn monitor<C: Check>(
    binding: Option<C>,
    mut stop: watch::Receiver<bool>,
    failed: watch::Sender<bool>,
) -> Result<()> {
    let Some(binding) = binding else {
        cancelled(&mut stop).await;
        return Ok(());
    };
    loop {
        if *stop.borrow() || stop.has_changed().is_err() {
            return Ok(());
        }
        let check = binding.check();
        tokio::pin!(check);
        let result = tokio::select! {
            biased;
            result=&mut check=>result,
            _=cancelled(&mut stop)=>{
                // Stop new checks, not the already issued original query.
                return check.await;
            },
            _=sleep(CHECK_DEADLINE)=>{
                failed.send_replace(true);
                // Native input can now retire, but the monitor cannot pretend
                // a dropped waiter means its SQL owner has finished.
                let _=check.await;
                bail!("source authorization check exceeded its deadline");
            },
        };
        if let Err(error) = result {
            failed.send_replace(true);
            return Err(error);
        }
        tokio::select! {biased;_=cancelled(&mut stop)=>return Ok(()),_=sleep(CHECK_INTERVAL)=>{}}
    }
}

#[cfg(test)]
mod tests;
