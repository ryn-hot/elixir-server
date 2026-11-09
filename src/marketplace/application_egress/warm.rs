//! One installed-controller tunnel, not a pool of source permissions or HTTP
//! connections. A borrower returns it only after its original child joins.
use super::*;
use crate::marketplace::{
    grant_store::installation::runtime::RuntimeOwner,
    quarantine::execution::{GatewayLaunch, HttpLaunch, NativeInvocation},
    worker::{IndependentAdmission, SdkHttpAuthority},
};
use anyhow::{Result, bail};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::{Mutex, OwnedMutexGuard};

#[derive(Default)]
struct State {
    gateway: Option<GatewayLaunch>,
    poisoned: bool,
}
pub(in crate::marketplace) struct Pool {
    owner: Arc<RuntimeOwner>,
    runtime: Arc<dyn crate::runtime::RuntimeManager>,
    database: crate::db::Database,
    secrets: Arc<crate::secrets::SecretsManager>,
    network: String,
    state: Arc<Mutex<State>>,
    busy: Arc<AtomicBool>,
}
impl Pool {
    pub(in crate::marketplace) fn new(
        owner: Arc<RuntimeOwner>,
        runtime: Arc<dyn crate::runtime::RuntimeManager>,
        database: crate::db::Database,
        secrets: crate::secrets::SecretsManager,
        network: String,
    ) -> Self {
        Self {
            owner,
            runtime,
            database,
            secrets: Arc::new(secrets),
            network,
            state: Arc::default(),
            busy: Arc::default(),
        }
    }
    pub(in crate::marketplace) async fn acquire(
        &self,
        native: Arc<NativeInvocation>,
        authority: Arc<dyn SdkHttpAuthority>,
        admission: Arc<IndependentAdmission>,
    ) -> Result<Option<Lease>> {
        let mut state = self
            .state
            .clone()
            .try_lock_owned()
            .map_err(|_| anyhow::anyhow!("the original warm tunnel borrower is still owned"))?;
        if state.poisoned || self.busy.load(Ordering::SeqCst) {
            bail!("warm tunnel has unconfirmed original HTTP retirement");
        }
        authority.current_native(&native).await?;
        if let Some(gateway) = state.gateway.as_mut() {
            if !gateway.can_borrow(&native) || gateway.current().await.is_err() {
                gateway.cancel().await?;
                state.gateway = None;
            }
        }
        if state.gateway.is_none() {
            state.gateway = GatewayLaunch::begin_warm(
                self.owner.clone(),
                self.runtime.clone(),
                self.database.clone(),
                self.secrets.clone(),
                &self.network,
                admission,
                self.busy.clone(),
            )
            .await?;
        }
        if state.gateway.is_none() {
            return Ok(None);
        }
        self.busy.store(true, Ordering::SeqCst);
        // No await after binding the borrower and before its outer call owns it.
        Ok(Some(Lease {
            borrow: Borrow {
                state: Some(state),
                busy: self.busy.clone(),
            },
            native,
            authority,
        }))
    }
    pub(in crate::marketplace) async fn shutdown(&self) -> Result<()> {
        let mut state =
            self.state.clone().try_lock_owned().map_err(|_| {
                anyhow::anyhow!("warm tunnel shutdown awaits its original borrower")
            })?;
        if self.busy.load(Ordering::SeqCst) || state.poisoned {
            bail!("warm tunnel cleanup is unconfirmed");
        }
        if let Some(gateway) = state.gateway.as_mut() {
            gateway.cancel().await?;
        }
        state.gateway = None;
        Ok(())
    }
}
struct Borrow {
    state: Option<OwnedMutexGuard<State>>,
    busy: Arc<AtomicBool>,
}
impl Borrow {
    fn gateway(&self) -> Result<&GatewayLaunch> {
        self.state
            .as_ref()
            .and_then(|state| state.gateway.as_ref())
            .ok_or_else(|| anyhow::anyhow!("warm borrower already returned"))
    }
    fn gateway_mut(&mut self) -> Result<&mut GatewayLaunch> {
        self.state
            .as_mut()
            .and_then(|state| state.gateway.as_mut())
            .ok_or_else(|| anyhow::anyhow!("warm borrower already returned"))
    }
    fn release(&mut self) -> Result<()> {
        if self.state.is_none() {
            return Ok(());
        }
        self.busy.store(false, Ordering::SeqCst);
        self.state = None;
        Ok(())
    }
}
impl Drop for Borrow {
    fn drop(&mut self) {
        if let Some(state) = self.state.as_mut() {
            state.poisoned = true;
            // Leave busy latched. An abandoned guard is NOT cleanup evidence.
        }
    }
}
pub(in crate::marketplace) struct Lease {
    borrow: Borrow,
    native: Arc<NativeInvocation>,
    authority: Arc<dyn SdkHttpAuthority>,
}
impl Lease {
    pub(super) async fn materialized(&mut self) -> Result<()> {
        self.borrow.gateway_mut()?.materialized().await?;
        Ok(())
    }
    pub(super) async fn observe(&self) -> Result<ApplicationGatewayObservation> {
        self.authority.current_native(&self.native).await?;
        self.borrow.gateway()?.observe().await
    }
    pub(super) async fn wait_tunnel(&self) -> Result<ApplicationGatewayObservation> {
        self.authority.current_native(&self.native).await?;
        self.borrow.gateway()?.wait_tunnel().await
    }
    pub(super) async fn launch_http(
        &mut self,
        image: &str,
        mode: crate::marketplace::runtime::application_http::HttpMode,
    ) -> Result<HttpLaunch> {
        let native = self.native.clone();
        let authority = self.authority.clone();
        self.borrow
            .gateway_mut()?
            .launch_borrowed_http(image, native, authority, mode)
            .await
    }
    /// Only Stream::cancel after its SAME HTTP child/CLI/readers have joined.
    pub(super) fn release(&mut self) -> Result<()> {
        self.borrow.release()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn an_abandoned_borrow_is_not_cleanup_and_cannot_be_reborrowed() {
        let state = Arc::new(Mutex::new(State::default()));
        let busy = Arc::new(AtomicBool::new(true));
        let guard = Borrow {
            state: Some(state.clone().try_lock_owned().unwrap()),
            busy: busy.clone(),
        };
        assert!(state.clone().try_lock_owned().is_err());
        drop(guard);
        assert!(busy.load(Ordering::SeqCst));
        assert!(state.lock().await.poisoned);
    }
    #[tokio::test]
    async fn only_explicit_joined_return_clears_the_fixed_borrow_slot() -> Result<()> {
        let state = Arc::new(Mutex::new(State::default()));
        let busy = Arc::new(AtomicBool::new(true));
        let mut guard = Borrow {
            state: Some(state.clone().try_lock_owned().unwrap()),
            busy: busy.clone(),
        };
        guard.release()?;
        guard.release()?;
        drop(guard);
        assert!(!busy.load(Ordering::SeqCst));
        assert!(!state.lock().await.poisoned);
        Ok(())
    }
}
