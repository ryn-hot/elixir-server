//! Tunnel lifetime is distinct from permission for any installed HTTP effect.
use super::*;
use crate::marketplace::{
    grant_store::installation::runtime::RuntimeOwner, worker::IndependentAdmission,
};

pub(super) enum Owner {
    Invocation {
        native: Arc<NativeInvocation>,
        authority: Arc<dyn SdkHttpAuthority>,
    },
    Warm {
        owner: Arc<RuntimeOwner>,
        lease: Arc<ControllerLease>,
        database: crate::db::Database,
        runtime: Arc<dyn RuntimeManager>,
        expires: Instant,
        admission: Arc<IndependentAdmission>,
        busy: Arc<AtomicBool>,
    },
}
impl Owner {
    pub(super) fn check(&self) -> Result<()> {
        match self {
            Self::Invocation { native, .. } => native.check(),
            Self::Warm { owner, expires, .. } => {
                owner.check()?;
                if Instant::now() >= *expires {
                    bail!("warm application tunnel lifetime expired");
                }
                Ok(())
            }
        }
    }
    pub(super) fn runtime(&self) -> &Arc<dyn RuntimeManager> {
        match self {
            Self::Invocation { native, .. } => &native.runtime,
            Self::Warm { runtime, .. } => runtime,
        }
    }
    pub(super) fn lease(&self) -> &ControllerLease {
        match self {
            Self::Invocation { native, .. } => &native.lease,
            Self::Warm { lease, .. } => lease,
        }
    }
    pub(super) fn expires(&self) -> Instant {
        match self {
            Self::Invocation { native, .. } => native.expires,
            Self::Warm { expires, .. } => *expires,
        }
    }
    pub(super) fn native(&self) -> Result<&Arc<NativeInvocation>> {
        match self {
            Self::Invocation { native, .. } => Ok(native),
            Self::Warm { .. } => bail!("warm tunnel supplies no installed invocation authority"),
        }
    }
    pub(super) async fn current(&self) -> Result<()> {
        self.check()?;
        match self {
            Self::Invocation { native, authority } => authority.current_native(native).await?,
            Self::Warm {
                owner,
                database,
                admission,
                ..
            } => {
                owner.current(database).await?;
                admission.application_server_current().await?;
            }
        }
        self.check()
    }
    pub(super) fn can_remove(&self) -> bool {
        match self {
            Self::Invocation { .. } => true,
            Self::Warm { busy, .. } => !busy.load(Ordering::SeqCst),
        }
    }
    pub(super) fn interval(&self) -> Duration {
        if matches!(self,Self::Warm{busy,..} if !busy.load(Ordering::SeqCst)) {
            Duration::from_secs(5)
        } else {
            CHECK_INTERVAL
        }
    }
    pub(super) async fn observed(
        &self,
        actual: &crate::marketplace::application_egress::ApplicationGatewayObservation,
    ) -> Result<()> {
        if let Self::Warm { admission, .. } = self {
            admission.application_current(actual).await?;
        }
        Ok(())
    }
}
