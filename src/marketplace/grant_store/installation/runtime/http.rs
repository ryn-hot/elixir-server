//! Actual installed/native scope retained by each protected HTTP job. No
//! serialized "approved" flag, global credentials, lifecycle effects or retry.
use super::*;
mod local;

/// Original effect owner retained outside the VM, regardless of transport.
pub(super) enum Call {
    Protected(SdkHttpCall),
    Local(crate::marketplace::local_http::Call),
    LocalStream(crate::marketplace::local_http::StreamCall),
    Application {
        stream: crate::marketplace::application_egress::Stream,
        buffered: bool,
        maximum: usize,
    },
}
impl Call {
    fn application_idle(&self) -> bool {
        matches!(self, Self::Application { stream, .. } if stream.idle())
    }
    pub(super) async fn finish(&mut self) -> Result<SourceSdkReply> {
        match self {
            Self::Protected(call) => call.finish().await,
            Self::Local(call) => call.finish().await,
            Self::LocalStream(_) => bail!("stream requires its original read operation"),
            Self::Application {
                stream,
                buffered: true,
                maximum,
            } => stream.buffered(*maximum).await,
            Self::Application { .. } => bail!("stream requires its original read operation"),
        }
    }
    pub(super) async fn cancel(&mut self) -> Result<()> {
        match self {
            Self::Protected(call) => call.cancel().await,
            Self::Local(call) => call.cancel().await,
            Self::LocalStream(call) => call.cancel().await,
            Self::Application { stream, .. } => stream.cancel().await,
        }
    }
}
enum Prepared {
    New(Call),
    Reused,
    Unavailable,
}

struct Authority {
    database: Database,
    binding: Arc<InstalledBinding>,
    native: Arc<NativeInvocation>,
    stop: RunControl,
    expires: Deadline,
    application_admission: Option<Arc<IndependentAdmission>>,
}
impl Authority {
    fn check(&self) -> Result<()> {
        restrictions(&self.stop, self.expires)?;
        self.native.check()
    }
}
#[async_trait::async_trait]
impl SdkHttpAuthority for Authority {
    async fn current_native(&self, native: &NativeInvocation) -> Result<()> {
        self.native.require_same(native)?;
        // current() already freshly inspects this original executor before
        // its SQL grant check. Do not inspect it twice in the same boundary.
        self.current().await?;
        native.check()
    }
    async fn current(&self) -> Result<()> {
        self.check()?;
        if let Some(admission) = &self.application_admission {
            admission.application_server_current().await?;
        }
        self.native.current().await?;
        let allowed = super::super::super::sdk::authorize_installed_http(
            &self.database,
            &self.binding,
            || self.check(),
        )
        .await?;
        if !allowed {
            bail!("installed HTTP is outside the approved scope");
        }
        self.check()
    }
}
impl InstalledBroker<'_> {
    pub(super) async fn request_http(
        &mut self,
        raw: &RuntimeJson,
        maximum: usize,
    ) -> Result<SourceSdkReply> {
        self.check()?;
        if maximum == 0 || maximum > elixir_marketplace::source_execution::MAX_JSON_BYTES {
            bail!("invalid installed HTTP reply allowance");
        }
        let native = self
            .native
            .as_ref()
            .context("HTTP requires its actual native executor")?
            .clone();
        native.current().await?;
        if !super::super::super::sdk::authorize_installed_http(self.database, &self.binding, || {
            self.check()?;
            native.check()
        })
        .await?
        {
            return Ok(SourceSdkReply::Denied);
        }
        use elixir_marketplace::source_execution::SourceSdkRequest;
        let operation = SourceSdkRequest::parse(raw)?;
        if let SourceSdkRequest::FetchRead { stream_id }
        | SourceSdkRequest::FetchClose { stream_id } = &operation
        {
            let closing = matches!(operation, SourceSdkRequest::FetchClose { .. });
            let answer = match self.http_call.as_mut() {
                Some(Call::LocalStream(call)) if call.id == *stream_id => {
                    if closing {
                        call.cancel().await?;
                        SourceSdkReply::Value(RuntimeJson::from_value(serde_json::json!(true))?)
                    } else {
                        call.read(maximum).await?
                    }
                }
                Some(Call::Application {
                    stream,
                    buffered: false,
                    ..
                }) if stream.id() == Some(*stream_id) => {
                    if closing {
                        stream.close().await?
                    } else {
                        stream.read(maximum).await?
                    }
                }
                _ => return Ok(SourceSdkReply::Denied),
            };
            if closing && !self.http_call.as_ref().is_some_and(Call::application_idle) {
                *self.http_call = None;
            }
            self.check()?;
            self.binding.current(self.database).await?;
            self.check()?;
            return Ok(answer);
        }
        if self
            .http_call
            .as_ref()
            .is_some_and(|call| !call.application_idle())
        {
            bail!("the original installed HTTP call is still owned");
        }
        let Some(SourceSdkRequest::Fetch { url, .. }) = operation.http_request() else {
            bail!("installed HTTP operation required")
        };
        let endpoint = super::super::super::sdk::local_http::load(
            self.database,
            self.secrets,
            &self.binding,
            url,
        )
        .await?;
        let streaming = matches!(operation, SourceSdkRequest::FetchOpen { .. });
        let call = if let Some(endpoint) = endpoint {
            // Switching to an approved LAN endpoint retires an idle public
            // gateway first. A selected LAN endpoint still NEVER falls back.
            if let Some(call) = self.http_call.as_mut() {
                call.cancel().await?;
                *self.http_call = None;
            }
            // A selected local endpoint NEVER falls back to the public helper,
            // even when disabled/unavailable or its private-only DNS fails.
            let Some(call) = self.local_call(raw, maximum, endpoint, native.clone(), streaming)?
            else {
                return Ok(SourceSdkReply::Unavailable);
            };
            Some(call)
        } else {
            if self.application_http.is_some() {
                match self
                    .application_call(raw, maximum, native.clone(), streaming)
                    .await?
                {
                    Prepared::New(call) => Some(call),
                    Prepared::Reused => None,
                    Prepared::Unavailable => return Ok(SourceSdkReply::Unavailable),
                }
            } else {
                // The existing fixed public helper only implements buffered v1.
                // Never simulate streaming by retrying, Range requests or host I/O.
                if streaming {
                    return Ok(SourceSdkReply::Unavailable);
                }
                let Some(client) = self.http else {
                    return Ok(SourceSdkReply::Unavailable);
                };
                let authority = Arc::new(Authority {
                    database: self.database.clone(),
                    binding: self.binding.clone(),
                    native: native.clone(),
                    stop: self.stop.clone(),
                    expires: self.expires,
                    application_admission: None,
                });
                // Admission unavailability is not permission to retry a request.
                match client.start(raw, maximum, authority) {
                    Ok(call) => Some(Call::Protected(call)),
                    Err(error) => {
                        tracing::debug!(reason = %error, "installed SDK HTTP admission unavailable");
                        self.check()?;
                        return Ok(SourceSdkReply::Unavailable);
                    }
                }
            }
        };
        if let Some(call) = call {
            *self.http_call = Some(call);
        }
        if let Some(Call::LocalStream(call)) = self.http_call.as_mut() {
            let answer = call.headers().await?;
            self.check()?;
            self.binding.current(self.database).await?;
            self.check()?;
            return Ok(answer);
        }
        if let Some(Call::Application {
            stream,
            buffered: false,
            ..
        }) = self.http_call.as_mut()
        {
            let answer = stream.open().await?;
            self.check()?;
            self.binding.current(self.database).await?;
            self.check()?;
            return Ok(answer);
        }
        let result = self
            .http_call
            .as_mut()
            .expect("retained original HTTP call")
            .finish()
            .await;
        // Keep the SAME call in the outer owner on failure/cancelled waiter.
        // Unknown retirement MUST survive VM cleanup and block controller reuse.
        if result.is_err() || !self.http_call.as_ref().is_some_and(Call::application_idle) {
            self.http_call
                .as_mut()
                .expect("retained original HTTP call")
                .cancel()
                .await?;
            *self.http_call = None;
        }
        self.check()?;
        self.binding.current(self.database).await?;
        self.check()?;
        result
    }

    async fn application_call(
        &mut self,
        raw: &RuntimeJson,
        maximum: usize,
        native: Arc<NativeInvocation>,
        streaming: bool,
    ) -> Result<Prepared> {
        let configured = self
            .application_http
            .context("application HTTP configuration absent")?;
        let Some((network_id, family)) = configured.network() else {
            return Ok(Prepared::Unavailable);
        };
        let admission = self
            .application_admission
            .as_ref()
            .context("application HTTP resource owner absent")?;
        admission.application_server_current().await?;
        // Reject malformed requests BEFORE any native gateway task/marker is
        // created, so construction cannot discard a partially started owner.
        let protocol = crate::marketplace::application_egress::Request::new(
            raw,
            maximum,
            Digest::of(Uuid::new_v4().as_bytes()),
            family,
        )?;
        if let Some(Call::Application {
            stream,
            buffered,
            maximum: allowance,
        }) = self.http_call.as_mut()
        {
            stream.reset(protocol)?;
            *buffered = !streaming;
            *allowance = maximum;
            return Ok(Prepared::Reused);
        }
        let authority = Arc::new(Authority {
            database: self.database.clone(),
            binding: self.binding.clone(),
            native: native.clone(),
            stop: self.stop.clone(),
            expires: self.expires,
            application_admission: Some(admission.clone()),
        });
        if let Some(pool) = configured.pool() {
            let Some(lease) = pool.acquire(native, authority, admission.clone()).await? else {
                return Ok(Prepared::Unavailable);
            };
            return Ok(Prepared::New(Call::Application {
                stream: crate::marketplace::application_egress::Stream::warm(
                    lease,
                    self.application_image.to_owned(),
                    protocol,
                    family,
                    admission.clone(),
                ),
                buffered: !streaming,
                maximum,
            }));
        }
        let Some(gateway) = native
            .launch_application_gateway(
                self.application_deployment,
                network_id,
                self.database.clone(),
                Arc::new(self.secrets.clone()),
                authority,
            )
            .await?
        else {
            return Ok(Prepared::Unavailable);
        };
        // No await/fallible work between receiving the original gateway and
        // returning the owner retained by the installed call outside its VM.
        Ok(Prepared::New(Call::Application {
            stream: crate::marketplace::application_egress::Stream::new(
                gateway,
                self.application_image.to_owned(),
                protocol,
                family,
                admission.clone(),
            ),
            buffered: !streaming,
            maximum,
        }))
    }
}
