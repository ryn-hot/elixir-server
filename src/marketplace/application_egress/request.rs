//! Request/codec state only. No source, VPN, native or HTTP effect authority.
use anyhow::{Result, bail};
use elixir_marketplace::{
    connectivity::ProbeFamily,
    identity::Digest,
    sdk_http::{HttpRequest, stream::StreamProtocol},
    source_execution::{RuntimeJson, SourceSdkRequest},
};

pub(in crate::marketplace) enum Request {
    Buffered { request: HttpRequest, done: bool },
    Streaming(StreamProtocol),
}
impl Request {
    pub(in crate::marketplace) fn new(
        raw: &RuntimeJson,
        maximum: usize,
        nonce: Digest,
        family: ProbeFamily,
    ) -> Result<Self> {
        Ok(match SourceSdkRequest::parse(raw)? {
            SourceSdkRequest::Fetch { .. } => Self::Buffered {
                request: HttpRequest::new(raw, maximum, nonce, family)?,
                done: false,
            },
            SourceSdkRequest::FetchOpen { .. } => {
                Self::Streaming(StreamProtocol::new(raw, maximum, nonce, family)?)
            }
            _ => bail!("application HTTP requires a fresh buffered request or stream open"),
        })
    }
    pub(super) fn id(&self) -> Option<u32> {
        match self {
            Self::Streaming(stream) => Some(stream.id()),
            Self::Buffered { .. } => None,
        }
    }
    pub(super) fn closed(&self) -> bool {
        match self {
            Self::Streaming(stream) => stream.closed(),
            Self::Buffered { done, .. } => *done,
        }
    }
    pub(super) fn stream(&mut self) -> Result<&mut StreamProtocol> {
        match self {
            Self::Streaming(stream) => Ok(stream),
            _ => bail!("streaming HTTP operation required"),
        }
    }
    pub(super) fn buffered(&self) -> Result<&HttpRequest> {
        match self {
            Self::Buffered {
                request,
                done: false,
            } => Ok(request),
            _ => bail!("fresh buffered HTTP operation required"),
        }
    }
    pub(super) fn complete_buffered(&mut self) -> Result<()> {
        match self {
            Self::Buffered { done, .. } if !*done => {
                *done = true;
                Ok(())
            }
            _ => bail!("buffered HTTP already consumed or wrong mode"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn buffered_request_preserves_exact_original_input_and_cannot_be_a_stream() -> Result<()> {
        let raw = RuntimeJson::from_value(
            json!({"operation":"fetch","method":"POST","url":"https://example.com/", "headers":[],"body":"original"}),
        )?;
        let nonce = Digest::of("original HTTP request");
        let mut request = Request::new(&raw, 4096, nonce, ProbeFamily::Ipv4)?;
        assert_eq!(request.id(), None);
        assert!(!request.closed());
        assert!(request.stream().is_err());
        let bytes = request.buffered()?.to_bytes()?;
        let decoded = HttpRequest::parse(&bytes)?;
        assert_eq!(decoded.nonce, nonce);
        assert_eq!(decoded.maximum_reply_bytes, 4096);
        assert_eq!(decoded.request.to_json()?.as_str(), raw.as_str());
        request.complete_buffered()?;
        assert!(request.closed());
        assert!(request.buffered().is_err());
        assert!(request.complete_buffered().is_err());
        Ok(())
    }
    #[test]
    fn streaming_is_still_a_separate_ordered_codec_and_non_open_operations_are_rejected()
    -> Result<()> {
        let fetch = json!({"operation":"fetch","method":"GET","url":"https://example.com/", "headers":[],"body":null});
        let raw = RuntimeJson::from_value(
            json!({"operation":"fetch_open","stream_id":7,"request":fetch,"maximum_body_bytes":65536}),
        )?;
        let mut request = Request::new(&raw, 4096, Digest::of("stream"), ProbeFamily::Ipv4)?;
        assert_eq!(request.id(), Some(7));
        assert!(request.buffered().is_err());
        assert!(request.complete_buffered().is_err());
        assert!(!request.stream()?.open()?.is_empty());
        assert!(request.stream()?.open().is_err());
        for raw in [
            json!({"operation":"fetch_read","stream_id":7}),
            json!({"operation":"fetch_close","stream_id":7}),
            json!({"operation":"state_get","key":"cursor"}),
        ] {
            assert!(
                Request::new(
                    &RuntimeJson::from_value(raw)?,
                    4096,
                    Digest::of("other"),
                    ProbeFamily::Ipv4
                )
                .is_err()
            );
        }
        Ok(())
    }
}
