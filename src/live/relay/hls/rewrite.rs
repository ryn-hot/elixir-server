use std::{collections::HashSet, fmt};

use quick_m3u8::{
    HlsLine, Reader, Writer,
    config::ParsingOptionsBuilder,
    tag::{KnownTag, hls},
};
use reqwest::Url;
use thiserror::Error;

use super::resource::{
    HlsByteRange, HlsManifestScope, HlsResourceId, HlsResourceKind, HlsResourceMap,
};

const MAX_HLS_VERSION: u64 = 100;
const MAX_TARGET_DURATION_SECONDS: u64 = 86_400;
const MAX_SEGMENT_DURATION_SECONDS: f64 = 86_400.0;
const MAX_BYTE_RANGE_LENGTH: u64 = 8 * 1024 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HlsManifestKind {
    Master,
    Media,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HlsRewriteConfig {
    pub max_body_bytes: usize,
    pub max_output_bytes: usize,
    pub max_lines: usize,
    pub max_line_bytes: usize,
    pub max_uri_bytes: usize,
}

impl Default for HlsRewriteConfig {
    fn default() -> Self {
        Self {
            max_body_bytes: 1_048_576,
            max_output_bytes: 2_097_152,
            max_lines: 10_000,
            max_line_bytes: 8_192,
            max_uri_bytes: 4_096,
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct HlsRewriteResult {
    body: Vec<u8>,
    kind: HlsManifestKind,
    resource_count: usize,
    target_duration_seconds: Option<u64>,
    media_sequence: Option<u64>,
    end_list: bool,
}

impl HlsRewriteResult {
    pub fn body(&self) -> &[u8] {
        self.body.as_slice()
    }

    pub const fn kind(&self) -> HlsManifestKind {
        self.kind
    }

    pub const fn resource_count(&self) -> usize {
        self.resource_count
    }

    pub const fn target_duration_seconds(&self) -> Option<u64> {
        self.target_duration_seconds
    }

    pub const fn media_sequence(&self) -> Option<u64> {
        self.media_sequence
    }

    pub const fn end_list(&self) -> bool {
        self.end_list
    }
}

impl fmt::Debug for HlsRewriteResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HlsRewriteResult")
            .field("body", &format_args!("[{} BYTES]", self.body.len()))
            .field("kind", &self.kind)
            .field("resource_count", &self.resource_count)
            .field("target_duration_seconds", &self.target_duration_seconds)
            .field("media_sequence", &self.media_sequence)
            .field("end_list", &self.end_list)
            .finish()
    }
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum HlsRewriteError {
    #[error("the HLS resource map configuration is invalid")]
    InvalidResourceMapConfiguration,
    #[error("the HLS manifest body is empty")]
    EmptyBody,
    #[error("the HLS manifest exceeds its byte limit")]
    BodyLimitExceeded,
    #[error("the HLS manifest is not valid UTF-8")]
    InvalidUtf8,
    #[error("the HLS manifest contains a forbidden byte-order mark")]
    ByteOrderMark,
    #[error("the HLS manifest contains a forbidden control character")]
    ControlCharacter,
    #[error("the HLS manifest contains a bare carriage return")]
    BareCarriageReturn,
    #[error("the HLS manifest exceeds its line-count limit")]
    LineLimitExceeded,
    #[error("an HLS manifest line exceeds its byte limit")]
    LineLengthExceeded,
    #[error("the HLS manifest does not begin with an exact EXTM3U header")]
    MissingHeader,
    #[error("the HLS parser rejected the manifest")]
    ParseFailed,
    #[error("the HLS manifest contains an unknown, invalid, or unsupported tag")]
    UnsupportedTag,
    #[error("the HLS manifest repeats a singleton tag")]
    DuplicateTag,
    #[error("the HLS manifest mixes master and media playlist tags")]
    MixedPlaylistKinds,
    #[error("the HLS manifest has an invalid tag or URI order")]
    InvalidTagOrder,
    #[error("the HLS manifest has no playable resource")]
    MissingResource,
    #[error("the HLS media playlist has no target duration")]
    MissingTargetDuration,
    #[error("the HLS manifest contains an invalid numeric value")]
    InvalidNumericValue,
    #[error("the HLS manifest uses an unsupported encryption mode")]
    UnsupportedEncryption,
    #[error("the HLS manifest contains an invalid resource URI")]
    InvalidResourceUri,
    #[error("the HLS relay route base is invalid")]
    InvalidRouteBase,
    #[error("the HLS manifest exceeds its resource limit")]
    ResourceLimitExceeded,
    #[error("the HLS resource identifier could not be generated")]
    ResourceIdGenerationFailed,
    #[error("the HLS resource revision counter is exhausted")]
    ResourceRevisionExhausted,
    #[error("the HLS manifest resource scope is invalid")]
    InvalidManifestScope,
    #[error("the HLS resource map is internally inconsistent")]
    ResourceMapInvariant,
    #[error("the HLS resource does not exist")]
    UnknownResource,
    #[error("the HLS operation used a stale control fencing token")]
    StaleControlFence,
    #[error("the rewritten HLS manifest exceeds its output limit")]
    OutputLimitExceeded,
    #[error("the rewritten HLS manifest failed structural verification")]
    OutputVerificationFailed,
}

#[derive(Debug, Clone)]
pub struct HlsRewriter {
    config: HlsRewriteConfig,
}

impl HlsRewriter {
    pub fn new(config: HlsRewriteConfig) -> Result<Self, HlsRewriteError> {
        if config.max_body_bytes == 0
            || config.max_output_bytes == 0
            || config.max_lines == 0
            || config.max_line_bytes == 0
            || config.max_uri_bytes == 0
        {
            return Err(HlsRewriteError::InvalidResourceMapConfiguration);
        }
        Ok(Self { config })
    }

    pub fn rewrite(
        &self,
        resources: &mut HlsResourceMap,
        control_fencing_token: i64,
        parent_url: &Url,
        route_base: &str,
        body: &[u8],
    ) -> Result<HlsRewriteResult, HlsRewriteError> {
        let scope = HlsManifestScope::from_stable_key(parent_url.as_str().as_bytes())?;
        self.rewrite_scoped(
            resources,
            control_fencing_token,
            scope,
            parent_url,
            route_base,
            body,
        )
    }

    pub fn rewrite_scoped(
        &self,
        resources: &mut HlsResourceMap,
        control_fencing_token: i64,
        scope: HlsManifestScope,
        parent_url: &Url,
        route_base: &str,
        body: &[u8],
    ) -> Result<HlsRewriteResult, HlsRewriteError> {
        self.rewrite_scoped_with_validator(
            resources,
            control_fencing_token,
            scope,
            parent_url,
            route_base,
            body,
            |_| Ok(()),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn rewrite_scoped_with_validator<F>(
        &self,
        resources: &mut HlsResourceMap,
        control_fencing_token: i64,
        scope: HlsManifestScope,
        parent_url: &Url,
        route_base: &str,
        body: &[u8],
        mut validator: F,
    ) -> Result<HlsRewriteResult, HlsRewriteError>
    where
        F: FnMut(&super::HlsResourceDescriptor) -> Result<(), HlsRewriteError>,
    {
        let text = self.preflight(body)?;
        self.validate_parent_url(parent_url)?;
        validate_route_base(route_base)?;

        let skipped_segments = skipped_segment_count(text)?;
        let mut staged = resources.clone();
        staged.begin_revision(scope, control_fencing_token)?;
        let mut state = RewriteState {
            skipped_segments,
            ..RewriteState::default()
        };
        let options = ParsingOptionsBuilder::new()
            .with_parsing_for_all_tags()
            .build();
        let mut reader = Reader::from_str(text, options);
        let mut writer = Writer::new(Vec::new());

        while let Some(line) = reader
            .read_line()
            .map_err(|_| HlsRewriteError::ParseFailed)?
        {
            state.line_count += 1;
            match line {
                HlsLine::KnownTag(KnownTag::Hls(tag)) => {
                    self.rewrite_tag(
                        tag,
                        parent_url,
                        route_base,
                        &mut staged,
                        &mut state,
                        &mut writer,
                    )?;
                }
                HlsLine::KnownTag(_) | HlsLine::UnknownTag(_) => {
                    return Err(HlsRewriteError::UnsupportedTag);
                }
                HlsLine::Uri(uri) => {
                    self.rewrite_uri_line(
                        uri.as_ref(),
                        parent_url,
                        route_base,
                        &mut staged,
                        &mut state,
                        &mut writer,
                    )?;
                }
                HlsLine::Comment(_) => {
                    if state.pending_uri.is_some() {
                        return Err(HlsRewriteError::InvalidTagOrder);
                    }
                    // Comments are intentionally omitted because providers may place secrets in them.
                }
                HlsLine::Blank => {
                    if state.pending_uri.is_some() {
                        return Err(HlsRewriteError::InvalidTagOrder);
                    }
                    write_line(&mut writer, HlsLine::Blank)?;
                }
            }
        }

        let kind = state.finish()?;
        let output = writer.into_inner();
        if output.len() > self.config.max_output_bytes {
            return Err(HlsRewriteError::OutputLimitExceeded);
        }
        verify_output(&output, route_base, self.config.max_uri_bytes)?;
        staged.validate_active_resources(&mut validator)?;
        staged.finish_revision()?;
        *resources = staged;
        Ok(HlsRewriteResult {
            body: output,
            kind,
            resource_count: state.touched_resources.len(),
            target_duration_seconds: state.target_duration,
            media_sequence: state.media_sequence,
            end_list: state.end_list,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn rewrite_tag<'a>(
        &self,
        tag: hls::Tag<'a>,
        parent_url: &Url,
        route_base: &str,
        resources: &mut HlsResourceMap,
        state: &mut RewriteState,
        writer: &mut Writer<Vec<u8>>,
    ) -> Result<(), HlsRewriteError> {
        if state.end_list {
            return Err(HlsRewriteError::InvalidTagOrder);
        }
        match tag {
            hls::Tag::M3u(tag) => {
                mark_singleton(&mut state.header)?;
                if state.line_count != 1 {
                    return Err(HlsRewriteError::MissingHeader);
                }
                write_line(writer, HlsLine::from(tag))
            }
            hls::Tag::Version(tag) => {
                mark_singleton(&mut state.version)?;
                if tag.version() == 0 || tag.version() > MAX_HLS_VERSION {
                    return Err(HlsRewriteError::InvalidNumericValue);
                }
                state.require_no_pending()?;
                write_line(writer, HlsLine::from(tag))
            }
            hls::Tag::IndependentSegments(tag) => {
                mark_singleton(&mut state.independent_segments)?;
                state.require_no_pending()?;
                write_line(writer, HlsLine::from(tag))
            }
            hls::Tag::Start(tag) => {
                mark_singleton(&mut state.start)?;
                if !tag.time_offset().is_finite()
                    || tag.time_offset().abs() > MAX_SEGMENT_DURATION_SECONDS
                {
                    return Err(HlsRewriteError::InvalidNumericValue);
                }
                state.require_no_pending()?;
                write_line(writer, HlsLine::from(tag))
            }
            hls::Tag::Define(_) => Err(HlsRewriteError::UnsupportedTag),
            hls::Tag::Targetduration(tag) => {
                state.mark_media()?;
                mark_singleton(&mut state.has_target_duration)?;
                let target = tag.target_duration();
                if target == 0 || target > MAX_TARGET_DURATION_SECONDS {
                    return Err(HlsRewriteError::InvalidNumericValue);
                }
                state.target_duration = Some(target);
                state.require_no_pending()?;
                write_line(writer, HlsLine::from(tag))
            }
            hls::Tag::MediaSequence(mut tag) => {
                state.mark_media()?;
                mark_singleton(&mut state.has_media_sequence)?;
                let mut media_sequence = tag.media_sequence();
                if let Some(skipped_segments) = state.skipped_segments {
                    media_sequence = media_sequence
                        .checked_add(skipped_segments)
                        .ok_or(HlsRewriteError::InvalidNumericValue)?;
                    tag.set_media_sequence(media_sequence);
                }
                state.media_sequence = Some(media_sequence);
                state.require_no_pending()?;
                write_line(writer, HlsLine::from(tag))
            }
            hls::Tag::DiscontinuitySequence(tag) => {
                state.mark_media()?;
                mark_singleton(&mut state.discontinuity_sequence)?;
                state.require_no_pending()?;
                write_line(writer, HlsLine::from(tag))
            }
            hls::Tag::Endlist(tag) => {
                state.mark_media()?;
                mark_singleton(&mut state.end_list)?;
                state.require_no_pending()?;
                write_line(writer, HlsLine::from(tag))
            }
            hls::Tag::PlaylistType(tag) => {
                state.mark_media()?;
                mark_singleton(&mut state.playlist_type)?;
                state.require_no_pending()?;
                write_line(writer, HlsLine::from(tag))
            }
            hls::Tag::IFramesOnly(tag) => {
                state.mark_media()?;
                mark_singleton(&mut state.i_frames_only)?;
                state.require_no_pending()?;
                write_line(writer, HlsLine::from(tag))
            }
            hls::Tag::PartInf(tag) => {
                state.mark_media()?;
                state.require_no_pending()?;
                mark_singleton(&mut state.part_inf)?;
                if !valid_ignored_duration(tag.part_target()) {
                    return Err(HlsRewriteError::InvalidNumericValue);
                }
                Ok(())
            }
            hls::Tag::ServerControl(tag) => {
                state.mark_media()?;
                state.require_no_pending()?;
                mark_singleton(&mut state.server_control)?;
                if [tag.can_skip_until(), tag.hold_back(), tag.part_hold_back()]
                    .into_iter()
                    .flatten()
                    .any(|duration| !valid_ignored_duration(duration))
                {
                    return Err(HlsRewriteError::InvalidNumericValue);
                }
                Ok(())
            }
            hls::Tag::Part(tag) => {
                state.mark_media()?;
                state.require_no_pending()?;
                if !valid_ignored_duration(tag.duration()) {
                    return Err(HlsRewriteError::InvalidNumericValue);
                }
                resolve_resource_uri(parent_url, tag.uri(), self.config.max_uri_bytes)?;
                if tag.byterange().is_some_and(|range| {
                    range.length == 0
                        || range.length > MAX_BYTE_RANGE_LENGTH
                        || range
                            .offset
                            .is_some_and(|offset| offset.checked_add(range.length).is_none())
                }) {
                    return Err(HlsRewriteError::InvalidNumericValue);
                }
                Ok(())
            }
            hls::Tag::Skip(tag) => {
                state.mark_media()?;
                state.require_no_pending()?;
                if !state.has_media_sequence
                    || !state.segment_durations.is_empty()
                    || state.skipped_segments != Some(tag.skipped_segments())
                {
                    return Err(HlsRewriteError::InvalidTagOrder);
                }
                Ok(())
            }
            hls::Tag::PreloadHint(tag) => {
                state.mark_media()?;
                state.require_no_pending()?;
                if tag.hint_type().known().is_none() {
                    return Err(HlsRewriteError::UnsupportedTag);
                }
                resolve_resource_uri(parent_url, tag.uri(), self.config.max_uri_bytes)?;
                if tag.byterange_length().is_some_and(|length| {
                    length == 0
                        || length > MAX_BYTE_RANGE_LENGTH
                        || tag.byterange_start().checked_add(length).is_none()
                }) {
                    return Err(HlsRewriteError::InvalidNumericValue);
                }
                Ok(())
            }
            hls::Tag::RenditionReport(tag) => {
                state.mark_media()?;
                state.require_no_pending()?;
                resolve_resource_uri(parent_url, tag.uri(), self.config.max_uri_bytes)?;
                Ok(())
            }
            hls::Tag::Inf(tag) => {
                state.mark_media()?;
                state.require_no_pending()?;
                let duration = tag.duration();
                if !duration.is_finite()
                    || duration <= 0.0
                    || duration > MAX_SEGMENT_DURATION_SECONDS
                {
                    return Err(HlsRewriteError::InvalidNumericValue);
                }
                state.segment_durations.push(duration);
                state.pending_uri = Some(PendingUri::MediaSegment);
                write_line(writer, HlsLine::from(tag))
            }
            hls::Tag::Byterange(tag) => {
                state.mark_media()?;
                if state.pending_uri != Some(PendingUri::MediaSegment)
                    || state.pending_byte_range.is_some()
                    || tag.length() == 0
                    || tag.length() > MAX_BYTE_RANGE_LENGTH
                    || tag
                        .offset()
                        .is_some_and(|offset| offset.checked_add(tag.length()).is_none())
                {
                    return Err(HlsRewriteError::InvalidTagOrder);
                }
                state.pending_byte_range = Some(HlsByteRange {
                    length: tag.length(),
                    offset: tag.offset(),
                });
                write_line(writer, HlsLine::from(tag))
            }
            hls::Tag::Discontinuity(tag) => {
                state.mark_media()?;
                state.require_no_pending()?;
                write_line(writer, HlsLine::from(tag))
            }
            hls::Tag::Key(mut tag) => {
                state.mark_media()?;
                state.require_no_pending()?;
                match tag.method().known().copied() {
                    Some(hls::Method::None) => {
                        if tag.uri().is_some()
                            || tag.iv().is_some()
                            || tag.keyformat() != "identity"
                            || tag.keyformatversions().is_some()
                        {
                            return Err(HlsRewriteError::UnsupportedEncryption);
                        }
                    }
                    Some(hls::Method::Aes128) => {
                        validate_identity_key(&tag)?;
                        let uri = tag.uri().ok_or(HlsRewriteError::UnsupportedEncryption)?;
                        let route = self.register_uri(
                            resources,
                            parent_url,
                            route_base,
                            uri,
                            HlsResourceKind::EncryptionKey,
                            None,
                            state,
                        )?;
                        tag.set_uri(route);
                    }
                    Some(hls::Method::SampleAes | hls::Method::SampleAesCtr) | None => {
                        return Err(HlsRewriteError::UnsupportedEncryption);
                    }
                }
                write_line(writer, HlsLine::from(tag))
            }
            hls::Tag::Map(mut tag) => {
                state.mark_media()?;
                state.require_no_pending()?;
                let byte_range = tag.byterange().map(|range| HlsByteRange {
                    length: range.length,
                    offset: Some(range.offset),
                });
                if byte_range.is_some_and(|range| {
                    range.length == 0
                        || range.length > MAX_BYTE_RANGE_LENGTH
                        || range
                            .offset
                            .and_then(|offset| offset.checked_add(range.length))
                            .is_none()
                }) {
                    return Err(HlsRewriteError::InvalidNumericValue);
                }
                let route = self.register_uri(
                    resources,
                    parent_url,
                    route_base,
                    tag.uri(),
                    HlsResourceKind::InitializationSegment,
                    byte_range,
                    state,
                )?;
                tag.set_uri(route);
                write_line(writer, HlsLine::from(tag))
            }
            hls::Tag::ProgramDateTime(tag) => {
                state.mark_media()?;
                state.require_no_pending()?;
                write_line(writer, HlsLine::from(tag))
            }
            hls::Tag::Gap(tag) => {
                state.mark_media()?;
                if state.pending_uri != Some(PendingUri::MediaSegment) {
                    return Err(HlsRewriteError::InvalidTagOrder);
                }
                write_line(writer, HlsLine::from(tag))
            }
            hls::Tag::Bitrate(tag) => {
                state.mark_media()?;
                if tag.bitrate() == 0 || state.pending_uri != Some(PendingUri::MediaSegment) {
                    return Err(HlsRewriteError::InvalidTagOrder);
                }
                write_line(writer, HlsLine::from(tag))
            }
            // DATERANGE permits extension attributes with URI semantics. It stays off until those
            // attributes have an explicit policy rather than being blindly round-tripped.
            hls::Tag::Daterange(_) => Err(HlsRewriteError::UnsupportedTag),
            hls::Tag::Media(mut tag) => {
                state.mark_master()?;
                state.require_no_pending()?;
                let media_type = tag
                    .media_type()
                    .known()
                    .copied()
                    .ok_or(HlsRewriteError::UnsupportedTag)?;
                match (media_type, tag.uri()) {
                    (hls::MediaType::ClosedCaptions, None) => {}
                    (hls::MediaType::ClosedCaptions, Some(_)) => {
                        return Err(HlsRewriteError::UnsupportedTag);
                    }
                    (_, Some(uri)) => {
                        let route = self.register_uri(
                            resources,
                            parent_url,
                            route_base,
                            uri,
                            HlsResourceKind::Playlist,
                            None,
                            state,
                        )?;
                        tag.set_uri(route);
                    }
                    (_, None) => {}
                }
                write_line(writer, HlsLine::from(tag))
            }
            hls::Tag::StreamInf(tag) => {
                state.mark_master()?;
                state.require_no_pending()?;
                state.pending_uri = Some(PendingUri::Playlist);
                write_line(writer, HlsLine::from(tag))
            }
            hls::Tag::IFrameStreamInf(mut tag) => {
                state.mark_master()?;
                state.require_no_pending()?;
                let route = self.register_uri(
                    resources,
                    parent_url,
                    route_base,
                    tag.uri(),
                    HlsResourceKind::Playlist,
                    None,
                    state,
                )?;
                tag.set_uri(route);
                write_line(writer, HlsLine::from(tag))
            }
            hls::Tag::SessionData(_) | hls::Tag::ContentSteering(_) => {
                Err(HlsRewriteError::UnsupportedTag)
            }
            hls::Tag::SessionKey(mut tag) => {
                state.mark_master()?;
                state.require_no_pending()?;
                if tag.method().known().copied() != Some(hls::Method::Aes128)
                    || tag.keyformat() != "identity"
                    || !valid_keyformat_versions(tag.keyformatversions())
                    || !valid_iv(tag.iv())
                {
                    return Err(HlsRewriteError::UnsupportedEncryption);
                }
                let route = self.register_uri(
                    resources,
                    parent_url,
                    route_base,
                    tag.uri(),
                    HlsResourceKind::EncryptionKey,
                    None,
                    state,
                )?;
                tag.set_uri(route);
                write_line(writer, HlsLine::from(tag))
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn rewrite_uri_line(
        &self,
        uri: &str,
        parent_url: &Url,
        route_base: &str,
        resources: &mut HlsResourceMap,
        state: &mut RewriteState,
        writer: &mut Writer<Vec<u8>>,
    ) -> Result<(), HlsRewriteError> {
        if state.end_list {
            return Err(HlsRewriteError::InvalidTagOrder);
        }
        let pending = state
            .pending_uri
            .take()
            .ok_or(HlsRewriteError::InvalidTagOrder)?;
        let (kind, byte_range) = match pending {
            PendingUri::Playlist => (HlsResourceKind::Playlist, None),
            PendingUri::MediaSegment => (
                HlsResourceKind::MediaSegment,
                state.pending_byte_range.take(),
            ),
        };
        let route = self.register_uri(
            resources, parent_url, route_base, uri, kind, byte_range, state,
        )?;
        write_line(writer, HlsLine::uri(route))
    }

    #[allow(clippy::too_many_arguments)]
    fn register_uri(
        &self,
        resources: &mut HlsResourceMap,
        parent_url: &Url,
        route_base: &str,
        raw_uri: &str,
        kind: HlsResourceKind,
        mut byte_range: Option<HlsByteRange>,
        state: &mut RewriteState,
    ) -> Result<String, HlsRewriteError> {
        let url = resolve_resource_uri(parent_url, raw_uri, self.config.max_uri_bytes)?;
        if kind == HlsResourceKind::MediaSegment {
            if let Some(range) = &mut byte_range {
                if range.offset.is_none() {
                    let (previous_url, previous_end) = state
                        .last_segment_range
                        .as_ref()
                        .ok_or(HlsRewriteError::InvalidTagOrder)?;
                    if previous_url != &url {
                        return Err(HlsRewriteError::InvalidTagOrder);
                    }
                    range.offset = Some(*previous_end);
                }
                let end = range
                    .offset
                    .and_then(|offset| offset.checked_add(range.length))
                    .ok_or(HlsRewriteError::InvalidNumericValue)?;
                state.last_segment_range = Some((url.clone(), end));
            } else {
                state.last_segment_range = None;
            }
        }
        let resource_id = resources.register(url, kind, byte_range)?;
        state.touched_resources.insert(resource_id.clone());
        Ok(resource_route(route_base, &resource_id))
    }

    fn preflight<'a>(&self, body: &'a [u8]) -> Result<&'a str, HlsRewriteError> {
        if body.is_empty() {
            return Err(HlsRewriteError::EmptyBody);
        }
        if body.len() > self.config.max_body_bytes {
            return Err(HlsRewriteError::BodyLimitExceeded);
        }
        if body.starts_with(&[0xef, 0xbb, 0xbf]) {
            return Err(HlsRewriteError::ByteOrderMark);
        }
        let text = std::str::from_utf8(body).map_err(|_| HlsRewriteError::InvalidUtf8)?;
        let bytes = text.as_bytes();
        for (index, byte) in bytes.iter().copied().enumerate() {
            match byte {
                b'\n' => {}
                b'\r' if bytes.get(index + 1) == Some(&b'\n') => {}
                b'\r' => return Err(HlsRewriteError::BareCarriageReturn),
                0x00..=0x1f | 0x7f => return Err(HlsRewriteError::ControlCharacter),
                _ => {}
            }
        }
        if text
            .chars()
            .any(|character| character.is_control() && !matches!(character, '\r' | '\n'))
        {
            return Err(HlsRewriteError::ControlCharacter);
        }
        let mut lines = 0_usize;
        for line in text.split_terminator('\n') {
            lines += 1;
            if lines > self.config.max_lines {
                return Err(HlsRewriteError::LineLimitExceeded);
            }
            let line = line.strip_suffix('\r').unwrap_or(line);
            if line.len() > self.config.max_line_bytes {
                return Err(HlsRewriteError::LineLengthExceeded);
            }
        }
        if text.ends_with('\n') && lines == 0 {
            lines = 1;
        }
        if lines == 0 || text.lines().next() != Some("#EXTM3U") {
            return Err(HlsRewriteError::MissingHeader);
        }
        Ok(text)
    }

    fn validate_parent_url(&self, parent_url: &Url) -> Result<(), HlsRewriteError> {
        validate_url(parent_url, self.config.max_uri_bytes)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingUri {
    Playlist,
    MediaSegment,
}

#[derive(Default)]
struct RewriteState {
    line_count: usize,
    header: bool,
    version: bool,
    independent_segments: bool,
    start: bool,
    has_target_duration: bool,
    has_media_sequence: bool,
    discontinuity_sequence: bool,
    playlist_type: bool,
    i_frames_only: bool,
    part_inf: bool,
    server_control: bool,
    end_list: bool,
    saw_master: bool,
    saw_media: bool,
    pending_uri: Option<PendingUri>,
    pending_byte_range: Option<HlsByteRange>,
    target_duration: Option<u64>,
    media_sequence: Option<u64>,
    skipped_segments: Option<u64>,
    segment_durations: Vec<f64>,
    touched_resources: HashSet<HlsResourceId>,
    last_segment_range: Option<(Url, u64)>,
}

impl RewriteState {
    fn mark_master(&mut self) -> Result<(), HlsRewriteError> {
        if self.saw_media {
            return Err(HlsRewriteError::MixedPlaylistKinds);
        }
        self.saw_master = true;
        Ok(())
    }

    fn mark_media(&mut self) -> Result<(), HlsRewriteError> {
        if self.saw_master {
            return Err(HlsRewriteError::MixedPlaylistKinds);
        }
        self.saw_media = true;
        Ok(())
    }

    fn require_no_pending(&self) -> Result<(), HlsRewriteError> {
        if self.pending_uri.is_some() || self.pending_byte_range.is_some() {
            return Err(HlsRewriteError::InvalidTagOrder);
        }
        Ok(())
    }

    fn finish(&self) -> Result<HlsManifestKind, HlsRewriteError> {
        if !self.header {
            return Err(HlsRewriteError::MissingHeader);
        }
        self.require_no_pending()?;
        if self.touched_resources.is_empty() {
            return Err(HlsRewriteError::MissingResource);
        }
        if self.saw_master == self.saw_media {
            return Err(HlsRewriteError::MixedPlaylistKinds);
        }
        if self.saw_media {
            let target = self
                .target_duration
                .ok_or(HlsRewriteError::MissingTargetDuration)?;
            if self
                .segment_durations
                .iter()
                .any(|duration| duration.round() > target as f64)
            {
                return Err(HlsRewriteError::InvalidNumericValue);
            }
            Ok(HlsManifestKind::Media)
        } else {
            Ok(HlsManifestKind::Master)
        }
    }
}

fn mark_singleton(seen: &mut bool) -> Result<(), HlsRewriteError> {
    if *seen {
        return Err(HlsRewriteError::DuplicateTag);
    }
    *seen = true;
    Ok(())
}

fn skipped_segment_count(text: &str) -> Result<Option<u64>, HlsRewriteError> {
    let options = ParsingOptionsBuilder::new()
        .with_parsing_for_all_tags()
        .build();
    let mut reader = Reader::from_str(text, options);
    let mut skipped_segments = None;
    while let Some(line) = reader
        .read_line()
        .map_err(|_| HlsRewriteError::ParseFailed)?
    {
        if let HlsLine::KnownTag(KnownTag::Hls(hls::Tag::Skip(tag))) = line {
            if tag.skipped_segments() == 0
                || skipped_segments.replace(tag.skipped_segments()).is_some()
            {
                return Err(HlsRewriteError::InvalidNumericValue);
            }
        }
    }
    Ok(skipped_segments)
}

fn valid_ignored_duration(duration: f64) -> bool {
    duration.is_finite() && duration > 0.0 && duration <= MAX_SEGMENT_DURATION_SECONDS
}

fn write_line<'a>(writer: &mut Writer<Vec<u8>>, line: HlsLine<'a>) -> Result<(), HlsRewriteError> {
    writer
        .write_line(line)
        .map(|_| ())
        .map_err(|_| HlsRewriteError::OutputVerificationFailed)
}

fn validate_identity_key(tag: &hls::Key<'_>) -> Result<(), HlsRewriteError> {
    if tag.keyformat() != "identity"
        || !valid_keyformat_versions(tag.keyformatversions())
        || !valid_iv(tag.iv())
    {
        return Err(HlsRewriteError::UnsupportedEncryption);
    }
    Ok(())
}

fn valid_keyformat_versions(value: Option<&str>) -> bool {
    value.is_none_or(|value| value == "1")
}

fn valid_iv(value: Option<&str>) -> bool {
    value.is_none_or(|value| {
        value.len() == 34
            && value.starts_with("0x")
            && value[2..].bytes().all(|byte| byte.is_ascii_hexdigit())
    })
}

fn validate_route_base(route_base: &str) -> Result<(), HlsRewriteError> {
    if !route_base.starts_with('/')
        || route_base.ends_with('/')
        || route_base.contains("//")
        || route_base.contains("..")
        || route_base.contains(['?', '#', '%', '\\'])
        || route_base.chars().any(char::is_whitespace)
        || route_base.chars().any(char::is_control)
    {
        return Err(HlsRewriteError::InvalidRouteBase);
    }
    Ok(())
}

fn resource_route(route_base: &str, resource_id: &HlsResourceId) -> String {
    format!("{route_base}/resources/{resource_id}")
}

fn resolve_resource_uri(
    parent_url: &Url,
    raw_uri: &str,
    max_uri_bytes: usize,
) -> Result<Url, HlsRewriteError> {
    if raw_uri.is_empty()
        || raw_uri.len() > max_uri_bytes
        || raw_uri.trim() != raw_uri
        || raw_uri.contains('\\')
        || raw_uri.chars().any(char::is_whitespace)
        || raw_uri.chars().any(char::is_control)
        || contains_percent_encoded_control(raw_uri)
    {
        return Err(HlsRewriteError::InvalidResourceUri);
    }
    let url = parent_url
        .join(raw_uri)
        .map_err(|_| HlsRewriteError::InvalidResourceUri)?;
    validate_url(&url, max_uri_bytes)?;
    Ok(url)
}

fn validate_url(url: &Url, max_uri_bytes: usize) -> Result<(), HlsRewriteError> {
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || url.as_str().len() > max_uri_bytes
        || contains_percent_encoded_control(url.as_str())
    {
        return Err(HlsRewriteError::InvalidResourceUri);
    }
    Ok(())
}

fn contains_percent_encoded_control(value: &str) -> bool {
    let bytes = value.as_bytes();
    let mut index = 0;
    while index + 2 < bytes.len() {
        if bytes[index] == b'%'
            && let (Some(high), Some(low)) =
                (hex_value(bytes[index + 1]), hex_value(bytes[index + 2]))
        {
            let decoded = (high << 4) | low;
            if decoded <= 0x1f || decoded == 0x7f {
                return true;
            }
            index += 3;
        } else {
            index += 1;
        }
    }
    false
}

const fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn verify_output(
    output: &[u8],
    route_base: &str,
    max_uri_bytes: usize,
) -> Result<(), HlsRewriteError> {
    let text =
        std::str::from_utf8(output).map_err(|_| HlsRewriteError::OutputVerificationFailed)?;
    let route_prefix = format!("{route_base}/resources/{RESOURCE_ID_SENTINEL}");
    let route_prefix = route_prefix.trim_end_matches(RESOURCE_ID_SENTINEL);
    let options = ParsingOptionsBuilder::new()
        .with_parsing_for_all_tags()
        .build();
    let mut reader = Reader::from_str(text, options);
    let mut line_number = 0_usize;
    while let Some(line) = reader
        .read_line()
        .map_err(|_| HlsRewriteError::OutputVerificationFailed)?
    {
        line_number += 1;
        match line {
            HlsLine::KnownTag(KnownTag::Hls(tag)) => {
                let uri = match &tag {
                    hls::Tag::Media(tag) => tag.uri(),
                    hls::Tag::IFrameStreamInf(tag) => Some(tag.uri()),
                    hls::Tag::Key(tag) => tag.uri(),
                    hls::Tag::Map(tag) => Some(tag.uri()),
                    hls::Tag::SessionKey(tag) => Some(tag.uri()),
                    hls::Tag::Part(tag) => Some(tag.uri()),
                    hls::Tag::PreloadHint(tag) => Some(tag.uri()),
                    hls::Tag::RenditionReport(tag) => Some(tag.uri()),
                    hls::Tag::SessionData(tag) => tag.uri(),
                    hls::Tag::ContentSteering(tag) => Some(tag.server_uri()),
                    _ => None,
                };
                if let Some(uri) = uri {
                    verify_route_uri(uri, route_prefix, max_uri_bytes)?;
                }
            }
            HlsLine::Uri(uri) => verify_route_uri(uri.as_ref(), route_prefix, max_uri_bytes)?,
            HlsLine::UnknownTag(_) | HlsLine::KnownTag(_) | HlsLine::Comment(_) => {
                return Err(HlsRewriteError::OutputVerificationFailed);
            }
            HlsLine::Blank => {}
        }
    }
    if line_number == 0 {
        return Err(HlsRewriteError::OutputVerificationFailed);
    }
    Ok(())
}

const RESOURCE_ID_SENTINEL: &str = "__RESOURCE_ID__";

fn verify_route_uri(
    uri: &str,
    route_prefix: &str,
    max_uri_bytes: usize,
) -> Result<(), HlsRewriteError> {
    let id = uri
        .strip_prefix(route_prefix)
        .and_then(HlsResourceId::parse)
        .ok_or(HlsRewriteError::OutputVerificationFailed)?;
    if uri.len() > max_uri_bytes || uri != format!("{route_prefix}{id}") {
        return Err(HlsRewriteError::OutputVerificationFailed);
    }
    Ok(())
}
