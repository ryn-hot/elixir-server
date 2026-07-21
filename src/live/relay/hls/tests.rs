use quick_m3u8::{
    HlsLine, Reader,
    config::ParsingOptionsBuilder,
    tag::{KnownTag, hls},
};
use reqwest::Url;
use uuid::Uuid;

use super::*;

const ROUTE_BASE: &str = "/api/v1/live/sessions/11111111-1111-4111-8111-111111111111/delivery/hls";
const MASTER: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../fixtures/live/origin-suite/corpus/hls/valid/master.m3u8"
));
const DVR: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../fixtures/live/origin-suite/corpus/hls/valid/dvr.m3u8"
));
const LOW_LATENCY: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../fixtures/live/origin-suite/corpus/hls/valid/low-latency.m3u8"
));
const FROZEN: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../fixtures/live/origin-suite/corpus/hls/adversarial/frozen.m3u8"
));
const MALFORMED: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../fixtures/live/origin-suite/corpus/hls/malicious/malformed.m3u8"
));
const SAMPLE_AES: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../fixtures/live/origin-suite/corpus/hls/malicious/sample-aes.m3u8"
));

fn rewriter() -> HlsRewriter {
    HlsRewriter::new(HlsRewriteConfig::default()).expect("valid test rewrite config")
}

fn resource_map(limits: HlsResourceLimits) -> HlsResourceMap {
    HlsResourceMap::new(
        Uuid::parse_str("11111111-1111-4111-8111-111111111111").expect("valid UUID"),
        7,
        limits,
    )
    .expect("valid test resource map")
}

fn parent(path: &str) -> Url {
    Url::parse(&format!("https://origin.example.test{path}")).expect("valid fixture parent URL")
}

fn route_uris(body: &[u8]) -> Vec<String> {
    let text = std::str::from_utf8(body).expect("rewritten output is UTF-8");
    let options = ParsingOptionsBuilder::new()
        .with_parsing_for_all_tags()
        .build();
    let mut reader = Reader::from_str(text, options);
    let mut uris = Vec::new();
    while let Some(line) = reader.read_line().expect("rewritten output reparses") {
        match line {
            HlsLine::Uri(uri) => uris.push(uri.into_owned()),
            HlsLine::KnownTag(KnownTag::Hls(tag)) => {
                let uri = match tag {
                    hls::Tag::Media(tag) => tag.uri().map(str::to_string),
                    hls::Tag::IFrameStreamInf(tag) => Some(tag.uri().to_string()),
                    hls::Tag::Key(tag) => tag.uri().map(str::to_string),
                    hls::Tag::Map(tag) => Some(tag.uri().to_string()),
                    hls::Tag::SessionKey(tag) => Some(tag.uri().to_string()),
                    _ => None,
                };
                if let Some(uri) = uri {
                    uris.push(uri);
                }
            }
            _ => {}
        }
    }
    uris
}

fn resource_id(route: &str) -> HlsResourceId {
    HlsResourceId::parse(route.rsplit('/').next().expect("route has identifier"))
        .expect("route has opaque identifier")
}

#[test]
fn r11_master_rewrites_variants_tracks_and_signed_queries_to_opaque_routes() {
    let mut resources = resource_map(HlsResourceLimits::default());
    let result = rewriter()
        .rewrite(
            &mut resources,
            7,
            &parent("/hls/master.m3u8"),
            ROUTE_BASE,
            MASTER.as_bytes(),
        )
        .expect("valid master rewrites");

    assert_eq!(result.kind(), HlsManifestKind::Master);
    assert_eq!(result.resource_count(), 4);
    assert_eq!(resources.len(), 4);
    let output = std::str::from_utf8(result.body()).expect("UTF-8 output");
    assert!(!output.contains("ELIXIR_LIVE_CANARY_QUERY"));
    assert!(!output.contains("origin.example.test"));
    assert!(!output.contains("live.m3u8"));

    let routes = route_uris(result.body());
    assert_eq!(routes.len(), 4);
    assert!(routes.iter().all(|route| route.starts_with(ROUTE_BASE)));
    let descriptors = routes
        .iter()
        .map(|route| {
            resources
                .resolve(&resource_id(route), 7)
                .expect("resource resolves")
        })
        .collect::<Vec<_>>();
    assert!(
        descriptors
            .iter()
            .all(|descriptor| descriptor.kind() == HlsResourceKind::Playlist)
    );
    assert!(descriptors.iter().any(|descriptor| {
        descriptor.url().query() == Some("variant=720p&sig=ELIXIR_LIVE_CANARY_QUERY")
    }));
}

#[test]
fn r11_media_rewrites_aes_map_ranges_and_preserves_dvr_semantics() {
    const MEDIA: &str = concat!(
        "#EXTM3U\n",
        "#EXT-X-VERSION:7\n",
        "#EXT-X-TARGETDURATION:4\n",
        "#EXT-X-MEDIA-SEQUENCE:42\n",
        "#EXT-X-KEY:METHOD=AES-128,URI=\"../keys/key.bin?token=KEY_CANARY\",IV=0x0000000000000000000000000000002a\n",
        "#EXT-X-MAP:URI=\"init.mp4?token=MAP_CANARY\",BYTERANGE=\"1024@0\"\n",
        "#EXT-X-PROGRAM-DATE-TIME:2026-07-12T18:00:00Z\n",
        "#EXTINF:4.000,\n",
        "#EXT-X-BYTERANGE:2048@1024\n",
        "segment.mp4?token=SEGMENT_CANARY\n",
        "#EXT-X-DISCONTINUITY\n",
        "#EXTINF:4.000,\n",
        "#EXT-X-BYTERANGE:2048\n",
        "segment.mp4?token=SEGMENT_CANARY\n",
        "#EXT-X-ENDLIST\n",
    );
    let mut resources = resource_map(HlsResourceLimits::default());
    let result = rewriter()
        .rewrite(
            &mut resources,
            7,
            &parent("/events/live/index.m3u8"),
            ROUTE_BASE,
            MEDIA.as_bytes(),
        )
        .expect("valid encrypted media rewrites");

    assert_eq!(result.kind(), HlsManifestKind::Media);
    assert_eq!(result.target_duration_seconds(), Some(4));
    assert_eq!(result.media_sequence(), Some(42));
    assert!(result.end_list());
    assert_eq!(result.resource_count(), 4);
    let output = std::str::from_utf8(result.body()).expect("UTF-8 output");
    for canary in ["KEY_CANARY", "MAP_CANARY", "SEGMENT_CANARY"] {
        assert!(!output.contains(canary));
    }
    assert!(output.contains("#EXT-X-BYTERANGE:2048@1024"));
    assert!(output.contains("#EXT-X-DISCONTINUITY"));
    assert!(output.contains("#EXT-X-PROGRAM-DATE-TIME"));

    let descriptors = route_uris(result.body())
        .iter()
        .map(|route| {
            resources
                .resolve(&resource_id(route), 7)
                .expect("resource resolves")
        })
        .collect::<Vec<_>>();
    assert!(descriptors.iter().any(|descriptor| {
        descriptor.kind() == HlsResourceKind::EncryptionKey
            && descriptor.url().query() == Some("token=KEY_CANARY")
    }));
    assert!(descriptors.iter().any(|descriptor| {
        descriptor.kind() == HlsResourceKind::InitializationSegment
            && descriptor.byte_range()
                == Some(HlsByteRange {
                    length: 1024,
                    offset: Some(0),
                })
    }));
    assert!(descriptors.iter().any(|descriptor| {
        descriptor.kind() == HlsResourceKind::MediaSegment
            && descriptor.byte_range()
                == Some(HlsByteRange {
                    length: 2048,
                    offset: Some(1024),
                })
    }));
    assert!(descriptors.iter().any(|descriptor| {
        descriptor.kind() == HlsResourceKind::MediaSegment
            && descriptor.byte_range()
                == Some(HlsByteRange {
                    length: 2048,
                    offset: Some(3072),
                })
    }));
}

#[test]
fn r11_legacy_allow_cache_media_playlist_rewrites_without_weakening_unknown_tag_policy() {
    const MEDIA: &str = concat!(
        "#EXTM3U\n",
        "#EXT-X-VERSION:3\n",
        "#EXT-X-TARGETDURATION:6\n",
        "#EXT-X-MEDIA-SEQUENCE:14525\n",
        "#EXT-X-DISCONTINUITY-SEQUENCE:15\n",
        "#EXT-X-ALLOW-CACHE:NO\n",
        "#EXT-X-PROGRAM-DATE-TIME:2026-07-21T04:02:08.834Z\n",
        "#EXTINF:6.000,\n",
        "segment.ts?token=SIGNED_CANARY\n",
    );
    let mut resources = resource_map(HlsResourceLimits::default());
    let result = rewriter()
        .rewrite(
            &mut resources,
            7,
            &parent("/hls/live/index.m3u8"),
            ROUTE_BASE,
            MEDIA.as_bytes(),
        )
        .expect("legacy provider playlist rewrites");

    assert_eq!(result.kind(), HlsManifestKind::Media);
    assert_eq!(result.resource_count(), 1);
    let output = std::str::from_utf8(result.body()).expect("UTF-8 output");
    assert!(!output.contains("EXT-X-ALLOW-CACHE"));
    assert!(!output.contains("SIGNED_CANARY"));

    for rejected in [
        MEDIA.replace("EXT-X-ALLOW-CACHE:NO", "EXT-X-ALLOW-CACHE:MAYBE"),
        MEDIA.replace(
            "#EXT-X-ALLOW-CACHE:NO\n",
            "#EXT-X-ALLOW-CACHE:NO\n#EXT-X-ALLOW-CACHE:YES\n",
        ),
    ] {
        let mut resources = resource_map(HlsResourceLimits::default());
        assert!(
            rewriter()
                .rewrite(
                    &mut resources,
                    7,
                    &parent("/hls/live/index.m3u8"),
                    ROUTE_BASE,
                    rejected.as_bytes(),
                )
                .is_err()
        );
        assert!(resources.is_empty());
    }
}

#[test]
fn r11_checked_in_dvr_and_frozen_playlists_preserve_window_metadata() {
    for (manifest, expected_sequence, expected_resources) in [(DVR, 400, 4), (FROZEN, 900, 1)] {
        let mut resources = resource_map(HlsResourceLimits::default());
        let result = rewriter()
            .rewrite(
                &mut resources,
                7,
                &parent("/hls/window/index.m3u8"),
                ROUTE_BASE,
                manifest.as_bytes(),
            )
            .expect("valid media corpus rewrites");
        assert_eq!(result.kind(), HlsManifestKind::Media);
        assert_eq!(result.target_duration_seconds(), Some(4));
        assert_eq!(result.media_sequence(), Some(expected_sequence));
        assert_eq!(result.resource_count(), expected_resources);
        assert!(
            std::str::from_utf8(result.body())
                .expect("UTF-8 output")
                .contains("#EXT-X-PROGRAM-DATE-TIME")
        );
    }
}

#[test]
fn r11_malformed_unknown_mixed_and_ambiguous_manifests_fail_closed() {
    let cases = [
        MALFORMED,
        "#EXT-X-TARGETDURATION:4\n#EXTINF:4,\na.ts\n",
        "#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXT-X-TARGETDURATION:5\n#EXTINF:4,\na.ts\n",
        "#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXT-X-STREAM-INF:BANDWIDTH=1\na.m3u8\n",
        "#EXTM3U\n#EXT-X-TARGETDURATION:4\na.ts\n",
        "#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXT-X-UNKNOWN:URI=\"secret\"\n#EXTINF:4,\na.ts\n",
        "#EXTM3U\n#EXT-X-DEFINE:NAME=\"x\",VALUE=\"a.ts\"\n#EXT-X-STREAM-INF:BANDWIDTH=1\n{$x}\n",
        "#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXT-X-DATERANGE:ID=\"x\",START-DATE=\"2026-07-12T18:00:00Z\",X-ASSET-URI=\"https://secret.invalid/x\"\n#EXTINF:4,\na.ts\n",
        "#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXTINF:5.6,\na.ts\n",
        "#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXTINF:4,\na.ts\n#EXT-X-ENDLIST\n#EXTINF:4,\nb.ts\n",
    ];
    for manifest in cases {
        let mut resources = resource_map(HlsResourceLimits::default());
        assert!(
            rewriter()
                .rewrite(
                    &mut resources,
                    7,
                    &parent("/hls/index.m3u8"),
                    ROUTE_BASE,
                    manifest.as_bytes(),
                )
                .is_err(),
            "manifest unexpectedly accepted: {manifest}"
        );
        assert!(resources.is_empty());
    }
}

#[test]
fn r11_encryption_policy_accepts_only_aes128_identity() {
    let rejected = [
        SAMPLE_AES,
        "#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXT-X-KEY:METHOD=UNKNOWN,URI=\"key\"\n#EXTINF:4,\na.ts\n",
        "#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXT-X-KEY:METHOD=AES-128,URI=\"key\",KEYFORMAT=\"com.apple.streamingkeydelivery\"\n#EXTINF:4,\na.ts\n",
        "#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXT-X-KEY:METHOD=AES-128,URI=\"key\",IV=0x01\n#EXTINF:4,\na.ts\n",
        "#EXTM3U\n#EXT-X-SESSION-KEY:METHOD=SAMPLE-AES,URI=\"skd://license\"\n#EXT-X-STREAM-INF:BANDWIDTH=1\na.m3u8\n",
    ];
    for manifest in rejected {
        let mut resources = resource_map(HlsResourceLimits::default());
        assert!(
            rewriter()
                .rewrite(
                    &mut resources,
                    7,
                    &parent("/hls/index.m3u8"),
                    ROUTE_BASE,
                    manifest.as_bytes(),
                )
                .is_err()
        );
        assert!(resources.is_empty());
    }
}

#[test]
fn r11_low_latency_tags_fall_back_to_complete_standard_segments() {
    let mut resources = resource_map(HlsResourceLimits::default());
    let result = rewriter()
        .rewrite(
            &mut resources,
            7,
            &parent("/hls/ll/index.m3u8"),
            ROUTE_BASE,
            LOW_LATENCY.as_bytes(),
        )
        .expect("LL-HLS extensions fall back to the complete segment");

    assert_eq!(result.kind(), HlsManifestKind::Media);
    assert_eq!(result.target_duration_seconds(), Some(4));
    assert_eq!(result.media_sequence(), Some(100));
    assert_eq!(result.resource_count(), 1);
    assert_eq!(resources.len(), 1);

    let output = std::str::from_utf8(result.body()).expect("UTF-8 output");
    assert!(output.contains("#EXTINF:4.000"));
    for ignored_tag in [
        "#EXT-X-PART-INF",
        "#EXT-X-SERVER-CONTROL",
        "#EXT-X-PART:",
        "#EXT-X-PRELOAD-HINT",
        "#EXT-X-RENDITION-REPORT",
    ] {
        assert!(!output.contains(ignored_tag));
    }
    assert!(!output.contains("?part="));

    let routes = route_uris(result.body());
    assert_eq!(routes.len(), 1);
    let descriptor = resources
        .resolve(&resource_id(&routes[0]), 7)
        .expect("complete segment resolves");
    assert_eq!(descriptor.kind(), HlsResourceKind::MediaSegment);
    assert_eq!(descriptor.url().path(), "/hls/media/segment-100.ts");
    assert_eq!(descriptor.url().query(), None);
}

#[test]
fn r11_delta_update_skip_falls_back_with_adjusted_media_sequence() {
    const DELTA: &str = concat!(
        "#EXTM3U\n",
        "#EXT-X-VERSION:9\n",
        "#EXT-X-TARGETDURATION:4\n",
        "#EXT-X-MEDIA-SEQUENCE:100\n",
        "#EXT-X-SKIP:SKIPPED-SEGMENTS=2\n",
        "#EXTINF:4.000,\n",
        "segment-102.ts\n",
    );
    let mut resources = resource_map(HlsResourceLimits::default());
    let result = rewriter()
        .rewrite(
            &mut resources,
            7,
            &parent("/hls/ll/index.m3u8"),
            ROUTE_BASE,
            DELTA.as_bytes(),
        )
        .expect("delta update falls back to its complete segment");

    assert_eq!(result.media_sequence(), Some(102));
    assert_eq!(result.resource_count(), 1);
    let output = std::str::from_utf8(result.body()).expect("UTF-8 output");
    assert!(output.contains("#EXT-X-MEDIA-SEQUENCE:102"));
    assert!(!output.contains("#EXT-X-SKIP"));
}

#[test]
fn r11_malformed_low_latency_extensions_fail_transactionally() {
    let cases = [
        "#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXT-X-PART-INF:PART-TARGET=0\n#EXTINF:4,\na.ts\n",
        "#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXT-X-PART:DURATION=0,URI=\"part.ts\"\n#EXTINF:4,\na.ts\n",
        "#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXT-X-PART:DURATION=1,URI=\"file:///etc/passwd\"\n#EXTINF:4,\na.ts\n",
        "#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXT-X-PRELOAD-HINT:TYPE=UNKNOWN,URI=\"part.ts\"\n#EXTINF:4,\na.ts\n",
        "#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXT-X-RENDITION-REPORT:URI=\"file:///etc/passwd\",LAST-MSN=1\n#EXTINF:4,\na.ts\n",
        "#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXT-X-SKIP:SKIPPED-SEGMENTS=1\n#EXTINF:4,\na.ts\n",
        "#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXTINF:4,\n#EXT-X-PART:DURATION=1,URI=\"part.ts\"\na.ts\n",
    ];
    for manifest in cases {
        let mut resources = resource_map(HlsResourceLimits::default());
        assert!(
            rewriter()
                .rewrite(
                    &mut resources,
                    7,
                    &parent("/hls/ll/index.m3u8"),
                    ROUTE_BASE,
                    manifest.as_bytes(),
                )
                .is_err(),
            "manifest unexpectedly accepted: {manifest}"
        );
        assert!(resources.is_empty());
    }
}

#[test]
fn r11_partial_only_low_latency_playlist_is_not_exposed_as_empty_media() {
    const PARTIAL_ONLY: &str = concat!(
        "#EXTM3U\n",
        "#EXT-X-VERSION:9\n",
        "#EXT-X-TARGETDURATION:4\n",
        "#EXT-X-PART-INF:PART-TARGET=1\n",
        "#EXT-X-MEDIA-SEQUENCE:100\n",
        "#EXT-X-PART:DURATION=1,URI=\"segment-100.part0.ts\"\n",
        "#EXT-X-PRELOAD-HINT:TYPE=PART,URI=\"segment-100.part1.ts\"\n",
    );
    let mut resources = resource_map(HlsResourceLimits::default());
    assert_eq!(
        rewriter().rewrite(
            &mut resources,
            7,
            &parent("/hls/ll/index.m3u8"),
            ROUTE_BASE,
            PARTIAL_ONLY.as_bytes(),
        ),
        Err(HlsRewriteError::MissingResource)
    );
    assert!(resources.is_empty());
}

#[test]
fn r11_relative_traversal_is_normalized_and_signed_query_stays_server_side() {
    const MANIFEST: &str = concat!(
        "#EXTM3U\n",
        "#EXT-X-TARGETDURATION:4\n",
        "#EXTINF:4,\n",
        "../media/./segment.ts?token=SIGNED_CANARY\n",
    );
    let mut resources = resource_map(HlsResourceLimits::default());
    let result = rewriter()
        .rewrite(
            &mut resources,
            7,
            &parent("/events/live/index.m3u8"),
            ROUTE_BASE,
            MANIFEST.as_bytes(),
        )
        .expect("relative URI rewrites");
    let route = route_uris(result.body()).pop().expect("one route");
    let descriptor = resources
        .resolve(&resource_id(&route), 7)
        .expect("resource resolves");
    assert_eq!(descriptor.url().path(), "/events/media/segment.ts");
    assert_eq!(descriptor.url().query(), Some("token=SIGNED_CANARY"));
    assert!(
        !result
            .body()
            .windows(13)
            .any(|window| window == b"SIGNED_CANARY")
    );
}

#[test]
fn r11_rewrite_is_transactional_and_enforces_resource_capacity() {
    let mut resources = resource_map(HlsResourceLimits {
        max_resources: 1,
        retired_revision_grace: 1,
    });
    let one = "#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXTINF:4,\na.ts\n";
    let accepted = rewriter()
        .rewrite(
            &mut resources,
            7,
            &parent("/hls/index.m3u8"),
            ROUTE_BASE,
            one.as_bytes(),
        )
        .expect("first manifest fits");
    let original_route = route_uris(accepted.body()).pop().expect("one route");
    let original_id = resource_id(&original_route);

    let two = "#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXTINF:4,\na.ts\n#EXTINF:4,\nb.ts\n";
    assert_eq!(
        rewriter().rewrite(
            &mut resources,
            7,
            &parent("/hls/index.m3u8"),
            ROUTE_BASE,
            two.as_bytes(),
        ),
        Err(HlsRewriteError::ResourceLimitExceeded)
    );
    assert_eq!(resources.len(), 1);
    assert!(resources.resolve(&original_id, 7).is_ok());
}

#[test]
fn r11_refresh_reuses_ids_retires_old_resources_and_rejects_stale_fences() {
    let mut resources = resource_map(HlsResourceLimits {
        max_resources: 8,
        retired_revision_grace: 1,
    });
    let manifest = |name: &str| format!("#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXTINF:4,\n{name}\n");
    let first = rewriter()
        .rewrite(
            &mut resources,
            7,
            &parent("/hls/index.m3u8"),
            ROUTE_BASE,
            manifest("a.ts").as_bytes(),
        )
        .expect("first refresh");
    let first_id = resource_id(&route_uris(first.body())[0]);
    let repeated = rewriter()
        .rewrite(
            &mut resources,
            7,
            &parent("/hls/index.m3u8"),
            ROUTE_BASE,
            manifest("a.ts").as_bytes(),
        )
        .expect("same refresh");
    assert_eq!(first_id, resource_id(&route_uris(repeated.body())[0]));

    rewriter()
        .rewrite(
            &mut resources,
            7,
            &parent("/hls/index.m3u8"),
            ROUTE_BASE,
            manifest("b.ts").as_bytes(),
        )
        .expect("rotated refresh");
    assert!(resources.resolve(&first_id, 7).is_ok());
    rewriter()
        .rewrite(
            &mut resources,
            7,
            &parent("/hls/index.m3u8"),
            ROUTE_BASE,
            manifest("c.ts").as_bytes(),
        )
        .expect("grace expiry refresh");
    assert_eq!(
        resources.resolve(&first_id, 7),
        Err(HlsRewriteError::UnknownResource)
    );
    assert_eq!(
        resources.resolve(&resource_id(&route_uris(repeated.body())[0]), 6),
        Err(HlsRewriteError::StaleControlFence)
    );
    resources.take_over(7, 8).expect("new owner takes over");
    assert!(resources.is_empty());
    assert_eq!(
        rewriter().rewrite(
            &mut resources,
            7,
            &parent("/hls/index.m3u8"),
            ROUTE_BASE,
            manifest("d.ts").as_bytes(),
        ),
        Err(HlsRewriteError::StaleControlFence)
    );
}

#[test]
fn r11_manifest_scopes_cannot_retire_each_others_resources() {
    let mut resources = resource_map(HlsResourceLimits {
        max_resources: 32,
        retired_revision_grace: 1,
    });
    let master = rewriter()
        .rewrite_scoped(
            &mut resources,
            7,
            HlsManifestScope::from_stable_key(b"root").expect("valid root scope"),
            &parent("/hls/master.m3u8"),
            ROUTE_BASE,
            MASTER.as_bytes(),
        )
        .expect("master rewrites");
    let master_ids = route_uris(master.body())
        .iter()
        .map(|route| resource_id(route))
        .collect::<Vec<_>>();
    let variant_scope =
        HlsManifestScope::from_stable_key(b"variant-720p").expect("valid variant scope");
    for sequence in 1..=5 {
        let media = format!(
            "#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXT-X-MEDIA-SEQUENCE:{sequence}\n#EXTINF:4,\nsegment-{sequence}.ts\n"
        );
        rewriter()
            .rewrite_scoped(
                &mut resources,
                7,
                variant_scope,
                &parent("/hls/variant.m3u8"),
                ROUTE_BASE,
                media.as_bytes(),
            )
            .expect("variant refresh rewrites");
    }
    assert!(
        master_ids
            .iter()
            .all(|resource_id| resources.resolve(resource_id, 7).is_ok())
    );
}

#[test]
fn r11_bounds_controls_uri_policy_and_debug_output_do_not_leak_secrets() {
    let mut tiny = HlsRewriteConfig::default();
    tiny.max_body_bytes = 48;
    let tiny = HlsRewriter::new(tiny).expect("valid tiny config");
    let mut resources = resource_map(HlsResourceLimits::default());
    assert_eq!(
        tiny.rewrite(
            &mut resources,
            7,
            &parent("/hls/index.m3u8"),
            ROUTE_BASE,
            b"#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXTINF:4,\nsegment.ts\n",
        ),
        Err(HlsRewriteError::BodyLimitExceeded)
    );

    for manifest in [
        b"#EXTM3U\r#EXT-X-TARGETDURATION:4\n".as_slice(),
        b"#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXTINF:4,\nseg\0.ts\n".as_slice(),
        b"#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXTINF:4,\nhttps://user:pass@origin.invalid/seg.ts\n"
            .as_slice(),
        b"#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXTINF:4,\nseg.ts#fragment\n".as_slice(),
        b"#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXTINF:4,\nseg%0a.ts\n".as_slice(),
    ] {
        assert!(
            rewriter()
                .rewrite(
                    &mut resources,
                    7,
                    &parent("/hls/index.m3u8"),
                    ROUTE_BASE,
                    manifest,
                )
                .is_err()
        );
    }

    let secret = "#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXTINF:4,\nseg.ts?token=TOP_SECRET\n";
    rewriter()
        .rewrite(
            &mut resources,
            7,
            &parent("/hls/index.m3u8"),
            ROUTE_BASE,
            secret.as_bytes(),
        )
        .expect("secret query is contained");
    let debug = format!("{resources:?}");
    assert!(!debug.contains("TOP_SECRET"));
    assert!(!debug.contains("origin.example.test"));
    let route = route_uris(
        rewriter()
            .rewrite(
                &mut resources,
                7,
                &parent("/hls/index.m3u8"),
                ROUTE_BASE,
                secret.as_bytes(),
            )
            .expect("stable secret rewrite")
            .body(),
    )[0]
    .clone();
    let descriptor = resources
        .resolve(&resource_id(&route), 7)
        .expect("secret resource resolves");
    assert_eq!(format!("{descriptor:?}").contains("TOP_SECRET"), false);
}

#[test]
fn r11_deterministic_mutation_corpus_never_partially_commits() {
    let seed = b"#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXTINF:4,\nsegment.ts?token=MUTATION_CANARY\n";
    for index in 0..seed.len() {
        for replacement in [0_u8, b'\r', 0x7f, b'#', b' '] {
            let mut mutated = seed.to_vec();
            mutated[index] = replacement;
            let mut resources = resource_map(HlsResourceLimits::default());
            let result = rewriter().rewrite(
                &mut resources,
                7,
                &parent("/hls/index.m3u8"),
                ROUTE_BASE,
                mutated.as_slice(),
            );
            if result.is_err() {
                assert!(resources.is_empty());
            } else {
                let output = result.expect("checked success");
                assert!(route_uris(output.body()).iter().all(|route| {
                    route.starts_with(ROUTE_BASE)
                        && HlsResourceId::parse(route.rsplit('/').next().unwrap()).is_some()
                }));
            }
        }
    }
}
