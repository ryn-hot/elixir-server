//! Fixed production-shaped requests used to prime and probe the local
//! anime-matching worker. Exact expected responses are retained for corpus and
//! certification assertions, never hardware-profile selection.

use anyhow::{Context, Result, anyhow, ensure};
use serde::Deserialize;

use super::{
    ANIME_MATCH_SCHEMA_VERSION, AnimeCandidateMatch, AnimeMatchAudioProfile, AnimeMatchRequest,
    AnimeMatchResponse,
};

const PROFILE_PROBE_PRIMING_REQUEST_ID: &str = "alm9-hardware-tokyo-ghoul-s2e1";
const PROFILE_PROBE_MEASURED_REQUEST_ID: &str = "alm9-hardware-cross-script-absolute";
const PROFILE_PROBE_REQUEST_CORPUS_BYTES: &[u8] =
    include_bytes!("fixtures/hardware-certification-requests.json");

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ProfileProbeRequestCorpus {
    schema_version: u32,
    status: String,
    requests: Vec<AnimeMatchRequest>,
}

/// Return the fixed production-shaped request used to prime a newly loaded
/// worker before it serves user inference.
pub(crate) fn prime_request() -> Result<AnimeMatchRequest> {
    let [priming, _] = smoke_requests()?;
    Ok(priming)
}

/// Return two fixed, production-shaped requests used to exercise a local runtime
/// profile. The first primes a newly loaded worker; the second is distinct, so
/// only their shared production prefix is reusable and the measurement cannot
/// pass through exact user-prompt reuse. Semantic capability is scored by the
/// frozen corpus. Both remain part of the physical-certification corpus.
pub(crate) fn smoke_requests() -> Result<[AnimeMatchRequest; 2]> {
    let corpus: ProfileProbeRequestCorpus =
        serde_json::from_slice(PROFILE_PROBE_REQUEST_CORPUS_BYTES)
            .context("decoding compiled profile-probe request corpus")?;
    ensure!(
        corpus.schema_version == 1 && corpus.status == "frozen",
        "compiled profile-probe request corpus is not frozen schema v1"
    );
    let mut priming = None;
    let mut measured = None;
    for request in corpus.requests {
        let slot = match request.request_id.as_str() {
            PROFILE_PROBE_PRIMING_REQUEST_ID => &mut priming,
            PROFILE_PROBE_MEASURED_REQUEST_ID => &mut measured,
            _ => continue,
        };
        ensure!(
            slot.is_none(),
            "compiled profile-probe request is duplicated"
        );
        *slot = Some(request);
    }
    Ok([
        priming.ok_or_else(|| anyhow!("compiled profile-probe priming request is missing"))?,
        measured.ok_or_else(|| anyhow!("compiled profile-probe measured request is missing"))?,
    ])
}

/// Validate the exact response required from the shared priming request.
#[cfg(test)]
pub(crate) fn prime_response_passed(response: &AnimeMatchResponse) -> bool {
    profile_probe_response_passed(PROFILE_PROBE_PRIMING_REQUEST_ID, response)
}

#[cfg(test)]
pub(crate) fn smoke_responses_passed(
    priming_response: &AnimeMatchResponse,
    response: &AnimeMatchResponse,
) -> bool {
    prime_response_passed(priming_response)
        && profile_probe_response_passed(PROFILE_PROBE_MEASURED_REQUEST_ID, response)
}

pub(crate) fn profile_probe_response_passed(
    request_id: &str,
    response: &AnimeMatchResponse,
) -> bool {
    profile_probe_expected_response(request_id).is_some_and(|expected| response == &expected)
}

fn profile_probe_expected_response(request_id: &str) -> Option<AnimeMatchResponse> {
    let (target_key, audio_profile) = match request_id {
        PROFILE_PROBE_PRIMING_REQUEST_ID => ("S02E01", AnimeMatchAudioProfile::DualAudio),
        PROFILE_PROBE_MEASURED_REQUEST_ID => ("S01E13", AnimeMatchAudioProfile::Unknown),
        _ => return None,
    };
    Some(AnimeMatchResponse {
        schema_version: ANIME_MATCH_SCHEMA_VERSION,
        matches: vec![AnimeCandidateMatch {
            candidate_key: "candidate-0".to_string(),
            matched_target_keys: vec![target_key.to_string()],
            audio_profile,
            selected_file_keys: Some(vec!["candidate-0-file-0".to_string()]),
        }],
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::anime_matching::{AnimeMatchAliasKind, AnimeMatchAudioPreferenceMode};

    #[test]
    fn alm9_profile_probe_fixture_is_production_shaped_and_reference_closed() {
        let [priming, measured] = smoke_requests().expect("compiled profile-probe requests");
        assert_eq!(prime_request().expect("compiled prime request"), priming);
        assert_ne!(priming.request_id, measured.request_id);
        for request in [&priming, &measured] {
            let encoded = serde_json::to_vec(request).unwrap();
            assert!((1_800..4 * 1024).contains(&encoded.len()));
            assert_eq!(request.candidates.len(), 4);
            assert_eq!(request.candidates[0].candidate_key, "candidate-0");
            assert_eq!(
                request.candidates[0].files[0].file_key,
                "candidate-0-file-0"
            );
        }

        assert_eq!(priming.request_id, PROFILE_PROBE_PRIMING_REQUEST_ID);
        assert_eq!(priming.target.wanted_target_keys, ["S02E01"]);
        assert_eq!(
            priming.target.audio_preference.mode,
            AnimeMatchAudioPreferenceMode::RequireDub
        );
        assert_eq!(priming.target.absolute_episode_numbers, [13]);
        assert_eq!(priming.context.seasons.len(), 1);
        assert_eq!(priming.context.seasons[0].targets.len(), 2);
        assert_eq!(
            priming.context.seasons[0]
                .aliases
                .iter()
                .map(|alias| alias.kind)
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([
                AnimeMatchAliasKind::English,
                AnimeMatchAliasKind::Romaji,
                AnimeMatchAliasKind::Native,
                AnimeMatchAliasKind::Generated,
            ])
        );
        assert_eq!(measured.request_id, PROFILE_PROBE_MEASURED_REQUEST_ID);
        assert_eq!(measured.target.wanted_target_keys, ["S01E13"]);
        assert_eq!(
            measured.target.audio_preference.mode,
            AnimeMatchAudioPreferenceMode::Any
        );
        assert_eq!(measured.context.seasons[0].targets.len(), 3);
        assert!(
            measured.context.seasons[0]
                .aliases
                .iter()
                .any(|alias| alias.kind == AnimeMatchAliasKind::Native)
        );
    }

    #[test]
    fn alm9_profile_probe_requires_both_complete_expected_mappings() {
        let priming = profile_probe_expected_response(PROFILE_PROBE_PRIMING_REQUEST_ID).unwrap();
        let measured = profile_probe_expected_response(PROFILE_PROBE_MEASURED_REQUEST_ID).unwrap();
        assert!(smoke_responses_passed(&priming, &measured));
        assert!(!profile_probe_response_passed("unknown", &priming));

        let mut wrong_audio = priming.clone();
        wrong_audio.matches[0].audio_profile = AnimeMatchAudioProfile::Subbed;
        assert!(!smoke_responses_passed(&wrong_audio, &measured));

        let mut missing_file = measured.clone();
        missing_file.matches[0].selected_file_keys = None;
        assert!(!smoke_responses_passed(&priming, &missing_file));

        let mut duplicate_target = priming.clone();
        duplicate_target.matches[0]
            .matched_target_keys
            .push("S02E01".to_string());
        assert!(!smoke_responses_passed(&duplicate_target, &measured));

        let mut extra_candidate = measured.clone();
        extra_candidate.matches.push(AnimeCandidateMatch {
            candidate_key: "candidate-2".to_string(),
            matched_target_keys: vec!["S01E13".to_string()],
            audio_profile: AnimeMatchAudioProfile::Subbed,
            selected_file_keys: Some(vec!["candidate-2-file-0".to_string()]),
        });
        assert!(!smoke_responses_passed(&priming, &extra_candidate));
    }
}
