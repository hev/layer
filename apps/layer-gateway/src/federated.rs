//! Federated multi-surface search with reciprocal rank fusion.
//!
//! Groundwork for a cross-surface search (for example kit's `traces`,
//! `board`, `orders` and `evidence`): one request carries a text query (`q`
//! plus up to seven `also` rephrasings), optional media queries, and
//! per-surface filters. The executor plans one leg per (surface, query) pair
//! the surface can answer, runs the legs in parallel under a concurrency
//! cap and a per-leg timeout, and fuses every leg's ranking with RRF,
//! `Σ 1/(k + rank)`.
//!
//! A surface that cannot read any of the request's query modalities is
//! reported `skipped` with `reason: "modality_unsupported"` rather than
//! failing the request: a media-only query over a text-only surface is a
//! normal outcome. A surface whose legs all fail or time out is reported
//! `failed`; the request still returns the other surfaces' hits.
//!
//! The executor is independent of any store: a [`SurfaceSearcher`] runs one
//! leg. No public route mounts it yet.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::embedding::multimodal::{
    validate_input, EmbedInput, InputModality, MultimodalError, MultimodalModelProfile,
    ValidatedInput, ValidatedMedia,
};

/// Rephrasings allowed beside `q`.
pub const MAX_ALSO: usize = 7;
pub const MAX_MEDIA: usize = 4;
pub const MAX_SURFACES: usize = 8;
pub const MAX_TOP_K: u32 = 100;
pub const DEFAULT_TOP_K: u32 = 10;
pub const DEFAULT_RANK_CONSTANT: u64 = 60;

/// Executor bounds.
#[derive(Debug, Clone)]
pub struct FederatedOptions {
    /// Legs in flight at once across all surfaces.
    pub max_concurrency: usize,
    /// Each leg is abandoned (and counted as timed out) after this.
    pub leg_timeout: Duration,
    /// Hits requested from each leg, capped by [`MAX_TOP_K`].
    pub per_leg_top_k: u32,
    pub rank_constant: u64,
}

impl Default for FederatedOptions {
    fn default() -> Self {
        Self {
            max_concurrency: 8,
            leg_timeout: Duration::from_secs(5),
            per_leg_top_k: 50,
            rank_constant: DEFAULT_RANK_CONSTANT,
        }
    }
}

/// One searchable surface: a name callers use, the namespace behind it, and
/// what its index can be queried with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SurfaceSpec {
    pub name: String,
    pub namespace: String,
    /// Whether a text query can be run against this surface.
    pub text: bool,
    /// Media modalities whose embeddings share this surface's vector space.
    pub media: Vec<InputModality>,
    /// Route label reported for text legs (e.g. `hybrid`).
    pub text_route: String,
    /// Route label reported for media legs (e.g. `multimodal_ann`).
    pub media_route: String,
}

impl SurfaceSpec {
    pub fn text_only(name: &str, namespace: &str) -> Self {
        Self {
            name: name.to_string(),
            namespace: namespace.to_string(),
            text: true,
            media: Vec::new(),
            text_route: "text".to_string(),
            media_route: "media".to_string(),
        }
    }

    pub fn multimodal(name: &str, namespace: &str) -> Self {
        Self {
            media: vec![InputModality::Image, InputModality::Video],
            ..Self::text_only(name, namespace)
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct FederatedSearchRequest {
    pub q: Option<String>,
    pub also: Vec<String>,
    pub media: Vec<EmbedInput>,
    /// Surface names to search, in order. Empty means every registered one.
    pub surfaces: Vec<String>,
    /// Filters keyed by surface name, passed through to that surface's legs.
    pub filters: BTreeMap<String, Value>,
    pub top_k: Option<u32>,
}

/// A time slice of a video hit. Reserved: v1 embeds whole clips, so it is
/// always `None` and omitted from the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Segment {
    pub start_ms: u64,
    pub end_ms: u64,
}

/// What one leg queries with.
#[derive(Debug, Clone)]
pub enum LegQuery {
    Text(String),
    Media(Arc<ValidatedMedia>),
}

impl LegQuery {
    pub fn modality(&self) -> InputModality {
        match self {
            Self::Text(_) => InputModality::Text,
            Self::Media(media) => media.modality(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct FederatedLeg {
    /// `q`, `also[0]`.., `media[0]`..: stable across surfaces.
    pub label: String,
    pub surface: Arc<SurfaceSpec>,
    pub query: LegQuery,
    pub filters: Option<Value>,
    pub top_k: u32,
}

/// One ranked row from one leg.
#[derive(Debug, Clone, PartialEq)]
pub struct SurfaceHit {
    pub id: String,
    pub score: Option<f64>,
    pub attributes: Map<String, Value>,
    pub segment: Option<Segment>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LegError {
    #[error("leg timed out")]
    Timeout,
    #[error("{0}")]
    Failed(String),
}

/// Runs one leg against one surface's store.
#[async_trait]
pub trait SurfaceSearcher: Send + Sync {
    async fn search(&self, leg: &FederatedLeg) -> Result<Vec<SurfaceHit>, LegError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SurfaceStatus {
    Searched,
    Skipped,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SurfaceReport {
    pub surface: String,
    pub namespace: String,
    pub status: SurfaceStatus,
    /// Routes the legs ran on, `+`-joined (`text`, `media`, `text+media`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub route: Option<String>,
    /// Why a surface was skipped or failed (`modality_unsupported`,
    /// `timeout`, `leg_error`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub legs: usize,
    pub failed_legs: usize,
    pub hits: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct LegRank {
    pub label: String,
    pub rank: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FusedHit {
    pub surface: String,
    pub namespace: String,
    pub id: String,
    /// The RRF sum.
    pub score: f64,
    pub legs: Vec<LegRank>,
    pub attributes: Map<String, Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub segment: Option<Segment>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FederatedSearchResponse {
    pub hits: Vec<FusedHit>,
    pub surfaces: Vec<SurfaceReport>,
}

/// Plans, runs and fuses federated searches over a fixed set of surfaces.
pub struct FederatedSearch {
    surfaces: Vec<Arc<SurfaceSpec>>,
    searcher: Arc<dyn SurfaceSearcher>,
    media_profile: &'static MultimodalModelProfile,
    options: FederatedOptions,
}

impl FederatedSearch {
    pub fn new(
        surfaces: Vec<SurfaceSpec>,
        searcher: Arc<dyn SurfaceSearcher>,
        media_profile: &'static MultimodalModelProfile,
        options: FederatedOptions,
    ) -> Result<Self, String> {
        if surfaces.is_empty() || surfaces.len() > MAX_SURFACES {
            return Err(format!(
                "federated search needs between 1 and {MAX_SURFACES} surfaces"
            ));
        }
        let mut names = HashSet::new();
        for surface in &surfaces {
            if !names.insert(surface.name.clone()) {
                return Err(format!("surface `{}` is registered twice", surface.name));
            }
            if surface.media.contains(&InputModality::Text) {
                return Err(format!(
                    "surface `{}` lists text as a media modality",
                    surface.name
                ));
            }
        }
        if options.max_concurrency == 0
            || options.leg_timeout.is_zero()
            || options.per_leg_top_k == 0
        {
            return Err("federated search bounds must be non-zero".to_string());
        }
        Ok(Self {
            surfaces: surfaces.into_iter().map(Arc::new).collect(),
            searcher,
            media_profile,
            options,
        })
    }

    pub async fn search(
        &self,
        request: FederatedSearchRequest,
    ) -> Result<FederatedSearchResponse, MultimodalError> {
        let (texts, media) = self.validate_queries(&request)?;
        let selected = self.select_surfaces(&request)?;
        let top_k = request.top_k.unwrap_or(DEFAULT_TOP_K);
        if top_k == 0 || top_k > MAX_TOP_K {
            return Err(MultimodalError::InvalidRequest(format!(
                "top_k must be between 1 and {MAX_TOP_K}"
            )));
        }
        let per_leg_top_k = self.options.per_leg_top_k.min(MAX_TOP_K).max(top_k);

        let mut legs = Vec::new();
        let mut reports = Vec::new();
        for surface in &selected {
            let filters = request.filters.get(&surface.name).cloned();
            let mut routes = Vec::new();
            let before = legs.len();
            if surface.text && !texts.is_empty() {
                routes.push(surface.text_route.clone());
                for (label, text) in &texts {
                    legs.push(FederatedLeg {
                        label: label.clone(),
                        surface: Arc::clone(surface),
                        query: LegQuery::Text(text.clone()),
                        filters: filters.clone(),
                        top_k: per_leg_top_k,
                    });
                }
            }
            let readable: Vec<_> = media
                .iter()
                .filter(|(_, item)| surface.media.contains(&item.modality()))
                .collect();
            if !readable.is_empty() {
                routes.push(surface.media_route.clone());
                for (label, item) in readable {
                    legs.push(FederatedLeg {
                        label: label.clone(),
                        surface: Arc::clone(surface),
                        query: LegQuery::Media(Arc::clone(item)),
                        filters: filters.clone(),
                        top_k: per_leg_top_k,
                    });
                }
            }
            let planned = legs.len() - before;
            reports.push(SurfaceReport {
                surface: surface.name.clone(),
                namespace: surface.namespace.clone(),
                status: if planned == 0 {
                    SurfaceStatus::Skipped
                } else {
                    SurfaceStatus::Searched
                },
                route: (planned > 0).then(|| routes.join("+")),
                reason: (planned == 0).then(|| "modality_unsupported".to_string()),
                legs: planned,
                failed_legs: 0,
                hits: 0,
            });
        }

        let timeout = self.options.leg_timeout;
        let searcher = &self.searcher;
        let outcomes: Vec<(usize, Result<Vec<SurfaceHit>, LegError>)> =
            stream::iter(legs.iter().enumerate())
                .map(|(index, leg)| async move {
                    let result = match tokio::time::timeout(timeout, searcher.search(leg)).await {
                        Ok(result) => result,
                        Err(_) => Err(LegError::Timeout),
                    };
                    (index, result)
                })
                .buffer_unordered(self.options.max_concurrency)
                .collect()
                .await;
        let mut outcomes: Vec<_> = outcomes.into_iter().collect();
        // Fusion order must not depend on completion order.
        outcomes.sort_by_key(|(index, _)| *index);

        let report_index: HashMap<String, usize> = reports
            .iter()
            .enumerate()
            .map(|(index, report)| (report.surface.clone(), index))
            .collect();
        let mut timeouts: HashMap<usize, usize> = HashMap::new();
        let mut runs = Vec::new();
        for (index, outcome) in outcomes {
            let leg = &legs[index];
            let slot = report_index[&leg.surface.name];
            match outcome {
                Ok(mut hits) => {
                    hits.truncate(leg.top_k as usize);
                    runs.push((leg, hits));
                }
                Err(error) => {
                    reports[slot].failed_legs += 1;
                    if error == LegError::Timeout {
                        *timeouts.entry(slot).or_default() += 1;
                    }
                }
            }
        }
        for (slot, report) in reports.iter_mut().enumerate() {
            if report.status == SurfaceStatus::Searched && report.failed_legs == report.legs {
                report.status = SurfaceStatus::Failed;
                report.reason = Some(
                    if timeouts.get(&slot) == Some(&report.failed_legs) {
                        "timeout"
                    } else {
                        "leg_error"
                    }
                    .to_string(),
                );
            }
        }

        let mut hits = rrf_fuse(&runs, self.options.rank_constant);
        hits.truncate(top_k as usize);
        for hit in &hits {
            reports[report_index[&hit.surface]].hits += 1;
        }
        Ok(FederatedSearchResponse {
            hits,
            surfaces: reports,
        })
    }

    #[allow(clippy::type_complexity)]
    fn validate_queries(
        &self,
        request: &FederatedSearchRequest,
    ) -> Result<(Vec<(String, String)>, Vec<(String, Arc<ValidatedMedia>)>), MultimodalError> {
        let q = request
            .q
            .as_deref()
            .map(str::trim)
            .filter(|q| !q.is_empty());
        if q.is_none() && !request.also.is_empty() {
            return Err(MultimodalError::InvalidRequest(
                "`also` requires `q`".to_string(),
            ));
        }
        if request.also.len() > MAX_ALSO {
            return Err(MultimodalError::InvalidRequest(format!(
                "`also` accepts at most {MAX_ALSO} rephrasings"
            )));
        }
        if request.media.len() > MAX_MEDIA {
            return Err(MultimodalError::InvalidRequest(format!(
                "`media` accepts at most {MAX_MEDIA} inputs"
            )));
        }
        if q.is_none() && request.media.is_empty() {
            return Err(MultimodalError::InvalidRequest(
                "a search needs `q` or `media`".to_string(),
            ));
        }
        let mut texts = Vec::new();
        let mut seen = HashSet::new();
        if let Some(q) = q {
            seen.insert(q.to_string());
            texts.push(("q".to_string(), q.to_string()));
        }
        for (index, also) in request.also.iter().enumerate() {
            let also = also.trim();
            if also.is_empty() {
                return Err(MultimodalError::InvalidRequest(format!(
                    "`also[{index}]` is empty"
                )));
            }
            // A repeated phrasing would double-count its own ranking.
            if seen.insert(also.to_string()) {
                texts.push((format!("also[{index}]"), also.to_string()));
            }
        }
        let mut media = Vec::new();
        for (index, input) in request.media.iter().enumerate() {
            if input.modality() == InputModality::Text {
                return Err(MultimodalError::InvalidRequest(format!(
                    "`media[{index}]` is text; put text in `q` or `also`"
                )));
            }
            let ValidatedInput::Media(validated) = validate_input(self.media_profile, input)?
            else {
                unreachable!("non-text input validates to media");
            };
            media.push((format!("media[{index}]"), Arc::new(validated)));
        }
        Ok((texts, media))
    }

    fn select_surfaces(
        &self,
        request: &FederatedSearchRequest,
    ) -> Result<Vec<Arc<SurfaceSpec>>, MultimodalError> {
        let selected: Vec<Arc<SurfaceSpec>> = if request.surfaces.is_empty() {
            self.surfaces.clone()
        } else {
            if request.surfaces.len() > MAX_SURFACES {
                return Err(MultimodalError::InvalidRequest(format!(
                    "at most {MAX_SURFACES} surfaces per search"
                )));
            }
            let mut seen = HashSet::new();
            let mut selected = Vec::new();
            for name in &request.surfaces {
                let surface = self
                    .surfaces
                    .iter()
                    .find(|surface| &surface.name == name)
                    .ok_or_else(|| {
                        MultimodalError::InvalidRequest(format!("unknown surface `{name}`"))
                    })?;
                if !seen.insert(name.clone()) {
                    return Err(MultimodalError::InvalidRequest(format!(
                        "surface `{name}` is listed twice"
                    )));
                }
                selected.push(Arc::clone(surface));
            }
            selected
        };
        for name in request.filters.keys() {
            if !selected.iter().any(|surface| &surface.name == name) {
                return Err(MultimodalError::InvalidRequest(format!(
                    "filters name surface `{name}`, which this search does not include"
                )));
            }
        }
        Ok(selected)
    }
}

/// RRF over every successful leg. Identity is (surface, id): the same id in
/// two surfaces is two documents. Order: score descending, then surface and
/// id ascending.
pub fn rrf_fuse(runs: &[(&FederatedLeg, Vec<SurfaceHit>)], rank_constant: u64) -> Vec<FusedHit> {
    let mut fused: HashMap<(String, String), FusedHit> = HashMap::new();
    for (leg, hits) in runs {
        for (index, hit) in hits.iter().enumerate() {
            let rank = index as u64 + 1;
            let entry = fused
                .entry((leg.surface.name.clone(), hit.id.clone()))
                .or_insert_with(|| FusedHit {
                    surface: leg.surface.name.clone(),
                    namespace: leg.surface.namespace.clone(),
                    id: hit.id.clone(),
                    score: 0.0,
                    legs: Vec::new(),
                    attributes: hit.attributes.clone(),
                    segment: hit.segment,
                });
            entry.score += 1.0 / (rank_constant as f64 + rank as f64);
            entry.legs.push(LegRank {
                label: leg.label.clone(),
                rank,
                score: hit.score,
            });
        }
    }
    let mut hits: Vec<FusedHit> = fused.into_values().collect();
    hits.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.surface.cmp(&b.surface))
            .then_with(|| a.id.cmp(&b.id))
    });
    hits
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::embedding::multimodal::{fixtures, MediaSource, EMBEDDINGGEMMA_2};
    use std::sync::Mutex;

    #[derive(Default)]
    struct Recorder {
        legs: Mutex<Vec<(String, String, InputModality)>>,
    }

    #[async_trait]
    impl SurfaceSearcher for Recorder {
        async fn search(&self, leg: &FederatedLeg) -> Result<Vec<SurfaceHit>, LegError> {
            self.legs.lock().unwrap().push((
                leg.surface.name.clone(),
                leg.label.clone(),
                leg.query.modality(),
            ));
            Ok(vec![SurfaceHit {
                id: format!("{}-{}", leg.surface.name, leg.query.modality().label()),
                score: None,
                attributes: Map::new(),
                segment: None,
            }])
        }
    }

    fn search(recorder: Arc<Recorder>) -> FederatedSearch {
        FederatedSearch::new(
            vec![
                SurfaceSpec::text_only("board", "kit-board"),
                SurfaceSpec::multimodal("evidence", "kit-evidence"),
            ],
            recorder,
            &EMBEDDINGGEMMA_2,
            FederatedOptions::default(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn also_is_bounded_and_deduplicated() {
        let recorder = Arc::new(Recorder::default());
        let search = search(Arc::clone(&recorder));
        let request = FederatedSearchRequest {
            q: Some("deploy".into()),
            also: vec!["rollout".into(); 8],
            ..Default::default()
        };
        assert!(search.search(request).await.is_err());
        let request = FederatedSearchRequest {
            q: Some("deploy".into()),
            also: vec!["deploy".into(), "rollout".into(), "rollout".into()],
            ..Default::default()
        };
        let response = search.search(request).await.unwrap();
        // q + one distinct rephrasing, on both surfaces.
        assert_eq!(recorder.legs.lock().unwrap().len(), 4);
        assert!(response.surfaces.iter().all(|r| r.legs == 2));
    }

    #[tokio::test]
    async fn rejects_bad_requests_before_any_leg() {
        let recorder = Arc::new(Recorder::default());
        let search = search(Arc::clone(&recorder));
        for request in [
            FederatedSearchRequest::default(),
            FederatedSearchRequest {
                also: vec!["x".into()],
                ..Default::default()
            },
            FederatedSearchRequest {
                q: Some("x".into()),
                surfaces: vec!["nope".into()],
                ..Default::default()
            },
            FederatedSearchRequest {
                q: Some("x".into()),
                surfaces: vec!["board".into()],
                filters: BTreeMap::from([("evidence".into(), Value::Null)]),
                ..Default::default()
            },
            FederatedSearchRequest {
                q: Some("x".into()),
                top_k: Some(101),
                ..Default::default()
            },
            FederatedSearchRequest {
                media: vec![EmbedInput::Image {
                    mime: None,
                    bytes_or_ref: MediaSource::Bytes(b"<svg/>".to_vec()),
                }],
                ..Default::default()
            },
        ] {
            assert!(search.search(request).await.is_err());
        }
        assert!(recorder.legs.lock().unwrap().is_empty());
    }

    #[test]
    fn segment_is_omitted_in_whole_clip_mode() {
        let hit = FusedHit {
            surface: "evidence".into(),
            namespace: "kit-evidence".into(),
            id: "e1".into(),
            score: 0.1,
            legs: Vec::new(),
            attributes: Map::new(),
            segment: None,
        };
        let value = serde_json::to_value(&hit).unwrap();
        assert!(value.get("segment").is_none());
        let value = serde_json::to_value(FusedHit {
            segment: Some(Segment {
                start_ms: 0,
                end_ms: 1_000,
            }),
            ..hit
        })
        .unwrap();
        assert_eq!(value["segment"]["end_ms"], 1_000);
    }

    #[tokio::test]
    async fn media_query_uses_validated_png() {
        let recorder = Arc::new(Recorder::default());
        let search = search(Arc::clone(&recorder));
        let response = search
            .search(FederatedSearchRequest {
                media: vec![EmbedInput::Image {
                    mime: Some("image/png".into()),
                    bytes_or_ref: MediaSource::Bytes(fixtures::png()),
                }],
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(response.hits.len(), 1);
        assert_eq!(response.surfaces[0].status, SurfaceStatus::Skipped);
    }
}
