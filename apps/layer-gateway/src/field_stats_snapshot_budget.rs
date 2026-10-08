//! Spend cap for the shared snapshot scan of namespaces with a reviewed budget
//! entry (`LAYER_FIELD_STATS_SNAPSHOT_BUDGET_CONFIG`).
//!
//! The legacy snapshot scan is the one scan that fills approximate field stats
//! when guarded capture is not installed. For a namespace listed here it runs
//! under the SAME durable ledger as guarded capture (`layer_transform::scan_budget`:
//! 963,000 micro-USD of reserved spend and 144 attempts per rolling 24 h, keyed
//! by provider and namespace, so capture and this path share one allowance):
//! an attempt is reserved before the first page, every page is reserved at its
//! reviewed ceiling before dispatch, receipts settle before the next page, and
//! nothing is refunded. A namespace NOT listed here keeps the previous uncapped
//! behavior; listing is how an operator puts a namespace under the cap.
//!
//! The review's `schema_identity` is the sha256 of the compact, key-sorted JSON of
//! the PROJECTED attributes' schema entries only (not the whole schema), so an
//! unrelated new attribute does not stop the scan; a change to a projected
//! attribute does.
//!
//! This is not capture: no fences, witness or exactness are involved, so the
//! stats it produces are always `approximate`. The review inputs are supplied
//! assertions, as in capture; there is no independent client authority on this
//! path, so the "actual client" binding is the configured store reference.
use crate::field_stats_applicability_registry::ApplicabilityReview;
use crate::field_stats_capture::ReviewedPageCalibration;
use crate::field_stats_estimate_adapter::StaticMetadataEstimate;
use crate::field_stats_scan_budget::{
    CalibratedPageEstimate, CaptureBudget, CurrentEstimateInputs, EstimateApplicability,
    ScanBudgetLease, ScanCostCeiling,
};
use layer_transform::scan_budget::{ScanBudgetScope, ScanPageCostBound};
use layer_transform::udf::UdfStore;
use serde_json::Value;
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, RwLock};

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotBudgetConfiguration {
    pub store_ref: String,
    pub namespaces: Vec<SnapshotBudgetNamespace>,
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotBudgetNamespace {
    pub review: ApplicabilityReview,
    pub calibration: ReviewedPageCalibration,
    pub metadata_estimate: StaticMetadataEstimate,
}

impl SnapshotBudgetConfiguration {
    /// None keeps the previous behavior for every namespace.
    pub fn load_from_env() -> Result<Option<Self>, String> {
        let Some(path) = std::env::var_os("LAYER_FIELD_STATS_SNAPSHOT_BUDGET_CONFIG") else {
            return Ok(None);
        };
        let bytes = std::fs::read(path).map_err(|_| "snapshot budget configuration unreadable")?;
        Self::parse_at_startup(&bytes, now()?).map(Some)
    }
    /// Startup must not take the gateway down because a review window lapsed
    /// while the process was running or between renders. An expired entry still
    /// loads (so its namespace stays under the cap) and every page estimate
    /// refuses against the real clock, i.e. the scan fails closed until the
    /// file is re-rendered and the gateway restarts. Structure and every other
    /// check stay strict.
    pub fn parse_at_startup(bytes: &[u8], now: u64) -> Result<Self, String> {
        let lenient = |d: &Self| -> u64 {
            d.namespaces
                .iter()
                .map(|n| {
                    n.review
                        .valid_until_unix_seconds
                        .min(n.calibration.valid_until_unix_seconds)
                        .min(n.metadata_estimate.valid_until_unix_seconds)
                })
                .min()
                .unwrap_or(0)
        };
        let config: Self = serde_json::from_slice(bytes)
            .map_err(|_| "snapshot budget configuration malformed or incomplete")?;
        let earliest_end = lenient(&config);
        if earliest_end <= now {
            tracing::warn!(
                "Snapshot budget review expired; covered namespaces will refuse to scan until it is re-rendered"
            );
        }
        // Validate as of one second before the earliest window end, never later
        // than now, so only expiry is excused.
        Self::parse(bytes, now.min(earliest_end.saturating_sub(1)))
    }
    pub fn parse(bytes: &[u8], now: u64) -> Result<Self, String> {
        let config: Self = serde_json::from_slice(bytes)
            .map_err(|_| "snapshot budget configuration malformed or incomplete")?;
        let mut seen = BTreeSet::new();
        if config.store_ref.trim().is_empty()
            || config.namespaces.is_empty()
            || config
                .namespaces
                .iter()
                .any(|n| !seen.insert(n.review.namespace.as_str()))
        {
            return Err("snapshot budget needs a store and unique namespaces".into());
        }
        for n in &config.namespaces {
            crate::field_stats_capture_bootstrap::validate_reviewed_budget(
                &n.review,
                &n.calibration,
                &n.metadata_estimate,
                now,
            )?;
        }
        Ok(config)
    }
}

fn now() -> Result<u64, String> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .map_err(|_| "budget clock unavailable".into())
}

/// Per-namespace page-cost ceiling. Inputs are sampled once per attempt from the
/// reconcile's own metadata read; the reviewed `maximum_input_age_seconds` must
/// cover a whole scan and the reviewed uplift covers growth inside it.
pub struct NamespaceSnapshotBudget {
    model: CalibratedPageEstimate,
    inputs: RwLock<Option<CurrentEstimateInputs>>,
}
impl NamespaceSnapshotBudget {
    fn new(store_ref: &str, n: SnapshotBudgetNamespace) -> Self {
        let review = n.review;
        let c = n.calibration;
        Self {
            model: CalibratedPageEstimate {
                policy_id: review.model_revision,
                namespace: review.namespace,
                fields: review.projection,
                schema_identity: review.schema_identity,
                applicability: EstimateApplicability {
                    actual_client_binding: format!("snapshot-scan/{store_ref}"),
                    account_binding: review.account_binding,
                    pricing_revision: review.pricing_revision,
                    calibration_identity: review.calibration_revision,
                    temperature: review.temperature,
                },
                baseline_rows: c.baseline_rows,
                baseline_namespace_bytes: c.baseline_namespace_bytes,
                measured_max_queried_bytes: c.measured_max_queried_bytes,
                measured_max_returned_bytes: c.measured_max_returned_bytes,
                uplift_numerator: c.uplift_numerator,
                uplift_denominator: c.uplift_denominator,
                valid_until_unix_seconds: c.valid_until_unix_seconds,
                maximum_input_age_seconds: c.maximum_input_age_seconds,
            },
            inputs: RwLock::new(None),
        }
    }
    /// Missing or malformed size/schema metadata leaves the inputs unusable, so
    /// admission refuses (fail closed) rather than assuming a size.
    pub fn sample(&self, meta: &Value, at_unix_seconds: u64) {
        let rows = meta.get("approx_row_count").and_then(Value::as_u64);
        let bytes = meta.get("approx_logical_bytes").and_then(Value::as_u64);
        // The scan reads only the projected attributes, so its cost model is tied
        // to THEIR schema. Hashing the whole schema would refuse every scan each
        // time a pipeline or Function adds an unrelated attribute. A projected
        // attribute missing from the schema leaves no identity and refuses.
        let schema = meta
            .get("schema")
            .and_then(Value::as_object)
            .and_then(|all| {
                let projected: serde_json::Map<String, Value> = self
                    .model
                    .fields
                    .iter()
                    .map(|f| all.get(f).map(|v| (f.clone(), v.clone())))
                    .collect::<Option<_>>()?;
                crate::field_stats_estimate_adapter::schema_identity(&Value::Object(projected)).ok()
            });
        *self.inputs.write().unwrap() = match (rows, bytes, schema) {
            (Some(rows), Some(namespace_bytes), Some(schema_identity)) => {
                Some(CurrentEstimateInputs {
                    applicability: Some(self.model.applicability.clone()),
                    namespace: self.model.namespace.clone(),
                    schema_identity,
                    rows,
                    namespace_bytes,
                    sampled_at_unix_seconds: at_unix_seconds,
                })
            }
            _ => None,
        };
    }
}
impl ScanCostCeiling for NamespaceSnapshotBudget {
    fn page_bound(
        &self,
        namespace: &str,
        fields: &[String],
    ) -> Result<Option<ScanPageCostBound>, String> {
        if namespace != self.model.namespace {
            return Err("snapshot budget namespace mismatch".into());
        }
        let inputs = self.inputs.read().unwrap().clone();
        Ok(inputs.and_then(|i| self.model.estimate(fields, &i, now().ok()?)))
    }
}

pub struct SnapshotBudgets {
    store_ref: String,
    by_namespace: HashMap<String, Arc<NamespaceSnapshotBudget>>,
}
impl SnapshotBudgets {
    pub fn from_configuration(config: SnapshotBudgetConfiguration) -> Self {
        let store_ref = config.store_ref;
        let by_namespace = config
            .namespaces
            .into_iter()
            .map(|n| {
                (
                    n.review.namespace.clone(),
                    Arc::new(NamespaceSnapshotBudget::new(&store_ref, n)),
                )
            })
            .collect();
        Self {
            store_ref,
            by_namespace,
        }
    }
    pub fn namespaces(&self) -> Vec<String> {
        let mut names: Vec<String> = self.by_namespace.keys().cloned().collect();
        names.sort();
        names
    }
    pub fn covers(&self, namespace: &str) -> bool {
        self.by_namespace.contains_key(namespace)
    }
    /// Reserve an attempt against the shared ledger and return the lease whose
    /// permit and billing observer gate every page. Refuses when the namespace
    /// is uncovered, the inputs are unusable, or the ledger denies admission.
    pub async fn begin(
        &self,
        store: Option<Arc<dyn UdfStore>>,
        namespace: &str,
        projection: &[String],
        meta: &Value,
    ) -> Result<Arc<dyn ScanBudgetLease>, String> {
        let budget = self
            .by_namespace
            .get(namespace)
            .ok_or("namespace has no reviewed snapshot budget")?
            .clone();
        let store = store.ok_or("snapshot scan budget needs the shared financial store")?;
        budget.sample(meta, now()?);
        CaptureBudget {
            store,
            ceiling: budget,
        }
        .begin(
            ScanBudgetScope {
                provider: "turbopuffer".into(),
                store: self.store_ref.clone(),
                domain: "snapshot-scan".into(),
                namespace: namespace.into(),
            },
            projection.to_vec(),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn schema() -> Value {
        json!({"doc_type": {"type": "string", "filterable": true}})
    }
    fn config(now: u64, edit: impl FnOnce(&mut Value)) -> Vec<u8> {
        let mut v = json!({
            "store_ref": "s",
            "namespaces": [{
                "review": {
                    "version": 1, "namespace": "ns", "account_binding": "a",
                    "account_association_evidence": "e", "pricing_revision": "tpuf-public-enterprise-2026-10-04",
                    "model_revision": "m", "calibration_revision": "c", "calibration_evidence": "e",
                    "temperature": "hot",
                    "schema_identity": crate::field_stats_estimate_adapter::schema_identity(&schema()).unwrap(),
                    "projection": ["doc_type"], "valid_from_unix_seconds": 0, "valid_until_unix_seconds": now + 1000
                },
                "calibration": {
                    "measured_page_size": 1000, "baseline_rows": 100, "baseline_namespace_bytes": 1000,
                    "measured_max_queried_bytes": 1_000_000_000, "measured_max_returned_bytes": 1000,
                    "uplift_numerator": 2, "uplift_denominator": 1,
                    "valid_until_unix_seconds": now + 1000, "maximum_input_age_seconds": 600
                },
                "metadata_estimate": {"policy_id": "p", "maximum_micro_usd": 32, "valid_until_unix_seconds": now + 1000}
            }]
        });
        edit(&mut v);
        serde_json::to_vec(&v).unwrap()
    }
    fn budget() -> NamespaceSnapshotBudget {
        let now = super::now().unwrap();
        let c = SnapshotBudgetConfiguration::parse(&config(now, |_| {}), now).unwrap();
        let store = c.store_ref.clone();
        NamespaceSnapshotBudget::new(&store, c.namespaces.into_iter().next().unwrap())
    }
    fn meta(rows: u64, bytes: u64) -> Value {
        json!({"approx_row_count": rows, "approx_logical_bytes": bytes, "schema": schema()})
    }
    fn fields() -> Vec<String> {
        vec!["doc_type".into()]
    }

    #[test]
    fn field_stats_snapshot_budget_estimates_only_the_reviewed_projection_and_size() {
        let b = budget();
        let now = super::now().unwrap();
        // No sampled inputs: refuse.
        assert!(b.page_bound("ns", &fields()).unwrap().is_none());
        b.sample(&meta(100, 1000), now);
        let at_baseline = b.page_bound("ns", &fields()).unwrap().unwrap();
        assert!(at_baseline.maximum_micro_usd > 0);
        // Growth scales the ceiling; shrinkage never lowers it.
        b.sample(&meta(100, 2000), now);
        let grown = b.page_bound("ns", &fields()).unwrap().unwrap();
        assert!(grown.maximum_micro_usd > at_baseline.maximum_micro_usd);
        b.sample(&meta(10, 100), now);
        assert_eq!(
            b.page_bound("ns", &fields())
                .unwrap()
                .unwrap()
                .maximum_micro_usd,
            at_baseline.maximum_micro_usd
        );
        // A different projection, namespace, schema or stale input refuses.
        assert!(b
            .page_bound("ns", &["other".to_string()])
            .unwrap()
            .is_none());
        assert!(b.page_bound("other", &fields()).is_err());
        b.sample(&meta(100, 1000), now - 601);
        assert!(b.page_bound("ns", &fields()).unwrap().is_none());
        // An unrelated new attribute keeps the review valid; a projected
        // attribute that changed or vanished does not.
        let mut unrelated = meta(100, 1000);
        unrelated["schema"]["new_attribute"] = json!({"type": "string"});
        b.sample(&unrelated, now);
        assert!(b.page_bound("ns", &fields()).unwrap().is_some());
        let mut retyped = meta(100, 1000);
        retyped["schema"]["doc_type"] = json!({"type": "uint", "filterable": true});
        b.sample(&retyped, now);
        assert!(b.page_bound("ns", &fields()).unwrap().is_none());
        let mut dropped = meta(100, 1000);
        dropped["schema"]
            .as_object_mut()
            .unwrap()
            .remove("doc_type");
        b.sample(&dropped, now);
        assert!(b.page_bound("ns", &fields()).unwrap().is_none());
        // Metadata without sizes cannot be assumed.
        b.sample(&json!({"schema": schema()}), now);
        assert!(b.page_bound("ns", &fields()).unwrap().is_none());
    }

    /// Deployer check: parse a rendered configuration exactly as startup does
    /// (a refused file panics the gateway), e.g.
    /// `SNAPSHOT_BUDGET_CONFIG_UNDER_REVIEW=/path cargo test ... -- --ignored`.
    #[test]
    #[ignore = "requires SNAPSHOT_BUDGET_CONFIG_UNDER_REVIEW"]
    fn field_stats_snapshot_budget_rendered_configuration_is_accepted() {
        let path = std::env::var("SNAPSHOT_BUDGET_CONFIG_UNDER_REVIEW")
            .expect("path of the rendered configuration required");
        let config = SnapshotBudgetConfiguration::parse(
            &std::fs::read(path).unwrap(),
            super::now().unwrap(),
        )
        .expect("configuration refused");
        println!(
            "accepted: store {} namespaces {:?}",
            config.store_ref,
            config
                .namespaces
                .iter()
                .map(|n| &n.review.namespace)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn field_stats_snapshot_budget_configuration_requires_explicit_active_review() {
        let now = super::now().unwrap();
        let parse =
            |edit: fn(&mut Value)| SnapshotBudgetConfiguration::parse(&config(now, edit), now);
        assert!(parse(|_| {}).is_ok());
        assert!(parse(|v| v["namespaces"] = json!([])).is_err());
        assert!(parse(|v| v["store_ref"] = json!(" ")).is_err());
        assert!(parse(|v| {
            let n = v["namespaces"][0].clone();
            v["namespaces"] = json!([n.clone(), n]);
        })
        .is_err());
        assert!(
            parse(|v| v["namespaces"][0]["calibration"]["uplift_numerator"] = json!(0)).is_err()
        );
        assert!(
            parse(|v| v["namespaces"][0]["calibration"]["measured_page_size"] = json!(500))
                .is_err()
        );
        assert!(
            parse(|v| v["namespaces"][0]["metadata_estimate"]["maximum_micro_usd"] = json!(0))
                .is_err()
        );
        assert!(parse(|v| v["namespaces"][0]["unknown"] = json!(1)).is_err());
        assert!(SnapshotBudgetConfiguration::parse(&config(now, |_| {}), now + 5000).is_err());
        // Startup tolerates a lapsed window (the scan then refuses) but not a
        // malformed or inconsistent file.
        let expired =
            SnapshotBudgetConfiguration::parse_at_startup(&config(now, |_| {}), now + 5000)
                .expect("expired review still loads");
        let store = expired.store_ref.clone();
        let b =
            NamespaceSnapshotBudget::new(&store, expired.namespaces.into_iter().next().unwrap());
        b.sample(&meta(100, 1000), now + 5000);
        assert!(b.page_bound("ns", &fields()).unwrap().is_none());
        assert!(SnapshotBudgetConfiguration::parse_at_startup(
            &config(now, |v| v["namespaces"][0]["calibration"]
                ["uplift_numerator"] = json!(0)),
            now + 5000
        )
        .is_err());
    }
}
