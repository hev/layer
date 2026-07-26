//! Cost data model, rate cards, and live billing readers.
//!
//! Turbopuffer spend is priced from the upstream `billing` object that the
//! metrics wrapper copies into Prometheus counters. AWS spend is read from
//! Cost Explorer, tag-scoped to the Layer stack, and cached in S3 so the API
//! fails open with stale data instead of hiding the last known invoice view.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration as StdDuration, SystemTime, UNIX_EPOCH};

use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::warn;

use crate::AppState;

const DECIMAL_GB: f64 = 1_000_000_000.0;
const DECIMAL_TB: f64 = 1_000_000_000_000.0;

const TPUF_WRITES_RATE: f64 = 2.00;
const TPUF_QUERIES_SCANNED_RATE: f64 = 0.001;
const TPUF_QUERIES_RETURNED_RATE: f64 = 0.05;
const TPUF_STORAGE_RATE: f64 = 0.0004517083;
const MILLION_TOKENS: f64 = 1_000_000.0;

const TPUF_EMBEDDING_RATES: &[(&str, f64)] = &[
    ("baai/bge-m3", 0.01),
    ("cohere/embed-english-v3.0", 0.10),
    ("cohere/embed-v4.0", 0.12),
    ("google/gemini-embedding-001", 0.15),
    ("google/gemini-embedding-2", 0.20),
    ("qwen/qwen3-embedding-0p6b", 0.01),
    ("qwen/qwen3-embedding-4b", 0.02),
    ("qwen/qwen3-embedding-8b", 0.05),
    ("voyage/voyage-4", 0.06),
    ("voyage/voyage-4-large", 0.12),
    ("voyage/voyage-4-lite", 0.02),
    ("voyage/voyage-code-3", 0.18),
];

const AWS_COST_SOURCE: &str = "cost_explorer";
const AWS_ESTIMATOR_REFRESHED_AT_MS: u64 = 1_781_568_000_000; // 2026-06-16T00:00:00Z
const AWS_ESTIMATOR_TTL_SECONDS: u64 = 86_400;
const COST_SAMPLE_INTERVAL: StdDuration = StdDuration::from_secs(60);
const COST_SAMPLE_STARTUP_DELAY: StdDuration = StdDuration::from_secs(5);

struct AwsInstancePriceStatic {
    instance_type: &'static str,
    hourly_usd: f64,
}

// us-east-1 on-demand Linux prices. This mirrors the dashboard's live
// estimator table so `/v2/cost/rate-card` exposes the estimator intent in
// the gateway API while Cost Explorer remains the authoritative AWS total.
const AWS_INSTANCE_PRICE_TABLE: &[AwsInstancePriceStatic] = &[
    AwsInstancePriceStatic {
        instance_type: "t3.micro",
        hourly_usd: 0.0104,
    },
    AwsInstancePriceStatic {
        instance_type: "t3.small",
        hourly_usd: 0.0208,
    },
    AwsInstancePriceStatic {
        instance_type: "t3.medium",
        hourly_usd: 0.0416,
    },
    AwsInstancePriceStatic {
        instance_type: "t3.large",
        hourly_usd: 0.0832,
    },
    AwsInstancePriceStatic {
        instance_type: "t3.xlarge",
        hourly_usd: 0.1664,
    },
    AwsInstancePriceStatic {
        instance_type: "t3.2xlarge",
        hourly_usd: 0.3328,
    },
    AwsInstancePriceStatic {
        instance_type: "m5.large",
        hourly_usd: 0.096,
    },
    AwsInstancePriceStatic {
        instance_type: "m5.xlarge",
        hourly_usd: 0.192,
    },
    AwsInstancePriceStatic {
        instance_type: "m5.2xlarge",
        hourly_usd: 0.384,
    },
    AwsInstancePriceStatic {
        instance_type: "m5.4xlarge",
        hourly_usd: 0.768,
    },
    AwsInstancePriceStatic {
        instance_type: "m5.8xlarge",
        hourly_usd: 1.536,
    },
    AwsInstancePriceStatic {
        instance_type: "m6i.large",
        hourly_usd: 0.0966,
    },
    AwsInstancePriceStatic {
        instance_type: "m6i.xlarge",
        hourly_usd: 0.1933,
    },
    AwsInstancePriceStatic {
        instance_type: "m6i.2xlarge",
        hourly_usd: 0.3866,
    },
    AwsInstancePriceStatic {
        instance_type: "m6i.4xlarge",
        hourly_usd: 0.7733,
    },
    AwsInstancePriceStatic {
        instance_type: "m6i.8xlarge",
        hourly_usd: 1.5466,
    },
    AwsInstancePriceStatic {
        instance_type: "m7i.large",
        hourly_usd: 0.1008,
    },
    AwsInstancePriceStatic {
        instance_type: "m7i.xlarge",
        hourly_usd: 0.2016,
    },
    AwsInstancePriceStatic {
        instance_type: "m7i.2xlarge",
        hourly_usd: 0.4032,
    },
    AwsInstancePriceStatic {
        instance_type: "m7i.4xlarge",
        hourly_usd: 0.8064,
    },
    AwsInstancePriceStatic {
        instance_type: "m7i.8xlarge",
        hourly_usd: 1.6128,
    },
    AwsInstancePriceStatic {
        instance_type: "c5.large",
        hourly_usd: 0.085,
    },
    AwsInstancePriceStatic {
        instance_type: "c5.xlarge",
        hourly_usd: 0.17,
    },
    AwsInstancePriceStatic {
        instance_type: "c5.2xlarge",
        hourly_usd: 0.34,
    },
    AwsInstancePriceStatic {
        instance_type: "c5.4xlarge",
        hourly_usd: 0.68,
    },
    AwsInstancePriceStatic {
        instance_type: "c6i.large",
        hourly_usd: 0.085,
    },
    AwsInstancePriceStatic {
        instance_type: "c6i.xlarge",
        hourly_usd: 0.17,
    },
    AwsInstancePriceStatic {
        instance_type: "c6i.2xlarge",
        hourly_usd: 0.34,
    },
    AwsInstancePriceStatic {
        instance_type: "c6i.4xlarge",
        hourly_usd: 0.68,
    },
    AwsInstancePriceStatic {
        instance_type: "r5.large",
        hourly_usd: 0.126,
    },
    AwsInstancePriceStatic {
        instance_type: "r5.xlarge",
        hourly_usd: 0.252,
    },
    AwsInstancePriceStatic {
        instance_type: "r5.2xlarge",
        hourly_usd: 0.504,
    },
    AwsInstancePriceStatic {
        instance_type: "r5.4xlarge",
        hourly_usd: 1.008,
    },
    AwsInstancePriceStatic {
        instance_type: "r6i.large",
        hourly_usd: 0.126,
    },
    AwsInstancePriceStatic {
        instance_type: "r6i.xlarge",
        hourly_usd: 0.252,
    },
    AwsInstancePriceStatic {
        instance_type: "r6i.2xlarge",
        hourly_usd: 0.504,
    },
    AwsInstancePriceStatic {
        instance_type: "r6i.4xlarge",
        hourly_usd: 1.008,
    },
    AwsInstancePriceStatic {
        instance_type: "i3.large",
        hourly_usd: 0.156,
    },
    AwsInstancePriceStatic {
        instance_type: "i3.xlarge",
        hourly_usd: 0.312,
    },
    AwsInstancePriceStatic {
        instance_type: "i3.2xlarge",
        hourly_usd: 0.624,
    },
    AwsInstancePriceStatic {
        instance_type: "i4i.large",
        hourly_usd: 0.172,
    },
    AwsInstancePriceStatic {
        instance_type: "i4i.xlarge",
        hourly_usd: 0.343,
    },
    AwsInstancePriceStatic {
        instance_type: "i4i.2xlarge",
        hourly_usd: 0.686,
    },
    AwsInstancePriceStatic {
        instance_type: "m6id.large",
        hourly_usd: 0.1187,
    },
    AwsInstancePriceStatic {
        instance_type: "m6id.xlarge",
        hourly_usd: 0.2373,
    },
    AwsInstancePriceStatic {
        instance_type: "m6id.2xlarge",
        hourly_usd: 0.4746,
    },
    AwsInstancePriceStatic {
        instance_type: "c6a.large",
        hourly_usd: 0.0765,
    },
    AwsInstancePriceStatic {
        instance_type: "c6a.xlarge",
        hourly_usd: 0.153,
    },
    AwsInstancePriceStatic {
        instance_type: "c6a.2xlarge",
        hourly_usd: 0.306,
    },
    AwsInstancePriceStatic {
        instance_type: "c6a.4xlarge",
        hourly_usd: 0.612,
    },
    AwsInstancePriceStatic {
        instance_type: "c6a.8xlarge",
        hourly_usd: 1.224,
    },
    AwsInstancePriceStatic {
        instance_type: "c7a.medium",
        hourly_usd: 0.02185,
    },
    AwsInstancePriceStatic {
        instance_type: "c7a.large",
        hourly_usd: 0.04369,
    },
    AwsInstancePriceStatic {
        instance_type: "c7a.xlarge",
        hourly_usd: 0.08739,
    },
    AwsInstancePriceStatic {
        instance_type: "c7a.2xlarge",
        hourly_usd: 0.17478,
    },
    AwsInstancePriceStatic {
        instance_type: "c7a.4xlarge",
        hourly_usd: 0.34956,
    },
    AwsInstancePriceStatic {
        instance_type: "c7a.8xlarge",
        hourly_usd: 0.69912,
    },
    AwsInstancePriceStatic {
        instance_type: "m6a.large",
        hourly_usd: 0.0864,
    },
    AwsInstancePriceStatic {
        instance_type: "m6a.xlarge",
        hourly_usd: 0.1728,
    },
    AwsInstancePriceStatic {
        instance_type: "m6a.2xlarge",
        hourly_usd: 0.3456,
    },
    AwsInstancePriceStatic {
        instance_type: "m6a.4xlarge",
        hourly_usd: 0.6912,
    },
    AwsInstancePriceStatic {
        instance_type: "m6a.8xlarge",
        hourly_usd: 1.3824,
    },
    AwsInstancePriceStatic {
        instance_type: "m7a.medium",
        hourly_usd: 0.05796,
    },
    AwsInstancePriceStatic {
        instance_type: "m7a.large",
        hourly_usd: 0.11592,
    },
    AwsInstancePriceStatic {
        instance_type: "m7a.xlarge",
        hourly_usd: 0.23184,
    },
    AwsInstancePriceStatic {
        instance_type: "m7a.2xlarge",
        hourly_usd: 0.46368,
    },
    AwsInstancePriceStatic {
        instance_type: "g5.xlarge",
        hourly_usd: 1.006,
    },
    AwsInstancePriceStatic {
        instance_type: "g5.2xlarge",
        hourly_usd: 1.212,
    },
    AwsInstancePriceStatic {
        instance_type: "g5.4xlarge",
        hourly_usd: 1.624,
    },
    AwsInstancePriceStatic {
        instance_type: "g5.8xlarge",
        hourly_usd: 2.448,
    },
    AwsInstancePriceStatic {
        instance_type: "g5.12xlarge",
        hourly_usd: 5.672,
    },
    AwsInstancePriceStatic {
        instance_type: "g5.16xlarge",
        hourly_usd: 4.096,
    },
    AwsInstancePriceStatic {
        instance_type: "g5.24xlarge",
        hourly_usd: 8.144,
    },
    AwsInstancePriceStatic {
        instance_type: "g5.48xlarge",
        hourly_usd: 16.288,
    },
    AwsInstancePriceStatic {
        instance_type: "g6.xlarge",
        hourly_usd: 0.8048,
    },
    AwsInstancePriceStatic {
        instance_type: "g6.2xlarge",
        hourly_usd: 0.9776,
    },
    AwsInstancePriceStatic {
        instance_type: "g6e.xlarge",
        hourly_usd: 1.861,
    },
    AwsInstancePriceStatic {
        instance_type: "g6e.2xlarge",
        hourly_usd: 2.24208,
    },
];

/// Code-resident Turbopuffer rate card. Bump `version` and `verified_at`
/// whenever rates are re-checked against a real invoice.
pub const TURBOPUFFER_RATE_CARD: TurbopufferRateCardStatic = TurbopufferRateCardStatic {
    version: "2026-07",
    verified_by: "hev",
    verified_at: "2026-07-25",
    source: "invoice+published_embedding_prices",
    lines: &[
        TurbopufferRateLineStatic {
            service: "tpuf_writes",
            unit: "logical_gb_written",
            usd: TPUF_WRITES_RATE,
        },
        TurbopufferRateLineStatic {
            service: "tpuf_queries_scanned",
            unit: "tb_queried",
            usd: TPUF_QUERIES_SCANNED_RATE,
        },
        TurbopufferRateLineStatic {
            service: "tpuf_queries_returned",
            unit: "gb_returned",
            usd: TPUF_QUERIES_RETURNED_RATE,
        },
        TurbopufferRateLineStatic {
            service: "tpuf_storage",
            unit: "logical_gb_hour",
            usd: TPUF_STORAGE_RATE,
        },
        TurbopufferRateLineStatic {
            service: "tpuf_embeddings:baai/bge-m3",
            unit: "million_tokens",
            usd: 0.01,
        },
        TurbopufferRateLineStatic {
            service: "tpuf_embeddings:cohere/embed-english-v3.0",
            unit: "million_tokens",
            usd: 0.10,
        },
        TurbopufferRateLineStatic {
            service: "tpuf_embeddings:cohere/embed-v4.0",
            unit: "million_tokens",
            usd: 0.12,
        },
        TurbopufferRateLineStatic {
            service: "tpuf_embeddings:google/gemini-embedding-001",
            unit: "million_tokens",
            usd: 0.15,
        },
        TurbopufferRateLineStatic {
            service: "tpuf_embeddings:google/gemini-embedding-2",
            unit: "million_tokens",
            usd: 0.20,
        },
        TurbopufferRateLineStatic {
            service: "tpuf_embeddings:qwen/qwen3-embedding-0p6b",
            unit: "million_tokens",
            usd: 0.01,
        },
        TurbopufferRateLineStatic {
            service: "tpuf_embeddings:qwen/qwen3-embedding-4b",
            unit: "million_tokens",
            usd: 0.02,
        },
        TurbopufferRateLineStatic {
            service: "tpuf_embeddings:qwen/qwen3-embedding-8b",
            unit: "million_tokens",
            usd: 0.05,
        },
        TurbopufferRateLineStatic {
            service: "tpuf_embeddings:voyage/voyage-4",
            unit: "million_tokens",
            usd: 0.06,
        },
        TurbopufferRateLineStatic {
            service: "tpuf_embeddings:voyage/voyage-4-large",
            unit: "million_tokens",
            usd: 0.12,
        },
        TurbopufferRateLineStatic {
            service: "tpuf_embeddings:voyage/voyage-4-lite",
            unit: "million_tokens",
            usd: 0.02,
        },
        TurbopufferRateLineStatic {
            service: "tpuf_embeddings:voyage/voyage-code-3",
            unit: "million_tokens",
            usd: 0.18,
        },
    ],
};

pub struct TurbopufferRateCardStatic {
    pub version: &'static str,
    pub verified_by: &'static str,
    pub verified_at: &'static str,
    pub source: &'static str,
    pub lines: &'static [TurbopufferRateLineStatic],
}

pub struct TurbopufferRateLineStatic {
    pub service: &'static str,
    pub unit: &'static str,
    pub usd: f64,
}

impl TurbopufferRateCardStatic {
    pub fn to_owned_card(&self) -> TurbopufferRateCard {
        TurbopufferRateCard {
            version: self.version.to_string(),
            verified_by: self.verified_by.to_string(),
            verified_at: self.verified_at.to_string(),
            source: self.source.to_string(),
            lines: self
                .lines
                .iter()
                .map(|l| TurbopufferRateLine {
                    service: l.service.to_string(),
                    unit: l.unit.to_string(),
                    usd: l.usd,
                })
                .collect(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AwsCostConfig {
    pub enabled: bool,
    pub region: String,
    pub tag_key: String,
    pub tag_value: String,
    pub site: String,
    pub cache_ttl_seconds: u64,
}

impl Default for AwsCostConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            region: "us-east-1".to_string(),
            tag_key: "Project".to_string(),
            tag_value: "hevlayer".to_string(),
            site: "main".to_string(),
            cache_ttl_seconds: 86_400,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
pub enum CostWindow {
    #[serde(rename = "1h")]
    OneHour,
    #[serde(rename = "6h")]
    SixHours,
    #[serde(rename = "24h")]
    #[default]
    TwentyFourHours,
    #[serde(rename = "7d")]
    SevenDays,
    #[serde(rename = "30d")]
    ThirtyDays,
}

impl CostWindow {
    pub fn seconds(self) -> u64 {
        match self {
            CostWindow::OneHour => 3_600,
            CostWindow::SixHours => 6 * 3_600,
            CostWindow::TwentyFourHours => 24 * 3_600,
            CostWindow::SevenDays => 7 * 24 * 3_600,
            CostWindow::ThirtyDays => 30 * 24 * 3_600,
        }
    }

    pub fn prom_range(self) -> &'static str {
        match self {
            CostWindow::OneHour => "1h",
            CostWindow::SixHours => "6h",
            CostWindow::TwentyFourHours => "24h",
            CostWindow::SevenDays => "7d",
            CostWindow::ThirtyDays => "30d",
        }
    }

    pub fn default_step(self) -> CostStep {
        match self {
            CostWindow::OneHour | CostWindow::SixHours => CostStep::FiveMinutes,
            CostWindow::TwentyFourHours => CostStep::ThirtyMinutes,
            CostWindow::SevenDays => CostStep::OneHour,
            CostWindow::ThirtyDays => CostStep::SixHours,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
pub enum CostStep {
    #[serde(rename = "5m")]
    FiveMinutes,
    #[serde(rename = "30m")]
    ThirtyMinutes,
    #[serde(rename = "1h")]
    OneHour,
    #[serde(rename = "6h")]
    SixHours,
    #[serde(rename = "1d")]
    OneDay,
}

impl CostStep {
    pub fn seconds(self) -> u64 {
        match self {
            CostStep::FiveMinutes => 300,
            CostStep::ThirtyMinutes => 1_800,
            CostStep::OneHour => 3_600,
            CostStep::SixHours => 6 * 3_600,
            CostStep::OneDay => 24 * 3_600,
        }
    }

    pub fn prom_range(self) -> &'static str {
        match self {
            CostStep::FiveMinutes => "5m",
            CostStep::ThirtyMinutes => "30m",
            CostStep::OneHour => "1h",
            CostStep::SixHours => "6h",
            CostStep::OneDay => "1d",
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CostBasis {
    Metered,
    Invoice,
    Estimate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostTotals {
    pub total_usd: f64,
    pub aws_usd: f64,
    pub turbopuffer_usd: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cost_per_query_usd: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cost_per_document_usd: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cost_per_tib_indexed_usd: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostLine {
    pub provider: String,
    pub service: String,
    pub basis: CostBasis,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_detail: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub site: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_card_version: Option<String>,
    pub amount_usd: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub qty: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unit: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub qty_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub breakdown: Option<Vec<Value>>,
}

impl CostLine {
    fn authoritative(&self) -> bool {
        matches!(self.basis, CostBasis::Metered | CostBasis::Invoice)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostRateCardStatus {
    pub turbopuffer_rate_card_version: String,
    pub aws_cost_source: String,
    pub aws_cost_refreshed_at_ms: u64,
    pub aws_cost_stale: bool,
    pub aws_pricing_stale: bool,
    pub aws_pricing_refreshed_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostSnapshot {
    pub as_of_ms: u64,
    pub window_seconds: u64,
    pub totals: CostTotals,
    pub lines: Vec<CostLine>,
    pub rate_card_status: CostRateCardStatus,
    pub caveats: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostSample(pub i64, pub f64);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostSeries {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub basis: Option<CostBasis>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_detail: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub site: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_card_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    pub samples: Vec<CostSample>,
}

#[derive(Debug, Serialize)]
pub struct CostTimeseries {
    pub window_seconds: u64,
    pub step_seconds: u64,
    pub series: Vec<CostSeries>,
}

#[derive(Debug, Serialize)]
pub struct AwsInstancePrice {
    pub instance_type: String,
    pub family: String,
    pub vcpu: u32,
    pub memory_gib: f64,
    pub nvme_gib: f64,
    pub hourly_usd: f64,
}

#[derive(Debug, Serialize)]
pub struct AwsRateCard {
    pub role: String,
    pub region: String,
    pub refreshed_at_ms: u64,
    pub ttl_seconds: u64,
    pub stale: bool,
    pub items: Vec<AwsInstancePrice>,
}

#[derive(Debug, Serialize)]
pub struct TurbopufferRateLine {
    pub service: String,
    pub unit: String,
    pub usd: f64,
}

#[derive(Debug, Serialize)]
pub struct TurbopufferRateCard {
    pub version: String,
    pub verified_by: String,
    pub verified_at: String,
    pub source: String,
    pub lines: Vec<TurbopufferRateLine>,
}

#[derive(Debug, Serialize)]
pub struct RateCard {
    pub aws: AwsRateCard,
    pub turbopuffer: TurbopufferRateCard,
}

struct AwsCostLoad {
    lines: Vec<CostLine>,
    refreshed_at_ms: u64,
    stale: bool,
    caveats: Vec<String>,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn rounded_money(value: f64) -> f64 {
    (value * 100_000_000.0).round() / 100_000_000.0
}

fn bytes_to_u64(value: f64) -> Option<u64> {
    if value.is_finite() && value >= 0.0 {
        Some(value.round().min(u64::MAX as f64) as u64)
    } else {
        None
    }
}

fn tpuf_line(service: &str, unit: &str, qty: f64, rate: f64, qty_bytes: Option<u64>) -> CostLine {
    CostLine {
        provider: "turbopuffer".to_string(),
        service: service.to_string(),
        basis: CostBasis::Metered,
        service_detail: None,
        region: None,
        site: None,
        rate_card_version: Some(TURBOPUFFER_RATE_CARD.version.to_string()),
        amount_usd: rounded_money(qty * rate),
        qty: Some(qty),
        unit: Some(unit.to_string()),
        qty_bytes,
        breakdown: None,
    }
}

fn basis_label(basis: CostBasis) -> &'static str {
    match basis {
        CostBasis::Metered => "metered",
        CostBasis::Invoice => "invoice",
        CostBasis::Estimate => "estimate",
    }
}

fn basis_from_label(value: &str) -> Option<CostBasis> {
    Some(match value {
        "metered" => CostBasis::Metered,
        "invoice" => CostBasis::Invoice,
        "estimate" => CostBasis::Estimate,
        _ => return None,
    })
}

fn estimator_family(instance_type: &str) -> String {
    instance_type
        .split_once('.')
        .map(|(family, _)| family.to_string())
        .unwrap_or_else(|| instance_type.to_string())
}

fn standard_vcpu(size: &str) -> u32 {
    if size == "medium" {
        1
    } else if size == "large" {
        2
    } else if size == "xlarge" {
        4
    } else if let Some(multiplier) = size.strip_suffix("xlarge") {
        multiplier.parse::<u32>().unwrap_or(0).saturating_mul(4)
    } else {
        0
    }
}

fn estimator_shape(instance_type: &str) -> (u32, f64, f64) {
    let Some((family, size)) = instance_type.split_once('.') else {
        return (0, 0.0, 0.0);
    };
    let vcpu = match family {
        "t3" => match size {
            "micro" | "small" | "medium" | "large" => 2,
            "xlarge" => 4,
            "2xlarge" => 8,
            _ => 0,
        },
        _ => standard_vcpu(size),
    };
    let memory_gib = match family {
        "t3" => match size {
            "micro" => 1.0,
            "small" => 2.0,
            "medium" => 4.0,
            "large" => 8.0,
            "xlarge" => 16.0,
            "2xlarge" => 32.0,
            _ => 0.0,
        },
        "c5" | "c6i" | "c6a" | "c7a" => f64::from(vcpu) * 2.0,
        "m5" | "m6i" | "m7i" | "m6id" | "m6a" | "m7a" => f64::from(vcpu) * 4.0,
        "r5" | "r6i" | "i4i" => f64::from(vcpu) * 8.0,
        "i3" => match size {
            "large" => 15.25,
            "xlarge" => 30.5,
            "2xlarge" => 61.0,
            _ => 0.0,
        },
        "g5" | "g6" => f64::from(vcpu) * 4.0,
        "g6e" => f64::from(vcpu) * 8.0,
        _ => 0.0,
    };
    let nvme_gib = match instance_type {
        "m6id.large" => 118.0,
        "m6id.xlarge" => 237.0,
        "m6id.2xlarge" => 474.0,
        "i3.large" => 475.0,
        "i3.xlarge" => 950.0,
        "i3.2xlarge" => 1900.0,
        "i4i.large" => 468.0,
        "i4i.xlarge" => 937.0,
        "i4i.2xlarge" => 1875.0,
        "g5.xlarge" => 250.0,
        "g5.2xlarge" => 450.0,
        "g5.4xlarge" => 600.0,
        "g5.8xlarge" => 900.0,
        "g5.12xlarge" => 3800.0,
        "g5.16xlarge" => 1900.0,
        "g5.24xlarge" => 3800.0,
        "g5.48xlarge" => 7600.0,
        _ => 0.0,
    };

    (vcpu, memory_gib, nvme_gib)
}

fn aws_estimator_items() -> Vec<AwsInstancePrice> {
    AWS_INSTANCE_PRICE_TABLE
        .iter()
        .map(|price| {
            let (vcpu, memory_gib, nvme_gib) = estimator_shape(price.instance_type);
            AwsInstancePrice {
                instance_type: price.instance_type.to_string(),
                family: estimator_family(price.instance_type),
                vcpu,
                memory_gib,
                nvme_gib,
                hourly_usd: price.hourly_usd,
            }
        })
        .collect()
}

async fn read_tpuf_lines(
    metrics_backend_url: Option<&str>,
    window: CostWindow,
    caveats: &mut Vec<String>,
) -> (Vec<CostLine>, Option<f64>) {
    let Some(base_url) = metrics_backend_url else {
        caveats.push(
            "metrics backend not configured; Turbopuffer metered lines unavailable".to_string(),
        );
        return (Vec::new(), None);
    };

    let range = window.prom_range();
    let hours = window.seconds() as f64 / 3_600.0;
    let mut query_count = None;

    let written_bytes = query_scalar(
        base_url,
        &format!("sum(increase(hevlayer_tpuf_billable_bytes_written_total[{range}]))"),
    )
    .await;
    let queried_bytes = query_scalar(
        base_url,
        &format!("sum(increase(hevlayer_tpuf_billable_bytes_queried_total[{range}]))"),
    )
    .await;
    let returned_bytes = query_scalar(
        base_url,
        &format!("sum(increase(hevlayer_tpuf_billable_bytes_returned_total[{range}]))"),
    )
    .await;
    let avg_storage_bytes = query_scalar(
        base_url,
        &format!("sum(avg_over_time(hevlayer_tpuf_logical_bytes[{range}]))"),
    )
    .await;

    match query_scalar(
        base_url,
        &format!(
            "sum(increase(layer_query_shape_total{{status=\"ok\"}}[{range}])) + \
             sum(increase(hevlayer_multi_query_total{{status=\"ok\"}}[{range}])) + \
             sum(increase(hevlayer_hybrid_text_queries_total{{status=\"ok\"}}[{range}]))"
        ),
    )
    .await
    {
        Ok(count) if count > 0.0 => query_count = Some(count),
        Ok(_) => {}
        Err(error) => caveats.push(format!("query-count metric unavailable: {error}")),
    }

    let mut lines = Vec::new();
    match written_bytes {
        Ok(bytes) => lines.push(tpuf_line(
            "tpuf_writes",
            "logical_gb_written",
            bytes / DECIMAL_GB,
            TPUF_WRITES_RATE,
            bytes_to_u64(bytes),
        )),
        Err(error) => caveats.push(format!(
            "Turbopuffer write billing metric unavailable: {error}"
        )),
    }
    match queried_bytes {
        Ok(bytes) => lines.push(tpuf_line(
            "tpuf_queries_scanned",
            "tb_queried",
            bytes / DECIMAL_TB,
            TPUF_QUERIES_SCANNED_RATE,
            bytes_to_u64(bytes),
        )),
        Err(error) => caveats.push(format!(
            "Turbopuffer scanned billing metric unavailable: {error}"
        )),
    }
    match returned_bytes {
        Ok(bytes) => lines.push(tpuf_line(
            "tpuf_queries_returned",
            "gb_returned",
            bytes / DECIMAL_GB,
            TPUF_QUERIES_RETURNED_RATE,
            bytes_to_u64(bytes),
        )),
        Err(error) => caveats.push(format!(
            "Turbopuffer returned billing metric unavailable: {error}"
        )),
    }
    match avg_storage_bytes {
        Ok(bytes) => lines.push(tpuf_line(
            "tpuf_storage",
            "logical_gb_hour",
            (bytes / DECIMAL_GB) * hours,
            TPUF_STORAGE_RATE,
            None,
        )),
        Err(error) => caveats.push(format!(
            "Turbopuffer storage billing metric unavailable: {error}"
        )),
    }

    match query_instant_labeled(
        base_url,
        &format!("sum by (model) (increase(hevlayer_embed_tokens_total[{range}]))"),
    )
    .await
    {
        Ok(rows) => {
            for (labels, tokens) in rows {
                let Some(model) = labels.get("model") else {
                    continue;
                };
                let Some((_, rate)) = TPUF_EMBEDDING_RATES
                    .iter()
                    .find(|(candidate, _)| candidate == model)
                else {
                    caveats.push(format!(
                        "Turbopuffer embedding price unavailable for model {model}"
                    ));
                    continue;
                };
                let mut line = tpuf_line(
                    "tpuf_embeddings",
                    "million_tokens",
                    tokens / MILLION_TOKENS,
                    *rate,
                    None,
                );
                line.service_detail = Some(model.clone());
                lines.push(line);
            }
        }
        Err(error) => caveats.push(format!(
            "Turbopuffer embedding token metric unavailable: {error}"
        )),
    }

    (lines, query_count)
}

pub async fn current_snapshot(state: &AppState, window: CostWindow) -> CostSnapshot {
    let as_of_ms = now_ms();
    let mut caveats = Vec::new();
    let (mut lines, query_count) =
        read_tpuf_lines(state.metrics_backend_url.as_deref(), window, &mut caveats).await;

    let aws = load_aws_cost_lines(state, window).await;
    caveats.extend(aws.caveats.clone());
    lines.extend(aws.lines);

    let turbopuffer_usd: f64 = lines
        .iter()
        .filter(|line| line.provider == "turbopuffer" && line.authoritative())
        .map(|line| line.amount_usd)
        .sum();
    let aws_usd: f64 = lines
        .iter()
        .filter(|line| line.provider == "aws" && line.authoritative())
        .map(|line| line.amount_usd)
        .sum();
    let total_usd = rounded_money(aws_usd + turbopuffer_usd);
    let cost_per_query_usd = query_count
        .filter(|count| *count > 0.0)
        .map(|count| rounded_money(total_usd / count));

    CostSnapshot {
        as_of_ms,
        window_seconds: window.seconds(),
        totals: CostTotals {
            total_usd,
            aws_usd: rounded_money(aws_usd),
            turbopuffer_usd: rounded_money(turbopuffer_usd),
            cost_per_query_usd,
            cost_per_document_usd: None,
            cost_per_tib_indexed_usd: None,
        },
        lines,
        rate_card_status: CostRateCardStatus {
            turbopuffer_rate_card_version: TURBOPUFFER_RATE_CARD.version.to_string(),
            aws_cost_source: AWS_COST_SOURCE.to_string(),
            aws_cost_refreshed_at_ms: aws.refreshed_at_ms,
            aws_cost_stale: aws.stale,
            aws_pricing_stale: false,
            aws_pricing_refreshed_at_ms: AWS_ESTIMATOR_REFRESHED_AT_MS,
        },
        caveats,
    }
}

pub async fn current_timeseries(
    state: &AppState,
    window: CostWindow,
    step: CostStep,
) -> CostTimeseries {
    let Some(base_url) = state.metrics_backend_url.as_deref() else {
        return CostTimeseries {
            window_seconds: window.seconds(),
            step_seconds: step.seconds(),
            series: Vec::new(),
        };
    };

    let end = Utc::now().timestamp();
    let start = end.saturating_sub(window.seconds() as i64);

    match query_range_labeled(
        base_url,
        "hevlayer_cost_usd_per_hour{basis=~\"metered|invoice\"}",
        start,
        end,
        step.seconds(),
    )
    .await
    {
        Ok(rows) if !rows.is_empty() => {
            return CostTimeseries {
                window_seconds: window.seconds(),
                step_seconds: step.seconds(),
                series: cost_series_from_labeled_rows(rows),
            };
        }
        Ok(_) => {}
        Err(error) => {
            warn!(%error, "sampled cost timeseries query failed; falling back to Turbopuffer billing counters")
        }
    }

    let rate_window = step.prom_range();
    let mut series = Vec::new();
    let mut total = BTreeMap::<i64, f64>::new();

    let mut specs = vec![
        (
            "tpuf_writes",
            None,
            format!(
                "sum(rate(hevlayer_tpuf_billable_bytes_written_total[{rate_window}])) * 3600 / {DECIMAL_GB} * {TPUF_WRITES_RATE}"
            ),
        ),
        (
            "tpuf_queries_scanned",
            None,
            format!(
                "sum(rate(hevlayer_tpuf_billable_bytes_queried_total[{rate_window}])) * 3600 / {DECIMAL_TB} * {TPUF_QUERIES_SCANNED_RATE}"
            ),
        ),
        (
            "tpuf_queries_returned",
            None,
            format!(
                "sum(rate(hevlayer_tpuf_billable_bytes_returned_total[{rate_window}])) * 3600 / {DECIMAL_GB} * {TPUF_QUERIES_RETURNED_RATE}"
            ),
        ),
        (
            "tpuf_storage",
            None,
            format!("sum(hevlayer_tpuf_logical_bytes) / {DECIMAL_GB} * {TPUF_STORAGE_RATE}"),
        ),
    ];

    specs.extend(TPUF_EMBEDDING_RATES.iter().map(|(model, rate)| {
        (
            "tpuf_embeddings",
            Some((*model).to_string()),
            format!(
                "sum(rate(hevlayer_embed_tokens_total{{model=\"{}\"}}[{rate_window}])) * 3600 / {MILLION_TOKENS} * {rate}",
                prometheus_label_value(model)
            ),
        )
    }));

    for (service, service_detail, expr) in specs {
        match query_range(base_url, &expr, start, end, step.seconds()).await {
            Ok(samples) => {
                for sample in &samples {
                    *total.entry(sample.0).or_insert(0.0) += sample.1;
                }
                series.push(CostSeries {
                    provider: Some("turbopuffer".to_string()),
                    service: Some(service.to_string()),
                    basis: Some(CostBasis::Metered),
                    service_detail,
                    region: None,
                    site: None,
                    rate_card_version: Some(TURBOPUFFER_RATE_CARD.version.to_string()),
                    label: None,
                    samples,
                });
            }
            Err(error) => warn!(%service, %error, "cost timeseries query failed"),
        }
    }

    if !total.is_empty() {
        series.push(CostSeries {
            provider: None,
            service: None,
            basis: None,
            service_detail: None,
            region: None,
            site: None,
            rate_card_version: None,
            label: Some("total".to_string()),
            samples: total
                .into_iter()
                .map(|(ts, value)| CostSample(ts, rounded_money(value)))
                .collect(),
        });
    }

    CostTimeseries {
        window_seconds: window.seconds(),
        step_seconds: step.seconds(),
        series,
    }
}

fn cost_series_from_labeled_rows(
    mut rows: Vec<(BTreeMap<String, String>, Vec<CostSample>)>,
) -> Vec<CostSeries> {
    rows.sort_by(|(left, _), (right, _)| {
        (
            left.get("provider"),
            left.get("service"),
            left.get("service_detail"),
        )
            .cmp(&(
                right.get("provider"),
                right.get("service"),
                right.get("service_detail"),
            ))
    });

    let mut series = Vec::new();
    let mut total = BTreeMap::<i64, f64>::new();
    for (labels, samples) in rows {
        for sample in &samples {
            *total.entry(sample.0).or_insert(0.0) += sample.1;
        }
        series.push(CostSeries {
            provider: labels.get("provider").cloned(),
            service: labels.get("service").cloned(),
            basis: labels
                .get("basis")
                .and_then(|value| basis_from_label(value)),
            service_detail: labels.get("service_detail").cloned(),
            region: labels.get("region").cloned(),
            site: labels.get("site").cloned(),
            rate_card_version: labels.get("rate_card_version").cloned(),
            label: labels.get("label").cloned(),
            samples,
        });
    }

    if !total.is_empty() {
        series.push(CostSeries {
            provider: None,
            service: None,
            basis: None,
            service_detail: None,
            region: None,
            site: None,
            rate_card_version: None,
            label: Some("total".to_string()),
            samples: total
                .into_iter()
                .map(|(ts, value)| CostSample(ts, rounded_money(value)))
                .collect(),
        });
    }

    series
}

pub fn current_rate_card(state: Option<&AppState>) -> RateCard {
    RateCard {
        aws: aws_estimator_rate_card(state),
        turbopuffer: TURBOPUFFER_RATE_CARD.to_owned_card(),
    }
}

fn aws_estimator_rate_card(state: Option<&AppState>) -> AwsRateCard {
    let region = state
        .map(|state| state.aws_cost_config.region.clone())
        .unwrap_or_else(|| AwsCostConfig::default().region);
    AwsRateCard {
        role: "estimator".to_string(),
        region,
        refreshed_at_ms: AWS_ESTIMATOR_REFRESHED_AT_MS,
        ttl_seconds: AWS_ESTIMATOR_TTL_SECONDS,
        stale: false,
        items: aws_estimator_items(),
    }
}

async fn load_aws_cost_lines(state: &AppState, window: CostWindow) -> AwsCostLoad {
    let config = &state.aws_cost_config;
    if !config.enabled {
        return AwsCostLoad {
            lines: Vec::new(),
            refreshed_at_ms: 0,
            stale: true,
            caveats: vec!["AWS Cost Explorer integration disabled".to_string()],
        };
    }
    #[cfg(not(feature = "pro"))]
    {
        let _ = window;
        AwsCostLoad {
            lines: Vec::new(),
            refreshed_at_ms: 0,
            stale: true,
            caveats: vec!["AWS Cost Explorer integration requires the pro gateway".to_string()],
        }
    }
}

// AWS Cost Explorer live billing is pro-only and is not included in the public mirror.

async fn query_scalar(base_url: &str, expr: &str) -> Result<f64, String> {
    let url = format!("{}/api/v1/query", base_url.trim_end_matches('/'));
    let response: Value = reqwest::Client::new()
        .get(url)
        .query(&[("query", expr)])
        .send()
        .await
        .map_err(|e| e.to_string())?
        .error_for_status()
        .map_err(|e| e.to_string())?
        .json()
        .await
        .map_err(|e| e.to_string())?;
    prometheus_instant_value(&response)
        .ok_or_else(|| format!("empty Prometheus result for `{expr}`"))
}

async fn query_instant_labeled(
    base_url: &str,
    expr: &str,
) -> Result<Vec<(BTreeMap<String, String>, f64)>, String> {
    let url = format!("{}/api/v1/query", base_url.trim_end_matches('/'));
    let response: Value = reqwest::Client::new()
        .get(url)
        .query(&[("query", expr)])
        .send()
        .await
        .map_err(|e| e.to_string())?
        .error_for_status()
        .map_err(|e| e.to_string())?
        .json()
        .await
        .map_err(|e| e.to_string())?;
    Ok(prometheus_instant_labeled_values(&response))
}

async fn query_range(
    base_url: &str,
    expr: &str,
    start: i64,
    end: i64,
    step_seconds: u64,
) -> Result<Vec<CostSample>, String> {
    let url = format!("{}/api/v1/query_range", base_url.trim_end_matches('/'));
    let response: Value = reqwest::Client::new()
        .get(url)
        .query(&[
            ("query", expr.to_string()),
            ("start", start.to_string()),
            ("end", end.to_string()),
            ("step", step_seconds.to_string()),
        ])
        .send()
        .await
        .map_err(|e| e.to_string())?
        .error_for_status()
        .map_err(|e| e.to_string())?
        .json()
        .await
        .map_err(|e| e.to_string())?;
    Ok(prometheus_range_values(&response)
        .into_iter()
        .map(|sample| CostSample(sample.0, rounded_money(sample.1)))
        .collect())
}

async fn query_range_labeled(
    base_url: &str,
    expr: &str,
    start: i64,
    end: i64,
    step_seconds: u64,
) -> Result<Vec<(BTreeMap<String, String>, Vec<CostSample>)>, String> {
    let url = format!("{}/api/v1/query_range", base_url.trim_end_matches('/'));
    let response: Value = reqwest::Client::new()
        .get(url)
        .query(&[
            ("query", expr.to_string()),
            ("start", start.to_string()),
            ("end", end.to_string()),
            ("step", step_seconds.to_string()),
        ])
        .send()
        .await
        .map_err(|e| e.to_string())?
        .error_for_status()
        .map_err(|e| e.to_string())?
        .json()
        .await
        .map_err(|e| e.to_string())?;
    Ok(prometheus_range_labeled_values(&response))
}

fn prometheus_instant_value(response: &Value) -> Option<f64> {
    response
        .get("data")?
        .get("result")?
        .as_array()?
        .first()?
        .get("value")?
        .as_array()
        .and_then(|value| value.get(1))?
        .as_str()?
        .parse()
        .ok()
}

fn prometheus_instant_labeled_values(response: &Value) -> Vec<(BTreeMap<String, String>, f64)> {
    response
        .get("data")
        .and_then(|data| data.get("result"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|result| {
            let labels = result
                .get("metric")
                .and_then(Value::as_object)?
                .iter()
                .filter_map(|(key, value)| Some((key.clone(), value.as_str()?.to_string())))
                .collect();
            let value = result
                .get("value")?
                .as_array()?
                .get(1)?
                .as_str()?
                .parse()
                .ok()?;
            Some((labels, value))
        })
        .collect()
}

fn prometheus_range_values(response: &Value) -> Vec<(i64, f64)> {
    response
        .get("data")
        .and_then(|data| data.get("result"))
        .and_then(Value::as_array)
        .and_then(|results| results.first())
        .and_then(|result| result.get("values"))
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(|sample| {
                    let pair = sample.as_array()?;
                    let ts = pair.first()?.as_f64()? as i64;
                    let value = pair.get(1)?.as_str()?.parse::<f64>().ok()?;
                    Some((ts, value))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn prometheus_range_labeled_values(
    response: &Value,
) -> Vec<(BTreeMap<String, String>, Vec<CostSample>)> {
    response
        .get("data")
        .and_then(|data| data.get("result"))
        .and_then(Value::as_array)
        .map(|results| {
            results
                .iter()
                .filter_map(|result| {
                    let labels = result
                        .get("metric")
                        .and_then(Value::as_object)
                        .map(|labels| {
                            labels
                                .iter()
                                .filter_map(|(key, value)| {
                                    Some((key.clone(), value.as_str()?.to_string()))
                                })
                                .collect::<BTreeMap<_, _>>()
                        })
                        .unwrap_or_default();
                    let samples = result
                        .get("values")
                        .and_then(Value::as_array)?
                        .iter()
                        .filter_map(|sample| {
                            let pair = sample.as_array()?;
                            let ts = pair.first()?.as_f64()? as i64;
                            let value = pair.get(1)?.as_str()?.parse::<f64>().ok()?;
                            Some(CostSample(ts, rounded_money(value)))
                        })
                        .collect::<Vec<_>>();
                    Some((labels, samples))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn prometheus_label_value(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn format_cost_metric_line(labels: &BTreeMap<&str, String>, value: f64) -> String {
    let rendered_labels = labels
        .iter()
        .map(|(key, value)| format!("{key}=\"{}\"", prometheus_label_value(value)))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "hevlayer_cost_usd_per_hour{{{rendered_labels}}} {}\n",
        rounded_money(value)
    )
}

fn format_cost_metrics(snapshot: &CostSnapshot, site: &str) -> String {
    let hours = snapshot.window_seconds as f64 / 3_600.0;
    if hours <= 0.0 {
        return String::new();
    }

    let mut body = String::new();
    for line in snapshot.lines.iter().filter(|line| line.authoritative()) {
        let mut labels = BTreeMap::new();
        labels.insert("provider", line.provider.clone());
        labels.insert("service", line.service.clone());
        labels.insert("basis", basis_label(line.basis).to_string());
        if let Some(service_detail) = line.service_detail.as_ref() {
            labels.insert("service_detail", service_detail.clone());
        }
        if let Some(region) = line.region.as_ref() {
            labels.insert("region", region.clone());
        }
        labels.insert(
            "site",
            line.site.clone().unwrap_or_else(|| site.to_string()),
        );
        if let Some(rate_card_version) = line.rate_card_version.as_ref() {
            labels.insert("rate_card_version", rate_card_version.clone());
        }
        body.push_str(&format_cost_metric_line(&labels, line.amount_usd / hours));
    }
    body
}

async fn push_prometheus(base_url: &str, body: String) -> Result<(), String> {
    if body.trim().is_empty() {
        return Ok(());
    }
    let url = format!(
        "{}/api/v1/import/prometheus",
        base_url.trim_end_matches('/')
    );
    let response = reqwest::Client::new()
        .post(url)
        .body(body)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    if response.status().is_success() {
        Ok(())
    } else {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        Err(format!("status {status}: {body}"))
    }
}

async fn sample_once(state: &AppState) -> Result<(), String> {
    let Some(base_url) = state.metrics_backend_url.as_deref() else {
        return Ok(());
    };
    let snapshot = current_snapshot(state, CostWindow::TwentyFourHours).await;
    let body = format_cost_metrics(&snapshot, &state.aws_cost_config.site);
    push_prometheus(base_url, body).await
}

pub async fn run_sampler(state: Arc<AppState>) {
    tokio::time::sleep(COST_SAMPLE_STARTUP_DELAY).await;
    loop {
        if let Err(error) = sample_once(&state).await {
            warn!(%error, "cost sampler tick failed");
        }
        tokio::time::sleep(COST_SAMPLE_INTERVAL).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cost_window_seconds_match_spec() {
        assert_eq!(CostWindow::OneHour.seconds(), 3_600);
        assert_eq!(CostWindow::SixHours.seconds(), 6 * 3_600);
        assert_eq!(CostWindow::TwentyFourHours.seconds(), 86_400);
        assert_eq!(CostWindow::SevenDays.seconds(), 604_800);
        assert_eq!(CostWindow::ThirtyDays.seconds(), 2_592_000);
    }

    #[test]
    fn default_step_scales_with_window() {
        assert_eq!(CostWindow::OneHour.default_step(), CostStep::FiveMinutes);
        assert_eq!(
            CostWindow::TwentyFourHours.default_step(),
            CostStep::ThirtyMinutes
        );
        assert_eq!(CostWindow::SevenDays.default_step(), CostStep::OneHour);
        assert_eq!(CostWindow::ThirtyDays.default_step(), CostStep::SixHours);
    }

    #[test]
    fn rate_card_exposes_real_turbopuffer_lines() {
        let card = current_rate_card(None);
        assert_eq!(card.aws.role, "estimator");
        assert!(!card.aws.stale);
        assert!(card.aws.items.iter().any(|item| {
            item.instance_type == "m6id.2xlarge"
                && item.vcpu == 8
                && item.memory_gib == 32.0
                && item.nvme_gib == 474.0
        }));
        assert_eq!(card.turbopuffer.version, "2026-07");
        assert!(card.turbopuffer.lines.len() > 4);
        let services: Vec<&str> = card
            .turbopuffer
            .lines
            .iter()
            .map(|l| l.service.as_str())
            .collect();
        assert!(services.starts_with(&[
            "tpuf_writes",
            "tpuf_queries_scanned",
            "tpuf_queries_returned",
            "tpuf_storage"
        ]));
        assert!(services.contains(&"tpuf_embeddings:baai/bge-m3"));
    }

    #[test]
    fn cost_window_parses_from_query_string() {
        let w: CostWindow = serde_json::from_str("\"7d\"").unwrap();
        assert_eq!(w, CostWindow::SevenDays);
    }

    #[test]
    fn totals_ignore_estimate_lines() {
        let metered = CostLine {
            provider: "turbopuffer".to_string(),
            service: "tpuf_writes".to_string(),
            basis: CostBasis::Metered,
            service_detail: None,
            region: None,
            site: None,
            rate_card_version: Some("2026-06".to_string()),
            amount_usd: 1.0,
            qty: None,
            unit: None,
            qty_bytes: None,
            breakdown: None,
        };
        let estimate = CostLine {
            basis: CostBasis::Estimate,
            amount_usd: 100.0,
            ..metered.clone()
        };
        assert!(metered.authoritative());
        assert!(!estimate.authoritative());
    }

    #[test]
    fn prometheus_scalar_parser_reads_vector_value() {
        let body = serde_json::json!({
            "status": "success",
            "data": {"resultType": "vector", "result": [
                {"metric": {}, "value": [1750000000, "12.5"]}
            ]}
        });
        assert_eq!(prometheus_instant_value(&body), Some(12.5));
    }
}
