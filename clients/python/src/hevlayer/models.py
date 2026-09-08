from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

JSONValue = Any

class LicenseSurfaceState(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    state: Literal["licensed", "grace", "floor"]
    seconds_to_deadline: int
    grace_seconds_remaining: int

class LicenseState(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    valid: bool
    state: Literal["floor"] | None = None
    reason: str | None = None
    sub: str | None = None
    tier: str | None = None
    features: list[str] | None = None
    limits: dict[str, int] | None = None
    exp: str | None = None
    gateway: LicenseSurfaceState

class CreatePipelineRequest(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    id: str
    target_namespace: str
    distance_metric: str | None = "cosine_distance"

class Pipeline(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    id: str
    target_namespace: str
    distance_metric: str
    created_at: str

class PipelineList(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    pipelines: list[Pipeline]

class PipelineStatus(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    pipeline_id: str
    status: Literal["idle", "pending", "waiting_on_upstream"]
    counts: dict[str, int]
    failed_reasons: dict[str, int]
    pending_count: int
    processing_count: int
    failed_count: int
    indexed_rate_per_min: float
    rate_window_seconds: int

class ClaimDocumentsRequest(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    stage: str | None = "pending"
    claim_stage: str | None = "embedding"
    limit: int | None = 100
    worker_id: str
    lease_seconds: int | None = 900
    document_id_prefix: str | None = None

class ClaimDocumentsResponse(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    pipeline_id: str
    stage: str
    claim_stage: str
    worker_id: str
    documents: list[str]

class HeartbeatDocumentsRequest(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    document_ids: list[str]
    stage: str | None = "embedding"
    worker_id: str

class SetDocumentsStageRequest(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    document_ids: list[str]
    stage: str
    from_stage: str | None = None
    worker_id: str | None = None
    create_missing: bool | None = False

class DocumentsStageResponse(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    pipeline_id: str
    stage: str
    updated: int

class StageDocumentResponse(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    pipeline_id: str
    document_id: str
    stage: str
    chunk_count: int
    chunk_ids: list[str]

class Chunk(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    id: str
    text: str | None = None
    metadata: dict[str, Any] | None = None

class PutChunksRequest(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    chunks: list[Chunk]

GetChunksResponse = list[Chunk]

class VectorEntry(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    id: str
    vector: list[float] | None = None
    vectors: list[list[float]] | None = None
    attributes: dict[str, Any] | None = None

class PutVectorsRequest(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    vectors: list[VectorEntry]

class CreateUdfRequest(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    id: str
    spec: UdfSpec

class UpdateUdfRequest(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    spec: UdfSpec

class Udf(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    id: str
    spec: UdfSpec
    paused: bool
    created_at: str
    updated_at: str

class UdfList(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    udfs: list[Udf]

class GetUdfResponse(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    udf: Udf
    status: UdfStatus

class UdfStatus(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    udf_id: str
    paused: bool
    active_namespaces: list[str]
    discovery: UdfDiscoveryStatus
    counts: dict[str, int]
    pending_count: int
    processing_count: int
    failed_count: int
    indexed_rate_per_min: float
    rate_window_seconds: int

class UdfDiscoveryStatus(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    sweeps_completed: int
    last_completed_at: str | None

class UdfSpec(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    index_selector: Any | None = None
    target_namespaces: list[str] | None = []
    inputs: list[str] | None = []
    version: str | None = "v1"
    filter: Any | None = None
    worker: UdfWorkerSpec
    schedule: UdfScheduleSpec | None = None
    retry: UdfRetrySpec | None = None
    triggers: list[UdfTrigger] | None = ["discovery"]
    invalidates: list[str] | None = []

UdfTrigger = Literal["discovery", "write"]

class UdfWorkerSpec(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    image: str | None = None
    url: str | None = None
    port: int | None = None
    batch_size: int | None = 32
    timeout_seconds: int | None = 30
    pod_spec: Any | None = None

class UdfScheduleSpec(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    discovery_interval_seconds: int | None = 60
    lease_seconds: int | None = 120
    max_in_flight_batches: int | None = 8
    max_concurrent_scans: int | None = 4

class UdfRetrySpec(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    max_attempts: int | None = 8
    initial_backoff_seconds: int | None = 5
    max_backoff_seconds: int | None = 300

class UdfDiscoverRequest(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    namespaces: list[str] | None = []
    page_size: int | None = 10000

class UdfDiscoverResponse(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    udf_id: str
    enqueued: int
    namespaces: list[str]

class UdfClaimRequest(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    worker_id: str
    limit: int | None = 32
    lease_seconds: int | None = 120

class UdfClaimedItem(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    namespace: str
    id: str
    input: dict[str, Any]

class UdfClaimResponse(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    udf_id: str
    worker_id: str
    items: list[UdfClaimedItem]

class UdfItemRef(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    namespace: str
    id: str

class UdfHeartbeatRequest(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    worker_id: str
    items: list[UdfItemRef]

class UdfCompleteRequest(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    worker_id: str
    items: list[UdfCompleteItem]

class UdfCompleteItem(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    namespace: str
    id: str
    vector: list[float] | None = None
    vectors: list[list[float]] | None = None
    attributes: dict[str, Any] | None = {}

UdfErrorKind = Literal["transient", "permanent"]

class UdfFailRequest(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    worker_id: str
    items: list[UdfFailItem]

class UdfFailItem(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    namespace: str
    id: str
    kind: UdfErrorKind
    message: str | None = None

class UdfItemsResponse(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    udf_id: str
    updated: int

CostWindow = Literal["1h", "6h", "24h", "7d", "30d"]

CostStep = Literal["5m", "30m", "1h", "6h", "1d"]

CostBasis = Literal["metered", "invoice", "estimate"]

class CostTotals(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    total_usd: float
    aws_usd: float
    turbopuffer_usd: float
    cost_per_query_usd: float | None = None
    cost_per_document_usd: float | None = None
    cost_per_tib_indexed_usd: float | None = None

class CostLine(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    provider: Literal["aws", "turbopuffer"]
    service: str
    basis: CostBasis
    service_detail: str | None = None
    region: str | None = None
    site: str | None = None
    rate_card_version: str | None = None
    amount_usd: float
    qty: float | None = None
    unit: str | None = None
    qty_bytes: int | None = None
    breakdown: list[dict[str, Any]] | None = None

class CostRateCardStatus(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    turbopuffer_rate_card_version: str
    aws_cost_source: Literal["cost_explorer"]
    aws_cost_refreshed_at_ms: int
    aws_cost_stale: bool
    aws_pricing_stale: bool
    aws_pricing_refreshed_at_ms: int

class CostSnapshot(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    as_of_ms: int
    window_seconds: int
    totals: CostTotals
    lines: list[CostLine]
    rate_card_status: CostRateCardStatus
    caveats: list[str]

CostSample = list[Any]

class CostSeries(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    provider: Literal["aws", "turbopuffer"] | None = None
    service: str | None = None
    basis: CostBasis | None = None
    service_detail: str | None = None
    region: str | None = None
    site: str | None = None
    rate_card_version: str | None = None
    label: str | None = None
    samples: list[CostSample]

class CostTimeseries(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    window_seconds: int
    step_seconds: int
    series: list[CostSeries]

class AwsInstancePrice(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    instance_type: str
    family: str
    vcpu: int
    memory_gib: float
    nvme_gib: float
    hourly_usd: float

class AwsRateCard(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    role: Literal["estimator"]
    region: str
    refreshed_at_ms: int
    ttl_seconds: int
    stale: bool
    items: list[AwsInstancePrice]

class TurbopufferRateLine(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    service: str
    unit: str
    usd: float

class TurbopufferRateCard(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    version: str
    verified_by: str
    verified_at: str
    source: Literal["invoice"]
    lines: list[TurbopufferRateLine]

class RateCard(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    aws: AwsRateCard
    turbopuffer: TurbopufferRateCard

class Document(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    id: str
    attributes: dict[str, Any]

class FetchDocumentsRequest(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    ids: list[str]
    include_attributes: list[str] | None = None

class FetchDocumentsResponse(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    documents: list[Document]
    missing: list[str]

class StatusResponse(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    status: str
    message: str | None = None
    rows_affected: int | None = None
    rows_upserted: int | None = None
    rows_patched: int | None = None
    rows_deleted: int | None = None
    billing: dict[str, Any] | None = None

class BlobPutResponse(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    ref: str
    sha256: str
    size: int

class TurbopufferNamespaceSummary(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    id: str

class TurbopufferNamespaceList(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    namespaces: list[TurbopufferNamespaceSummary]
    next_cursor: str | None = None

class TurbopufferSchema(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    pass

class TurbopufferMetadataPatch(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    pinning: Any | None = None

class TurbopufferWriteRequest(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    pass

class TurbopufferBranchFromRequest(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    branch_from_namespace: dict[str, Any]

class TurbopufferCopyFromRequest(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    copy_from_namespace: str | dict[str, Any]

class TurbopufferWriteResponse(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    status: str
    message: str
    rows_affected: int
    rows_upserted: int | None = None
    rows_patched: int | None = None
    rows_deleted: int | None = None
    rows_remaining: bool | None = None
    upserted_ids: list[Any] | None = None
    patched_ids: list[Any] | None = None
    deleted_ids: list[Any] | None = None
    billing: dict[str, Any]
    performance: dict[str, Any] | None = None

class TurbopufferQueryRequest(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    pass

class TurbopufferQueryResponse(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    rows: list[dict[str, Any]] | None = None
    aggregations: dict[str, Any] | None = None
    aggregation_groups: list[dict[str, Any]] | None = None
    billing: dict[str, Any] | None = None
    performance: dict[str, Any] | None = None

class BatchQueryRequest(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    queries: list[TurbopufferQueryRequest]
    consistency: dict[str, Any] | None = None
    vector_encoding: str | None = None

class BatchQueryResponse(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    results: list[TurbopufferQueryResponse]
    billing: dict[str, Any] | None = None
    performance: dict[str, Any] | None = None
    stable_as_of: int | None = None

class TurbopufferExplainQueryResponse(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    plan_text: str | None = None

class TurbopufferRecallRequest(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    num: int | None = None
    top_k: int | None = None
    filters: Any | None = None
    rank_by: Any | None = None
    include_ground_truth: bool | None = None

class TurbopufferRecallResponse(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    avg_recall: float
    avg_exhaustive_count: float
    avg_ann_count: float
    ground_truth: list[dict[str, Any]] | None = None

class HintCacheWarmResponse(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    namespace: str | None = None
    turbopuffer: WarmStepResponse | None = None
    documents: WarmDocumentsResponse | None = None
    snapshots: WarmSnapshotsResponse | None = None
    blobs: WarmBlobsResponse | None = None

JobStatus = Literal["running", "completed", "failed"]

SnapshotSource = Literal["auto", "stored", "cache", "origin"]

ScanSource = Literal["auto", "cache", "origin", "snapshot"]

ScanCountSource = Literal["auto", "snapshot", "cache", "origin"]

ScanMode = Literal["ids", "count", "values"]

ScanCountServedBy = Literal["snapshot", "cache", "origin"]

class CreateSnapshotRequest(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    field: str
    source: SnapshotSource | None = None
    filters: Any | None = None
    page_size: int | None = 1000

class SnapshotPolicy(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    facetFields: list[str] | None = []
    interval: str | None = "5m"
    retention: str | None = "never"

class CreateCheckpointRequest(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    label: str

class Checkpoint(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    namespace: str
    label: str
    watermark_ms: int
    sha: str
    row_count: int

class CheckpointList(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    checkpoints: list[Checkpoint]
    next_cursor: str | None = None

class CreateScanRequest(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    source: ScanCountSource | None = None
    filters: Any | None = None
    as_of: int | None = None
    between: list[int] | None = None
    fts: FtsScan | None = None
    hybrid_text: HybridTextScan | None = None
    ann: AnnScan | None = None
    mode: ScanMode | None = None
    field: str | None = None
    exhaustive: bool | None = False
    threads: int | None = None
    page_size: int | None = 1000
    timeout_seconds: int | None = 30

class FtsScan(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    field: str
    query: str

class HybridTextScan(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    field: str
    query: str
    fuzziness: Literal["auto"] | int | None = None

class AnnScan(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    vector: list[float]
    field: str | None = "vector"
    radius: float

WarmStepStatus = Literal["skipped", "completed", "started", "no_snapshot"]

class WarmStepResponse(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    enabled: bool
    status: WarmStepStatus

class WarmDocumentsResponse(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    enabled: bool
    status: WarmStepStatus
    job: WarmJob | None = None

class WarmSnapshotsResponse(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    enabled: bool
    status: WarmStepStatus
    key: str | None = None
    watermark_ms: int | None = None
    sha: str | None = None

class WarmBlobsResponse(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    enabled: bool
    status: WarmStepStatus
    attributes: list[str] | None = None
    budget_bytes: int | None = None
    documents_scanned: int
    refs_seen: int
    objects: int
    bytes: int
    missing: int
    invalid_refs: int
    budget_exhausted: bool

class WarmCacheResponse(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    namespace: str
    turbopuffer: WarmStepResponse
    documents: WarmDocumentsResponse
    snapshots: WarmSnapshotsResponse
    blobs: WarmBlobsResponse

class JobBase(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    id: str
    namespace: str
    status: JobStatus
    progress: float
    documents_scanned: int
    stable_as_of: int | None = None
    created_at: str
    completed_at: str | None = None
    error: str | None = None

class SnapshotJob(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    id: str
    namespace: str
    status: JobStatus
    progress: float
    documents_scanned: int
    stable_as_of: int | None = None
    created_at: str
    completed_at: str | None = None
    error: str | None = None
    field: str
    source: SnapshotSource
    effective_source: SnapshotSource | None = None
    sha: str | None = None

class SnapshotJobList(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    snapshot_jobs: list[SnapshotJob]

class WarmJob(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    id: str
    namespace: str
    status: JobStatus
    progress: float
    documents_scanned: int
    stable_as_of: int | None = None
    created_at: str
    completed_at: str | None = None
    error: str | None = None

class WarmJobList(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    warm_jobs: list[WarmJob]

class ScanJob(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    id: str
    namespace: str
    status: JobStatus
    progress: float
    documents_scanned: int
    stable_as_of: int | None = None
    created_at: str
    completed_at: str | None = None
    error: str | None = None
    mode: ScanMode
    field: str | None = None
    source: ScanSource
    effective_source: ScanSource | None = None
    unique_values: int | None = None
    truncated: bool | None = None
    bounded: bool | None = None
    approximate: bool | None = None
    snapshot_sha: str | None = None
    watermark_ms: int | None = None
    threads: int | None = None

class ScanJobList(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    scans: list[ScanJob]

class ScanValue(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    v: str
    n: int

class ScanValuesResponse(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    values: list[ScanValue]
    total: int
    truncated: bool

class ScanIdsResponse(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    ids: list[str]
    total: int

class ScanCountResponse(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    count: int
    served_by: ScanCountServedBy
    snapshot_sha: str | None = None
    watermark_ms: int | None = None
    bounded: bool | None = None
    timed_out: bool | None = None
    shards_saturated: int | None = None
    shards_total: int | None = None
    approximate: bool | None = None
    threads: int | None = None
    elapsed_ms: int

class NamespaceList(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    namespaces: list[NamespaceListEntry]
    next_cursor: str | None = None

class NamespaceListEntry(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    name: str
    row_count: int | None = None
    size_bytes: int | None = None
    stable_as_of_ms: int | None = None
    is_stable: bool | None = None
    schema_summary: NamespaceSchemaSummary | None = None
    index: IndexState | None = None
    cache_state: NamespaceCacheState | None = None
    last_write_ms: int | None = None
    shadow: bool | None = None
    labels: dict[str, str] | None = None
    metadata_error: str | None = None

class NamespaceSchemaSummary(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    vector_dim: int | None = None
    fields: list[str] | None = None

class IndexState(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    status: Literal["updating", "up-to-date"] | None = None
    unindexed_bytes: int | None = None

class NamespaceCacheState(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    state: Literal["cold", "warming", "warm"]
    warmed_through_ms: int | None = None
    warm_inflight: bool

class NamespaceMetadata(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    id: str
    schema: dict[str, Any]
    approx_logical_bytes: int
    approx_row_count: int
    created_at: str
    last_write_at: str | None = None
    updated_at: str
    config: dict[str, Any] | None = None
    index: IndexState | None = None
    layer: NamespaceMetadataLayer | None = None

class NamespaceMetadataLayer(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    stable_as_of: int | None = None
    is_stable: bool | None = None
    indexed: bool | None = None
    index_lag_rows: int | None = None
    schema_version: int | None = None
    init_state: Literal["running", "ready"] | None = None
    init_lag_rows: int | None = None
    shard_count: int | None = None
    shard_state: Literal["unsharded", "backfilling", "ready"] | None = None
    shard_lag_rows: int | None = None
    scatter_gather_active: bool | None = None

class InitNamespaceRequest(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    schema_version: int | None = 1
    shard_count: int | None = None

class InitNamespaceResponse(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    namespace: str
    layer: NamespaceMetadataLayer

class QueryRequest(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    vector: list[float] | None = None
    nearest_to_id: list[str] | None = None
    top_k: int | None = 10
    filters: Any | None = None
    as_of: int | None = None
    between: list[int] | None = None
    include_attributes: bool | list[str] | None = None
    include_leg_breakdown: bool | None = False
    cursor: str | None = None
    rank_by: list[Any] | None = None

class FederatedQueryRequest(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    vector: list[float] | None = None
    nearest_to_id: list[str] | None = None
    top_k: int | None = 10
    filters: Any | None = None
    as_of: int | None = None
    between: list[int] | None = None
    include_attributes: bool | list[str] | None = None
    include_leg_breakdown: bool | None = False
    cursor: str | None = None
    rank_by: list[Any] | None = None
    namespaces: list[str] | None = None
    strict: bool | None = False
    fusion: FederatedFusionOptions | None = None

class FederatedFusionOptions(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    per_namespace_limit: int | None = None
    rank_constant: int | None = 60

class AgentQueryRequest(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    query: str
    vector: list[float] | None = None
    top_k: int | None = 10

class AgentQueryResponse(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    rows: list[dict[str, Any]]
    merge: dict[str, Any]
    routing: RoutingEcho | None = None
    hybrid: HybridEcho | None = None
    namespaces: list[FederatedNamespaceResult]
    errors: list[FederatedNamespaceError] | None = None
    agent: AgentEcho | None = None

class AgentEcho(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    turns: Literal[1, 2]
    deadlineHit: bool
    recallDepth: int
    relevanceWeight: float
    queries: list[dict[str, Any]]
    trace: str | None = None

class FederatedQueryResponse(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    rows: list[dict[str, Any]]
    merge: dict[str, Any]
    routing: RoutingEcho | None = None
    hybrid: HybridEcho | None = None
    namespaces: list[FederatedNamespaceResult]
    errors: list[FederatedNamespaceError] | None = None

class FederatedNamespaceResult(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    namespace: str
    stable_as_of: int | None = None
    matched: int

class FederatedNamespaceError(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    namespace: str
    error: str

class HybridEcho(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    tokens: list[str]
    tokens_dropped: int
    fuzziness: Literal["auto"] | Literal[0, 1, 2]
    rank_constant: int
    legs: int
    per_leg_limit: int
    surfaced: bool | None = None
    threads: int | None = None

class RoutingEcho(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    route: Literal["hybrid_text", "semantic", "fused"]
    policy: str
    tokens: int
    executed: bool

class QueryResponse(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    rows: list[dict[str, Any]]
    aggregations: dict[str, Any] | None = None
    aggregation_groups: list[dict[str, Any]] | None = None
    billing: dict[str, Any] | None = None
    performance: dict[str, Any] | None = None
    stable_as_of: int | None = None
    next_cursor: str | None = None
    hybrid: HybridEcho | None = None
    routing: RoutingEcho | None = None

class Error(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    error: str
    message: str

class SnapshotHistoryEntry(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    watermark_ms: int
    sha: str
    tags: list[str] | None = None

class SnapshotBody(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    namespace: str
    watermark_ms: int
    sha: str
    row_count: int | None = None
    fields: list[SnapshotField]
    fields_skipped: list[SnapshotFieldSkipped]

class SnapshotField(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    name: str
    values: list[SnapshotValueCount]

SnapshotSkipReason = Literal["exceeded_cap"]

class SnapshotFieldSkipped(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    name: str
    reason: SnapshotSkipReason
    distinct_observed: int
    cap: int

class SnapshotValueCount(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    v: str
    n: int

class SnapshotActivityEvent(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    ts_ms: int
    namespace: str
    sha: str

class SnapshotActivityList(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    events: list[SnapshotActivityEvent]
    next_cursor: str | None = None
    truncated: bool | None = None

MetricKind = Literal["counter", "gauge", "histogram"]

MetricFamily = Literal["query", "upsert", "fetch", "cache", "pipeline", "storage", "saturation"]

class MetricAlert(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    summary: str
    expr: str
    for_: str = Field(..., alias="for")

class MetricCatalogEntry(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    name: str
    kind: MetricKind
    family: MetricFamily
    labels: list[str]
    description: str
    example_promql: str
    alert: MetricAlert | None = None

class MetricCatalog(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    version: str
    entries: list[MetricCatalogEntry]

class PrometheusResponse(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    pass

class SearchHistoryEntry(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    timestamp: str
    timestamp_nanos: int
    namespace: str
    trace_id: str | None = None
    raw_query: str | None = None
    stable_as_of: int | None = None
    query: dict[str, Any]
    top_result_ids: list[str]
    tags: list[str]

class SearchHistoryListResponse(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    entries: list[SearchHistoryEntry]
    next_cursor: str | None = None

class ClickstreamEvent(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    timestamp: str
    timestamp_nanos: int
    trace_id: str
    namespace: str
    doc_id: str
    tags: list[str]
    source: str
    served_from: str

class ClickstreamListResponse(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    events: list[ClickstreamEvent]
    next_cursor: str | None = None

class KubernetesCondition(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    pass

class SecretKeyRef(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    name: str
    key: str

class VectorStoreEndpoint(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    url: str
    region: str

class VectorStoreTurbopuffer(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    orgId: str

class VectorStoreCredential(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    secretRef: SecretKeyRef

class VectorStoreInboundAuth(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    mode: Literal["deriveFromStore", "keys", "open"] | None = None

class VectorStoreStatus(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    reachable: bool | None = None
    observedGeneration: int | None = None
    conditions: list[KubernetesCondition]

class VectorStore(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    name: str
    kind: Literal["turbopuffer", "search", "search-embedded"]
    default: bool
    endpoint: VectorStoreEndpoint
    turbopuffer: VectorStoreTurbopuffer | None = None
    credential: VectorStoreCredential
    inboundAuth: VectorStoreInboundAuth | None = None
    status: VectorStoreStatus
    turbopufferUrl: str | None = None

class VectorStoreList(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    vectorstores: list[VectorStore]

class WarehouseSecretRef(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    name: str

class WarehousePool(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    size: int
    timeout: str

class SnowflakeWarehouse(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    account: str
    user: str
    role: str | None = None
    warehouse: str
    keyPairSecretRef: WarehouseSecretRef
    pool: WarehousePool | None = None

class RestWarehouse(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    baseUrl: str
    auth: RestWarehouseAuth | None = None
    rateLimit: RestWarehouseRateLimit | None = None
    verify: RestWarehouseVerify

class RestWarehouseAuth(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    in_: Literal["query", "header"] = Field(..., alias="in")
    name: str
    secretRef: WarehouseSecretRef

class RestWarehouseRateLimit(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    requestsPerSecond: float

class RestWarehouseVerify(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    path: str
    query: dict[str, str] | None = None

WarehousePhase = Literal["Pending", "Verified", "Failed"]

class WarehouseConsumers(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    pipelines: int
    apiKeys: int

class WarehouseStatus(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    phase: WarehousePhase | None = None
    verifiedAt: str | None = None
    failureReason: str | None = None
    consumers: WarehouseConsumers
    observedGeneration: int | None = None
    conditions: list[KubernetesCondition]

class Warehouse(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    name: str
    namespace: str
    kind: str
    snowflake: SnowflakeWarehouse | None = None
    rest: RestWarehouse | None = None
    verifyInterval: str
    status: WarehouseStatus

class WarehouseList(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    warehouses: list[Warehouse]

class ApiKeyEntitlement(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    scopes: list[Literal["read", "write", "admin"]] | None = None
    namespaces: list[str] | None = None
    claims: list[str] | None = None

ApiKeyEntitlements = dict[str, ApiKeyEntitlement]

ApiKeyPhase = Literal["Pending", "Active", "Revoked", "Expired"]

class ApiKey(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    keyId: str
    name: str
    owner: str | None = None
    description: str | None = None
    entitlements: ApiKeyEntitlements
    expiresAfter: str | None = None
    phase: ApiKeyPhase
    createdAt: str
    expiresAt: str | None = None
    revokedAt: str | None = None
    lastSeenAt: str | None = None
    lookupHash: str | None = None
    secretRef: dict[str, Any] | None = None

class ApiKeyList(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    keys: list[ApiKey]

class MintKeyRequest(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    name: str
    owner: str | None = None
    description: str | None = None
    entitlements: ApiKeyEntitlements | None = None
    expiresAfter: str | None = "365d"

class MintKeyResponse(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    keyId: str
    name: str
    owner: str | None = None
    description: str | None = None
    entitlements: ApiKeyEntitlements
    expiresAfter: str | None = None
    phase: ApiKeyPhase
    createdAt: str
    expiresAt: str | None = None
    token: str

class AuthenticateKeyRequest(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    token: str

class AuthenticateKeyResponse(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    keyId: str
    name: str
    owner: str | None = None
    entitlements: ApiKeyEntitlements
    expiresAt: str | None = None
