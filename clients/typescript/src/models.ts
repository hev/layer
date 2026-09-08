export type JSONValue = null | boolean | number | string | JSONValue[] | { [key: string]: JSONValue };
export type TurbopufferFilter =
  | [string, string, JSONValue]
  | ["And" | "Or", TurbopufferFilter[]]
  | ["Not", TurbopufferFilter]
  | JSONValue[]
  | Record<string, JSONValue>;
export type TurbopufferRankBy =
  | [string, "ANN", number[]]
  | [string, "BM25", string]
  | [string, string, JSONValue]
  | JSONValue[]
  | Record<string, JSONValue>;

export interface LicenseSurfaceState {
  [key: string]: unknown;
  state: "licensed" | "grace" | "floor";
  seconds_to_deadline: number;
  grace_seconds_remaining: number;
}

export interface LicenseState {
  [key: string]: unknown;
  valid: boolean;
  state?: "floor";
  reason?: string;
  sub?: string;
  tier?: string;
  features?: string[];
  limits?: Record<string, number>;
  exp?: string;
  gateway: LicenseSurfaceState;
}

export interface CreatePipelineRequest {
  [key: string]: unknown;
  id: string;
  target_namespace: string;
  distance_metric?: string;
}

export interface Pipeline {
  [key: string]: unknown;
  id: string;
  target_namespace: string;
  distance_metric: string;
  created_at: string;
}

export interface PipelineList {
  [key: string]: unknown;
  pipelines: Pipeline[];
}

export interface PipelineStatus {
  [key: string]: unknown;
  pipeline_id: string;
  status: "idle" | "pending" | "waiting_on_upstream";
  counts: Record<string, number>;
  failed_reasons: Record<string, number>;
  pending_count: number;
  processing_count: number;
  failed_count: number;
  indexed_rate_per_min: number;
  rate_window_seconds: number;
}

export interface ClaimDocumentsRequest {
  [key: string]: unknown;
  stage?: string;
  claim_stage?: string;
  limit?: number;
  worker_id: string;
  lease_seconds?: number;
  document_id_prefix?: string | null;
}

export interface ClaimDocumentsResponse {
  [key: string]: unknown;
  pipeline_id: string;
  stage: string;
  claim_stage: string;
  worker_id: string;
  documents: string[];
}

export interface HeartbeatDocumentsRequest {
  [key: string]: unknown;
  document_ids: string[];
  stage?: string;
  worker_id: string;
}

export interface SetDocumentsStageRequest {
  [key: string]: unknown;
  document_ids: string[];
  stage: string;
  from_stage?: string | null;
  worker_id?: string | null;
  create_missing?: boolean;
}

export interface DocumentsStageResponse {
  [key: string]: unknown;
  pipeline_id: string;
  stage: string;
  updated: number;
}

export interface StageDocumentResponse {
  [key: string]: unknown;
  pipeline_id: string;
  document_id: string;
  stage: string;
  chunk_count: number;
  chunk_ids: string[];
}

export interface Chunk {
  [key: string]: unknown;
  id: string;
  text?: string;
  metadata?: Record<string, unknown>;
}

export interface PutChunksRequest {
  [key: string]: unknown;
  chunks: Chunk[];
}

export type GetChunksResponse = Chunk[];

export interface VectorEntry {
  [key: string]: unknown;
  id: string;
  vector?: number[];
  vectors?: number[][];
  attributes?: Record<string, unknown>;
}

export interface PutVectorsRequest {
  [key: string]: unknown;
  vectors: VectorEntry[];
}

export interface CreateUdfRequest {
  [key: string]: unknown;
  id: string;
  spec: UdfSpec;
}

export interface UpdateUdfRequest {
  [key: string]: unknown;
  spec: UdfSpec;
}

export interface Udf {
  [key: string]: unknown;
  id: string;
  spec: UdfSpec;
  paused: boolean;
  created_at: string;
  updated_at: string;
}

export interface UdfList {
  [key: string]: unknown;
  udfs: Udf[];
}

export interface GetUdfResponse {
  [key: string]: unknown;
  udf: Udf;
  status: UdfStatus;
}

export interface UdfStatus {
  [key: string]: unknown;
  udf_id: string;
  paused: boolean;
  active_namespaces: string[];
  discovery: UdfDiscoveryStatus;
  counts: Record<string, number>;
  pending_count: number;
  processing_count: number;
  failed_count: number;
  indexed_rate_per_min: number;
  rate_window_seconds: number;
}

export interface UdfDiscoveryStatus {
  [key: string]: unknown;
  sweeps_completed: number;
  last_completed_at: string | null;
}

export interface UdfSpec {
  [key: string]: unknown;
  index_selector?: unknown;
  target_namespaces?: string[];
  inputs?: string[];
  version?: string;
  filter?: TurbopufferFilter;
  worker: UdfWorkerSpec;
  schedule?: UdfScheduleSpec;
  retry?: UdfRetrySpec;
  triggers?: UdfTrigger[];
  invalidates?: string[];
}

export type UdfTrigger = "discovery" | "write";

export interface UdfWorkerSpec {
  [key: string]: unknown;
  image?: string | null;
  url?: string | null;
  port?: number | null;
  batch_size?: number;
  timeout_seconds?: number;
  pod_spec?: unknown;
}

export interface UdfScheduleSpec {
  [key: string]: unknown;
  discovery_interval_seconds?: number;
  lease_seconds?: number;
  max_in_flight_batches?: number;
  max_concurrent_scans?: number;
}

export interface UdfRetrySpec {
  [key: string]: unknown;
  max_attempts?: number;
  initial_backoff_seconds?: number;
  max_backoff_seconds?: number;
}

export interface UdfDiscoverRequest {
  [key: string]: unknown;
  namespaces?: string[];
  page_size?: number;
}

export interface UdfDiscoverResponse {
  [key: string]: unknown;
  udf_id: string;
  enqueued: number;
  namespaces: string[];
}

export interface UdfClaimRequest {
  [key: string]: unknown;
  worker_id: string;
  limit?: number;
  lease_seconds?: number;
}

export interface UdfClaimedItem {
  [key: string]: unknown;
  "namespace": string;
  id: string;
  input: Record<string, unknown>;
}

export interface UdfClaimResponse {
  [key: string]: unknown;
  udf_id: string;
  worker_id: string;
  items: UdfClaimedItem[];
}

export interface UdfItemRef {
  [key: string]: unknown;
  "namespace": string;
  id: string;
}

export interface UdfHeartbeatRequest {
  [key: string]: unknown;
  worker_id: string;
  items: UdfItemRef[];
}

export interface UdfCompleteRequest {
  [key: string]: unknown;
  worker_id: string;
  items: UdfCompleteItem[];
}

export interface UdfCompleteItem {
  [key: string]: unknown;
  "namespace": string;
  id: string;
  vector?: number[];
  vectors?: number[][];
  attributes?: Record<string, unknown>;
}

export type UdfErrorKind = "transient" | "permanent";

export interface UdfFailRequest {
  [key: string]: unknown;
  worker_id: string;
  items: UdfFailItem[];
}

export interface UdfFailItem {
  [key: string]: unknown;
  "namespace": string;
  id: string;
  kind: UdfErrorKind;
  message?: string | null;
}

export interface UdfItemsResponse {
  [key: string]: unknown;
  udf_id: string;
  updated: number;
}

export type CostWindow = "1h" | "6h" | "24h" | "7d" | "30d";

export type CostStep = "5m" | "30m" | "1h" | "6h" | "1d";

export type CostBasis = "metered" | "invoice" | "estimate";

export interface CostTotals {
  [key: string]: unknown;
  total_usd: number;
  aws_usd: number;
  turbopuffer_usd: number;
  cost_per_query_usd?: number;
  cost_per_document_usd?: number;
  cost_per_tib_indexed_usd?: number;
}

export interface CostLine {
  [key: string]: unknown;
  provider: "aws" | "turbopuffer";
  service: string;
  basis: CostBasis;
  service_detail?: string;
  region?: string;
  site?: string;
  rate_card_version?: string;
  amount_usd: number;
  qty?: number;
  unit?: string;
  qty_bytes?: number;
  breakdown?: Record<string, unknown>[];
}

export interface CostRateCardStatus {
  [key: string]: unknown;
  turbopuffer_rate_card_version: string;
  aws_cost_source: "cost_explorer";
  aws_cost_refreshed_at_ms: number;
  aws_cost_stale: boolean;
  aws_pricing_stale: boolean;
  aws_pricing_refreshed_at_ms: number;
}

export interface CostSnapshot {
  [key: string]: unknown;
  as_of_ms: number;
  window_seconds: number;
  totals: CostTotals;
  lines: CostLine[];
  rate_card_status: CostRateCardStatus;
  caveats: string[];
}

export type CostSample = unknown[];

export interface CostSeries {
  [key: string]: unknown;
  provider?: "aws" | "turbopuffer";
  service?: string;
  basis?: CostBasis;
  service_detail?: string;
  region?: string;
  site?: string;
  rate_card_version?: string;
  label?: string;
  samples: CostSample[];
}

export interface CostTimeseries {
  [key: string]: unknown;
  window_seconds: number;
  step_seconds: number;
  series: CostSeries[];
}

export interface AwsInstancePrice {
  [key: string]: unknown;
  instance_type: string;
  family: string;
  vcpu: number;
  memory_gib: number;
  nvme_gib: number;
  hourly_usd: number;
}

export interface AwsRateCard {
  [key: string]: unknown;
  role: "estimator";
  region: string;
  refreshed_at_ms: number;
  ttl_seconds: number;
  stale: boolean;
  items: AwsInstancePrice[];
}

export interface TurbopufferRateLine {
  [key: string]: unknown;
  service: string;
  unit: string;
  usd: number;
}

export interface TurbopufferRateCard {
  [key: string]: unknown;
  version: string;
  verified_by: string;
  verified_at: string;
  source: "invoice";
  lines: TurbopufferRateLine[];
}

export interface RateCard {
  [key: string]: unknown;
  aws: AwsRateCard;
  turbopuffer: TurbopufferRateCard;
}

export interface Document {
  [key: string]: unknown;
  id: string;
  attributes: Record<string, unknown>;
}

export interface FetchDocumentsRequest {
  [key: string]: unknown;
  ids: string[];
  include_attributes?: string[];
}

export interface FetchDocumentsResponse {
  [key: string]: unknown;
  documents: Document[];
  missing: string[];
}

export interface StatusResponse {
  [key: string]: unknown;
  status: string;
  message?: string;
  rows_affected?: number;
  rows_upserted?: number;
  rows_patched?: number;
  rows_deleted?: number;
  billing?: Record<string, unknown>;
}

export interface BlobPutResponse {
  [key: string]: unknown;
  ref: string;
  sha256: string;
  size: number;
}

export interface TurbopufferNamespaceSummary {
  [key: string]: unknown;
  id: string;
}

export interface TurbopufferNamespaceList {
  [key: string]: unknown;
  namespaces: TurbopufferNamespaceSummary[];
  next_cursor?: string;
}

export type TurbopufferSchema = Record<string, unknown>;

export interface TurbopufferMetadataPatch {
  [key: string]: unknown;
  pinning?: unknown;
}

export type TurbopufferWriteRequest = Record<string, unknown>;

export interface TurbopufferBranchFromRequest {
  [key: string]: unknown;
  branch_from_namespace: Record<string, unknown>;
}

export interface TurbopufferCopyFromRequest {
  [key: string]: unknown;
  copy_from_namespace: string | Record<string, unknown>;
}

export interface TurbopufferWriteResponse {
  [key: string]: unknown;
  status: string;
  message: string;
  rows_affected: number;
  rows_upserted?: number;
  rows_patched?: number;
  rows_deleted?: number;
  rows_remaining?: boolean;
  upserted_ids?: unknown[];
  patched_ids?: unknown[];
  deleted_ids?: unknown[];
  billing: Record<string, unknown>;
  performance?: Record<string, unknown>;
}

export interface TurbopufferQueryRequest {
  [key: string]: unknown;
  filters?: TurbopufferFilter;
  rank_by?: TurbopufferRankBy;
}

export interface TurbopufferQueryResponse {
  [key: string]: unknown;
  rows?: Record<string, unknown>[];
  aggregations?: Record<string, unknown>;
  aggregation_groups?: Record<string, unknown>[];
  billing?: Record<string, unknown>;
  performance?: Record<string, unknown>;
}

export interface BatchQueryRequest {
  [key: string]: unknown;
  queries: TurbopufferQueryRequest[];
  consistency?: Record<string, unknown>;
  vector_encoding?: string;
}

export interface BatchQueryResponse {
  [key: string]: unknown;
  results: TurbopufferQueryResponse[];
  billing?: Record<string, unknown>;
  performance?: Record<string, unknown>;
  stable_as_of?: number | null;
}

export interface TurbopufferExplainQueryResponse {
  [key: string]: unknown;
  plan_text?: string;
}

export interface TurbopufferRecallRequest {
  [key: string]: unknown;
  num?: number;
  top_k?: number;
  filters?: TurbopufferFilter;
  rank_by?: TurbopufferRankBy;
  include_ground_truth?: boolean;
}

export interface TurbopufferRecallResponse {
  [key: string]: unknown;
  avg_recall: number;
  avg_exhaustive_count: number;
  avg_ann_count: number;
  ground_truth?: Record<string, unknown>[];
}

export interface HintCacheWarmResponse {
  [key: string]: unknown;
  "namespace"?: string;
  turbopuffer?: WarmStepResponse;
  documents?: WarmDocumentsResponse;
  snapshots?: WarmSnapshotsResponse;
  blobs?: WarmBlobsResponse;
}

export type JobStatus = "running" | "completed" | "failed";

export type SnapshotSource = "auto" | "stored" | "cache" | "origin";

export type ScanSource = "auto" | "cache" | "origin" | "snapshot";

export type ScanCountSource = "auto" | "snapshot" | "cache" | "origin";

export type ScanMode = "ids" | "count" | "values";

export type ScanCountServedBy = "snapshot" | "cache" | "origin";

export interface CreateSnapshotRequest {
  [key: string]: unknown;
  field: string;
  source?: SnapshotSource;
  filters?: TurbopufferFilter;
  page_size?: number;
}

export interface SnapshotPolicy {
  [key: string]: unknown;
  facetFields?: string[];
  interval?: string;
  retention?: string;
}

export interface CreateCheckpointRequest {
  [key: string]: unknown;
  label: string;
}

export interface Checkpoint {
  [key: string]: unknown;
  "namespace": string;
  label: string;
  watermark_ms: number;
  sha: string;
  row_count: number;
}

export interface CheckpointList {
  [key: string]: unknown;
  checkpoints: Checkpoint[];
  next_cursor?: string | null;
}

export interface CreateScanRequest {
  [key: string]: unknown;
  source?: ScanCountSource;
  filters?: TurbopufferFilter;
  as_of?: number;
  between?: number[];
  fts?: FtsScan;
  hybrid_text?: HybridTextScan;
  ann?: AnnScan;
  mode?: ScanMode;
  field?: string;
  exhaustive?: boolean;
  threads?: number;
  page_size?: number;
  timeout_seconds?: number;
}

export interface FtsScan {
  [key: string]: unknown;
  field: string;
  query: string;
}

export interface HybridTextScan {
  [key: string]: unknown;
  field: string;
  query: string;
  fuzziness?: "auto" | number;
}

export interface AnnScan {
  [key: string]: unknown;
  vector: number[];
  field?: string;
  radius: number;
}

export type WarmStepStatus = "skipped" | "completed" | "started" | "no_snapshot";

export interface WarmStepResponse {
  [key: string]: unknown;
  enabled: boolean;
  status: WarmStepStatus;
}

export interface WarmDocumentsResponse {
  [key: string]: unknown;
  enabled: boolean;
  status: WarmStepStatus;
  job?: WarmJob;
}

export interface WarmSnapshotsResponse {
  [key: string]: unknown;
  enabled: boolean;
  status: WarmStepStatus;
  key?: string;
  watermark_ms?: number;
  sha?: string;
}

export interface WarmBlobsResponse {
  [key: string]: unknown;
  enabled: boolean;
  status: WarmStepStatus;
  attributes?: string[];
  budget_bytes?: number;
  documents_scanned: number;
  refs_seen: number;
  objects: number;
  bytes: number;
  missing: number;
  invalid_refs: number;
  budget_exhausted: boolean;
}

export interface WarmCacheResponse {
  [key: string]: unknown;
  "namespace": string;
  turbopuffer: WarmStepResponse;
  documents: WarmDocumentsResponse;
  snapshots: WarmSnapshotsResponse;
  blobs: WarmBlobsResponse;
}

export interface JobBase {
  [key: string]: unknown;
  id: string;
  "namespace": string;
  status: JobStatus;
  progress: number;
  documents_scanned: number;
  stable_as_of?: number;
  created_at: string;
  completed_at?: string | null;
  error?: string | null;
}

export interface SnapshotJob {
  [key: string]: unknown;
  id: string;
  "namespace": string;
  status: JobStatus;
  progress: number;
  documents_scanned: number;
  stable_as_of?: number;
  created_at: string;
  completed_at?: string | null;
  error?: string | null;
  field: string;
  source: SnapshotSource;
  effective_source?: SnapshotSource;
  sha?: string | null;
}

export interface SnapshotJobList {
  [key: string]: unknown;
  snapshot_jobs: SnapshotJob[];
}

export interface WarmJob {
  [key: string]: unknown;
  id: string;
  "namespace": string;
  status: JobStatus;
  progress: number;
  documents_scanned: number;
  stable_as_of?: number;
  created_at: string;
  completed_at?: string | null;
  error?: string | null;
}

export interface WarmJobList {
  [key: string]: unknown;
  warm_jobs: WarmJob[];
}

export interface ScanJob {
  [key: string]: unknown;
  id: string;
  "namespace": string;
  status: JobStatus;
  progress: number;
  documents_scanned: number;
  stable_as_of?: number;
  created_at: string;
  completed_at?: string | null;
  error?: string | null;
  mode: ScanMode;
  field?: string;
  source: ScanSource;
  effective_source?: ScanSource;
  unique_values?: number;
  truncated?: boolean;
  bounded?: boolean;
  approximate?: boolean;
  snapshot_sha?: string;
  watermark_ms?: number;
  threads?: number;
}

export interface ScanJobList {
  [key: string]: unknown;
  scans: ScanJob[];
}

export interface ScanValue {
  [key: string]: unknown;
  v: string;
  n: number;
}

export interface ScanValuesResponse {
  [key: string]: unknown;
  values: ScanValue[];
  total: number;
  truncated: boolean;
}

export interface ScanIdsResponse {
  [key: string]: unknown;
  ids: string[];
  total: number;
}

export interface ScanCountResponse {
  [key: string]: unknown;
  count: number;
  served_by: ScanCountServedBy;
  snapshot_sha?: string;
  watermark_ms?: number;
  bounded?: boolean;
  timed_out?: boolean;
  shards_saturated?: number;
  shards_total?: number;
  approximate?: boolean;
  threads?: number;
  elapsed_ms: number;
}

export interface NamespaceList {
  [key: string]: unknown;
  namespaces: NamespaceListEntry[];
  next_cursor?: string | null;
}

export interface NamespaceListEntry {
  [key: string]: unknown;
  name: string;
  row_count?: number | null;
  size_bytes?: number | null;
  stable_as_of_ms?: number | null;
  is_stable?: boolean | null;
  schema_summary?: NamespaceSchemaSummary;
  index?: IndexState;
  cache_state?: NamespaceCacheState;
  last_write_ms?: number | null;
  shadow?: boolean;
  labels?: Record<string, string>;
  metadata_error?: string;
}

export interface NamespaceSchemaSummary {
  [key: string]: unknown;
  vector_dim?: number | null;
  fields?: string[];
}

export interface IndexState {
  [key: string]: unknown;
  status?: "updating" | "up-to-date";
  unindexed_bytes?: number | null;
}

export interface NamespaceCacheState {
  [key: string]: unknown;
  state: "cold" | "warming" | "warm";
  warmed_through_ms?: number | null;
  warm_inflight: boolean;
}

export interface NamespaceMetadata {
  [key: string]: unknown;
  id: string;
  schema: Record<string, unknown>;
  approx_logical_bytes: number;
  approx_row_count: number;
  created_at: string;
  last_write_at?: string | null;
  updated_at: string;
  config?: Record<string, unknown>;
  index?: IndexState;
  layer?: NamespaceMetadataLayer;
}

export interface NamespaceMetadataLayer {
  [key: string]: unknown;
  stable_as_of?: number | null;
  is_stable?: boolean;
  indexed?: boolean | null;
  index_lag_rows?: number | null;
  schema_version?: number;
  init_state?: "running" | "ready";
  init_lag_rows?: number;
  shard_count?: number | null;
  shard_state?: "unsharded" | "backfilling" | "ready";
  shard_lag_rows?: number;
  scatter_gather_active?: boolean;
}

export interface InitNamespaceRequest {
  [key: string]: unknown;
  schema_version?: number;
  shard_count?: number;
}

export interface InitNamespaceResponse {
  [key: string]: unknown;
  "namespace": string;
  layer: NamespaceMetadataLayer;
}

export interface QueryRequest {
  [key: string]: unknown;
  vector?: number[];
  nearest_to_id?: string[];
  top_k?: number;
  filters?: TurbopufferFilter;
  as_of?: number;
  between?: number[];
  include_attributes?: boolean | string[];
  include_leg_breakdown?: boolean;
  cursor?: string;
  rank_by?: TurbopufferRankBy;
}

export interface FederatedQueryRequest {
  [key: string]: unknown;
  vector?: number[];
  nearest_to_id?: string[];
  top_k?: number;
  filters?: TurbopufferFilter;
  as_of?: number;
  between?: number[];
  include_attributes?: boolean | string[];
  include_leg_breakdown?: boolean;
  cursor?: string;
  rank_by?: TurbopufferRankBy;
  namespaces?: string[];
  strict?: boolean;
  fusion?: FederatedFusionOptions;
}

export interface FederatedFusionOptions {
  [key: string]: unknown;
  per_namespace_limit?: number;
  rank_constant?: number;
}

export interface AgentQueryRequest {
  [key: string]: unknown;
  query: string;
  vector?: number[];
  top_k?: number;
}

export interface AgentQueryResponse {
  [key: string]: unknown;
  rows: Record<string, unknown>[];
  merge: Record<string, unknown>;
  routing?: RoutingEcho;
  hybrid?: HybridEcho;
  namespaces: FederatedNamespaceResult[];
  errors?: FederatedNamespaceError[];
  agent?: AgentEcho;
}

export interface AgentEcho {
  [key: string]: unknown;
  turns: 1 | 2;
  deadlineHit: boolean;
  recallDepth: number;
  relevanceWeight: number;
  queries: Record<string, unknown>[];
  trace?: string;
}

export interface FederatedQueryResponse {
  [key: string]: unknown;
  rows: Record<string, unknown>[];
  merge: Record<string, unknown>;
  routing?: RoutingEcho;
  hybrid?: HybridEcho;
  namespaces: FederatedNamespaceResult[];
  errors?: FederatedNamespaceError[];
}

export interface FederatedNamespaceResult {
  [key: string]: unknown;
  "namespace": string;
  stable_as_of?: number | null;
  matched: number;
}

export interface FederatedNamespaceError {
  [key: string]: unknown;
  "namespace": string;
  error: string;
}

export interface HybridEcho {
  [key: string]: unknown;
  tokens: string[];
  tokens_dropped: number;
  fuzziness: "auto" | 0 | 1 | 2;
  rank_constant: number;
  legs: number;
  per_leg_limit: number;
  surfaced?: boolean;
  threads?: number;
}

export interface RoutingEcho {
  [key: string]: unknown;
  route: "hybrid_text" | "semantic" | "fused";
  policy: string;
  tokens: number;
  executed: boolean;
}

export interface QueryResponse {
  [key: string]: unknown;
  rows: Record<string, unknown>[];
  aggregations?: Record<string, unknown>;
  aggregation_groups?: Record<string, unknown>[];
  billing?: Record<string, unknown>;
  performance?: Record<string, unknown>;
  stable_as_of?: number | null;
  next_cursor?: string | null;
  hybrid?: HybridEcho;
  routing?: RoutingEcho;
}

export interface Error {
  [key: string]: unknown;
  error: string;
  message: string;
}

export interface SnapshotHistoryEntry {
  [key: string]: unknown;
  watermark_ms: number;
  sha: string;
  tags?: string[];
}

export interface SnapshotBody {
  [key: string]: unknown;
  "namespace": string;
  watermark_ms: number;
  sha: string;
  row_count?: number;
  fields: SnapshotField[];
  fields_skipped: SnapshotFieldSkipped[];
}

export interface SnapshotField {
  [key: string]: unknown;
  name: string;
  values: SnapshotValueCount[];
}

export type SnapshotSkipReason = "exceeded_cap";

export interface SnapshotFieldSkipped {
  [key: string]: unknown;
  name: string;
  reason: SnapshotSkipReason;
  distinct_observed: number;
  cap: number;
}

export interface SnapshotValueCount {
  [key: string]: unknown;
  v: string;
  n: number;
}

export interface SnapshotActivityEvent {
  [key: string]: unknown;
  ts_ms: number;
  "namespace": string;
  sha: string;
}

export interface SnapshotActivityList {
  [key: string]: unknown;
  events: SnapshotActivityEvent[];
  next_cursor?: string;
  truncated?: boolean;
}

export type MetricKind = "counter" | "gauge" | "histogram";

export type MetricFamily = "query" | "upsert" | "fetch" | "cache" | "pipeline" | "storage" | "saturation";

export interface MetricAlert {
  [key: string]: unknown;
  summary: string;
  expr: string;
  "for": string;
}

export interface MetricCatalogEntry {
  [key: string]: unknown;
  name: string;
  kind: MetricKind;
  family: MetricFamily;
  labels: string[];
  description: string;
  example_promql: string;
  alert?: MetricAlert;
}

export interface MetricCatalog {
  [key: string]: unknown;
  version: string;
  entries: MetricCatalogEntry[];
}

export type PrometheusResponse = Record<string, unknown>;

export interface SearchHistoryEntry {
  [key: string]: unknown;
  timestamp: string;
  timestamp_nanos: number;
  "namespace": string;
  trace_id?: string;
  raw_query?: string;
  stable_as_of?: number;
  query: Record<string, unknown>;
  top_result_ids: string[];
  tags: string[];
}

export interface SearchHistoryListResponse {
  [key: string]: unknown;
  entries: SearchHistoryEntry[];
  next_cursor?: string;
}

export interface ClickstreamEvent {
  [key: string]: unknown;
  timestamp: string;
  timestamp_nanos: number;
  trace_id: string;
  "namespace": string;
  doc_id: string;
  tags: string[];
  source: string;
  served_from: string;
}

export interface ClickstreamListResponse {
  [key: string]: unknown;
  events: ClickstreamEvent[];
  next_cursor?: string;
}

export type KubernetesCondition = Record<string, unknown>;

export interface SecretKeyRef {
  [key: string]: unknown;
  name: string;
  key: string;
}

export interface VectorStoreEndpoint {
  [key: string]: unknown;
  url: string;
  region: string;
}

export interface VectorStoreTurbopuffer {
  [key: string]: unknown;
  orgId: string;
}

export interface VectorStoreCredential {
  [key: string]: unknown;
  secretRef: SecretKeyRef;
}

export interface VectorStoreInboundAuth {
  [key: string]: unknown;
  mode?: "deriveFromStore" | "keys" | "open";
}

export interface VectorStoreStatus {
  [key: string]: unknown;
  reachable?: boolean;
  observedGeneration?: number;
  conditions: KubernetesCondition[];
}

export interface VectorStore {
  [key: string]: unknown;
  name: string;
  kind: "turbopuffer" | "search" | "search-embedded";
  "default": boolean;
  endpoint: VectorStoreEndpoint;
  turbopuffer?: VectorStoreTurbopuffer;
  credential: VectorStoreCredential;
  inboundAuth?: VectorStoreInboundAuth;
  status: VectorStoreStatus;
  turbopufferUrl?: string;
}

export interface VectorStoreList {
  [key: string]: unknown;
  vectorstores: VectorStore[];
}

export interface WarehouseSecretRef {
  [key: string]: unknown;
  name: string;
}

export interface WarehousePool {
  [key: string]: unknown;
  size: number;
  timeout: string;
}

export interface SnowflakeWarehouse {
  [key: string]: unknown;
  account: string;
  user: string;
  role?: string;
  warehouse: string;
  keyPairSecretRef: WarehouseSecretRef;
  pool?: WarehousePool;
}

export interface RestWarehouse {
  [key: string]: unknown;
  baseUrl: string;
  auth?: RestWarehouseAuth;
  rateLimit?: RestWarehouseRateLimit;
  verify: RestWarehouseVerify;
}

export interface RestWarehouseAuth {
  [key: string]: unknown;
  "in": "query" | "header";
  name: string;
  secretRef: WarehouseSecretRef;
}

export interface RestWarehouseRateLimit {
  [key: string]: unknown;
  requestsPerSecond: number;
}

export interface RestWarehouseVerify {
  [key: string]: unknown;
  path: string;
  query?: Record<string, string>;
}

export type WarehousePhase = "Pending" | "Verified" | "Failed";

export interface WarehouseConsumers {
  [key: string]: unknown;
  pipelines: number;
  apiKeys: number;
}

export interface WarehouseStatus {
  [key: string]: unknown;
  phase?: WarehousePhase;
  verifiedAt?: string;
  failureReason?: string;
  consumers: WarehouseConsumers;
  observedGeneration?: number;
  conditions: KubernetesCondition[];
}

export interface Warehouse {
  [key: string]: unknown;
  name: string;
  "namespace": string;
  kind: string;
  snowflake?: SnowflakeWarehouse;
  rest?: RestWarehouse;
  verifyInterval: string;
  status: WarehouseStatus;
}

export interface WarehouseList {
  [key: string]: unknown;
  warehouses: Warehouse[];
}

export interface ApiKeyEntitlement {
  [key: string]: unknown;
  scopes?: ("read" | "write" | "admin")[];
  namespaces?: string[];
  claims?: string[];
}

export type ApiKeyEntitlements = Record<string, ApiKeyEntitlement>;

export type ApiKeyPhase = "Pending" | "Active" | "Revoked" | "Expired";

export interface ApiKey {
  [key: string]: unknown;
  keyId: string;
  name: string;
  owner?: string;
  description?: string;
  entitlements: ApiKeyEntitlements;
  expiresAfter?: string;
  phase: ApiKeyPhase;
  createdAt: string;
  expiresAt?: string;
  revokedAt?: string;
  lastSeenAt?: string;
  lookupHash?: string;
  secretRef?: Record<string, unknown>;
}

export interface ApiKeyList {
  [key: string]: unknown;
  keys: ApiKey[];
}

export interface MintKeyRequest {
  [key: string]: unknown;
  name: string;
  owner?: string;
  description?: string;
  entitlements?: ApiKeyEntitlements;
  expiresAfter?: string;
}

export interface MintKeyResponse {
  [key: string]: unknown;
  keyId: string;
  name: string;
  owner?: string;
  description?: string;
  entitlements: ApiKeyEntitlements;
  expiresAfter?: string;
  phase: ApiKeyPhase;
  createdAt: string;
  expiresAt?: string;
  token: string;
}

export interface AuthenticateKeyRequest {
  [key: string]: unknown;
  token: string;
}

export interface AuthenticateKeyResponse {
  [key: string]: unknown;
  keyId: string;
  name: string;
  owner?: string;
  entitlements: ApiKeyEntitlements;
  expiresAt?: string;
}
