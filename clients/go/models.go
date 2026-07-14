package hevlayer

type JSONValue = interface{}

type LicenseSurfaceState struct {
	State string `json:"state"`
	SecondsToDeadline int64 `json:"seconds_to_deadline"`
	GraceSecondsRemaining int64 `json:"grace_seconds_remaining"`
}

type LicenseState struct {
	Valid bool `json:"valid"`
	State string `json:"state,omitempty"`
	Reason string `json:"reason,omitempty"`
	Sub string `json:"sub,omitempty"`
	Tier string `json:"tier,omitempty"`
	Features []string `json:"features,omitempty"`
	Limits map[string]int64 `json:"limits,omitempty"`
	Exp string `json:"exp,omitempty"`
	Gateway LicenseSurfaceState `json:"gateway"`
}

type CreatePipelineRequest struct {
	ID string `json:"id"`
	TargetNamespace string `json:"target_namespace"`
	DistanceMetric string `json:"distance_metric,omitempty"`
}

type Pipeline struct {
	ID string `json:"id"`
	TargetNamespace string `json:"target_namespace"`
	DistanceMetric string `json:"distance_metric"`
	CreatedAt string `json:"created_at"`
}

type PipelineList struct {
	Pipelines []Pipeline `json:"pipelines"`
}

type PipelineStatus struct {
	PipelineID string `json:"pipeline_id"`
	Status string `json:"status"`
	Counts map[string]int64 `json:"counts"`
	FailedReasons map[string]int64 `json:"failed_reasons"`
	PendingCount int64 `json:"pending_count"`
	ProcessingCount int64 `json:"processing_count"`
	FailedCount int64 `json:"failed_count"`
	IndexedRatePerMin float64 `json:"indexed_rate_per_min"`
	RateWindowSeconds int64 `json:"rate_window_seconds"`
}

type ClaimDocumentsRequest struct {
	Stage string `json:"stage,omitempty"`
	ClaimStage string `json:"claim_stage,omitempty"`
	Limit int64 `json:"limit,omitempty"`
	WorkerID string `json:"worker_id"`
	LeaseSeconds int64 `json:"lease_seconds,omitempty"`
	DocumentIdPrefix string `json:"document_id_prefix,omitempty"`
}

type ClaimDocumentsResponse struct {
	PipelineID string `json:"pipeline_id"`
	Stage string `json:"stage"`
	ClaimStage string `json:"claim_stage"`
	WorkerID string `json:"worker_id"`
	Documents []string `json:"documents"`
}

type HeartbeatDocumentsRequest struct {
	DocumentIds []string `json:"document_ids"`
	Stage string `json:"stage,omitempty"`
	WorkerID string `json:"worker_id"`
}

type SetDocumentsStageRequest struct {
	DocumentIds []string `json:"document_ids"`
	Stage string `json:"stage"`
	FromStage string `json:"from_stage,omitempty"`
	WorkerID string `json:"worker_id,omitempty"`
	CreateMissing bool `json:"create_missing,omitempty"`
}

type DocumentsStageResponse struct {
	PipelineID string `json:"pipeline_id"`
	Stage string `json:"stage"`
	Updated int64 `json:"updated"`
}

type StageDocumentResponse struct {
	PipelineID string `json:"pipeline_id"`
	DocumentID string `json:"document_id"`
	Stage string `json:"stage"`
	ChunkCount int64 `json:"chunk_count"`
	ChunkIds []string `json:"chunk_ids"`
}

type Chunk struct {
	ID string `json:"id"`
	Text string `json:"text,omitempty"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

type PutChunksRequest struct {
	Chunks []Chunk `json:"chunks"`
}

type GetChunksResponse []Chunk

type VectorEntry struct {
	ID string `json:"id"`
	Vector []float64 `json:"vector,omitempty"`
	Vectors [][]float64 `json:"vectors,omitempty"`
	Attributes map[string]interface{} `json:"attributes,omitempty"`
}

type PutVectorsRequest struct {
	Vectors []VectorEntry `json:"vectors"`
}

type CreateUdfRequest struct {
	ID string `json:"id"`
	Spec UdfSpec `json:"spec"`
}

type UpdateUdfRequest struct {
	Spec UdfSpec `json:"spec"`
}

type Udf struct {
	ID string `json:"id"`
	Spec UdfSpec `json:"spec"`
	Paused bool `json:"paused"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

type UdfList struct {
	Udfs []Udf `json:"udfs"`
}

type GetUdfResponse struct {
	Udf Udf `json:"udf"`
	Status UdfStatus `json:"status"`
}

type UdfStatus struct {
	UdfID string `json:"udf_id"`
	Paused bool `json:"paused"`
	ActiveNamespaces []string `json:"active_namespaces"`
	Discovery UdfDiscoveryStatus `json:"discovery"`
	Counts map[string]int64 `json:"counts"`
	PendingCount int64 `json:"pending_count"`
	ProcessingCount int64 `json:"processing_count"`
	FailedCount int64 `json:"failed_count"`
	IndexedRatePerMin float64 `json:"indexed_rate_per_min"`
	RateWindowSeconds int64 `json:"rate_window_seconds"`
}

type UdfDiscoveryStatus struct {
	SweepsCompleted int64 `json:"sweeps_completed"`
	LastCompletedAt *string `json:"last_completed_at"`
}

type UdfSpec struct {
	IndexSelector interface{} `json:"index_selector,omitempty"`
	TargetNamespaces []string `json:"target_namespaces,omitempty"`
	Inputs []string `json:"inputs,omitempty"`
	Version string `json:"version,omitempty"`
	Filter interface{} `json:"filter,omitempty"`
	Worker UdfWorkerSpec `json:"worker"`
	Schedule UdfScheduleSpec `json:"schedule,omitempty"`
	Retry UdfRetrySpec `json:"retry,omitempty"`
	Triggers []UdfTrigger `json:"triggers,omitempty"`
	Invalidates []string `json:"invalidates,omitempty"`
}

type UdfTrigger string

type UdfWorkerSpec struct {
	Image string `json:"image,omitempty"`
	Url string `json:"url,omitempty"`
	Port int64 `json:"port,omitempty"`
	BatchSize int64 `json:"batch_size,omitempty"`
	TimeoutSeconds int64 `json:"timeout_seconds,omitempty"`
	PodSpec interface{} `json:"pod_spec,omitempty"`
}

type UdfScheduleSpec struct {
	DiscoveryIntervalSeconds int64 `json:"discovery_interval_seconds,omitempty"`
	LeaseSeconds int64 `json:"lease_seconds,omitempty"`
	MaxInFlightBatches int64 `json:"max_in_flight_batches,omitempty"`
	MaxConcurrentScans int64 `json:"max_concurrent_scans,omitempty"`
}

type UdfRetrySpec struct {
	MaxAttempts int64 `json:"max_attempts,omitempty"`
	InitialBackoffSeconds int64 `json:"initial_backoff_seconds,omitempty"`
	MaxBackoffSeconds int64 `json:"max_backoff_seconds,omitempty"`
}

type UdfDiscoverRequest struct {
	Namespaces []string `json:"namespaces,omitempty"`
	PageSize int64 `json:"page_size,omitempty"`
}

type UdfDiscoverResponse struct {
	UdfID string `json:"udf_id"`
	Enqueued int64 `json:"enqueued"`
	Namespaces []string `json:"namespaces"`
}

type UdfClaimRequest struct {
	WorkerID string `json:"worker_id"`
	Limit int64 `json:"limit,omitempty"`
	LeaseSeconds int64 `json:"lease_seconds,omitempty"`
}

type UdfClaimedItem struct {
	Namespace string `json:"namespace"`
	ID string `json:"id"`
	Input map[string]interface{} `json:"input"`
}

type UdfClaimResponse struct {
	UdfID string `json:"udf_id"`
	WorkerID string `json:"worker_id"`
	Items []UdfClaimedItem `json:"items"`
}

type UdfItemRef struct {
	Namespace string `json:"namespace"`
	ID string `json:"id"`
}

type UdfHeartbeatRequest struct {
	WorkerID string `json:"worker_id"`
	Items []UdfItemRef `json:"items"`
}

type UdfCompleteRequest struct {
	WorkerID string `json:"worker_id"`
	Items []UdfCompleteItem `json:"items"`
}

type UdfCompleteItem struct {
	Namespace string `json:"namespace"`
	ID string `json:"id"`
	Vector []float64 `json:"vector,omitempty"`
	Vectors [][]float64 `json:"vectors,omitempty"`
	Attributes map[string]interface{} `json:"attributes,omitempty"`
}

type UdfErrorKind string

type UdfFailRequest struct {
	WorkerID string `json:"worker_id"`
	Items []UdfFailItem `json:"items"`
}

type UdfFailItem struct {
	Namespace string `json:"namespace"`
	ID string `json:"id"`
	Kind UdfErrorKind `json:"kind"`
	Message string `json:"message,omitempty"`
}

type UdfItemsResponse struct {
	UdfID string `json:"udf_id"`
	Updated int64 `json:"updated"`
}

type CostWindow string

type CostStep string

type CostBasis string

type CostTotals struct {
	TotalUsd float64 `json:"total_usd"`
	AwsUsd float64 `json:"aws_usd"`
	TurbopufferUsd float64 `json:"turbopuffer_usd"`
	CostPerQueryUsd float64 `json:"cost_per_query_usd,omitempty"`
	CostPerDocumentUsd float64 `json:"cost_per_document_usd,omitempty"`
	CostPerTibIndexedUsd float64 `json:"cost_per_tib_indexed_usd,omitempty"`
}

type CostLine struct {
	Provider string `json:"provider"`
	Service string `json:"service"`
	Basis CostBasis `json:"basis"`
	ServiceDetail string `json:"service_detail,omitempty"`
	Region string `json:"region,omitempty"`
	Site string `json:"site,omitempty"`
	RateCardVersion string `json:"rate_card_version,omitempty"`
	AmountUsd float64 `json:"amount_usd"`
	Qty float64 `json:"qty,omitempty"`
	Unit string `json:"unit,omitempty"`
	QtyBytes int64 `json:"qty_bytes,omitempty"`
	Breakdown []map[string]interface{} `json:"breakdown,omitempty"`
}

type CostRateCardStatus struct {
	TurbopufferRateCardVersion string `json:"turbopuffer_rate_card_version"`
	AwsCostSource string `json:"aws_cost_source"`
	AwsCostRefreshedAtMs int64 `json:"aws_cost_refreshed_at_ms"`
	AwsCostStale bool `json:"aws_cost_stale"`
	AwsPricingStale bool `json:"aws_pricing_stale"`
	AwsPricingRefreshedAtMs int64 `json:"aws_pricing_refreshed_at_ms"`
}

type CostSnapshot struct {
	AsOfMs int64 `json:"as_of_ms"`
	WindowSeconds int64 `json:"window_seconds"`
	Totals CostTotals `json:"totals"`
	Lines []CostLine `json:"lines"`
	RateCardStatus CostRateCardStatus `json:"rate_card_status"`
	Caveats []string `json:"caveats"`
}

type CostSample []interface{}

type CostSeries struct {
	Provider string `json:"provider,omitempty"`
	Service string `json:"service,omitempty"`
	Basis CostBasis `json:"basis,omitempty"`
	ServiceDetail string `json:"service_detail,omitempty"`
	Region string `json:"region,omitempty"`
	Site string `json:"site,omitempty"`
	RateCardVersion string `json:"rate_card_version,omitempty"`
	Label string `json:"label,omitempty"`
	Samples []CostSample `json:"samples"`
}

type CostTimeseries struct {
	WindowSeconds int64 `json:"window_seconds"`
	StepSeconds int64 `json:"step_seconds"`
	Series []CostSeries `json:"series"`
}

type AwsInstancePrice struct {
	InstanceType string `json:"instance_type"`
	Family string `json:"family"`
	Vcpu int64 `json:"vcpu"`
	MemoryGib float64 `json:"memory_gib"`
	NvmeGib float64 `json:"nvme_gib"`
	HourlyUsd float64 `json:"hourly_usd"`
}

type AwsRateCard struct {
	Role string `json:"role"`
	Region string `json:"region"`
	RefreshedAtMs int64 `json:"refreshed_at_ms"`
	TtlSeconds int64 `json:"ttl_seconds"`
	Stale bool `json:"stale"`
	Items []AwsInstancePrice `json:"items"`
}

type TurbopufferRateLine struct {
	Service string `json:"service"`
	Unit string `json:"unit"`
	Usd float64 `json:"usd"`
}

type TurbopufferRateCard struct {
	Version string `json:"version"`
	VerifiedBy string `json:"verified_by"`
	VerifiedAt string `json:"verified_at"`
	Source string `json:"source"`
	Lines []TurbopufferRateLine `json:"lines"`
}

type RateCard struct {
	Aws AwsRateCard `json:"aws"`
	Turbopuffer TurbopufferRateCard `json:"turbopuffer"`
}

type Document struct {
	ID string `json:"id"`
	Attributes map[string]interface{} `json:"attributes"`
}

type FetchDocumentsRequest struct {
	Ids []string `json:"ids"`
	IncludeAttributes []string `json:"include_attributes,omitempty"`
}

type FetchDocumentsResponse struct {
	Documents []Document `json:"documents"`
	Missing []string `json:"missing"`
}

type StatusResponse struct {
	Status string `json:"status"`
	Message string `json:"message,omitempty"`
	RowsAffected int64 `json:"rows_affected,omitempty"`
	RowsUpserted int64 `json:"rows_upserted,omitempty"`
	RowsPatched int64 `json:"rows_patched,omitempty"`
	RowsDeleted int64 `json:"rows_deleted,omitempty"`
	Billing map[string]interface{} `json:"billing,omitempty"`
}

type BlobPutResponse struct {
	Ref string `json:"ref"`
	Sha256 string `json:"sha256"`
	Size int64 `json:"size"`
}

type TurbopufferNamespaceSummary struct {
	ID string `json:"id"`
}

type TurbopufferNamespaceList struct {
	Namespaces []TurbopufferNamespaceSummary `json:"namespaces"`
	NextCursor string `json:"next_cursor,omitempty"`
}

type TurbopufferSchema map[string]interface{}

type TurbopufferMetadataPatch struct {
	Pinning interface{} `json:"pinning,omitempty"`
}

type TurbopufferWriteRequest map[string]interface{}

type TurbopufferBranchFromRequest struct {
	BranchFromNamespace map[string]interface{} `json:"branch_from_namespace"`
}

type TurbopufferCopyFromRequest struct {
	CopyFromNamespace interface{} `json:"copy_from_namespace"`
}

type TurbopufferWriteResponse struct {
	Status string `json:"status"`
	Message string `json:"message"`
	RowsAffected int64 `json:"rows_affected"`
	RowsUpserted int64 `json:"rows_upserted,omitempty"`
	RowsPatched int64 `json:"rows_patched,omitempty"`
	RowsDeleted int64 `json:"rows_deleted,omitempty"`
	RowsRemaining bool `json:"rows_remaining,omitempty"`
	UpsertedIds []interface{} `json:"upserted_ids,omitempty"`
	PatchedIds []interface{} `json:"patched_ids,omitempty"`
	DeletedIds []interface{} `json:"deleted_ids,omitempty"`
	Billing map[string]interface{} `json:"billing"`
	Performance map[string]interface{} `json:"performance,omitempty"`
}

type TurbopufferQueryRequest map[string]interface{}

type TurbopufferQueryResponse struct {
	Rows []map[string]interface{} `json:"rows,omitempty"`
	Aggregations map[string]interface{} `json:"aggregations,omitempty"`
	AggregationGroups []map[string]interface{} `json:"aggregation_groups,omitempty"`
	Billing map[string]interface{} `json:"billing,omitempty"`
	Performance map[string]interface{} `json:"performance,omitempty"`
}

type BatchQueryRequest struct {
	Queries []TurbopufferQueryRequest `json:"queries"`
	Consistency map[string]interface{} `json:"consistency,omitempty"`
	VectorEncoding string `json:"vector_encoding,omitempty"`
}

type BatchQueryResponse struct {
	Results []TurbopufferQueryResponse `json:"results"`
	Billing map[string]interface{} `json:"billing,omitempty"`
	Performance map[string]interface{} `json:"performance,omitempty"`
	StableAsOf int64 `json:"stable_as_of,omitempty"`
}

type TurbopufferExplainQueryResponse struct {
	PlanText string `json:"plan_text,omitempty"`
}

type TurbopufferRecallRequest struct {
	Num int64 `json:"num,omitempty"`
	TopK int64 `json:"top_k,omitempty"`
	Filters interface{} `json:"filters,omitempty"`
	RankBy interface{} `json:"rank_by,omitempty"`
	IncludeGroundTruth bool `json:"include_ground_truth,omitempty"`
}

type TurbopufferRecallResponse struct {
	AvgRecall float64 `json:"avg_recall"`
	AvgExhaustiveCount float64 `json:"avg_exhaustive_count"`
	AvgAnnCount float64 `json:"avg_ann_count"`
	GroundTruth []map[string]interface{} `json:"ground_truth,omitempty"`
}

type HintCacheWarmResponse struct {
	Namespace string `json:"namespace,omitempty"`
	Turbopuffer WarmStepResponse `json:"turbopuffer,omitempty"`
	Documents WarmDocumentsResponse `json:"documents,omitempty"`
	Snapshots WarmSnapshotsResponse `json:"snapshots,omitempty"`
	Blobs WarmBlobsResponse `json:"blobs,omitempty"`
}

type JobStatus string

type SnapshotSource string

type ScanSource string

type ScanCountSource string

type ScanMode string

type ScanCountServedBy string

type CreateSnapshotRequest struct {
	Field string `json:"field"`
	Source SnapshotSource `json:"source,omitempty"`
	Filters interface{} `json:"filters,omitempty"`
	PageSize int64 `json:"page_size,omitempty"`
}

type SnapshotPolicy struct {
	FacetFields []string `json:"facetFields,omitempty"`
	Interval string `json:"interval,omitempty"`
	Retention string `json:"retention,omitempty"`
}

type CreateCheckpointRequest struct {
	Label string `json:"label"`
}

type Checkpoint struct {
	Namespace string `json:"namespace"`
	Label string `json:"label"`
	WatermarkMs int64 `json:"watermark_ms"`
	Sha string `json:"sha"`
	RowCount int64 `json:"row_count"`
}

type CheckpointList struct {
	Checkpoints []Checkpoint `json:"checkpoints"`
	NextCursor string `json:"next_cursor,omitempty"`
}

type CreateScanRequest struct {
	Source ScanCountSource `json:"source,omitempty"`
	Filters interface{} `json:"filters,omitempty"`
	AsOf int64 `json:"as_of,omitempty"`
	Between []int64 `json:"between,omitempty"`
	Fts FtsScan `json:"fts,omitempty"`
	HybridText HybridTextScan `json:"hybrid_text,omitempty"`
	Ann AnnScan `json:"ann,omitempty"`
	Mode ScanMode `json:"mode,omitempty"`
	Field string `json:"field,omitempty"`
	Exhaustive bool `json:"exhaustive,omitempty"`
	Threads int64 `json:"threads,omitempty"`
	PageSize int64 `json:"page_size,omitempty"`
	TimeoutSeconds int64 `json:"timeout_seconds,omitempty"`
}

type FtsScan struct {
	Field string `json:"field"`
	Query string `json:"query"`
}

type HybridTextScan struct {
	Field string `json:"field"`
	Query string `json:"query"`
	Fuzziness interface{} `json:"fuzziness,omitempty"`
}

type AnnScan struct {
	Vector []float64 `json:"vector"`
	Field string `json:"field,omitempty"`
	Radius float64 `json:"radius"`
}

type WarmStepStatus string

type WarmStepResponse struct {
	Enabled bool `json:"enabled"`
	Status WarmStepStatus `json:"status"`
}

type WarmDocumentsResponse struct {
	Enabled bool `json:"enabled"`
	Status WarmStepStatus `json:"status"`
	Job WarmJob `json:"job,omitempty"`
}

type WarmSnapshotsResponse struct {
	Enabled bool `json:"enabled"`
	Status WarmStepStatus `json:"status"`
	Key string `json:"key,omitempty"`
	WatermarkMs int64 `json:"watermark_ms,omitempty"`
	Sha string `json:"sha,omitempty"`
}

type WarmBlobsResponse struct {
	Enabled bool `json:"enabled"`
	Status WarmStepStatus `json:"status"`
	Attributes []string `json:"attributes,omitempty"`
	BudgetBytes int64 `json:"budget_bytes,omitempty"`
	DocumentsScanned int64 `json:"documents_scanned"`
	RefsSeen int64 `json:"refs_seen"`
	Objects int64 `json:"objects"`
	Bytes int64 `json:"bytes"`
	Missing int64 `json:"missing"`
	InvalidRefs int64 `json:"invalid_refs"`
	BudgetExhausted bool `json:"budget_exhausted"`
}

type WarmCacheResponse struct {
	Namespace string `json:"namespace"`
	Turbopuffer WarmStepResponse `json:"turbopuffer"`
	Documents WarmDocumentsResponse `json:"documents"`
	Snapshots WarmSnapshotsResponse `json:"snapshots"`
	Blobs WarmBlobsResponse `json:"blobs"`
}

type JobBase struct {
	ID string `json:"id"`
	Namespace string `json:"namespace"`
	Status JobStatus `json:"status"`
	Progress float64 `json:"progress"`
	DocumentsScanned int64 `json:"documents_scanned"`
	StableAsOf int64 `json:"stable_as_of,omitempty"`
	CreatedAt string `json:"created_at"`
	CompletedAt string `json:"completed_at,omitempty"`
	Error string `json:"error,omitempty"`
}

type SnapshotJob struct {
	ID string `json:"id"`
	Namespace string `json:"namespace"`
	Status JobStatus `json:"status"`
	Progress float64 `json:"progress"`
	DocumentsScanned int64 `json:"documents_scanned"`
	StableAsOf int64 `json:"stable_as_of,omitempty"`
	CreatedAt string `json:"created_at"`
	CompletedAt string `json:"completed_at,omitempty"`
	Error string `json:"error,omitempty"`
	Field string `json:"field"`
	Source SnapshotSource `json:"source"`
	EffectiveSource SnapshotSource `json:"effective_source,omitempty"`
	Sha string `json:"sha,omitempty"`
}

type SnapshotJobList struct {
	SnapshotJobs []SnapshotJob `json:"snapshot_jobs"`
}

type WarmJob struct {
	ID string `json:"id"`
	Namespace string `json:"namespace"`
	Status JobStatus `json:"status"`
	Progress float64 `json:"progress"`
	DocumentsScanned int64 `json:"documents_scanned"`
	StableAsOf int64 `json:"stable_as_of,omitempty"`
	CreatedAt string `json:"created_at"`
	CompletedAt string `json:"completed_at,omitempty"`
	Error string `json:"error,omitempty"`
}

type WarmJobList struct {
	WarmJobs []WarmJob `json:"warm_jobs"`
}

type ScanJob struct {
	ID string `json:"id"`
	Namespace string `json:"namespace"`
	Status JobStatus `json:"status"`
	Progress float64 `json:"progress"`
	DocumentsScanned int64 `json:"documents_scanned"`
	StableAsOf int64 `json:"stable_as_of,omitempty"`
	CreatedAt string `json:"created_at"`
	CompletedAt string `json:"completed_at,omitempty"`
	Error string `json:"error,omitempty"`
	Mode ScanMode `json:"mode"`
	Field string `json:"field,omitempty"`
	Source ScanSource `json:"source"`
	EffectiveSource ScanSource `json:"effective_source,omitempty"`
	UniqueValues int64 `json:"unique_values,omitempty"`
	Truncated bool `json:"truncated,omitempty"`
	Bounded bool `json:"bounded,omitempty"`
	Approximate bool `json:"approximate,omitempty"`
	SnapshotSha string `json:"snapshot_sha,omitempty"`
	WatermarkMs int64 `json:"watermark_ms,omitempty"`
	Threads int64 `json:"threads,omitempty"`
}

type ScanJobList struct {
	Scans []ScanJob `json:"scans"`
}

type ScanValue struct {
	V string `json:"v"`
	N int64 `json:"n"`
}

type ScanValuesResponse struct {
	Values []ScanValue `json:"values"`
	Total int64 `json:"total"`
	Truncated bool `json:"truncated"`
}

type ScanIdsResponse struct {
	Ids []string `json:"ids"`
	Total int64 `json:"total"`
}

type ScanCountResponse struct {
	Count int64 `json:"count"`
	ServedBy ScanCountServedBy `json:"served_by"`
	SnapshotSha string `json:"snapshot_sha,omitempty"`
	WatermarkMs int64 `json:"watermark_ms,omitempty"`
	Bounded bool `json:"bounded,omitempty"`
	TimedOut bool `json:"timed_out,omitempty"`
	ShardsSaturated int64 `json:"shards_saturated,omitempty"`
	ShardsTotal int64 `json:"shards_total,omitempty"`
	Approximate bool `json:"approximate,omitempty"`
	Threads int64 `json:"threads,omitempty"`
	ElapsedMs int64 `json:"elapsed_ms"`
}

type NamespaceList struct {
	Namespaces []NamespaceListEntry `json:"namespaces"`
	NextCursor string `json:"next_cursor,omitempty"`
}

type NamespaceListEntry struct {
	Name string `json:"name"`
	RowCount int64 `json:"row_count,omitempty"`
	SizeBytes int64 `json:"size_bytes,omitempty"`
	StableAsOfMs int64 `json:"stable_as_of_ms,omitempty"`
	IsStable bool `json:"is_stable,omitempty"`
	SchemaSummary NamespaceSchemaSummary `json:"schema_summary,omitempty"`
	Index IndexState `json:"index,omitempty"`
	CacheState NamespaceCacheState `json:"cache_state,omitempty"`
	LastWriteMs int64 `json:"last_write_ms,omitempty"`
	Shadow bool `json:"shadow,omitempty"`
	Labels map[string]string `json:"labels,omitempty"`
	MetadataError string `json:"metadata_error,omitempty"`
}

type NamespaceSchemaSummary struct {
	VectorDim int64 `json:"vector_dim,omitempty"`
	Fields []string `json:"fields,omitempty"`
}

type IndexState struct {
	Status string `json:"status,omitempty"`
	UnindexedBytes int64 `json:"unindexed_bytes,omitempty"`
}

type NamespaceCacheState struct {
	State string `json:"state"`
	WarmedThroughMs int64 `json:"warmed_through_ms,omitempty"`
	WarmInflight bool `json:"warm_inflight"`
}

type NamespaceMetadata struct {
	ID string `json:"id"`
	Schema map[string]interface{} `json:"schema"`
	ApproxLogicalBytes int64 `json:"approx_logical_bytes"`
	ApproxRowCount int64 `json:"approx_row_count"`
	CreatedAt string `json:"created_at"`
	LastWriteAt string `json:"last_write_at,omitempty"`
	UpdatedAt string `json:"updated_at"`
	Config map[string]interface{} `json:"config,omitempty"`
	Index IndexState `json:"index,omitempty"`
	Layer NamespaceMetadataLayer `json:"layer,omitempty"`
}

type NamespaceMetadataLayer struct {
	StableAsOf int64 `json:"stable_as_of,omitempty"`
	IsStable bool `json:"is_stable,omitempty"`
	Indexed bool `json:"indexed,omitempty"`
	IndexLagRows int64 `json:"index_lag_rows,omitempty"`
	SchemaVersion int64 `json:"schema_version,omitempty"`
	InitState string `json:"init_state,omitempty"`
	InitLagRows int64 `json:"init_lag_rows,omitempty"`
	ShardCount int64 `json:"shard_count,omitempty"`
	ShardState string `json:"shard_state,omitempty"`
	ShardLagRows int64 `json:"shard_lag_rows,omitempty"`
	ScatterGatherActive bool `json:"scatter_gather_active,omitempty"`
}

type InitNamespaceRequest struct {
	SchemaVersion int64 `json:"schema_version,omitempty"`
	ShardCount int64 `json:"shard_count,omitempty"`
}

type InitNamespaceResponse struct {
	Namespace string `json:"namespace"`
	Layer NamespaceMetadataLayer `json:"layer"`
}

type QueryRequest struct {
	Vector []float64 `json:"vector,omitempty"`
	NearestToID []string `json:"nearest_to_id,omitempty"`
	TopK int64 `json:"top_k,omitempty"`
	Filters interface{} `json:"filters,omitempty"`
	AsOf int64 `json:"as_of,omitempty"`
	Between []int64 `json:"between,omitempty"`
	IncludeAttributes interface{} `json:"include_attributes,omitempty"`
	IncludeLegBreakdown bool `json:"include_leg_breakdown,omitempty"`
	Cursor string `json:"cursor,omitempty"`
	RankBy []interface{} `json:"rank_by,omitempty"`
}

type FederatedQueryRequest struct {
	Vector []float64 `json:"vector,omitempty"`
	NearestToID []string `json:"nearest_to_id,omitempty"`
	TopK int64 `json:"top_k,omitempty"`
	Filters interface{} `json:"filters,omitempty"`
	AsOf int64 `json:"as_of,omitempty"`
	Between []int64 `json:"between,omitempty"`
	IncludeAttributes interface{} `json:"include_attributes,omitempty"`
	IncludeLegBreakdown bool `json:"include_leg_breakdown,omitempty"`
	Cursor string `json:"cursor,omitempty"`
	RankBy []interface{} `json:"rank_by,omitempty"`
	Namespaces []string `json:"namespaces,omitempty"`
	Strict bool `json:"strict,omitempty"`
	Fusion FederatedFusionOptions `json:"fusion,omitempty"`
}

type FederatedFusionOptions struct {
	PerNamespaceLimit int64 `json:"per_namespace_limit,omitempty"`
	RankConstant int64 `json:"rank_constant,omitempty"`
}

type AgentQueryRequest struct {
	Query string `json:"query"`
	Vector []float64 `json:"vector,omitempty"`
	TopK int64 `json:"top_k,omitempty"`
}

type AgentQueryResponse struct {
	Rows []map[string]interface{} `json:"rows"`
	Merge map[string]interface{} `json:"merge"`
	Routing RoutingEcho `json:"routing,omitempty"`
	Hybrid HybridEcho `json:"hybrid,omitempty"`
	Namespaces []FederatedNamespaceResult `json:"namespaces"`
	Errors []FederatedNamespaceError `json:"errors,omitempty"`
	Agent AgentEcho `json:"agent,omitempty"`
}

type AgentEcho struct {
	Turns string `json:"turns"`
	DeadlineHit bool `json:"deadlineHit"`
	RecallDepth int64 `json:"recallDepth"`
	RelevanceWeight float64 `json:"relevanceWeight"`
	Queries []map[string]interface{} `json:"queries"`
	Trace string `json:"trace,omitempty"`
}

type FederatedQueryResponse struct {
	Rows []map[string]interface{} `json:"rows"`
	Merge map[string]interface{} `json:"merge"`
	Routing RoutingEcho `json:"routing,omitempty"`
	Hybrid HybridEcho `json:"hybrid,omitempty"`
	Namespaces []FederatedNamespaceResult `json:"namespaces"`
	Errors []FederatedNamespaceError `json:"errors,omitempty"`
}

type FederatedNamespaceResult struct {
	Namespace string `json:"namespace"`
	StableAsOf int64 `json:"stable_as_of,omitempty"`
	Matched int64 `json:"matched"`
}

type FederatedNamespaceError struct {
	Namespace string `json:"namespace"`
	Error string `json:"error"`
}

type HybridEcho struct {
	Tokens []string `json:"tokens"`
	TokensDropped int64 `json:"tokens_dropped"`
	Fuzziness interface{} `json:"fuzziness"`
	RankConstant int64 `json:"rank_constant"`
	Legs int64 `json:"legs"`
	PerLegLimit int64 `json:"per_leg_limit"`
	Surfaced bool `json:"surfaced,omitempty"`
	Threads int64 `json:"threads,omitempty"`
}

type RoutingEcho struct {
	Route string `json:"route"`
	Policy string `json:"policy"`
	Tokens int64 `json:"tokens"`
	Executed bool `json:"executed"`
}

type QueryResponse struct {
	Rows []map[string]interface{} `json:"rows"`
	Aggregations map[string]interface{} `json:"aggregations,omitempty"`
	AggregationGroups []map[string]interface{} `json:"aggregation_groups,omitempty"`
	Billing map[string]interface{} `json:"billing,omitempty"`
	Performance map[string]interface{} `json:"performance,omitempty"`
	StableAsOf int64 `json:"stable_as_of,omitempty"`
	NextCursor string `json:"next_cursor,omitempty"`
	Hybrid HybridEcho `json:"hybrid,omitempty"`
	Routing RoutingEcho `json:"routing,omitempty"`
}

type Error struct {
	Error string `json:"error"`
	Message string `json:"message"`
}

type SnapshotHistoryEntry struct {
	WatermarkMs int64 `json:"watermark_ms"`
	Sha string `json:"sha"`
	Tags []string `json:"tags,omitempty"`
}

type SnapshotBody struct {
	Namespace string `json:"namespace"`
	WatermarkMs int64 `json:"watermark_ms"`
	Sha string `json:"sha"`
	RowCount int64 `json:"row_count,omitempty"`
	Fields []SnapshotField `json:"fields"`
	FieldsSkipped []SnapshotFieldSkipped `json:"fields_skipped"`
}

type SnapshotField struct {
	Name string `json:"name"`
	Values []SnapshotValueCount `json:"values"`
}

type SnapshotSkipReason string

type SnapshotFieldSkipped struct {
	Name string `json:"name"`
	Reason SnapshotSkipReason `json:"reason"`
	DistinctObserved int64 `json:"distinct_observed"`
	Cap int64 `json:"cap"`
}

type SnapshotValueCount struct {
	V string `json:"v"`
	N int64 `json:"n"`
}

type SnapshotActivityEvent struct {
	TsMs int64 `json:"ts_ms"`
	Namespace string `json:"namespace"`
	Sha string `json:"sha"`
}

type SnapshotActivityList struct {
	Events []SnapshotActivityEvent `json:"events"`
	NextCursor string `json:"next_cursor,omitempty"`
	Truncated bool `json:"truncated,omitempty"`
}

type MetricKind string

type MetricFamily string

type MetricAlert struct {
	Summary string `json:"summary"`
	Expr string `json:"expr"`
	For string `json:"for"`
}

type MetricCatalogEntry struct {
	Name string `json:"name"`
	Kind MetricKind `json:"kind"`
	Family MetricFamily `json:"family"`
	Labels []string `json:"labels"`
	Description string `json:"description"`
	ExamplePromql string `json:"example_promql"`
	Alert MetricAlert `json:"alert,omitempty"`
}

type MetricCatalog struct {
	Version string `json:"version"`
	Entries []MetricCatalogEntry `json:"entries"`
}

type PrometheusResponse map[string]interface{}

type SearchHistoryEntry struct {
	Timestamp string `json:"timestamp"`
	TimestampNanos int64 `json:"timestamp_nanos"`
	Namespace string `json:"namespace"`
	TraceID string `json:"trace_id,omitempty"`
	RawQuery string `json:"raw_query,omitempty"`
	StableAsOf int64 `json:"stable_as_of,omitempty"`
	Query map[string]interface{} `json:"query"`
	TopResultIds []string `json:"top_result_ids"`
	Tags []string `json:"tags"`
}

type SearchHistoryListResponse struct {
	Entries []SearchHistoryEntry `json:"entries"`
	NextCursor string `json:"next_cursor,omitempty"`
}

type ClickstreamEvent struct {
	Timestamp string `json:"timestamp"`
	TimestampNanos int64 `json:"timestamp_nanos"`
	TraceID string `json:"trace_id"`
	Namespace string `json:"namespace"`
	DocID string `json:"doc_id"`
	Tags []string `json:"tags"`
	Source string `json:"source"`
	ServedFrom string `json:"served_from"`
}

type ClickstreamListResponse struct {
	Events []ClickstreamEvent `json:"events"`
	NextCursor string `json:"next_cursor,omitempty"`
}

type KubernetesCondition map[string]interface{}

type SecretKeyRef struct {
	Name string `json:"name"`
	Key string `json:"key"`
}

type VectorStoreEndpoint struct {
	Url string `json:"url"`
	Region string `json:"region"`
}

type VectorStoreTurbopuffer struct {
	OrgID string `json:"orgId"`
}

type VectorStoreCredential struct {
	SecretRef SecretKeyRef `json:"secretRef"`
}

type VectorStoreInboundAuth struct {
	Mode string `json:"mode,omitempty"`
}

type VectorStoreStatus struct {
	Reachable bool `json:"reachable,omitempty"`
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	Conditions []KubernetesCondition `json:"conditions"`
}

type VectorStore struct {
	Name string `json:"name"`
	Kind string `json:"kind"`
	Default bool `json:"default"`
	Endpoint VectorStoreEndpoint `json:"endpoint"`
	Turbopuffer VectorStoreTurbopuffer `json:"turbopuffer,omitempty"`
	Credential VectorStoreCredential `json:"credential"`
	InboundAuth VectorStoreInboundAuth `json:"inboundAuth,omitempty"`
	Status VectorStoreStatus `json:"status"`
	TurbopufferUrl string `json:"turbopufferUrl,omitempty"`
}

type VectorStoreList struct {
	Vectorstores []VectorStore `json:"vectorstores"`
}

type WarehouseSecretRef struct {
	Name string `json:"name"`
}

type WarehousePool struct {
	Size int64 `json:"size"`
	Timeout string `json:"timeout"`
}

type SnowflakeWarehouse struct {
	Account string `json:"account"`
	User string `json:"user"`
	Role string `json:"role,omitempty"`
	Warehouse string `json:"warehouse"`
	KeyPairSecretRef WarehouseSecretRef `json:"keyPairSecretRef"`
	Pool WarehousePool `json:"pool,omitempty"`
}

type RestWarehouse struct {
	BaseUrl string `json:"baseUrl"`
	Auth RestWarehouseAuth `json:"auth,omitempty"`
	RateLimit RestWarehouseRateLimit `json:"rateLimit,omitempty"`
	Verify RestWarehouseVerify `json:"verify"`
}

type RestWarehouseAuth struct {
	In string `json:"in"`
	Name string `json:"name"`
	SecretRef WarehouseSecretRef `json:"secretRef"`
}

type RestWarehouseRateLimit struct {
	RequestsPerSecond float64 `json:"requestsPerSecond"`
}

type RestWarehouseVerify struct {
	Path string `json:"path"`
	Query map[string]string `json:"query,omitempty"`
}

type WarehousePhase string

type WarehouseConsumers struct {
	Pipelines int64 `json:"pipelines"`
	ApiKeys int64 `json:"apiKeys"`
}

type WarehouseStatus struct {
	Phase WarehousePhase `json:"phase,omitempty"`
	VerifiedAt string `json:"verifiedAt,omitempty"`
	FailureReason string `json:"failureReason,omitempty"`
	Consumers WarehouseConsumers `json:"consumers"`
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	Conditions []KubernetesCondition `json:"conditions"`
}

type Warehouse struct {
	Name string `json:"name"`
	Namespace string `json:"namespace"`
	Kind string `json:"kind"`
	Snowflake SnowflakeWarehouse `json:"snowflake,omitempty"`
	Rest RestWarehouse `json:"rest,omitempty"`
	VerifyInterval string `json:"verifyInterval"`
	Status WarehouseStatus `json:"status"`
}

type WarehouseList struct {
	Warehouses []Warehouse `json:"warehouses"`
}

type ApiKeyEntitlement struct {
	Scopes []string `json:"scopes,omitempty"`
	Namespaces []string `json:"namespaces,omitempty"`
	Claims []string `json:"claims,omitempty"`
}

type ApiKeyEntitlements map[string]ApiKeyEntitlement

type ApiKeyPhase string

type ApiKey struct {
	KeyID string `json:"keyId"`
	Name string `json:"name"`
	Owner string `json:"owner,omitempty"`
	Description string `json:"description,omitempty"`
	Entitlements ApiKeyEntitlements `json:"entitlements"`
	ExpiresAfter string `json:"expiresAfter,omitempty"`
	Phase ApiKeyPhase `json:"phase"`
	CreatedAt string `json:"createdAt"`
	ExpiresAt string `json:"expiresAt,omitempty"`
	RevokedAt string `json:"revokedAt,omitempty"`
	LastSeenAt string `json:"lastSeenAt,omitempty"`
	LookupHash string `json:"lookupHash,omitempty"`
	SecretRef map[string]interface{} `json:"secretRef,omitempty"`
}

type ApiKeyList struct {
	Keys []ApiKey `json:"keys"`
}

type MintKeyRequest struct {
	Name string `json:"name"`
	Owner string `json:"owner,omitempty"`
	Description string `json:"description,omitempty"`
	Entitlements ApiKeyEntitlements `json:"entitlements,omitempty"`
	ExpiresAfter string `json:"expiresAfter,omitempty"`
}

type MintKeyResponse struct {
	KeyID string `json:"keyId"`
	Name string `json:"name"`
	Owner string `json:"owner,omitempty"`
	Description string `json:"description,omitempty"`
	Entitlements ApiKeyEntitlements `json:"entitlements"`
	ExpiresAfter string `json:"expiresAfter,omitempty"`
	Phase ApiKeyPhase `json:"phase"`
	CreatedAt string `json:"createdAt"`
	ExpiresAt string `json:"expiresAt,omitempty"`
	Token string `json:"token"`
}

type AuthenticateKeyRequest struct {
	Token string `json:"token"`
}

type AuthenticateKeyResponse struct {
	KeyID string `json:"keyId"`
	Name string `json:"name"`
	Owner string `json:"owner,omitempty"`
	Entitlements ApiKeyEntitlements `json:"entitlements"`
	ExpiresAt string `json:"expiresAt,omitempty"`
}
