import type * as Models from "./models.js";

export type FetchLike = (input: string | URL | Request, init?: RequestInit) => Promise<Response>;

export interface HevlayerOptions {
  baseUrl?: string | null;
  apiKey?: string | null;
  timeout?: number | null;
  fetch?: FetchLike;
}

export interface RequestOptions {
  withPerf?: boolean;
  signal?: AbortSignal;
}

export interface FetchDocumentOptions extends RequestOptions {
  includeAttributes?: string[];
}

export interface GetCostSnapshotOptions extends RequestOptions {
  window?: Models.CostWindow;
}

export interface GetCostTimeseriesOptions extends RequestOptions {
  window?: Models.CostWindow;
  step?: Models.CostStep;
}

export interface GetScanResultsOptions extends RequestOptions {
  limit?: number;
  offset?: number;
}

export interface HintCacheWarmOptions extends RequestOptions {
  turbopuffer?: boolean;
  documents?: boolean;
  snapshots?: boolean;
  blobs?: boolean;
  blobBudgetBytes?: number;
  pageSize?: number;
}

export interface ListCheckpointsOptions extends RequestOptions {
  limit?: number;
  before?: string;
}

export interface ListClickstreamOptions extends RequestOptions {
  traceId?: string;
  tags?: string[];
  from_?: string;
  to?: string;
  before?: string;
  limit?: number;
}

export interface ListKeysOptions extends RequestOptions {
  includeRevoked?: boolean;
}

export interface ListMetricsCatalogOptions extends RequestOptions {
  family?: Models.MetricFamily;
}

export interface ListNamespaceHistoryOptions extends RequestOptions {
  limit?: number;
  before?: string;
}

export interface ListNamespacesOptions extends RequestOptions {
  prefix?: string;
  cursor?: string;
  pageSize?: number;
}

export interface ListSearchHistoryOptions extends RequestOptions {
  tags?: string[];
  from_?: string;
  to?: string;
  before?: string;
  limit?: number;
}

export interface ListSnapshotActivityOptions extends RequestOptions {
  since?: number;
  limit?: number;
  namespace_?: string;
  cursor?: string;
}

export interface ListTurbopufferNamespacesOptions extends RequestOptions {
  cursor?: string;
  prefix?: string;
  pageSize?: number;
}

export interface PutBlobOptions extends RequestOptions {
  warm?: boolean;
}

export interface QueryMetricsOptions extends RequestOptions {
  query?: string;
  time?: string;
  timeout?: string;
}

export interface QueryMetricsApiV1Options extends RequestOptions {
  query?: string;
  time?: string;
  timeout?: string;
}

export interface QueryMetricsRangeOptions extends RequestOptions {
  query?: string;
  start?: string;
  end?: string;
  step?: string;
  timeout?: string;
}

export interface QueryMetricsRangeApiV1Options extends RequestOptions {
  query?: string;
  start?: string;
  end?: string;
  step?: string;
  timeout?: string;
}

export interface QueryNamespaceOptions extends RequestOptions {
  searchQuery?: string;
  tags?: string[];
}

export interface WarmCacheOptions extends RequestOptions {
  pageSize?: number;
}

export interface LayerPerf {
  latencyMs: number;
  cacheStatus: string | null;
}

export interface LayerResponse<T> {
  data: T;
  perf: LayerPerf;
}

interface QueryParam {
  key: string;
  value: unknown;
}

interface JsonRequest {
  method: string;
  path: string;
  params?: QueryParam[];
  body?: unknown;
  bodyContentType?: string;
  headers?: Record<string, string>;
  withPerf?: boolean;
  signal?: AbortSignal;
}

class FetchTransportError {
  readonly cause: unknown;

  constructor(cause: unknown) {
    this.cause = cause;
  }
}

const DEFAULT_BASE_URL = "https://aws-us-east-1.hevlayer.com";
const SEARCH_HISTORY_MAX_TAGS = 32;
const SEARCH_HISTORY_MAX_TAG_LENGTH = 128;
const SEARCH_HISTORY_TAG_RE = /^[A-Za-z0-9:_\-.=/+]+$/;

export class HevlayerError extends Error {
  readonly statusCode: number;
  readonly kind: string | null;
  readonly body: unknown;
  readonly response: Response;

  constructor(statusCode: number, message: string, options: { kind?: string | null; body?: unknown; response: Response }) {
    super(message);
    this.name = "HevlayerError";
    this.statusCode = statusCode;
    this.kind = options.kind ?? null;
    this.body = options.body;
    this.response = options.response;
  }
}

export class Hevlayer {
  private readonly baseUrl: string;
  private readonly apiKey: string | null;
  private readonly timeout: number | null;
  private readonly fetchImpl: FetchLike;

  constructor(options: HevlayerOptions = {}) {
    this.baseUrl = cleanBaseUrl(options.baseUrl ?? DEFAULT_BASE_URL, DEFAULT_BASE_URL);
    this.apiKey = cleanToken(options.apiKey);
    this.timeout = options.timeout === undefined ? 30000 : options.timeout;
    this.fetchImpl = options.fetch ?? defaultFetch();
  }

  async authenticateKey(body: Models.AuthenticateKeyRequest | Record<string, unknown>, opts?: RequestOptions & { withPerf?: false }): Promise<Models.AuthenticateKeyResponse>;
  async authenticateKey(body: Models.AuthenticateKeyRequest | Record<string, unknown>, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.AuthenticateKeyResponse>>;
  async authenticateKey(body: Models.AuthenticateKeyRequest | Record<string, unknown>, opts: RequestOptions = {}): Promise<Models.AuthenticateKeyResponse | LayerResponse<Models.AuthenticateKeyResponse>> {
    return this.requestJson<Models.AuthenticateKeyResponse>({
      method: "POST",
      path: "/v2/keys/authenticate",
      params: undefined,
        body: body,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.AuthenticateKeyResponse | LayerResponse<Models.AuthenticateKeyResponse>>;
  }


  async batchQueryNamespace(namespace_: string, body: Models.BatchQueryRequest | Record<string, unknown>, opts?: RequestOptions & { withPerf?: false }): Promise<Models.BatchQueryResponse>;
  async batchQueryNamespace(namespace_: string, body: Models.BatchQueryRequest | Record<string, unknown>, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.BatchQueryResponse>>;
  async batchQueryNamespace(namespace_: string, body: Models.BatchQueryRequest | Record<string, unknown>, opts: RequestOptions = {}): Promise<Models.BatchQueryResponse | LayerResponse<Models.BatchQueryResponse>> {
    return this.requestJson<Models.BatchQueryResponse>({
      method: "POST",
      path: "/v2/namespaces/" + encodeURIComponent(String(namespace_)) + "/query",
      params: [
        { key: "stainless_overload", value: "multiQuery" }
      ],
        body: body,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.BatchQueryResponse | LayerResponse<Models.BatchQueryResponse>>;
  }


  async branchNamespace(namespace_: string, body: Models.TurbopufferBranchFromRequest | Record<string, unknown>, opts?: RequestOptions & { withPerf?: false }): Promise<Models.TurbopufferWriteResponse>;
  async branchNamespace(namespace_: string, body: Models.TurbopufferBranchFromRequest | Record<string, unknown>, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.TurbopufferWriteResponse>>;
  async branchNamespace(namespace_: string, body: Models.TurbopufferBranchFromRequest | Record<string, unknown>, opts: RequestOptions = {}): Promise<Models.TurbopufferWriteResponse | LayerResponse<Models.TurbopufferWriteResponse>> {
    return this.requestJson<Models.TurbopufferWriteResponse>({
      method: "POST",
      path: "/v2/namespaces/" + encodeURIComponent(String(namespace_)),
      params: [
        { key: "stainless_overload", value: "branchFrom" }
      ],
        body: body,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.TurbopufferWriteResponse | LayerResponse<Models.TurbopufferWriteResponse>>;
  }


  async claimDocuments(pipelineId: string, body: Models.ClaimDocumentsRequest | Record<string, unknown>, opts?: RequestOptions & { withPerf?: false }): Promise<Models.ClaimDocumentsResponse>;
  async claimDocuments(pipelineId: string, body: Models.ClaimDocumentsRequest | Record<string, unknown>, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.ClaimDocumentsResponse>>;
  async claimDocuments(pipelineId: string, body: Models.ClaimDocumentsRequest | Record<string, unknown>, opts: RequestOptions = {}): Promise<Models.ClaimDocumentsResponse | LayerResponse<Models.ClaimDocumentsResponse>> {
    return this.requestJson<Models.ClaimDocumentsResponse>({
      method: "POST",
      path: "/v2/pipelines/" + encodeURIComponent(String(pipelineId)) + "/claim",
      params: undefined,
        body: body,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.ClaimDocumentsResponse | LayerResponse<Models.ClaimDocumentsResponse>>;
  }


  async claimUdfItems(udfId: string, body: Models.UdfClaimRequest | Record<string, unknown>, opts?: RequestOptions & { withPerf?: false }): Promise<Models.UdfClaimResponse>;
  async claimUdfItems(udfId: string, body: Models.UdfClaimRequest | Record<string, unknown>, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.UdfClaimResponse>>;
  async claimUdfItems(udfId: string, body: Models.UdfClaimRequest | Record<string, unknown>, opts: RequestOptions = {}): Promise<Models.UdfClaimResponse | LayerResponse<Models.UdfClaimResponse>> {
    return this.requestJson<Models.UdfClaimResponse>({
      method: "POST",
      path: "/v2/udfs/" + encodeURIComponent(String(udfId)) + "/claim",
      params: undefined,
        body: body,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.UdfClaimResponse | LayerResponse<Models.UdfClaimResponse>>;
  }


  async completeUdfItems(udfId: string, body: Models.UdfCompleteRequest | Record<string, unknown>, opts?: RequestOptions & { withPerf?: false }): Promise<Models.UdfItemsResponse>;
  async completeUdfItems(udfId: string, body: Models.UdfCompleteRequest | Record<string, unknown>, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.UdfItemsResponse>>;
  async completeUdfItems(udfId: string, body: Models.UdfCompleteRequest | Record<string, unknown>, opts: RequestOptions = {}): Promise<Models.UdfItemsResponse | LayerResponse<Models.UdfItemsResponse>> {
    return this.requestJson<Models.UdfItemsResponse>({
      method: "POST",
      path: "/v2/udfs/" + encodeURIComponent(String(udfId)) + "/items/complete",
      params: undefined,
        body: body,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.UdfItemsResponse | LayerResponse<Models.UdfItemsResponse>>;
  }


  async copyNamespace(namespace_: string, body: Models.TurbopufferCopyFromRequest | Record<string, unknown>, opts?: RequestOptions & { withPerf?: false }): Promise<Models.TurbopufferWriteResponse>;
  async copyNamespace(namespace_: string, body: Models.TurbopufferCopyFromRequest | Record<string, unknown>, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.TurbopufferWriteResponse>>;
  async copyNamespace(namespace_: string, body: Models.TurbopufferCopyFromRequest | Record<string, unknown>, opts: RequestOptions = {}): Promise<Models.TurbopufferWriteResponse | LayerResponse<Models.TurbopufferWriteResponse>> {
    return this.requestJson<Models.TurbopufferWriteResponse>({
      method: "POST",
      path: "/v2/namespaces/" + encodeURIComponent(String(namespace_)),
      params: [
        { key: "stainless_overload", value: "copyFrom" }
      ],
        body: body,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.TurbopufferWriteResponse | LayerResponse<Models.TurbopufferWriteResponse>>;
  }


  async createCheckpoint(namespace_: string, body: Models.CreateCheckpointRequest | Record<string, unknown>, opts?: RequestOptions & { withPerf?: false }): Promise<Models.Checkpoint>;
  async createCheckpoint(namespace_: string, body: Models.CreateCheckpointRequest | Record<string, unknown>, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.Checkpoint>>;
  async createCheckpoint(namespace_: string, body: Models.CreateCheckpointRequest | Record<string, unknown>, opts: RequestOptions = {}): Promise<Models.Checkpoint | LayerResponse<Models.Checkpoint>> {
    return this.requestJson<Models.Checkpoint>({
      method: "POST",
      path: "/v2/namespaces/" + encodeURIComponent(String(namespace_)) + "/checkpoints",
      params: undefined,
        body: body,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.Checkpoint | LayerResponse<Models.Checkpoint>>;
  }


  async createPipeline(body: Models.CreatePipelineRequest | Record<string, unknown>, opts?: RequestOptions & { withPerf?: false }): Promise<Models.Pipeline>;
  async createPipeline(body: Models.CreatePipelineRequest | Record<string, unknown>, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.Pipeline>>;
  async createPipeline(body: Models.CreatePipelineRequest | Record<string, unknown>, opts: RequestOptions = {}): Promise<Models.Pipeline | LayerResponse<Models.Pipeline>> {
    return this.requestJson<Models.Pipeline>({
      method: "POST",
      path: "/v2/pipelines",
      params: undefined,
        body: body,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.Pipeline | LayerResponse<Models.Pipeline>>;
  }


  async createScan(namespace_: string, body: Models.CreateScanRequest | Record<string, unknown>, opts?: RequestOptions & { withPerf?: false }): Promise<Models.ScanCountResponse | Models.ScanJob>;
  async createScan(namespace_: string, body: Models.CreateScanRequest | Record<string, unknown>, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.ScanCountResponse | Models.ScanJob>>;
  async createScan(namespace_: string, body: Models.CreateScanRequest | Record<string, unknown>, opts: RequestOptions = {}): Promise<Models.ScanCountResponse | Models.ScanJob | LayerResponse<Models.ScanCountResponse | Models.ScanJob>> {
    return this.requestJson<Models.ScanCountResponse | Models.ScanJob>({
      method: "POST",
      path: "/v2/namespaces/" + encodeURIComponent(String(namespace_)) + "/scans",
      params: undefined,
        body: body,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.ScanCountResponse | Models.ScanJob | LayerResponse<Models.ScanCountResponse | Models.ScanJob>>;
  }


  async createSnapshot(namespace_: string, body: Models.CreateSnapshotRequest | Record<string, unknown>, opts?: RequestOptions & { withPerf?: false }): Promise<Models.SnapshotJob>;
  async createSnapshot(namespace_: string, body: Models.CreateSnapshotRequest | Record<string, unknown>, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.SnapshotJob>>;
  async createSnapshot(namespace_: string, body: Models.CreateSnapshotRequest | Record<string, unknown>, opts: RequestOptions = {}): Promise<Models.SnapshotJob | LayerResponse<Models.SnapshotJob>> {
    return this.requestJson<Models.SnapshotJob>({
      method: "POST",
      path: "/v2/namespaces/" + encodeURIComponent(String(namespace_)) + "/snapshots",
      params: undefined,
        body: body,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.SnapshotJob | LayerResponse<Models.SnapshotJob>>;
  }


  async createUdf(body: Models.CreateUdfRequest | Record<string, unknown>, opts?: RequestOptions & { withPerf?: false }): Promise<Models.Udf>;
  async createUdf(body: Models.CreateUdfRequest | Record<string, unknown>, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.Udf>>;
  async createUdf(body: Models.CreateUdfRequest | Record<string, unknown>, opts: RequestOptions = {}): Promise<Models.Udf | LayerResponse<Models.Udf>> {
    return this.requestJson<Models.Udf>({
      method: "POST",
      path: "/v2/udfs",
      params: undefined,
        body: body,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.Udf | LayerResponse<Models.Udf>>;
  }


  async deleteKey(keyId: string, opts?: RequestOptions & { withPerf?: false }): Promise<Models.StatusResponse>;
  async deleteKey(keyId: string, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.StatusResponse>>;
  async deleteKey(keyId: string, opts: RequestOptions = {}): Promise<Models.StatusResponse | LayerResponse<Models.StatusResponse>> {
    return this.requestJson<Models.StatusResponse>({
      method: "DELETE",
      path: "/v2/keys/" + encodeURIComponent(String(keyId)),
      params: undefined,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.StatusResponse | LayerResponse<Models.StatusResponse>>;
  }


  async deleteNamespace(namespace_: string, opts?: RequestOptions & { withPerf?: false }): Promise<Models.StatusResponse>;
  async deleteNamespace(namespace_: string, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.StatusResponse>>;
  async deleteNamespace(namespace_: string, opts: RequestOptions = {}): Promise<Models.StatusResponse | LayerResponse<Models.StatusResponse>> {
    return this.requestJson<Models.StatusResponse>({
      method: "DELETE",
      path: "/v2/namespaces/" + encodeURIComponent(String(namespace_)),
      params: undefined,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.StatusResponse | LayerResponse<Models.StatusResponse>>;
  }


  async deletePipeline(pipelineId: string, opts?: RequestOptions & { withPerf?: false }): Promise<Models.StatusResponse>;
  async deletePipeline(pipelineId: string, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.StatusResponse>>;
  async deletePipeline(pipelineId: string, opts: RequestOptions = {}): Promise<Models.StatusResponse | LayerResponse<Models.StatusResponse>> {
    return this.requestJson<Models.StatusResponse>({
      method: "DELETE",
      path: "/v2/pipelines/" + encodeURIComponent(String(pipelineId)),
      params: undefined,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.StatusResponse | LayerResponse<Models.StatusResponse>>;
  }


  async deleteScan(namespace_: string, scanId: string, opts?: RequestOptions & { withPerf?: false }): Promise<Models.StatusResponse>;
  async deleteScan(namespace_: string, scanId: string, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.StatusResponse>>;
  async deleteScan(namespace_: string, scanId: string, opts: RequestOptions = {}): Promise<Models.StatusResponse | LayerResponse<Models.StatusResponse>> {
    return this.requestJson<Models.StatusResponse>({
      method: "DELETE",
      path: "/v2/namespaces/" + encodeURIComponent(String(namespace_)) + "/scans/" + encodeURIComponent(String(scanId)),
      params: undefined,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.StatusResponse | LayerResponse<Models.StatusResponse>>;
  }


  async deleteUdf(udfId: string, opts?: RequestOptions & { withPerf?: false }): Promise<Models.StatusResponse>;
  async deleteUdf(udfId: string, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.StatusResponse>>;
  async deleteUdf(udfId: string, opts: RequestOptions = {}): Promise<Models.StatusResponse | LayerResponse<Models.StatusResponse>> {
    return this.requestJson<Models.StatusResponse>({
      method: "DELETE",
      path: "/v2/udfs/" + encodeURIComponent(String(udfId)),
      params: undefined,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.StatusResponse | LayerResponse<Models.StatusResponse>>;
  }


  async discoverUdf(udfId: string, body: Models.UdfDiscoverRequest | Record<string, unknown>, opts?: RequestOptions & { withPerf?: false }): Promise<Models.UdfDiscoverResponse>;
  async discoverUdf(udfId: string, body: Models.UdfDiscoverRequest | Record<string, unknown>, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.UdfDiscoverResponse>>;
  async discoverUdf(udfId: string, body: Models.UdfDiscoverRequest | Record<string, unknown>, opts: RequestOptions = {}): Promise<Models.UdfDiscoverResponse | LayerResponse<Models.UdfDiscoverResponse>> {
    return this.requestJson<Models.UdfDiscoverResponse>({
      method: "POST",
      path: "/v2/udfs/" + encodeURIComponent(String(udfId)) + "/discover",
      params: undefined,
        body: body,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.UdfDiscoverResponse | LayerResponse<Models.UdfDiscoverResponse>>;
  }


  async evaluateTurbopufferRecall(namespace_: string, body: Models.TurbopufferRecallRequest | Record<string, unknown>, opts?: RequestOptions & { withPerf?: false }): Promise<Models.TurbopufferRecallResponse>;
  async evaluateTurbopufferRecall(namespace_: string, body: Models.TurbopufferRecallRequest | Record<string, unknown>, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.TurbopufferRecallResponse>>;
  async evaluateTurbopufferRecall(namespace_: string, body: Models.TurbopufferRecallRequest | Record<string, unknown>, opts: RequestOptions = {}): Promise<Models.TurbopufferRecallResponse | LayerResponse<Models.TurbopufferRecallResponse>> {
    return this.requestJson<Models.TurbopufferRecallResponse>({
      method: "POST",
      path: "/v1/namespaces/" + encodeURIComponent(String(namespace_)) + "/_debug/recall",
      params: undefined,
        body: body,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.TurbopufferRecallResponse | LayerResponse<Models.TurbopufferRecallResponse>>;
  }


  async explainTurbopufferQuery(namespace_: string, body: Models.TurbopufferQueryRequest | Record<string, unknown>, opts?: RequestOptions & { withPerf?: false }): Promise<Models.TurbopufferExplainQueryResponse>;
  async explainTurbopufferQuery(namespace_: string, body: Models.TurbopufferQueryRequest | Record<string, unknown>, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.TurbopufferExplainQueryResponse>>;
  async explainTurbopufferQuery(namespace_: string, body: Models.TurbopufferQueryRequest | Record<string, unknown>, opts: RequestOptions = {}): Promise<Models.TurbopufferExplainQueryResponse | LayerResponse<Models.TurbopufferExplainQueryResponse>> {
    return this.requestJson<Models.TurbopufferExplainQueryResponse>({
      method: "POST",
      path: "/v2/namespaces/" + encodeURIComponent(String(namespace_)) + "/explain_query",
      params: undefined,
        body: body,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.TurbopufferExplainQueryResponse | LayerResponse<Models.TurbopufferExplainQueryResponse>>;
  }


  async failUdfItems(udfId: string, body: Models.UdfFailRequest | Record<string, unknown>, opts?: RequestOptions & { withPerf?: false }): Promise<Models.UdfItemsResponse>;
  async failUdfItems(udfId: string, body: Models.UdfFailRequest | Record<string, unknown>, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.UdfItemsResponse>>;
  async failUdfItems(udfId: string, body: Models.UdfFailRequest | Record<string, unknown>, opts: RequestOptions = {}): Promise<Models.UdfItemsResponse | LayerResponse<Models.UdfItemsResponse>> {
    return this.requestJson<Models.UdfItemsResponse>({
      method: "POST",
      path: "/v2/udfs/" + encodeURIComponent(String(udfId)) + "/items/fail",
      params: undefined,
        body: body,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.UdfItemsResponse | LayerResponse<Models.UdfItemsResponse>>;
  }


  async fetchDocument(namespace_: string, docId: string, opts?: FetchDocumentOptions & { withPerf?: false }): Promise<Models.Document>;
  async fetchDocument(namespace_: string, docId: string, opts: FetchDocumentOptions & { withPerf: true }): Promise<LayerResponse<Models.Document>>;
  async fetchDocument(namespace_: string, docId: string, opts: FetchDocumentOptions = {}): Promise<Models.Document | LayerResponse<Models.Document>> {
    return this.requestJson<Models.Document>({
      method: "GET",
      path: "/v2/namespaces/" + encodeURIComponent(String(namespace_)) + "/documents/" + encodeURIComponent(String(docId)),
      params: [
        { key: "include_attributes", value: opts.includeAttributes }
      ],
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.Document | LayerResponse<Models.Document>>;
  }


  async fetchDocuments(namespace_: string, body: Models.FetchDocumentsRequest | Record<string, unknown>, opts?: RequestOptions & { withPerf?: false }): Promise<Models.FetchDocumentsResponse>;
  async fetchDocuments(namespace_: string, body: Models.FetchDocumentsRequest | Record<string, unknown>, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.FetchDocumentsResponse>>;
  async fetchDocuments(namespace_: string, body: Models.FetchDocumentsRequest | Record<string, unknown>, opts: RequestOptions = {}): Promise<Models.FetchDocumentsResponse | LayerResponse<Models.FetchDocumentsResponse>> {
    return this.requestJson<Models.FetchDocumentsResponse>({
      method: "POST",
      path: "/v2/namespaces/" + encodeURIComponent(String(namespace_)) + "/documents",
      params: undefined,
        body: body,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.FetchDocumentsResponse | LayerResponse<Models.FetchDocumentsResponse>>;
  }


  async getBlob(namespace_: string, sha256: string, opts?: RequestOptions & { withPerf?: false }): Promise<Uint8Array>;
  async getBlob(namespace_: string, sha256: string, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Uint8Array>>;
  async getBlob(namespace_: string, sha256: string, opts: RequestOptions = {}): Promise<Uint8Array | LayerResponse<Uint8Array>> {
    return this.requestBytes({
      method: "GET",
      path: "/v1/namespaces/" + encodeURIComponent(String(namespace_)) + "/blobs/" + encodeURIComponent(String(sha256)),
      params: undefined,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Uint8Array | LayerResponse<Uint8Array>>;
  }


  async getCheckpoint(namespace_: string, label: string, opts?: RequestOptions & { withPerf?: false }): Promise<Models.Checkpoint>;
  async getCheckpoint(namespace_: string, label: string, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.Checkpoint>>;
  async getCheckpoint(namespace_: string, label: string, opts: RequestOptions = {}): Promise<Models.Checkpoint | LayerResponse<Models.Checkpoint>> {
    return this.requestJson<Models.Checkpoint>({
      method: "GET",
      path: "/v2/namespaces/" + encodeURIComponent(String(namespace_)) + "/checkpoints/" + encodeURIComponent(String(label)),
      params: undefined,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.Checkpoint | LayerResponse<Models.Checkpoint>>;
  }


  async getCostRateCard(opts?: RequestOptions & { withPerf?: false }): Promise<Models.RateCard>;
  async getCostRateCard(opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.RateCard>>;
  async getCostRateCard(opts: RequestOptions = {}): Promise<Models.RateCard | LayerResponse<Models.RateCard>> {
    return this.requestJson<Models.RateCard>({
      method: "GET",
      path: "/v2/cost/rate-card",
      params: undefined,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.RateCard | LayerResponse<Models.RateCard>>;
  }


  async getCostSnapshot(opts?: GetCostSnapshotOptions & { withPerf?: false }): Promise<Models.CostSnapshot>;
  async getCostSnapshot(opts: GetCostSnapshotOptions & { withPerf: true }): Promise<LayerResponse<Models.CostSnapshot>>;
  async getCostSnapshot(opts: GetCostSnapshotOptions = {}): Promise<Models.CostSnapshot | LayerResponse<Models.CostSnapshot>> {
    return this.requestJson<Models.CostSnapshot>({
      method: "GET",
      path: "/v2/cost",
      params: [
        { key: "window", value: opts.window }
      ],
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.CostSnapshot | LayerResponse<Models.CostSnapshot>>;
  }


  async getCostTimeseries(opts?: GetCostTimeseriesOptions & { withPerf?: false }): Promise<Models.CostTimeseries>;
  async getCostTimeseries(opts: GetCostTimeseriesOptions & { withPerf: true }): Promise<LayerResponse<Models.CostTimeseries>>;
  async getCostTimeseries(opts: GetCostTimeseriesOptions = {}): Promise<Models.CostTimeseries | LayerResponse<Models.CostTimeseries>> {
    return this.requestJson<Models.CostTimeseries>({
      method: "GET",
      path: "/v2/cost/timeseries",
      params: [
        { key: "window", value: opts.window },
        { key: "step", value: opts.step }
      ],
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.CostTimeseries | LayerResponse<Models.CostTimeseries>>;
  }


  async getKey(keyId: string, opts?: RequestOptions & { withPerf?: false }): Promise<Models.ApiKey>;
  async getKey(keyId: string, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.ApiKey>>;
  async getKey(keyId: string, opts: RequestOptions = {}): Promise<Models.ApiKey | LayerResponse<Models.ApiKey>> {
    return this.requestJson<Models.ApiKey>({
      method: "GET",
      path: "/v2/keys/" + encodeURIComponent(String(keyId)),
      params: undefined,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.ApiKey | LayerResponse<Models.ApiKey>>;
  }


  async getLicense(opts?: RequestOptions & { withPerf?: false }): Promise<Models.LicenseState>;
  async getLicense(opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.LicenseState>>;
  async getLicense(opts: RequestOptions = {}): Promise<Models.LicenseState | LayerResponse<Models.LicenseState>> {
    return this.requestJson<Models.LicenseState>({
      method: "GET",
      path: "/v2/license",
      params: undefined,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.LicenseState | LayerResponse<Models.LicenseState>>;
  }


  async getMetricCatalogEntry(name: string, opts?: RequestOptions & { withPerf?: false }): Promise<Models.MetricCatalogEntry>;
  async getMetricCatalogEntry(name: string, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.MetricCatalogEntry>>;
  async getMetricCatalogEntry(name: string, opts: RequestOptions = {}): Promise<Models.MetricCatalogEntry | LayerResponse<Models.MetricCatalogEntry>> {
    return this.requestJson<Models.MetricCatalogEntry>({
      method: "GET",
      path: "/v2/metrics/catalog/" + encodeURIComponent(String(name)),
      params: undefined,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.MetricCatalogEntry | LayerResponse<Models.MetricCatalogEntry>>;
  }


  async getNamespaceMetadata(namespace_: string, opts?: RequestOptions & { withPerf?: false }): Promise<Models.NamespaceMetadata>;
  async getNamespaceMetadata(namespace_: string, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.NamespaceMetadata>>;
  async getNamespaceMetadata(namespace_: string, opts: RequestOptions = {}): Promise<Models.NamespaceMetadata | LayerResponse<Models.NamespaceMetadata>> {
    return this.requestJson<Models.NamespaceMetadata>({
      method: "GET",
      path: "/v2/namespaces/" + encodeURIComponent(String(namespace_)) + "/metadata",
      params: undefined,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.NamespaceMetadata | LayerResponse<Models.NamespaceMetadata>>;
  }


  async getNamespaceSnapshot(namespace_: string, sha: string, opts?: RequestOptions & { withPerf?: false }): Promise<Models.SnapshotBody>;
  async getNamespaceSnapshot(namespace_: string, sha: string, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.SnapshotBody>>;
  async getNamespaceSnapshot(namespace_: string, sha: string, opts: RequestOptions = {}): Promise<Models.SnapshotBody | LayerResponse<Models.SnapshotBody>> {
    return this.requestJson<Models.SnapshotBody>({
      method: "GET",
      path: "/v2/namespaces/" + encodeURIComponent(String(namespace_)) + "/snapshots/" + encodeURIComponent(String(sha)),
      params: undefined,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.SnapshotBody | LayerResponse<Models.SnapshotBody>>;
  }


  async getPipelineDocumentChunks(pipelineId: string, docId: string, opts?: RequestOptions & { withPerf?: false }): Promise<Models.GetChunksResponse>;
  async getPipelineDocumentChunks(pipelineId: string, docId: string, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.GetChunksResponse>>;
  async getPipelineDocumentChunks(pipelineId: string, docId: string, opts: RequestOptions = {}): Promise<Models.GetChunksResponse | LayerResponse<Models.GetChunksResponse>> {
    return this.requestJson<Models.GetChunksResponse>({
      method: "GET",
      path: "/v2/pipelines/" + encodeURIComponent(String(pipelineId)) + "/documents/" + encodeURIComponent(String(docId)) + "/chunks",
      params: undefined,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.GetChunksResponse | LayerResponse<Models.GetChunksResponse>>;
  }


  async getPipelineStatus(pipelineId: string, opts?: RequestOptions & { withPerf?: false }): Promise<Models.PipelineStatus>;
  async getPipelineStatus(pipelineId: string, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.PipelineStatus>>;
  async getPipelineStatus(pipelineId: string, opts: RequestOptions = {}): Promise<Models.PipelineStatus | LayerResponse<Models.PipelineStatus>> {
    return this.requestJson<Models.PipelineStatus>({
      method: "GET",
      path: "/v2/pipelines/" + encodeURIComponent(String(pipelineId)) + "/status",
      params: undefined,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.PipelineStatus | LayerResponse<Models.PipelineStatus>>;
  }


  async getScan(namespace_: string, scanId: string, opts?: RequestOptions & { withPerf?: false }): Promise<Models.ScanJob>;
  async getScan(namespace_: string, scanId: string, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.ScanJob>>;
  async getScan(namespace_: string, scanId: string, opts: RequestOptions = {}): Promise<Models.ScanJob | LayerResponse<Models.ScanJob>> {
    return this.requestJson<Models.ScanJob>({
      method: "GET",
      path: "/v2/namespaces/" + encodeURIComponent(String(namespace_)) + "/scans/" + encodeURIComponent(String(scanId)),
      params: undefined,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.ScanJob | LayerResponse<Models.ScanJob>>;
  }


  async getScanResults(namespace_: string, scanId: string, opts?: GetScanResultsOptions & { withPerf?: false }): Promise<Models.ScanIdsResponse | Models.ScanValuesResponse>;
  async getScanResults(namespace_: string, scanId: string, opts: GetScanResultsOptions & { withPerf: true }): Promise<LayerResponse<Models.ScanIdsResponse | Models.ScanValuesResponse>>;
  async getScanResults(namespace_: string, scanId: string, opts: GetScanResultsOptions = {}): Promise<Models.ScanIdsResponse | Models.ScanValuesResponse | LayerResponse<Models.ScanIdsResponse | Models.ScanValuesResponse>> {
    return this.requestJson<Models.ScanIdsResponse | Models.ScanValuesResponse>({
      method: "GET",
      path: "/v2/namespaces/" + encodeURIComponent(String(namespace_)) + "/scans/" + encodeURIComponent(String(scanId)) + "/results",
      params: [
        { key: "limit", value: opts.limit },
        { key: "offset", value: opts.offset }
      ],
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.ScanIdsResponse | Models.ScanValuesResponse | LayerResponse<Models.ScanIdsResponse | Models.ScanValuesResponse>>;
  }


  async getSnapshotJob(namespace_: string, jobId: string, opts?: RequestOptions & { withPerf?: false }): Promise<Models.SnapshotJob>;
  async getSnapshotJob(namespace_: string, jobId: string, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.SnapshotJob>>;
  async getSnapshotJob(namespace_: string, jobId: string, opts: RequestOptions = {}): Promise<Models.SnapshotJob | LayerResponse<Models.SnapshotJob>> {
    return this.requestJson<Models.SnapshotJob>({
      method: "GET",
      path: "/v2/namespaces/" + encodeURIComponent(String(namespace_)) + "/snapshot-jobs/" + encodeURIComponent(String(jobId)),
      params: undefined,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.SnapshotJob | LayerResponse<Models.SnapshotJob>>;
  }


  async getSnapshotPolicy(namespace_: string, opts?: RequestOptions & { withPerf?: false }): Promise<Models.SnapshotPolicy>;
  async getSnapshotPolicy(namespace_: string, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.SnapshotPolicy>>;
  async getSnapshotPolicy(namespace_: string, opts: RequestOptions = {}): Promise<Models.SnapshotPolicy | LayerResponse<Models.SnapshotPolicy>> {
    return this.requestJson<Models.SnapshotPolicy>({
      method: "GET",
      path: "/v2/namespaces/" + encodeURIComponent(String(namespace_)) + "/snapshot-policy",
      params: undefined,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.SnapshotPolicy | LayerResponse<Models.SnapshotPolicy>>;
  }


  async getTurbopufferNamespaceSchema(namespace_: string, opts?: RequestOptions & { withPerf?: false }): Promise<Models.TurbopufferSchema>;
  async getTurbopufferNamespaceSchema(namespace_: string, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.TurbopufferSchema>>;
  async getTurbopufferNamespaceSchema(namespace_: string, opts: RequestOptions = {}): Promise<Models.TurbopufferSchema | LayerResponse<Models.TurbopufferSchema>> {
    return this.requestJson<Models.TurbopufferSchema>({
      method: "GET",
      path: "/v1/namespaces/" + encodeURIComponent(String(namespace_)) + "/schema",
      params: undefined,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.TurbopufferSchema | LayerResponse<Models.TurbopufferSchema>>;
  }


  async getTurbopufferV1NamespaceMetadata(namespace_: string, opts?: RequestOptions & { withPerf?: false }): Promise<Models.NamespaceMetadata>;
  async getTurbopufferV1NamespaceMetadata(namespace_: string, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.NamespaceMetadata>>;
  async getTurbopufferV1NamespaceMetadata(namespace_: string, opts: RequestOptions = {}): Promise<Models.NamespaceMetadata | LayerResponse<Models.NamespaceMetadata>> {
    return this.requestJson<Models.NamespaceMetadata>({
      method: "GET",
      path: "/v1/namespaces/" + encodeURIComponent(String(namespace_)) + "/metadata",
      params: undefined,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.NamespaceMetadata | LayerResponse<Models.NamespaceMetadata>>;
  }


  async getUdf(udfId: string, opts?: RequestOptions & { withPerf?: false }): Promise<Models.GetUdfResponse>;
  async getUdf(udfId: string, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.GetUdfResponse>>;
  async getUdf(udfId: string, opts: RequestOptions = {}): Promise<Models.GetUdfResponse | LayerResponse<Models.GetUdfResponse>> {
    return this.requestJson<Models.GetUdfResponse>({
      method: "GET",
      path: "/v2/udfs/" + encodeURIComponent(String(udfId)),
      params: undefined,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.GetUdfResponse | LayerResponse<Models.GetUdfResponse>>;
  }


  async getUdfStatus(udfId: string, opts?: RequestOptions & { withPerf?: false }): Promise<Models.UdfStatus>;
  async getUdfStatus(udfId: string, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.UdfStatus>>;
  async getUdfStatus(udfId: string, opts: RequestOptions = {}): Promise<Models.UdfStatus | LayerResponse<Models.UdfStatus>> {
    return this.requestJson<Models.UdfStatus>({
      method: "GET",
      path: "/v2/udfs/" + encodeURIComponent(String(udfId)) + "/status",
      params: undefined,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.UdfStatus | LayerResponse<Models.UdfStatus>>;
  }


  async getVectorstore(name: string, opts?: RequestOptions & { withPerf?: false }): Promise<Models.VectorStore>;
  async getVectorstore(name: string, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.VectorStore>>;
  async getVectorstore(name: string, opts: RequestOptions = {}): Promise<Models.VectorStore | LayerResponse<Models.VectorStore>> {
    return this.requestJson<Models.VectorStore>({
      method: "GET",
      path: "/v2/vectorstores/" + encodeURIComponent(String(name)),
      params: undefined,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.VectorStore | LayerResponse<Models.VectorStore>>;
  }


  async getWarehouse(name: string, opts?: RequestOptions & { withPerf?: false }): Promise<Models.Warehouse>;
  async getWarehouse(name: string, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.Warehouse>>;
  async getWarehouse(name: string, opts: RequestOptions = {}): Promise<Models.Warehouse | LayerResponse<Models.Warehouse>> {
    return this.requestJson<Models.Warehouse>({
      method: "GET",
      path: "/v2/warehouses/" + encodeURIComponent(String(name)),
      params: undefined,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.Warehouse | LayerResponse<Models.Warehouse>>;
  }


  async getWarmJob(namespace_: string, jobId: string, opts?: RequestOptions & { withPerf?: false }): Promise<Models.WarmJob>;
  async getWarmJob(namespace_: string, jobId: string, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.WarmJob>>;
  async getWarmJob(namespace_: string, jobId: string, opts: RequestOptions = {}): Promise<Models.WarmJob | LayerResponse<Models.WarmJob>> {
    return this.requestJson<Models.WarmJob>({
      method: "GET",
      path: "/v2/namespaces/" + encodeURIComponent(String(namespace_)) + "/warm-jobs/" + encodeURIComponent(String(jobId)),
      params: undefined,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.WarmJob | LayerResponse<Models.WarmJob>>;
  }


  async heartbeatDocuments(pipelineId: string, body: Models.HeartbeatDocumentsRequest | Record<string, unknown>, opts?: RequestOptions & { withPerf?: false }): Promise<Models.DocumentsStageResponse>;
  async heartbeatDocuments(pipelineId: string, body: Models.HeartbeatDocumentsRequest | Record<string, unknown>, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.DocumentsStageResponse>>;
  async heartbeatDocuments(pipelineId: string, body: Models.HeartbeatDocumentsRequest | Record<string, unknown>, opts: RequestOptions = {}): Promise<Models.DocumentsStageResponse | LayerResponse<Models.DocumentsStageResponse>> {
    return this.requestJson<Models.DocumentsStageResponse>({
      method: "POST",
      path: "/v2/pipelines/" + encodeURIComponent(String(pipelineId)) + "/documents/heartbeat",
      params: undefined,
        body: body,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.DocumentsStageResponse | LayerResponse<Models.DocumentsStageResponse>>;
  }


  async heartbeatUdfItems(udfId: string, body: Models.UdfHeartbeatRequest | Record<string, unknown>, opts?: RequestOptions & { withPerf?: false }): Promise<Models.UdfItemsResponse>;
  async heartbeatUdfItems(udfId: string, body: Models.UdfHeartbeatRequest | Record<string, unknown>, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.UdfItemsResponse>>;
  async heartbeatUdfItems(udfId: string, body: Models.UdfHeartbeatRequest | Record<string, unknown>, opts: RequestOptions = {}): Promise<Models.UdfItemsResponse | LayerResponse<Models.UdfItemsResponse>> {
    return this.requestJson<Models.UdfItemsResponse>({
      method: "POST",
      path: "/v2/udfs/" + encodeURIComponent(String(udfId)) + "/items/heartbeat",
      params: undefined,
        body: body,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.UdfItemsResponse | LayerResponse<Models.UdfItemsResponse>>;
  }


  async hintCacheWarm(namespace_: string, opts?: HintCacheWarmOptions & { withPerf?: false }): Promise<Models.HintCacheWarmResponse>;
  async hintCacheWarm(namespace_: string, opts: HintCacheWarmOptions & { withPerf: true }): Promise<LayerResponse<Models.HintCacheWarmResponse>>;
  async hintCacheWarm(namespace_: string, opts: HintCacheWarmOptions = {}): Promise<Models.HintCacheWarmResponse | LayerResponse<Models.HintCacheWarmResponse>> {
    return this.requestJson<Models.HintCacheWarmResponse>({
      method: "GET",
      path: "/v1/namespaces/" + encodeURIComponent(String(namespace_)) + "/hint_cache_warm",
      params: [
        { key: "turbopuffer", value: opts.turbopuffer },
        { key: "documents", value: opts.documents },
        { key: "snapshots", value: opts.snapshots },
        { key: "blobs", value: opts.blobs },
        { key: "blob_budget_bytes", value: opts.blobBudgetBytes },
        { key: "page_size", value: opts.pageSize }
      ],
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.HintCacheWarmResponse | LayerResponse<Models.HintCacheWarmResponse>>;
  }


  async importNamespace(namespace_: string, body: Uint8Array | ArrayBuffer | Blob, opts?: RequestOptions & { withPerf?: false }): Promise<Record<string, unknown>>;
  async importNamespace(namespace_: string, body: Uint8Array | ArrayBuffer | Blob, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Record<string, unknown>>>;
  async importNamespace(namespace_: string, body: Uint8Array | ArrayBuffer | Blob, opts: RequestOptions = {}): Promise<Record<string, unknown> | LayerResponse<Record<string, unknown>>> {
    return this.requestJson<Record<string, unknown>>({
      method: "POST",
      path: "/v2/namespaces/" + encodeURIComponent(String(namespace_)) + "/import",
      params: undefined,
        body: body,
        bodyContentType: "application/vnd.apache.arrow.stream",
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Record<string, unknown> | LayerResponse<Record<string, unknown>>>;
  }


  async initNamespace(namespace_: string, body: Models.InitNamespaceRequest | Record<string, unknown>, opts?: RequestOptions & { withPerf?: false }): Promise<Models.InitNamespaceResponse>;
  async initNamespace(namespace_: string, body: Models.InitNamespaceRequest | Record<string, unknown>, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.InitNamespaceResponse>>;
  async initNamespace(namespace_: string, body: Models.InitNamespaceRequest | Record<string, unknown>, opts: RequestOptions = {}): Promise<Models.InitNamespaceResponse | LayerResponse<Models.InitNamespaceResponse>> {
    return this.requestJson<Models.InitNamespaceResponse>({
      method: "POST",
      path: "/v2/namespaces/" + encodeURIComponent(String(namespace_)) + "/init",
      params: undefined,
        body: body,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.InitNamespaceResponse | LayerResponse<Models.InitNamespaceResponse>>;
  }


  async listCheckpoints(namespace_: string, opts?: ListCheckpointsOptions & { withPerf?: false }): Promise<Models.CheckpointList>;
  async listCheckpoints(namespace_: string, opts: ListCheckpointsOptions & { withPerf: true }): Promise<LayerResponse<Models.CheckpointList>>;
  async listCheckpoints(namespace_: string, opts: ListCheckpointsOptions = {}): Promise<Models.CheckpointList | LayerResponse<Models.CheckpointList>> {
    return this.requestJson<Models.CheckpointList>({
      method: "GET",
      path: "/v2/namespaces/" + encodeURIComponent(String(namespace_)) + "/checkpoints",
      params: [
        { key: "limit", value: opts.limit },
        { key: "before", value: opts.before }
      ],
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.CheckpointList | LayerResponse<Models.CheckpointList>>;
  }


  async listClickstream(namespace_: string, opts?: ListClickstreamOptions & { withPerf?: false }): Promise<Models.ClickstreamListResponse>;
  async listClickstream(namespace_: string, opts: ListClickstreamOptions & { withPerf: true }): Promise<LayerResponse<Models.ClickstreamListResponse>>;
  async listClickstream(namespace_: string, opts: ListClickstreamOptions = {}): Promise<Models.ClickstreamListResponse | LayerResponse<Models.ClickstreamListResponse>> {
    return this.requestJson<Models.ClickstreamListResponse>({
      method: "GET",
      path: "/v2/namespaces/" + encodeURIComponent(String(namespace_)) + "/clickstream",
      params: [
        { key: "trace_id", value: opts.traceId },
        { key: "tag", value: opts.tags },
        { key: "from", value: opts.from_ },
        { key: "to", value: opts.to },
        { key: "before", value: opts.before },
        { key: "limit", value: opts.limit }
      ],
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.ClickstreamListResponse | LayerResponse<Models.ClickstreamListResponse>>;
  }


  async listKeys(opts?: ListKeysOptions & { withPerf?: false }): Promise<Models.ApiKeyList>;
  async listKeys(opts: ListKeysOptions & { withPerf: true }): Promise<LayerResponse<Models.ApiKeyList>>;
  async listKeys(opts: ListKeysOptions = {}): Promise<Models.ApiKeyList | LayerResponse<Models.ApiKeyList>> {
    return this.requestJson<Models.ApiKeyList>({
      method: "GET",
      path: "/v2/keys",
      params: [
        { key: "includeRevoked", value: opts.includeRevoked }
      ],
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.ApiKeyList | LayerResponse<Models.ApiKeyList>>;
  }


  async listMetricsCatalog(opts?: ListMetricsCatalogOptions & { withPerf?: false }): Promise<Models.MetricCatalog>;
  async listMetricsCatalog(opts: ListMetricsCatalogOptions & { withPerf: true }): Promise<LayerResponse<Models.MetricCatalog>>;
  async listMetricsCatalog(opts: ListMetricsCatalogOptions = {}): Promise<Models.MetricCatalog | LayerResponse<Models.MetricCatalog>> {
    return this.requestJson<Models.MetricCatalog>({
      method: "GET",
      path: "/v2/metrics/catalog",
      params: [
        { key: "family", value: opts.family }
      ],
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.MetricCatalog | LayerResponse<Models.MetricCatalog>>;
  }


  async listNamespaceHistory(namespace_: string, opts?: ListNamespaceHistoryOptions & { withPerf?: false }): Promise<Models.SnapshotHistoryEntry[]>;
  async listNamespaceHistory(namespace_: string, opts: ListNamespaceHistoryOptions & { withPerf: true }): Promise<LayerResponse<Models.SnapshotHistoryEntry[]>>;
  async listNamespaceHistory(namespace_: string, opts: ListNamespaceHistoryOptions = {}): Promise<Models.SnapshotHistoryEntry[] | LayerResponse<Models.SnapshotHistoryEntry[]>> {
    return this.requestJson<Models.SnapshotHistoryEntry[]>({
      method: "GET",
      path: "/v2/namespaces/" + encodeURIComponent(String(namespace_)) + "/history",
      params: [
        { key: "limit", value: opts.limit },
        { key: "before", value: opts.before }
      ],
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.SnapshotHistoryEntry[] | LayerResponse<Models.SnapshotHistoryEntry[]>>;
  }


  async listNamespaces(opts?: ListNamespacesOptions & { withPerf?: false }): Promise<Models.NamespaceList>;
  async listNamespaces(opts: ListNamespacesOptions & { withPerf: true }): Promise<LayerResponse<Models.NamespaceList>>;
  async listNamespaces(opts: ListNamespacesOptions = {}): Promise<Models.NamespaceList | LayerResponse<Models.NamespaceList>> {
    return this.requestJson<Models.NamespaceList>({
      method: "GET",
      path: "/v2/namespaces",
      params: [
        { key: "prefix", value: opts.prefix },
        { key: "cursor", value: opts.cursor },
        { key: "page_size", value: opts.pageSize }
      ],
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.NamespaceList | LayerResponse<Models.NamespaceList>>;
  }


  async listPipelines(opts?: RequestOptions & { withPerf?: false }): Promise<Models.PipelineList>;
  async listPipelines(opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.PipelineList>>;
  async listPipelines(opts: RequestOptions = {}): Promise<Models.PipelineList | LayerResponse<Models.PipelineList>> {
    return this.requestJson<Models.PipelineList>({
      method: "GET",
      path: "/v2/pipelines",
      params: undefined,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.PipelineList | LayerResponse<Models.PipelineList>>;
  }


  async listScans(namespace_: string, opts?: RequestOptions & { withPerf?: false }): Promise<Models.ScanJobList>;
  async listScans(namespace_: string, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.ScanJobList>>;
  async listScans(namespace_: string, opts: RequestOptions = {}): Promise<Models.ScanJobList | LayerResponse<Models.ScanJobList>> {
    return this.requestJson<Models.ScanJobList>({
      method: "GET",
      path: "/v2/namespaces/" + encodeURIComponent(String(namespace_)) + "/scans",
      params: undefined,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.ScanJobList | LayerResponse<Models.ScanJobList>>;
  }


  async listSearchHistory(namespace_: string, opts?: ListSearchHistoryOptions & { withPerf?: false }): Promise<Models.SearchHistoryListResponse>;
  async listSearchHistory(namespace_: string, opts: ListSearchHistoryOptions & { withPerf: true }): Promise<LayerResponse<Models.SearchHistoryListResponse>>;
  async listSearchHistory(namespace_: string, opts: ListSearchHistoryOptions = {}): Promise<Models.SearchHistoryListResponse | LayerResponse<Models.SearchHistoryListResponse>> {
    return this.requestJson<Models.SearchHistoryListResponse>({
      method: "GET",
      path: "/v2/namespaces/" + encodeURIComponent(String(namespace_)) + "/search-history",
      params: [
        { key: "tag", value: opts.tags },
        { key: "from", value: opts.from_ },
        { key: "to", value: opts.to },
        { key: "before", value: opts.before },
        { key: "limit", value: opts.limit }
      ],
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.SearchHistoryListResponse | LayerResponse<Models.SearchHistoryListResponse>>;
  }


  async listSnapshotActivity(opts?: ListSnapshotActivityOptions & { withPerf?: false }): Promise<Models.SnapshotActivityList>;
  async listSnapshotActivity(opts: ListSnapshotActivityOptions & { withPerf: true }): Promise<LayerResponse<Models.SnapshotActivityList>>;
  async listSnapshotActivity(opts: ListSnapshotActivityOptions = {}): Promise<Models.SnapshotActivityList | LayerResponse<Models.SnapshotActivityList>> {
    return this.requestJson<Models.SnapshotActivityList>({
      method: "GET",
      path: "/v2/activity/snapshots",
      params: [
        { key: "since", value: opts.since },
        { key: "limit", value: opts.limit },
        { key: "namespace", value: opts.namespace_ },
        { key: "cursor", value: opts.cursor }
      ],
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.SnapshotActivityList | LayerResponse<Models.SnapshotActivityList>>;
  }


  async listSnapshotJobs(namespace_: string, opts?: RequestOptions & { withPerf?: false }): Promise<Models.SnapshotJobList>;
  async listSnapshotJobs(namespace_: string, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.SnapshotJobList>>;
  async listSnapshotJobs(namespace_: string, opts: RequestOptions = {}): Promise<Models.SnapshotJobList | LayerResponse<Models.SnapshotJobList>> {
    return this.requestJson<Models.SnapshotJobList>({
      method: "GET",
      path: "/v2/namespaces/" + encodeURIComponent(String(namespace_)) + "/snapshot-jobs",
      params: undefined,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.SnapshotJobList | LayerResponse<Models.SnapshotJobList>>;
  }


  async listTurbopufferNamespaces(opts?: ListTurbopufferNamespacesOptions & { withPerf?: false }): Promise<Models.TurbopufferNamespaceList>;
  async listTurbopufferNamespaces(opts: ListTurbopufferNamespacesOptions & { withPerf: true }): Promise<LayerResponse<Models.TurbopufferNamespaceList>>;
  async listTurbopufferNamespaces(opts: ListTurbopufferNamespacesOptions = {}): Promise<Models.TurbopufferNamespaceList | LayerResponse<Models.TurbopufferNamespaceList>> {
    return this.requestJson<Models.TurbopufferNamespaceList>({
      method: "GET",
      path: "/v1/namespaces",
      params: [
        { key: "cursor", value: opts.cursor },
        { key: "prefix", value: opts.prefix },
        { key: "page_size", value: opts.pageSize }
      ],
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.TurbopufferNamespaceList | LayerResponse<Models.TurbopufferNamespaceList>>;
  }


  async listUdfs(opts?: RequestOptions & { withPerf?: false }): Promise<Models.UdfList>;
  async listUdfs(opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.UdfList>>;
  async listUdfs(opts: RequestOptions = {}): Promise<Models.UdfList | LayerResponse<Models.UdfList>> {
    return this.requestJson<Models.UdfList>({
      method: "GET",
      path: "/v2/udfs",
      params: undefined,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.UdfList | LayerResponse<Models.UdfList>>;
  }


  async listVectorstores(opts?: RequestOptions & { withPerf?: false }): Promise<Models.VectorStoreList>;
  async listVectorstores(opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.VectorStoreList>>;
  async listVectorstores(opts: RequestOptions = {}): Promise<Models.VectorStoreList | LayerResponse<Models.VectorStoreList>> {
    return this.requestJson<Models.VectorStoreList>({
      method: "GET",
      path: "/v2/vectorstores",
      params: undefined,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.VectorStoreList | LayerResponse<Models.VectorStoreList>>;
  }


  async listWarehouses(opts?: RequestOptions & { withPerf?: false }): Promise<Models.WarehouseList>;
  async listWarehouses(opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.WarehouseList>>;
  async listWarehouses(opts: RequestOptions = {}): Promise<Models.WarehouseList | LayerResponse<Models.WarehouseList>> {
    return this.requestJson<Models.WarehouseList>({
      method: "GET",
      path: "/v2/warehouses",
      params: undefined,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.WarehouseList | LayerResponse<Models.WarehouseList>>;
  }


  async listWarmJobs(namespace_: string, opts?: RequestOptions & { withPerf?: false }): Promise<Models.WarmJobList>;
  async listWarmJobs(namespace_: string, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.WarmJobList>>;
  async listWarmJobs(namespace_: string, opts: RequestOptions = {}): Promise<Models.WarmJobList | LayerResponse<Models.WarmJobList>> {
    return this.requestJson<Models.WarmJobList>({
      method: "GET",
      path: "/v2/namespaces/" + encodeURIComponent(String(namespace_)) + "/warm-jobs",
      params: undefined,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.WarmJobList | LayerResponse<Models.WarmJobList>>;
  }


  async mintKey(body: Models.MintKeyRequest | Record<string, unknown>, opts?: RequestOptions & { withPerf?: false }): Promise<Models.MintKeyResponse>;
  async mintKey(body: Models.MintKeyRequest | Record<string, unknown>, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.MintKeyResponse>>;
  async mintKey(body: Models.MintKeyRequest | Record<string, unknown>, opts: RequestOptions = {}): Promise<Models.MintKeyResponse | LayerResponse<Models.MintKeyResponse>> {
    return this.requestJson<Models.MintKeyResponse>({
      method: "POST",
      path: "/v2/keys",
      params: undefined,
        body: body,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.MintKeyResponse | LayerResponse<Models.MintKeyResponse>>;
  }


  async pauseUdf(udfId: string, opts?: RequestOptions & { withPerf?: false }): Promise<Models.Udf>;
  async pauseUdf(udfId: string, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.Udf>>;
  async pauseUdf(udfId: string, opts: RequestOptions = {}): Promise<Models.Udf | LayerResponse<Models.Udf>> {
    return this.requestJson<Models.Udf>({
      method: "POST",
      path: "/v2/udfs/" + encodeURIComponent(String(udfId)) + "/pause",
      params: undefined,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.Udf | LayerResponse<Models.Udf>>;
  }


  async putBlob(namespace_: string, body: Uint8Array | ArrayBuffer | Blob, opts?: PutBlobOptions & { withPerf?: false }): Promise<Models.BlobPutResponse>;
  async putBlob(namespace_: string, body: Uint8Array | ArrayBuffer | Blob, opts: PutBlobOptions & { withPerf: true }): Promise<LayerResponse<Models.BlobPutResponse>>;
  async putBlob(namespace_: string, body: Uint8Array | ArrayBuffer | Blob, opts: PutBlobOptions = {}): Promise<Models.BlobPutResponse | LayerResponse<Models.BlobPutResponse>> {
    return this.requestJson<Models.BlobPutResponse>({
      method: "PUT",
      path: "/v1/namespaces/" + encodeURIComponent(String(namespace_)) + "/blobs",
      params: [
        { key: "warm", value: opts.warm }
      ],
        body: body,
        bodyContentType: "application/octet-stream",
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.BlobPutResponse | LayerResponse<Models.BlobPutResponse>>;
  }


  async putPipelineDocumentChunks(pipelineId: string, docId: string, body: Models.PutChunksRequest | Record<string, unknown>, opts?: RequestOptions & { withPerf?: false }): Promise<Models.StageDocumentResponse>;
  async putPipelineDocumentChunks(pipelineId: string, docId: string, body: Models.PutChunksRequest | Record<string, unknown>, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.StageDocumentResponse>>;
  async putPipelineDocumentChunks(pipelineId: string, docId: string, body: Models.PutChunksRequest | Record<string, unknown>, opts: RequestOptions = {}): Promise<Models.StageDocumentResponse | LayerResponse<Models.StageDocumentResponse>> {
    return this.requestJson<Models.StageDocumentResponse>({
      method: "PUT",
      path: "/v2/pipelines/" + encodeURIComponent(String(pipelineId)) + "/documents/" + encodeURIComponent(String(docId)),
      params: undefined,
        body: body,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.StageDocumentResponse | LayerResponse<Models.StageDocumentResponse>>;
  }


  async putPipelineDocumentVectors(pipelineId: string, docId: string, body: Models.PutVectorsRequest | Record<string, unknown>, opts?: RequestOptions & { withPerf?: false }): Promise<Models.StatusResponse>;
  async putPipelineDocumentVectors(pipelineId: string, docId: string, body: Models.PutVectorsRequest | Record<string, unknown>, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.StatusResponse>>;
  async putPipelineDocumentVectors(pipelineId: string, docId: string, body: Models.PutVectorsRequest | Record<string, unknown>, opts: RequestOptions = {}): Promise<Models.StatusResponse | LayerResponse<Models.StatusResponse>> {
    return this.requestJson<Models.StatusResponse>({
      method: "PUT",
      path: "/v2/pipelines/" + encodeURIComponent(String(pipelineId)) + "/documents/" + encodeURIComponent(String(docId)) + "/vectors",
      params: undefined,
        body: body,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.StatusResponse | LayerResponse<Models.StatusResponse>>;
  }


  async putSnapshotPolicy(namespace_: string, body: Models.SnapshotPolicy | Record<string, unknown>, opts?: RequestOptions & { withPerf?: false }): Promise<Models.SnapshotPolicy>;
  async putSnapshotPolicy(namespace_: string, body: Models.SnapshotPolicy | Record<string, unknown>, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.SnapshotPolicy>>;
  async putSnapshotPolicy(namespace_: string, body: Models.SnapshotPolicy | Record<string, unknown>, opts: RequestOptions = {}): Promise<Models.SnapshotPolicy | LayerResponse<Models.SnapshotPolicy>> {
    return this.requestJson<Models.SnapshotPolicy>({
      method: "PUT",
      path: "/v2/namespaces/" + encodeURIComponent(String(namespace_)) + "/snapshot-policy",
      params: undefined,
        body: body,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.SnapshotPolicy | LayerResponse<Models.SnapshotPolicy>>;
  }


  async query(body: Models.FederatedQueryRequest | Record<string, unknown>, opts?: RequestOptions & { withPerf?: false }): Promise<Models.FederatedQueryResponse>;
  async query(body: Models.FederatedQueryRequest | Record<string, unknown>, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.FederatedQueryResponse>>;
  async query(body: Models.FederatedQueryRequest | Record<string, unknown>, opts: RequestOptions = {}): Promise<Models.FederatedQueryResponse | LayerResponse<Models.FederatedQueryResponse>> {
    return this.requestJson<Models.FederatedQueryResponse>({
      method: "POST",
      path: "/v2/query",
      params: undefined,
        body: body,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.FederatedQueryResponse | LayerResponse<Models.FederatedQueryResponse>>;
  }


  async queryAgent(name: string, body: Models.AgentQueryRequest | Record<string, unknown>, opts?: RequestOptions & { withPerf?: false }): Promise<Models.AgentQueryResponse>;
  async queryAgent(name: string, body: Models.AgentQueryRequest | Record<string, unknown>, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.AgentQueryResponse>>;
  async queryAgent(name: string, body: Models.AgentQueryRequest | Record<string, unknown>, opts: RequestOptions = {}): Promise<Models.AgentQueryResponse | LayerResponse<Models.AgentQueryResponse>> {
    return this.requestJson<Models.AgentQueryResponse>({
      method: "POST",
      path: "/v2/agents/" + encodeURIComponent(String(name)) + "/query",
      params: undefined,
        body: body,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.AgentQueryResponse | LayerResponse<Models.AgentQueryResponse>>;
  }


  async queryMetrics(opts?: QueryMetricsOptions & { withPerf?: false }): Promise<Models.PrometheusResponse>;
  async queryMetrics(opts: QueryMetricsOptions & { withPerf: true }): Promise<LayerResponse<Models.PrometheusResponse>>;
  async queryMetrics(opts: QueryMetricsOptions = {}): Promise<Models.PrometheusResponse | LayerResponse<Models.PrometheusResponse>> {
    return this.requestJson<Models.PrometheusResponse>({
      method: "GET",
      path: "/v2/metrics/query",
      params: [
        { key: "query", value: opts.query },
        { key: "time", value: opts.time },
        { key: "timeout", value: opts.timeout }
      ],
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.PrometheusResponse | LayerResponse<Models.PrometheusResponse>>;
  }


  async queryMetricsApiV1(opts?: QueryMetricsApiV1Options & { withPerf?: false }): Promise<Models.PrometheusResponse>;
  async queryMetricsApiV1(opts: QueryMetricsApiV1Options & { withPerf: true }): Promise<LayerResponse<Models.PrometheusResponse>>;
  async queryMetricsApiV1(opts: QueryMetricsApiV1Options = {}): Promise<Models.PrometheusResponse | LayerResponse<Models.PrometheusResponse>> {
    return this.requestJson<Models.PrometheusResponse>({
      method: "GET",
      path: "/v2/metrics/api/v1/query",
      params: [
        { key: "query", value: opts.query },
        { key: "time", value: opts.time },
        { key: "timeout", value: opts.timeout }
      ],
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.PrometheusResponse | LayerResponse<Models.PrometheusResponse>>;
  }


  async queryMetricsRange(opts?: QueryMetricsRangeOptions & { withPerf?: false }): Promise<Models.PrometheusResponse>;
  async queryMetricsRange(opts: QueryMetricsRangeOptions & { withPerf: true }): Promise<LayerResponse<Models.PrometheusResponse>>;
  async queryMetricsRange(opts: QueryMetricsRangeOptions = {}): Promise<Models.PrometheusResponse | LayerResponse<Models.PrometheusResponse>> {
    return this.requestJson<Models.PrometheusResponse>({
      method: "GET",
      path: "/v2/metrics/query_range",
      params: [
        { key: "query", value: opts.query },
        { key: "start", value: opts.start },
        { key: "end", value: opts.end },
        { key: "step", value: opts.step },
        { key: "timeout", value: opts.timeout }
      ],
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.PrometheusResponse | LayerResponse<Models.PrometheusResponse>>;
  }


  async queryMetricsRangeApiV1(opts?: QueryMetricsRangeApiV1Options & { withPerf?: false }): Promise<Models.PrometheusResponse>;
  async queryMetricsRangeApiV1(opts: QueryMetricsRangeApiV1Options & { withPerf: true }): Promise<LayerResponse<Models.PrometheusResponse>>;
  async queryMetricsRangeApiV1(opts: QueryMetricsRangeApiV1Options = {}): Promise<Models.PrometheusResponse | LayerResponse<Models.PrometheusResponse>> {
    return this.requestJson<Models.PrometheusResponse>({
      method: "GET",
      path: "/v2/metrics/api/v1/query_range",
      params: [
        { key: "query", value: opts.query },
        { key: "start", value: opts.start },
        { key: "end", value: opts.end },
        { key: "step", value: opts.step },
        { key: "timeout", value: opts.timeout }
      ],
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.PrometheusResponse | LayerResponse<Models.PrometheusResponse>>;
  }


  async queryNamespace(namespace_: string, body: Models.QueryRequest | Record<string, unknown>, opts?: QueryNamespaceOptions & { withPerf?: false }): Promise<Models.QueryResponse>;
  async queryNamespace(namespace_: string, body: Models.QueryRequest | Record<string, unknown>, opts: QueryNamespaceOptions & { withPerf: true }): Promise<LayerResponse<Models.QueryResponse>>;
  async queryNamespace(namespace_: string, body: Models.QueryRequest | Record<string, unknown>, opts: QueryNamespaceOptions = {}): Promise<Models.QueryResponse | LayerResponse<Models.QueryResponse>> {
    return this.requestJson<Models.QueryResponse>({
      method: "POST",
      path: "/v2/namespaces/" + encodeURIComponent(String(namespace_)) + "/query",
      params: undefined,
        body: body,
        headers: this.searchHistoryHeaders(opts.searchQuery, opts.tags),
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.QueryResponse | LayerResponse<Models.QueryResponse>>;
  }


  async queryTurbopufferNamespace(namespace_: string, body: Models.TurbopufferQueryRequest | Record<string, unknown>, opts?: RequestOptions & { withPerf?: false }): Promise<Models.TurbopufferQueryResponse>;
  async queryTurbopufferNamespace(namespace_: string, body: Models.TurbopufferQueryRequest | Record<string, unknown>, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.TurbopufferQueryResponse>>;
  async queryTurbopufferNamespace(namespace_: string, body: Models.TurbopufferQueryRequest | Record<string, unknown>, opts: RequestOptions = {}): Promise<Models.TurbopufferQueryResponse | LayerResponse<Models.TurbopufferQueryResponse>> {
    return this.requestJson<Models.TurbopufferQueryResponse>({
      method: "POST",
      path: "/v1/namespaces/" + encodeURIComponent(String(namespace_)) + "/query",
      params: undefined,
        body: body,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.TurbopufferQueryResponse | LayerResponse<Models.TurbopufferQueryResponse>>;
  }


  async resetFailedUdf(udfId: string, opts?: RequestOptions & { withPerf?: false }): Promise<Models.UdfItemsResponse>;
  async resetFailedUdf(udfId: string, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.UdfItemsResponse>>;
  async resetFailedUdf(udfId: string, opts: RequestOptions = {}): Promise<Models.UdfItemsResponse | LayerResponse<Models.UdfItemsResponse>> {
    return this.requestJson<Models.UdfItemsResponse>({
      method: "POST",
      path: "/v2/udfs/" + encodeURIComponent(String(udfId)) + "/reset-failed",
      params: undefined,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.UdfItemsResponse | LayerResponse<Models.UdfItemsResponse>>;
  }


  async resumeUdf(udfId: string, opts?: RequestOptions & { withPerf?: false }): Promise<Models.Udf>;
  async resumeUdf(udfId: string, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.Udf>>;
  async resumeUdf(udfId: string, opts: RequestOptions = {}): Promise<Models.Udf | LayerResponse<Models.Udf>> {
    return this.requestJson<Models.Udf>({
      method: "POST",
      path: "/v2/udfs/" + encodeURIComponent(String(udfId)) + "/resume",
      params: undefined,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.Udf | LayerResponse<Models.Udf>>;
  }


  async revokeKey(keyId: string, opts?: RequestOptions & { withPerf?: false }): Promise<Models.ApiKey>;
  async revokeKey(keyId: string, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.ApiKey>>;
  async revokeKey(keyId: string, opts: RequestOptions = {}): Promise<Models.ApiKey | LayerResponse<Models.ApiKey>> {
    return this.requestJson<Models.ApiKey>({
      method: "POST",
      path: "/v2/keys/" + encodeURIComponent(String(keyId)) + "/revoke",
      params: undefined,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.ApiKey | LayerResponse<Models.ApiKey>>;
  }


  async setDocumentsStage(pipelineId: string, body: Models.SetDocumentsStageRequest | Record<string, unknown>, opts?: RequestOptions & { withPerf?: false }): Promise<Models.DocumentsStageResponse>;
  async setDocumentsStage(pipelineId: string, body: Models.SetDocumentsStageRequest | Record<string, unknown>, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.DocumentsStageResponse>>;
  async setDocumentsStage(pipelineId: string, body: Models.SetDocumentsStageRequest | Record<string, unknown>, opts: RequestOptions = {}): Promise<Models.DocumentsStageResponse | LayerResponse<Models.DocumentsStageResponse>> {
    return this.requestJson<Models.DocumentsStageResponse>({
      method: "POST",
      path: "/v2/pipelines/" + encodeURIComponent(String(pipelineId)) + "/documents/stage",
      params: undefined,
        body: body,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.DocumentsStageResponse | LayerResponse<Models.DocumentsStageResponse>>;
  }


  async updateTurbopufferNamespaceMetadata(namespace_: string, body: Models.TurbopufferMetadataPatch | Record<string, unknown>, opts?: RequestOptions & { withPerf?: false }): Promise<Models.NamespaceMetadata>;
  async updateTurbopufferNamespaceMetadata(namespace_: string, body: Models.TurbopufferMetadataPatch | Record<string, unknown>, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.NamespaceMetadata>>;
  async updateTurbopufferNamespaceMetadata(namespace_: string, body: Models.TurbopufferMetadataPatch | Record<string, unknown>, opts: RequestOptions = {}): Promise<Models.NamespaceMetadata | LayerResponse<Models.NamespaceMetadata>> {
    return this.requestJson<Models.NamespaceMetadata>({
      method: "PATCH",
      path: "/v1/namespaces/" + encodeURIComponent(String(namespace_)) + "/metadata",
      params: undefined,
        body: body,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.NamespaceMetadata | LayerResponse<Models.NamespaceMetadata>>;
  }


  async updateTurbopufferNamespaceSchema(namespace_: string, body: Models.TurbopufferSchema | Record<string, unknown>, opts?: RequestOptions & { withPerf?: false }): Promise<Models.TurbopufferSchema>;
  async updateTurbopufferNamespaceSchema(namespace_: string, body: Models.TurbopufferSchema | Record<string, unknown>, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.TurbopufferSchema>>;
  async updateTurbopufferNamespaceSchema(namespace_: string, body: Models.TurbopufferSchema | Record<string, unknown>, opts: RequestOptions = {}): Promise<Models.TurbopufferSchema | LayerResponse<Models.TurbopufferSchema>> {
    return this.requestJson<Models.TurbopufferSchema>({
      method: "POST",
      path: "/v1/namespaces/" + encodeURIComponent(String(namespace_)) + "/schema",
      params: undefined,
        body: body,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.TurbopufferSchema | LayerResponse<Models.TurbopufferSchema>>;
  }


  async upsertUdf(udfId: string, body: Models.UpdateUdfRequest | Record<string, unknown>, opts?: RequestOptions & { withPerf?: false }): Promise<Models.Udf>;
  async upsertUdf(udfId: string, body: Models.UpdateUdfRequest | Record<string, unknown>, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.Udf>>;
  async upsertUdf(udfId: string, body: Models.UpdateUdfRequest | Record<string, unknown>, opts: RequestOptions = {}): Promise<Models.Udf | LayerResponse<Models.Udf>> {
    return this.requestJson<Models.Udf>({
      method: "PUT",
      path: "/v2/udfs/" + encodeURIComponent(String(udfId)),
      params: undefined,
        body: body,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.Udf | LayerResponse<Models.Udf>>;
  }


  async warmCache(namespace_: string, opts?: WarmCacheOptions & { withPerf?: false }): Promise<Models.WarmJob>;
  async warmCache(namespace_: string, opts: WarmCacheOptions & { withPerf: true }): Promise<LayerResponse<Models.WarmJob>>;
  async warmCache(namespace_: string, opts: WarmCacheOptions = {}): Promise<Models.WarmJob | LayerResponse<Models.WarmJob>> {
    return this.requestJson<Models.WarmJob>({
      method: "POST",
      path: "/v2/namespaces/" + encodeURIComponent(String(namespace_)) + "/warm",
      params: [
        { key: "page_size", value: opts.pageSize }
      ],
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.WarmJob | LayerResponse<Models.WarmJob>>;
  }


  async writeNamespace(namespace_: string, body: Models.TurbopufferWriteRequest | Record<string, unknown>, opts?: RequestOptions & { withPerf?: false }): Promise<Models.TurbopufferWriteResponse>;
  async writeNamespace(namespace_: string, body: Models.TurbopufferWriteRequest | Record<string, unknown>, opts: RequestOptions & { withPerf: true }): Promise<LayerResponse<Models.TurbopufferWriteResponse>>;
  async writeNamespace(namespace_: string, body: Models.TurbopufferWriteRequest | Record<string, unknown>, opts: RequestOptions = {}): Promise<Models.TurbopufferWriteResponse | LayerResponse<Models.TurbopufferWriteResponse>> {
    return this.requestJson<Models.TurbopufferWriteResponse>({
      method: "POST",
      path: "/v2/namespaces/" + encodeURIComponent(String(namespace_)),
      params: undefined,
        body: body,
      withPerf: opts.withPerf === true,
      signal: opts.signal,
    }) as Promise<Models.TurbopufferWriteResponse | LayerResponse<Models.TurbopufferWriteResponse>>;
  }


  async ensurePipeline(body: Models.CreatePipelineRequest | Record<string, unknown>): Promise<Models.Pipeline> {
    try {
      return await this.createPipeline(body);
    } catch (error) {
      if (!(error instanceof HevlayerError) || error.statusCode !== 409) {
        throw error;
      }
      const pipelineId = isRecord(body) ? body.id : undefined;
      const pipelines = await this.listPipelines();
      for (const pipeline of pipelines.pipelines ?? []) {
        if (String(pipeline.id) === String(pipelineId)) {
          return pipeline;
        }
      }
      throw error;
    }
  }

  async releaseDocuments(
    pipelineId: string,
    documentIds: string[],
    opts?: DocumentStageOptions & { withPerf?: false },
  ): Promise<Models.DocumentsStageResponse>;
  async releaseDocuments(
    pipelineId: string,
    documentIds: string[],
    opts: DocumentStageOptions & { withPerf: true },
  ): Promise<LayerResponse<Models.DocumentsStageResponse>>;
  async releaseDocuments(
    pipelineId: string,
    documentIds: string[],
    opts: DocumentStageOptions = {},
  ): Promise<Models.DocumentsStageResponse | LayerResponse<Models.DocumentsStageResponse>> {
    return this.setDocumentsStageHelper(pipelineId, documentIds, "pending", opts);
  }

  async failDocuments(
    pipelineId: string,
    documentIds: string[],
    opts?: DocumentStageOptions & { withPerf?: false },
  ): Promise<Models.DocumentsStageResponse>;
  async failDocuments(
    pipelineId: string,
    documentIds: string[],
    opts: DocumentStageOptions & { withPerf: true },
  ): Promise<LayerResponse<Models.DocumentsStageResponse>>;
  async failDocuments(
    pipelineId: string,
    documentIds: string[],
    opts: DocumentStageOptions = {},
  ): Promise<Models.DocumentsStageResponse | LayerResponse<Models.DocumentsStageResponse>> {
    return this.setDocumentsStageHelper(pipelineId, documentIds, "failed", opts);
  }

  async completeDocuments(
    pipelineId: string,
    documentIds: string[],
    opts?: DocumentStageOptions & { withPerf?: false },
  ): Promise<Models.DocumentsStageResponse>;
  async completeDocuments(
    pipelineId: string,
    documentIds: string[],
    opts: DocumentStageOptions & { withPerf: true },
  ): Promise<LayerResponse<Models.DocumentsStageResponse>>;
  async completeDocuments(
    pipelineId: string,
    documentIds: string[],
    opts: DocumentStageOptions = {},
  ): Promise<Models.DocumentsStageResponse | LayerResponse<Models.DocumentsStageResponse>> {
    return this.setDocumentsStageHelper(pipelineId, documentIds, "indexed", opts);
  }

  async writeSingleVector(
    pipelineId: string,
    docId: string,
    vector: Models.VectorEntry | Record<string, unknown>,
    opts?: RequestOptions & { withPerf?: false },
  ): Promise<Models.StatusResponse>;
  async writeSingleVector(
    pipelineId: string,
    docId: string,
    vector: Models.VectorEntry | Record<string, unknown>,
    opts: RequestOptions & { withPerf: true },
  ): Promise<LayerResponse<Models.StatusResponse>>;
  async writeSingleVector(
    pipelineId: string,
    docId: string,
    vector: Models.VectorEntry | Record<string, unknown>,
    opts: RequestOptions = {},
  ): Promise<Models.StatusResponse | LayerResponse<Models.StatusResponse>> {
    return this.putPipelineDocumentVectors(pipelineId, docId, { vectors: [vector] }, opts as any);
  }

  async writeSingleMultivector(
    pipelineId: string,
    docId: string,
    id: string,
    vectors: number[][],
    attributes?: Record<string, unknown>,
    opts?: RequestOptions & { withPerf?: false },
  ): Promise<Models.StatusResponse>;
  async writeSingleMultivector(
    pipelineId: string,
    docId: string,
    id: string,
    vectors: number[][],
    attributes: Record<string, unknown> | undefined,
    opts: RequestOptions & { withPerf: true },
  ): Promise<LayerResponse<Models.StatusResponse>>;
  async writeSingleMultivector(
    pipelineId: string,
    docId: string,
    id: string,
    vectors: number[][],
    attributes?: Record<string, unknown>,
    opts: RequestOptions = {},
  ): Promise<Models.StatusResponse | LayerResponse<Models.StatusResponse>> {
    return this.putPipelineDocumentVectors(pipelineId, docId, { vectors: [{ id, vectors, attributes }] }, opts as any);
  }

  async waitForScan(namespace: string, scanId: string, opts: ScanWaitOptions = {}): Promise<Models.ScanJob> {
    const started = nowMs();
    let delay = opts.initialDelayMs ?? 50;
    const maxDelay = opts.maxDelayMs ?? 2000;
    while (true) {
      const scan = await this.getScan(namespace, scanId, { signal: opts.signal });
      if (scan.status === "completed" || scan.status === "failed") {
        return scan;
      }
      if (opts.timeoutMs !== undefined && nowMs() - started >= opts.timeoutMs) {
        throw new Error("scan " + JSON.stringify(scanId) + " did not finish within " + opts.timeoutMs + "ms");
      }
      await sleep(delay, opts.signal);
      delay = Math.min(delay * 2, maxDelay);
    }
  }

  async scan(namespace: string, body: Models.CreateScanRequest | Record<string, unknown>, opts: ScanOptions = {}): Promise<Models.ScanJob> {
    const created = await this.createScan(namespace, body, { signal: opts.signal });
    if (!isRecord(created) || typeof created.id !== "string") {
      throw new Error("scan create response did not include id");
    }
    return this.waitForScan(namespace, created.id, opts);
  }

  async warmNamespace(namespace: string, opts?: WarmCacheOptions & { withPerf?: false }): Promise<Models.WarmJob>;
  async warmNamespace(namespace: string, opts: WarmCacheOptions & { withPerf: true }): Promise<LayerResponse<Models.WarmJob>>;
  async warmNamespace(namespace: string, opts: WarmCacheOptions = {}): Promise<Models.WarmJob | LayerResponse<Models.WarmJob>> {
    return this.warmCache(namespace, opts as any);
  }

  async patchColumns(
    namespace: string,
    ids: string[],
    attrs: Record<string, unknown[]>,
    opts?: RequestOptions & { withPerf?: false },
  ): Promise<Models.TurbopufferWriteResponse>;
  async patchColumns(
    namespace: string,
    ids: string[],
    attrs: Record<string, unknown[]>,
    opts: RequestOptions & { withPerf: true },
  ): Promise<LayerResponse<Models.TurbopufferWriteResponse>>;
  async patchColumns(
    namespace: string,
    ids: string[],
    attrs: Record<string, unknown[]>,
    opts: RequestOptions = {},
  ): Promise<Models.TurbopufferWriteResponse | LayerResponse<Models.TurbopufferWriteResponse>> {
    if (ids.length === 0) {
      throw new Error("patchColumns requires at least one id");
    }
    for (const id of ids) {
      if (!id) {
        throw new Error("patchColumns ids must be non-empty");
      }
    }
    const columns: Record<string, unknown[]> = { id: [...ids] };
    for (const [name, values] of Object.entries(attrs)) {
      if (name === "id") {
        throw new Error("patchColumns attrs must not include id");
      }
      if (values.length !== ids.length) {
        throw new Error("patchColumns attr " + JSON.stringify(name) + " has " + values.length + " values for " + ids.length + " ids");
      }
      columns[name] = [...values];
    }
    return this.writeNamespace(namespace, { patch_columns: columns }, opts as any);
  }

  private async setDocumentsStageHelper(
    pipelineId: string,
    documentIds: string[],
    stage: string,
    opts: DocumentStageOptions,
  ): Promise<Models.DocumentsStageResponse | LayerResponse<Models.DocumentsStageResponse>> {
    return this.setDocumentsStage(
      pipelineId,
      {
        document_ids: documentIds,
        stage,
        from_stage: opts.fromStage,
        worker_id: opts.workerId,
      },
      opts as any,
    );
  }

  private async requestJson<T>(request: JsonRequest): Promise<T | LayerResponse<T>> {
    const started = nowMs();
    let response: Response;
    try {
      response = await this.fetchJson(this.baseUrl, this.apiKey, request);
    } catch (error) {
      throw unwrapTransportError(error);
    }
    const latencyMs = nowMs() - started;
    const cacheStatus = response.headers.get("x-layer-cache");
    const raw = await this.decodeJsonResponse(response);
    if (!response.ok) {
      throw this.errorFromResponse(response, raw);
    }
    const data = raw as T;
    this.applyLayerHeaders(data, response.headers);
    if (request.withPerf) {
      return { data, perf: { latencyMs, cacheStatus } };
    }
    return data;
  }

  private applyLayerHeaders(value: unknown, headers: Headers): void {
    if (!isRecord(value)) {
      return;
    }
    const stable = headers.get("x-layer-stable-as-of");
    if (stable !== null) {
      const parsed = Number.parseInt(stable, 10);
      if (Number.isFinite(parsed)) {
        value.stable_as_of = parsed;
      }
    }
    const nextCursor = headers.get("x-layer-next-cursor");
    if (nextCursor !== null) {
      value.next_cursor = nextCursor;
    }
  }

  private async requestBytes(request: JsonRequest): Promise<Uint8Array | LayerResponse<Uint8Array>> {
    const started = nowMs();
    let response: Response;
    try {
      response = await this.fetchJson(this.baseUrl, this.apiKey, request);
    } catch (error) {
      throw unwrapTransportError(error);
    }
    const latencyMs = nowMs() - started;
    const cacheStatus = response.headers.get("x-layer-cache");
    if (!response.ok) {
      throw this.errorFromResponse(response, await this.decodeJsonResponse(response));
    }
    const data = new Uint8Array(await response.arrayBuffer());
    if (request.withPerf) {
      return { data, perf: { latencyMs, cacheStatus } };
    }
    return data;
  }

  private async fetchJson(
    baseUrl: string,
    apiKey: string | null,
    request: JsonRequest,
  ): Promise<Response> {
    const headers = new Headers(request.headers);
    const init: RequestInit = {
      method: request.method,
      headers,
    };
    if (apiKey) {
      headers.set("Authorization", "Bearer " + apiKey);
    }
    if (request.bodyContentType) {
      headers.set("Content-Type", request.bodyContentType);
      init.body = request.body as BodyInit;
    } else if (request.body !== undefined && request.body !== null) {
      headers.set("Content-Type", "application/json");
      init.body = JSON.stringify(request.body);
    }
    const url = this.urlFor(baseUrl, request.path, request.params);
    const signal = this.requestSignal(request.signal);
    init.signal = signal.signal;
    try {
      return await this.fetchImpl(url, init);
    } catch (error) {
      throw new FetchTransportError(error);
    } finally {
      signal.cleanup();
    }
  }

  private requestSignal(signal: AbortSignal | undefined): { signal?: AbortSignal; cleanup: () => void } {
    if (this.timeout === null && !signal) {
      return { cleanup: () => {} };
    }
    if (this.timeout === null) {
      return { signal, cleanup: () => {} };
    }
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), this.timeout);
    const abortFromSignal = () => controller.abort(signal?.reason);
    signal?.addEventListener("abort", abortFromSignal, { once: true });
    return {
      signal: controller.signal,
      cleanup: () => {
        clearTimeout(timeout);
        signal?.removeEventListener("abort", abortFromSignal);
      },
    };
  }

  private urlFor(baseUrl: string, requestPath: string, params: QueryParam[] | undefined): string {
    const url = new URL(requestPath, baseUrl + "/");
    for (const param of params ?? []) {
      const value = this.queryParamValue(param.key, param.value);
      if (value !== null) {
        url.searchParams.set(param.key, value);
      }
    }
    return url.toString();
  }

  private queryParamValue(key: string, value: unknown): string | null {
    if (value === undefined || value === null) {
      return null;
    }
    if (Array.isArray(value)) {
      if (value.length === 0) {
        return null;
      }
      if (key === "tag") {
        const tags = cleanHistoryTags(value);
        return tags.length ? tags.join(",") : null;
      }
      return value.map((item) => String(item)).join(",");
    }
    return String(value);
  }

  private searchHistoryHeaders(searchQuery: string | undefined, tags: string[] | undefined): Record<string, string> | undefined {
    const headers: Record<string, string> = {};
    if (searchQuery !== undefined) {
      const query = String(searchQuery).trim();
      if (query) {
        headers["x-hevlayer-search-query"] = query;
      }
    }
    if (tags !== undefined) {
      const cleanTags = cleanHistoryTags(tags);
      if (cleanTags.length > 0) {
        headers["x-hevlayer-tags"] = cleanTags.join(",");
      }
    }
    return Object.keys(headers).length ? headers : undefined;
  }

  private async decodeJsonResponse(response: Response): Promise<unknown> {
    if (response.status === 204) {
      return undefined;
    }
    const text = await response.text();
    if (!text) {
      return undefined;
    }
    try {
      return JSON.parse(text);
    } catch {
      return text;
    }
  }

  private errorFromResponse(response: Response, body: unknown): HevlayerError {
    if (isRecord(body)) {
      const kind = typeof body.error === "string" ? body.error : null;
      const message = typeof body.message === "string" && body.message ? body.message : response.statusText;
      return new HevlayerError(response.status, message, { kind, body, response });
    }
    const message = typeof body === "string" && body ? body : response.statusText;
    return new HevlayerError(response.status, message, { body, response });
  }
}

export interface DocumentStageOptions extends RequestOptions {
  fromStage?: string;
  workerId?: string;
}

export interface ScanWaitOptions {
  initialDelayMs?: number;
  maxDelayMs?: number;
  timeoutMs?: number;
  signal?: AbortSignal;
}

export interface ScanOptions extends ScanWaitOptions {}

function defaultFetch(): FetchLike {
  if (typeof globalThis.fetch !== "function") {
    throw new Error("global fetch is unavailable; use Node 18+ or pass a fetch implementation");
  }
  return globalThis.fetch.bind(globalThis);
}

function cleanBaseUrl(value: string | null | undefined, fallback: string): string {
  const cleaned = String(value ?? "").trim();
  return (cleaned || fallback).replace(/\/+$/, "");
}

function cleanToken(value: string | null | undefined): string | null {
  const token = String(value ?? "").trim();
  return token ? token : null;
}

function cleanHistoryTags(tags: unknown[]): string[] {
  const cleaned: string[] = [];
  for (const rawTag of tags) {
    const tag = String(rawTag).trim();
    if (!tag) {
      continue;
    }
    if (new TextEncoder().encode(tag).length > SEARCH_HISTORY_MAX_TAG_LENGTH) {
      throw new Error("search-history tag " + JSON.stringify(tag) + " exceeds " + SEARCH_HISTORY_MAX_TAG_LENGTH + " bytes");
    }
    if (!SEARCH_HISTORY_TAG_RE.test(tag)) {
      throw new Error("search-history tags may contain only ASCII letters, digits, ':', '_', '-', '.', '/', '=', or '+'; commas separate tags and cannot be escaped");
    }
    cleaned.push(tag);
  }
  const unique = [...new Set(cleaned)].sort();
  if (unique.length > SEARCH_HISTORY_MAX_TAGS) {
    throw new Error("search-history tags are limited to " + SEARCH_HISTORY_MAX_TAGS + " unique tags");
  }
  return unique;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function unwrapTransportError(error: unknown): unknown {
  return error instanceof FetchTransportError ? error.cause : error;
}

function nowMs(): number {
  return typeof performance !== "undefined" ? performance.now() : Date.now();
}

function sleep(ms: number, signal: AbortSignal | undefined): Promise<void> {
  if (signal?.aborted) {
    return Promise.reject(signal.reason);
  }
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(resolve, ms);
    const abort = () => {
      clearTimeout(timeout);
      reject(signal?.reason);
    };
    signal?.addEventListener("abort", abort, { once: true });
  });
}
