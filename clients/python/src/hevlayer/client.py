from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass
from typing import Any, Generic, Protocol, TypeVar, get_args, get_origin

import httpx
from pydantic import BaseModel

from .models import *

T = TypeVar("T")

_SEARCH_HISTORY_MAX_TAGS = 32
_SEARCH_HISTORY_MAX_TAG_LENGTH = 128
_SEARCH_HISTORY_TAG_CHARS = set(":_-.=/+")


@dataclass(frozen=True)
class LayerPerf:
    latency_ms: float
    cache_status: str | None


@dataclass(frozen=True)
class LayerResponse(Generic[T]):
    data: T
    perf: LayerPerf


class HevlayerError(Exception):
    def __init__(
        self,
        status_code: int,
        message: str,
        *,
        error: str | None = None,
        response: httpx.Response | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.message = message
        self.error = error
        self.response = response


class HevlayerProtocol(Protocol):
    async def authenticate_key(self, body: AuthenticateKeyRequest | dict[str, Any], *, with_perf: bool = False) -> AuthenticateKeyResponse | LayerResponse[AuthenticateKeyResponse]: ...
    async def batch_query_namespace(self, namespace: str, body: BatchQueryRequest | dict[str, Any], *, with_perf: bool = False) -> BatchQueryResponse | LayerResponse[BatchQueryResponse]: ...
    async def branch_namespace(self, namespace: str, body: TurbopufferBranchFromRequest | dict[str, Any], *, with_perf: bool = False) -> TurbopufferWriteResponse | LayerResponse[TurbopufferWriteResponse]: ...
    async def claim_documents(self, pipeline_id: str, body: ClaimDocumentsRequest | dict[str, Any], *, with_perf: bool = False) -> ClaimDocumentsResponse | LayerResponse[ClaimDocumentsResponse]: ...
    async def claim_udf_items(self, udf_id: str, body: UdfClaimRequest | dict[str, Any], *, with_perf: bool = False) -> UdfClaimResponse | LayerResponse[UdfClaimResponse]: ...
    async def complete_udf_items(self, udf_id: str, body: UdfCompleteRequest | dict[str, Any], *, with_perf: bool = False) -> UdfItemsResponse | LayerResponse[UdfItemsResponse]: ...
    async def copy_namespace(self, namespace: str, body: TurbopufferCopyFromRequest | dict[str, Any], *, with_perf: bool = False) -> TurbopufferWriteResponse | LayerResponse[TurbopufferWriteResponse]: ...
    async def create_checkpoint(self, namespace: str, body: CreateCheckpointRequest | dict[str, Any], *, with_perf: bool = False) -> Checkpoint | LayerResponse[Checkpoint]: ...
    async def create_pipeline(self, body: CreatePipelineRequest | dict[str, Any], *, with_perf: bool = False) -> Pipeline | LayerResponse[Pipeline]: ...
    async def create_scan(self, namespace: str, body: CreateScanRequest | dict[str, Any], *, with_perf: bool = False) -> ScanCountResponse | ScanJob | LayerResponse[ScanCountResponse | ScanJob]: ...
    async def create_snapshot(self, namespace: str, body: CreateSnapshotRequest | dict[str, Any], *, with_perf: bool = False) -> SnapshotJob | LayerResponse[SnapshotJob]: ...
    async def create_udf(self, body: CreateUdfRequest | dict[str, Any], *, with_perf: bool = False) -> Udf | LayerResponse[Udf]: ...
    async def delete_key(self, keyId: str, *, with_perf: bool = False) -> StatusResponse | LayerResponse[StatusResponse]: ...
    async def delete_namespace(self, namespace: str, *, with_perf: bool = False) -> StatusResponse | LayerResponse[StatusResponse]: ...
    async def delete_pipeline(self, pipeline_id: str, *, with_perf: bool = False) -> StatusResponse | LayerResponse[StatusResponse]: ...
    async def delete_scan(self, namespace: str, scan_id: str, *, with_perf: bool = False) -> StatusResponse | LayerResponse[StatusResponse]: ...
    async def delete_udf(self, udf_id: str, *, with_perf: bool = False) -> StatusResponse | LayerResponse[StatusResponse]: ...
    async def discover_udf(self, udf_id: str, body: UdfDiscoverRequest | dict[str, Any], *, with_perf: bool = False) -> UdfDiscoverResponse | LayerResponse[UdfDiscoverResponse]: ...
    async def evaluate_turbopuffer_recall(self, namespace: str, body: TurbopufferRecallRequest | dict[str, Any], *, with_perf: bool = False) -> TurbopufferRecallResponse | LayerResponse[TurbopufferRecallResponse]: ...
    async def explain_turbopuffer_query(self, namespace: str, body: TurbopufferQueryRequest | dict[str, Any], *, with_perf: bool = False) -> TurbopufferExplainQueryResponse | LayerResponse[TurbopufferExplainQueryResponse]: ...
    async def fail_udf_items(self, udf_id: str, body: UdfFailRequest | dict[str, Any], *, with_perf: bool = False) -> UdfItemsResponse | LayerResponse[UdfItemsResponse]: ...
    async def fetch_document(self, namespace: str, doc_id: str, *, include_attributes: list[str] | None = None, with_perf: bool = False) -> Document | LayerResponse[Document]: ...
    async def fetch_documents(self, namespace: str, body: FetchDocumentsRequest | dict[str, Any], *, with_perf: bool = False) -> FetchDocumentsResponse | LayerResponse[FetchDocumentsResponse]: ...
    async def get_blob(self, namespace: str, sha256: str, *, with_perf: bool = False) -> bytes | LayerResponse[bytes]: ...
    async def get_checkpoint(self, namespace: str, label: str, *, with_perf: bool = False) -> Checkpoint | LayerResponse[Checkpoint]: ...
    async def get_cost_rate_card(self, *, with_perf: bool = False) -> RateCard | LayerResponse[RateCard]: ...
    async def get_cost_snapshot(self, *, window: CostWindow | None = None, with_perf: bool = False) -> CostSnapshot | LayerResponse[CostSnapshot]: ...
    async def get_cost_timeseries(self, *, window: CostWindow | None = None, step: CostStep | None = None, with_perf: bool = False) -> CostTimeseries | LayerResponse[CostTimeseries]: ...
    async def get_key(self, keyId: str, *, with_perf: bool = False) -> ApiKey | LayerResponse[ApiKey]: ...
    async def get_license(self, *, with_perf: bool = False) -> LicenseState | LayerResponse[LicenseState]: ...
    async def get_metric_catalog_entry(self, name: str, *, with_perf: bool = False) -> MetricCatalogEntry | LayerResponse[MetricCatalogEntry]: ...
    async def get_namespace_metadata(self, namespace: str, *, with_perf: bool = False) -> NamespaceMetadata | LayerResponse[NamespaceMetadata]: ...
    async def get_namespace_snapshot(self, namespace: str, sha: str, *, with_perf: bool = False) -> SnapshotBody | LayerResponse[SnapshotBody]: ...
    async def get_pipeline_document_chunks(self, pipeline_id: str, doc_id: str, *, with_perf: bool = False) -> GetChunksResponse | LayerResponse[GetChunksResponse]: ...
    async def get_pipeline_status(self, pipeline_id: str, *, with_perf: bool = False) -> PipelineStatus | LayerResponse[PipelineStatus]: ...
    async def get_scan(self, namespace: str, scan_id: str, *, with_perf: bool = False) -> ScanJob | LayerResponse[ScanJob]: ...
    async def get_scan_results(self, namespace: str, scan_id: str, *, limit: int | None = None, offset: int | None = None, with_perf: bool = False) -> ScanIdsResponse | ScanValuesResponse | LayerResponse[ScanIdsResponse | ScanValuesResponse]: ...
    async def get_snapshot_job(self, namespace: str, job_id: str, *, with_perf: bool = False) -> SnapshotJob | LayerResponse[SnapshotJob]: ...
    async def get_snapshot_policy(self, namespace: str, *, with_perf: bool = False) -> SnapshotPolicy | LayerResponse[SnapshotPolicy]: ...
    async def get_turbopuffer_namespace_schema(self, namespace: str, *, with_perf: bool = False) -> TurbopufferSchema | LayerResponse[TurbopufferSchema]: ...
    async def get_turbopuffer_v1_namespace_metadata(self, namespace: str, *, with_perf: bool = False) -> NamespaceMetadata | LayerResponse[NamespaceMetadata]: ...
    async def get_udf(self, udf_id: str, *, with_perf: bool = False) -> GetUdfResponse | LayerResponse[GetUdfResponse]: ...
    async def get_udf_status(self, udf_id: str, *, with_perf: bool = False) -> UdfStatus | LayerResponse[UdfStatus]: ...
    async def get_vectorstore(self, name: str, *, with_perf: bool = False) -> VectorStore | LayerResponse[VectorStore]: ...
    async def get_warehouse(self, name: str, *, with_perf: bool = False) -> Warehouse | LayerResponse[Warehouse]: ...
    async def get_warm_job(self, namespace: str, job_id: str, *, with_perf: bool = False) -> WarmJob | LayerResponse[WarmJob]: ...
    async def heartbeat_documents(self, pipeline_id: str, body: HeartbeatDocumentsRequest | dict[str, Any], *, with_perf: bool = False) -> DocumentsStageResponse | LayerResponse[DocumentsStageResponse]: ...
    async def heartbeat_udf_items(self, udf_id: str, body: UdfHeartbeatRequest | dict[str, Any], *, with_perf: bool = False) -> UdfItemsResponse | LayerResponse[UdfItemsResponse]: ...
    async def hint_cache_warm(self, namespace: str, *, turbopuffer: bool | None = None, documents: bool | None = None, snapshots: bool | None = None, blobs: bool | None = None, blob_budget_bytes: int | None = None, page_size: int | None = None, with_perf: bool = False) -> HintCacheWarmResponse | LayerResponse[HintCacheWarmResponse]: ...
    async def import_namespace(self, namespace: str, body: bytes, *, with_perf: bool = False) -> dict[str, Any] | LayerResponse[dict[str, Any]]: ...
    async def init_namespace(self, namespace: str, body: InitNamespaceRequest | dict[str, Any], *, with_perf: bool = False) -> InitNamespaceResponse | LayerResponse[InitNamespaceResponse]: ...
    async def list_checkpoints(self, namespace: str, *, limit: int | None = None, before: str | None = None, with_perf: bool = False) -> CheckpointList | LayerResponse[CheckpointList]: ...
    async def list_clickstream(self, namespace: str, *, trace_id: str | None = None, tags: list[str] | None = None, from_: str | None = None, to: str | None = None, before: str | None = None, limit: int | None = None, with_perf: bool = False) -> ClickstreamListResponse | LayerResponse[ClickstreamListResponse]: ...
    async def list_keys(self, *, includeRevoked: bool | None = None, with_perf: bool = False) -> ApiKeyList | LayerResponse[ApiKeyList]: ...
    async def list_metrics_catalog(self, *, family: MetricFamily | None = None, with_perf: bool = False) -> MetricCatalog | LayerResponse[MetricCatalog]: ...
    async def list_namespace_history(self, namespace: str, *, limit: int | None = None, before: str | None = None, with_perf: bool = False) -> list[SnapshotHistoryEntry] | LayerResponse[list[SnapshotHistoryEntry]]: ...
    async def list_namespaces(self, *, prefix: str | None = None, cursor: str | None = None, page_size: int | None = None, with_perf: bool = False) -> NamespaceList | LayerResponse[NamespaceList]: ...
    async def list_pipelines(self, *, with_perf: bool = False) -> PipelineList | LayerResponse[PipelineList]: ...
    async def list_scans(self, namespace: str, *, with_perf: bool = False) -> ScanJobList | LayerResponse[ScanJobList]: ...
    async def list_search_history(self, namespace: str, *, tags: list[str] | None = None, from_: str | None = None, to: str | None = None, before: str | None = None, limit: int | None = None, with_perf: bool = False) -> SearchHistoryListResponse | LayerResponse[SearchHistoryListResponse]: ...
    async def list_snapshot_activity(self, *, since: int | None = None, limit: int | None = None, namespace: str | None = None, cursor: str | None = None, with_perf: bool = False) -> SnapshotActivityList | LayerResponse[SnapshotActivityList]: ...
    async def list_snapshot_jobs(self, namespace: str, *, with_perf: bool = False) -> SnapshotJobList | LayerResponse[SnapshotJobList]: ...
    async def list_turbopuffer_namespaces(self, *, cursor: str | None = None, prefix: str | None = None, page_size: int | None = None, with_perf: bool = False) -> TurbopufferNamespaceList | LayerResponse[TurbopufferNamespaceList]: ...
    async def list_udfs(self, *, with_perf: bool = False) -> UdfList | LayerResponse[UdfList]: ...
    async def list_vectorstores(self, *, with_perf: bool = False) -> VectorStoreList | LayerResponse[VectorStoreList]: ...
    async def list_warehouses(self, *, with_perf: bool = False) -> WarehouseList | LayerResponse[WarehouseList]: ...
    async def list_warm_jobs(self, namespace: str, *, with_perf: bool = False) -> WarmJobList | LayerResponse[WarmJobList]: ...
    async def mint_key(self, body: MintKeyRequest | dict[str, Any], *, with_perf: bool = False) -> MintKeyResponse | LayerResponse[MintKeyResponse]: ...
    async def pause_udf(self, udf_id: str, *, with_perf: bool = False) -> Udf | LayerResponse[Udf]: ...
    async def put_blob(self, namespace: str, body: bytes, *, warm: bool | None = None, with_perf: bool = False) -> BlobPutResponse | LayerResponse[BlobPutResponse]: ...
    async def put_pipeline_document_chunks(self, pipeline_id: str, doc_id: str, body: PutChunksRequest | dict[str, Any], *, with_perf: bool = False) -> StageDocumentResponse | LayerResponse[StageDocumentResponse]: ...
    async def put_pipeline_document_vectors(self, pipeline_id: str, doc_id: str, body: PutVectorsRequest | dict[str, Any], *, with_perf: bool = False) -> StatusResponse | LayerResponse[StatusResponse]: ...
    async def put_snapshot_policy(self, namespace: str, body: SnapshotPolicy | dict[str, Any], *, with_perf: bool = False) -> SnapshotPolicy | LayerResponse[SnapshotPolicy]: ...
    async def query(self, body: FederatedQueryRequest | dict[str, Any], *, with_perf: bool = False) -> FederatedQueryResponse | LayerResponse[FederatedQueryResponse]: ...
    async def query_agent(self, name: str, body: AgentQueryRequest | dict[str, Any], *, with_perf: bool = False) -> AgentQueryResponse | LayerResponse[AgentQueryResponse]: ...
    async def query_metrics(self, *, query: str | None = None, time: str | None = None, timeout: str | None = None, with_perf: bool = False) -> PrometheusResponse | LayerResponse[PrometheusResponse]: ...
    async def query_metrics_api_v1(self, *, query: str | None = None, time: str | None = None, timeout: str | None = None, with_perf: bool = False) -> PrometheusResponse | LayerResponse[PrometheusResponse]: ...
    async def query_metrics_range(self, *, query: str | None = None, start: str | None = None, end: str | None = None, step: str | None = None, timeout: str | None = None, with_perf: bool = False) -> PrometheusResponse | LayerResponse[PrometheusResponse]: ...
    async def query_metrics_range_api_v1(self, *, query: str | None = None, start: str | None = None, end: str | None = None, step: str | None = None, timeout: str | None = None, with_perf: bool = False) -> PrometheusResponse | LayerResponse[PrometheusResponse]: ...
    async def query_namespace(self, namespace: str, body: QueryRequest | dict[str, Any], *, raw_query: str | None = None, tags: list[str] | None = None, with_perf: bool = False) -> QueryResponse | LayerResponse[QueryResponse]: ...
    async def query_turbopuffer_namespace(self, namespace: str, body: TurbopufferQueryRequest | dict[str, Any], *, with_perf: bool = False) -> TurbopufferQueryResponse | LayerResponse[TurbopufferQueryResponse]: ...
    async def reset_failed_udf(self, udf_id: str, *, with_perf: bool = False) -> UdfItemsResponse | LayerResponse[UdfItemsResponse]: ...
    async def resume_udf(self, udf_id: str, *, with_perf: bool = False) -> Udf | LayerResponse[Udf]: ...
    async def revoke_key(self, keyId: str, *, with_perf: bool = False) -> ApiKey | LayerResponse[ApiKey]: ...
    async def set_documents_stage(self, pipeline_id: str, body: SetDocumentsStageRequest | dict[str, Any], *, with_perf: bool = False) -> DocumentsStageResponse | LayerResponse[DocumentsStageResponse]: ...
    async def update_turbopuffer_namespace_metadata(self, namespace: str, body: TurbopufferMetadataPatch | dict[str, Any], *, with_perf: bool = False) -> NamespaceMetadata | LayerResponse[NamespaceMetadata]: ...
    async def update_turbopuffer_namespace_schema(self, namespace: str, body: TurbopufferSchema | dict[str, Any], *, with_perf: bool = False) -> TurbopufferSchema | LayerResponse[TurbopufferSchema]: ...
    async def upsert_udf(self, udf_id: str, body: UpdateUdfRequest | dict[str, Any], *, with_perf: bool = False) -> Udf | LayerResponse[Udf]: ...
    async def warm_cache(self, namespace: str, *, page_size: int | None = None, with_perf: bool = False) -> WarmJob | LayerResponse[WarmJob]: ...
    async def write_namespace(self, namespace: str, body: TurbopufferWriteRequest | dict[str, Any], *, with_perf: bool = False) -> TurbopufferWriteResponse | LayerResponse[TurbopufferWriteResponse]: ...
    async def ensure_pipeline(self, body: CreatePipelineRequest | dict[str, Any]) -> Pipeline: ...
    async def release_documents(self, pipeline_id: str, document_ids: list[str], *, from_stage: str | None = None, worker_id: str | None = None, with_perf: bool = False) -> DocumentsStageResponse | LayerResponse[DocumentsStageResponse]: ...
    async def fail_documents(self, pipeline_id: str, document_ids: list[str], *, from_stage: str | None = None, worker_id: str | None = None, with_perf: bool = False) -> DocumentsStageResponse | LayerResponse[DocumentsStageResponse]: ...
    async def complete_documents(self, pipeline_id: str, document_ids: list[str], *, from_stage: str | None = None, worker_id: str | None = None, with_perf: bool = False) -> DocumentsStageResponse | LayerResponse[DocumentsStageResponse]: ...
    async def write_single_vector(self, pipeline_id: str, doc_id: str, vector: VectorEntry | dict[str, Any], *, with_perf: bool = False) -> StatusResponse | LayerResponse[StatusResponse]: ...
    async def write_single_multivector(self, pipeline_id: str, doc_id: str, id: str, vectors: list[list[float]], *, attributes: dict[str, Any] | None = None, with_perf: bool = False) -> StatusResponse | LayerResponse[StatusResponse]: ...
    async def wait_for_scan(self, namespace: str, scan_id: str, *, initial_delay: float = 0.05, max_delay: float = 2.0, timeout: float | None = None) -> ScanJob: ...
    async def scan(self, namespace: str, body: CreateScanRequest | dict[str, Any], *, initial_delay: float = 0.05, max_delay: float = 2.0, timeout: float | None = None) -> ScanJob: ...
    async def warm_namespace(self, namespace: str, *, page_size: int | None = None, with_perf: bool = False) -> WarmJob | LayerResponse[WarmJob]: ...
    async def patch_columns(self, namespace: str, ids: list[str], attrs: dict[str, list[Any]], *, with_perf: bool = False) -> TurbopufferWriteResponse | LayerResponse[TurbopufferWriteResponse]: ...


class AsyncHevlayer:
    def __init__(
        self,
        *,
        api_key: str | None = None,
        base_url: str = "https://aws-us-east-1.hevlayer.com",
        http_client: httpx.AsyncClient | None = None,
        timeout: float | httpx.Timeout | None = 30.0,
    ) -> None:
        headers: dict[str, str] = {}
        token = self._clean_token(api_key)
        if token:
            headers["Authorization"] = f"Bearer {token}"

        self._owns_client = http_client is None
        if http_client is None:
            self._client = httpx.AsyncClient(base_url=base_url, headers=headers, timeout=timeout)
        else:
            self._client = http_client
            if headers:
                self._client.headers.update(headers)

    async def __aenter__(self) -> "AsyncHevlayer":
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def authenticate_key(self, body: AuthenticateKeyRequest | dict[str, Any], *, with_perf: bool = False) -> AuthenticateKeyResponse | LayerResponse[AuthenticateKeyResponse]:
        return await self._request_json(
            "POST",
            f"/v2/keys/authenticate",
            json=body, result_type=AuthenticateKeyResponse,
            with_perf=with_perf,
        )


    async def batch_query_namespace(self, namespace: str, body: BatchQueryRequest | dict[str, Any], *, with_perf: bool = False) -> BatchQueryResponse | LayerResponse[BatchQueryResponse]:
        return await self._request_json(
            "POST",
            f"/v2/namespaces/{namespace}/query",
            params={"stainless_overload": "multiQuery"}, json=body, result_type=BatchQueryResponse,
            with_perf=with_perf,
        )


    async def branch_namespace(self, namespace: str, body: TurbopufferBranchFromRequest | dict[str, Any], *, with_perf: bool = False) -> TurbopufferWriteResponse | LayerResponse[TurbopufferWriteResponse]:
        return await self._request_json(
            "POST",
            f"/v2/namespaces/{namespace}",
            params={"stainless_overload": "branchFrom"}, json=body, result_type=TurbopufferWriteResponse,
            with_perf=with_perf,
        )


    async def claim_documents(self, pipeline_id: str, body: ClaimDocumentsRequest | dict[str, Any], *, with_perf: bool = False) -> ClaimDocumentsResponse | LayerResponse[ClaimDocumentsResponse]:
        return await self._request_json(
            "POST",
            f"/v2/pipelines/{pipeline_id}/claim",
            json=body, result_type=ClaimDocumentsResponse,
            with_perf=with_perf,
        )


    async def claim_udf_items(self, udf_id: str, body: UdfClaimRequest | dict[str, Any], *, with_perf: bool = False) -> UdfClaimResponse | LayerResponse[UdfClaimResponse]:
        return await self._request_json(
            "POST",
            f"/v2/udfs/{udf_id}/claim",
            json=body, result_type=UdfClaimResponse,
            with_perf=with_perf,
        )


    async def complete_udf_items(self, udf_id: str, body: UdfCompleteRequest | dict[str, Any], *, with_perf: bool = False) -> UdfItemsResponse | LayerResponse[UdfItemsResponse]:
        return await self._request_json(
            "POST",
            f"/v2/udfs/{udf_id}/items/complete",
            json=body, result_type=UdfItemsResponse,
            with_perf=with_perf,
        )


    async def copy_namespace(self, namespace: str, body: TurbopufferCopyFromRequest | dict[str, Any], *, with_perf: bool = False) -> TurbopufferWriteResponse | LayerResponse[TurbopufferWriteResponse]:
        return await self._request_json(
            "POST",
            f"/v2/namespaces/{namespace}",
            params={"stainless_overload": "copyFrom"}, json=body, result_type=TurbopufferWriteResponse,
            with_perf=with_perf,
        )


    async def create_checkpoint(self, namespace: str, body: CreateCheckpointRequest | dict[str, Any], *, with_perf: bool = False) -> Checkpoint | LayerResponse[Checkpoint]:
        return await self._request_json(
            "POST",
            f"/v2/namespaces/{namespace}/checkpoints",
            json=body, result_type=Checkpoint,
            with_perf=with_perf,
        )


    async def create_pipeline(self, body: CreatePipelineRequest | dict[str, Any], *, with_perf: bool = False) -> Pipeline | LayerResponse[Pipeline]:
        return await self._request_json(
            "POST",
            f"/v2/pipelines",
            json=body, result_type=Pipeline,
            with_perf=with_perf,
        )


    async def create_scan(self, namespace: str, body: CreateScanRequest | dict[str, Any], *, with_perf: bool = False) -> ScanCountResponse | ScanJob | LayerResponse[ScanCountResponse | ScanJob]:
        return await self._request_json(
            "POST",
            f"/v2/namespaces/{namespace}/scans",
            json=body, result_type=(ScanCountResponse, ScanJob),
            with_perf=with_perf,
        )


    async def create_snapshot(self, namespace: str, body: CreateSnapshotRequest | dict[str, Any], *, with_perf: bool = False) -> SnapshotJob | LayerResponse[SnapshotJob]:
        return await self._request_json(
            "POST",
            f"/v2/namespaces/{namespace}/snapshots",
            json=body, result_type=SnapshotJob,
            with_perf=with_perf,
        )


    async def create_udf(self, body: CreateUdfRequest | dict[str, Any], *, with_perf: bool = False) -> Udf | LayerResponse[Udf]:
        return await self._request_json(
            "POST",
            f"/v2/udfs",
            json=body, result_type=Udf,
            with_perf=with_perf,
        )


    async def delete_key(self, keyId: str, *, with_perf: bool = False) -> StatusResponse | LayerResponse[StatusResponse]:
        return await self._request_json(
            "DELETE",
            f"/v2/keys/{keyId}",
            result_type=StatusResponse,
            with_perf=with_perf,
        )


    async def delete_namespace(self, namespace: str, *, with_perf: bool = False) -> StatusResponse | LayerResponse[StatusResponse]:
        return await self._request_json(
            "DELETE",
            f"/v2/namespaces/{namespace}",
            result_type=StatusResponse,
            with_perf=with_perf,
        )


    async def delete_pipeline(self, pipeline_id: str, *, with_perf: bool = False) -> StatusResponse | LayerResponse[StatusResponse]:
        return await self._request_json(
            "DELETE",
            f"/v2/pipelines/{pipeline_id}",
            result_type=StatusResponse,
            with_perf=with_perf,
        )


    async def delete_scan(self, namespace: str, scan_id: str, *, with_perf: bool = False) -> StatusResponse | LayerResponse[StatusResponse]:
        return await self._request_json(
            "DELETE",
            f"/v2/namespaces/{namespace}/scans/{scan_id}",
            result_type=StatusResponse,
            with_perf=with_perf,
        )


    async def delete_udf(self, udf_id: str, *, with_perf: bool = False) -> StatusResponse | LayerResponse[StatusResponse]:
        return await self._request_json(
            "DELETE",
            f"/v2/udfs/{udf_id}",
            result_type=StatusResponse,
            with_perf=with_perf,
        )


    async def discover_udf(self, udf_id: str, body: UdfDiscoverRequest | dict[str, Any], *, with_perf: bool = False) -> UdfDiscoverResponse | LayerResponse[UdfDiscoverResponse]:
        return await self._request_json(
            "POST",
            f"/v2/udfs/{udf_id}/discover",
            json=body, result_type=UdfDiscoverResponse,
            with_perf=with_perf,
        )


    async def evaluate_turbopuffer_recall(self, namespace: str, body: TurbopufferRecallRequest | dict[str, Any], *, with_perf: bool = False) -> TurbopufferRecallResponse | LayerResponse[TurbopufferRecallResponse]:
        return await self._request_json(
            "POST",
            f"/v1/namespaces/{namespace}/_debug/recall",
            json=body, result_type=TurbopufferRecallResponse,
            with_perf=with_perf,
        )


    async def explain_turbopuffer_query(self, namespace: str, body: TurbopufferQueryRequest | dict[str, Any], *, with_perf: bool = False) -> TurbopufferExplainQueryResponse | LayerResponse[TurbopufferExplainQueryResponse]:
        return await self._request_json(
            "POST",
            f"/v2/namespaces/{namespace}/explain_query",
            json=body, result_type=TurbopufferExplainQueryResponse,
            with_perf=with_perf,
        )


    async def fail_udf_items(self, udf_id: str, body: UdfFailRequest | dict[str, Any], *, with_perf: bool = False) -> UdfItemsResponse | LayerResponse[UdfItemsResponse]:
        return await self._request_json(
            "POST",
            f"/v2/udfs/{udf_id}/items/fail",
            json=body, result_type=UdfItemsResponse,
            with_perf=with_perf,
        )


    async def fetch_document(self, namespace: str, doc_id: str, *, include_attributes: list[str] | None = None, with_perf: bool = False) -> Document | LayerResponse[Document]:
        return await self._request_json(
            "GET",
            f"/v2/namespaces/{namespace}/documents/{doc_id}",
            params={"include_attributes": include_attributes}, result_type=Document,
            with_perf=with_perf,
        )


    async def fetch_documents(self, namespace: str, body: FetchDocumentsRequest | dict[str, Any], *, with_perf: bool = False) -> FetchDocumentsResponse | LayerResponse[FetchDocumentsResponse]:
        return await self._request_json(
            "POST",
            f"/v2/namespaces/{namespace}/documents",
            json=body, result_type=FetchDocumentsResponse,
            with_perf=with_perf,
        )


    async def get_blob(self, namespace: str, sha256: str, *, with_perf: bool = False) -> bytes | LayerResponse[bytes]:
        return await self._request_bytes(
            "GET",
            f"/v1/namespaces/{namespace}/blobs/{sha256}",
            with_perf=with_perf,
        )


    async def get_checkpoint(self, namespace: str, label: str, *, with_perf: bool = False) -> Checkpoint | LayerResponse[Checkpoint]:
        return await self._request_json(
            "GET",
            f"/v2/namespaces/{namespace}/checkpoints/{label}",
            result_type=Checkpoint,
            with_perf=with_perf,
        )


    async def get_cost_rate_card(self, *, with_perf: bool = False) -> RateCard | LayerResponse[RateCard]:
        return await self._request_json(
            "GET",
            f"/v2/cost/rate-card",
            result_type=RateCard,
            with_perf=with_perf,
        )


    async def get_cost_snapshot(self, *, window: CostWindow | None = None, with_perf: bool = False) -> CostSnapshot | LayerResponse[CostSnapshot]:
        return await self._request_json(
            "GET",
            f"/v2/cost",
            params={"window": window}, result_type=CostSnapshot,
            with_perf=with_perf,
        )


    async def get_cost_timeseries(self, *, window: CostWindow | None = None, step: CostStep | None = None, with_perf: bool = False) -> CostTimeseries | LayerResponse[CostTimeseries]:
        return await self._request_json(
            "GET",
            f"/v2/cost/timeseries",
            params={"window": window, "step": step}, result_type=CostTimeseries,
            with_perf=with_perf,
        )


    async def get_key(self, keyId: str, *, with_perf: bool = False) -> ApiKey | LayerResponse[ApiKey]:
        return await self._request_json(
            "GET",
            f"/v2/keys/{keyId}",
            result_type=ApiKey,
            with_perf=with_perf,
        )


    async def get_license(self, *, with_perf: bool = False) -> LicenseState | LayerResponse[LicenseState]:
        return await self._request_json(
            "GET",
            f"/v2/license",
            result_type=LicenseState,
            with_perf=with_perf,
        )


    async def get_metric_catalog_entry(self, name: str, *, with_perf: bool = False) -> MetricCatalogEntry | LayerResponse[MetricCatalogEntry]:
        return await self._request_json(
            "GET",
            f"/v2/metrics/catalog/{name}",
            result_type=MetricCatalogEntry,
            with_perf=with_perf,
        )


    async def get_namespace_metadata(self, namespace: str, *, with_perf: bool = False) -> NamespaceMetadata | LayerResponse[NamespaceMetadata]:
        return await self._request_json(
            "GET",
            f"/v2/namespaces/{namespace}/metadata",
            result_type=NamespaceMetadata,
            with_perf=with_perf,
        )


    async def get_namespace_snapshot(self, namespace: str, sha: str, *, with_perf: bool = False) -> SnapshotBody | LayerResponse[SnapshotBody]:
        return await self._request_json(
            "GET",
            f"/v2/namespaces/{namespace}/snapshots/{sha}",
            result_type=SnapshotBody,
            with_perf=with_perf,
        )


    async def get_pipeline_document_chunks(self, pipeline_id: str, doc_id: str, *, with_perf: bool = False) -> GetChunksResponse | LayerResponse[GetChunksResponse]:
        return await self._request_json(
            "GET",
            f"/v2/pipelines/{pipeline_id}/documents/{doc_id}/chunks",
            result_type=GetChunksResponse,
            with_perf=with_perf,
        )


    async def get_pipeline_status(self, pipeline_id: str, *, with_perf: bool = False) -> PipelineStatus | LayerResponse[PipelineStatus]:
        return await self._request_json(
            "GET",
            f"/v2/pipelines/{pipeline_id}/status",
            result_type=PipelineStatus,
            with_perf=with_perf,
        )


    async def get_scan(self, namespace: str, scan_id: str, *, with_perf: bool = False) -> ScanJob | LayerResponse[ScanJob]:
        return await self._request_json(
            "GET",
            f"/v2/namespaces/{namespace}/scans/{scan_id}",
            result_type=ScanJob,
            with_perf=with_perf,
        )


    async def get_scan_results(self, namespace: str, scan_id: str, *, limit: int | None = None, offset: int | None = None, with_perf: bool = False) -> ScanIdsResponse | ScanValuesResponse | LayerResponse[ScanIdsResponse | ScanValuesResponse]:
        return await self._request_json(
            "GET",
            f"/v2/namespaces/{namespace}/scans/{scan_id}/results",
            params={"limit": limit, "offset": offset}, result_type=(ScanIdsResponse, ScanValuesResponse),
            with_perf=with_perf,
        )


    async def get_snapshot_job(self, namespace: str, job_id: str, *, with_perf: bool = False) -> SnapshotJob | LayerResponse[SnapshotJob]:
        return await self._request_json(
            "GET",
            f"/v2/namespaces/{namespace}/snapshot-jobs/{job_id}",
            result_type=SnapshotJob,
            with_perf=with_perf,
        )


    async def get_snapshot_policy(self, namespace: str, *, with_perf: bool = False) -> SnapshotPolicy | LayerResponse[SnapshotPolicy]:
        return await self._request_json(
            "GET",
            f"/v2/namespaces/{namespace}/snapshot-policy",
            result_type=SnapshotPolicy,
            with_perf=with_perf,
        )


    async def get_turbopuffer_namespace_schema(self, namespace: str, *, with_perf: bool = False) -> TurbopufferSchema | LayerResponse[TurbopufferSchema]:
        return await self._request_json(
            "GET",
            f"/v1/namespaces/{namespace}/schema",
            result_type=TurbopufferSchema,
            with_perf=with_perf,
        )


    async def get_turbopuffer_v1_namespace_metadata(self, namespace: str, *, with_perf: bool = False) -> NamespaceMetadata | LayerResponse[NamespaceMetadata]:
        return await self._request_json(
            "GET",
            f"/v1/namespaces/{namespace}/metadata",
            result_type=NamespaceMetadata,
            with_perf=with_perf,
        )


    async def get_udf(self, udf_id: str, *, with_perf: bool = False) -> GetUdfResponse | LayerResponse[GetUdfResponse]:
        return await self._request_json(
            "GET",
            f"/v2/udfs/{udf_id}",
            result_type=GetUdfResponse,
            with_perf=with_perf,
        )


    async def get_udf_status(self, udf_id: str, *, with_perf: bool = False) -> UdfStatus | LayerResponse[UdfStatus]:
        return await self._request_json(
            "GET",
            f"/v2/udfs/{udf_id}/status",
            result_type=UdfStatus,
            with_perf=with_perf,
        )


    async def get_vectorstore(self, name: str, *, with_perf: bool = False) -> VectorStore | LayerResponse[VectorStore]:
        return await self._request_json(
            "GET",
            f"/v2/vectorstores/{name}",
            result_type=VectorStore,
            with_perf=with_perf,
        )


    async def get_warehouse(self, name: str, *, with_perf: bool = False) -> Warehouse | LayerResponse[Warehouse]:
        return await self._request_json(
            "GET",
            f"/v2/warehouses/{name}",
            result_type=Warehouse,
            with_perf=with_perf,
        )


    async def get_warm_job(self, namespace: str, job_id: str, *, with_perf: bool = False) -> WarmJob | LayerResponse[WarmJob]:
        return await self._request_json(
            "GET",
            f"/v2/namespaces/{namespace}/warm-jobs/{job_id}",
            result_type=WarmJob,
            with_perf=with_perf,
        )


    async def heartbeat_documents(self, pipeline_id: str, body: HeartbeatDocumentsRequest | dict[str, Any], *, with_perf: bool = False) -> DocumentsStageResponse | LayerResponse[DocumentsStageResponse]:
        return await self._request_json(
            "POST",
            f"/v2/pipelines/{pipeline_id}/documents/heartbeat",
            json=body, result_type=DocumentsStageResponse,
            with_perf=with_perf,
        )


    async def heartbeat_udf_items(self, udf_id: str, body: UdfHeartbeatRequest | dict[str, Any], *, with_perf: bool = False) -> UdfItemsResponse | LayerResponse[UdfItemsResponse]:
        return await self._request_json(
            "POST",
            f"/v2/udfs/{udf_id}/items/heartbeat",
            json=body, result_type=UdfItemsResponse,
            with_perf=with_perf,
        )


    async def hint_cache_warm(self, namespace: str, *, turbopuffer: bool | None = None, documents: bool | None = None, snapshots: bool | None = None, blobs: bool | None = None, blob_budget_bytes: int | None = None, page_size: int | None = None, with_perf: bool = False) -> HintCacheWarmResponse | LayerResponse[HintCacheWarmResponse]:
        return await self._request_json(
            "GET",
            f"/v1/namespaces/{namespace}/hint_cache_warm",
            params={"turbopuffer": turbopuffer, "documents": documents, "snapshots": snapshots, "blobs": blobs, "blob_budget_bytes": blob_budget_bytes, "page_size": page_size}, result_type=HintCacheWarmResponse,
            with_perf=with_perf,
        )


    async def import_namespace(self, namespace: str, body: bytes, *, with_perf: bool = False) -> dict[str, Any] | LayerResponse[dict[str, Any]]:
        return await self._request_json(
            "POST",
            f"/v2/namespaces/{namespace}/import",
            content=body, content_type="application/vnd.apache.arrow.stream", result_type=None,
            with_perf=with_perf,
        )


    async def init_namespace(self, namespace: str, body: InitNamespaceRequest | dict[str, Any], *, with_perf: bool = False) -> InitNamespaceResponse | LayerResponse[InitNamespaceResponse]:
        return await self._request_json(
            "POST",
            f"/v2/namespaces/{namespace}/init",
            json=body, result_type=InitNamespaceResponse,
            with_perf=with_perf,
        )


    async def list_checkpoints(self, namespace: str, *, limit: int | None = None, before: str | None = None, with_perf: bool = False) -> CheckpointList | LayerResponse[CheckpointList]:
        return await self._request_json(
            "GET",
            f"/v2/namespaces/{namespace}/checkpoints",
            params={"limit": limit, "before": before}, result_type=CheckpointList,
            with_perf=with_perf,
        )


    async def list_clickstream(self, namespace: str, *, trace_id: str | None = None, tags: list[str] | None = None, from_: str | None = None, to: str | None = None, before: str | None = None, limit: int | None = None, with_perf: bool = False) -> ClickstreamListResponse | LayerResponse[ClickstreamListResponse]:
        return await self._request_json(
            "GET",
            f"/v2/namespaces/{namespace}/clickstream",
            params={"trace_id": trace_id, "tag": tags, "from": from_, "to": to, "before": before, "limit": limit}, result_type=ClickstreamListResponse,
            with_perf=with_perf,
        )


    async def list_keys(self, *, includeRevoked: bool | None = None, with_perf: bool = False) -> ApiKeyList | LayerResponse[ApiKeyList]:
        return await self._request_json(
            "GET",
            f"/v2/keys",
            params={"includeRevoked": includeRevoked}, result_type=ApiKeyList,
            with_perf=with_perf,
        )


    async def list_metrics_catalog(self, *, family: MetricFamily | None = None, with_perf: bool = False) -> MetricCatalog | LayerResponse[MetricCatalog]:
        return await self._request_json(
            "GET",
            f"/v2/metrics/catalog",
            params={"family": family}, result_type=MetricCatalog,
            with_perf=with_perf,
        )


    async def list_namespace_history(self, namespace: str, *, limit: int | None = None, before: str | None = None, with_perf: bool = False) -> list[SnapshotHistoryEntry] | LayerResponse[list[SnapshotHistoryEntry]]:
        return await self._request_json(
            "GET",
            f"/v2/namespaces/{namespace}/history",
            params={"limit": limit, "before": before}, result_type=list[SnapshotHistoryEntry],
            with_perf=with_perf,
        )


    async def list_namespaces(self, *, prefix: str | None = None, cursor: str | None = None, page_size: int | None = None, with_perf: bool = False) -> NamespaceList | LayerResponse[NamespaceList]:
        return await self._request_json(
            "GET",
            f"/v2/namespaces",
            params={"prefix": prefix, "cursor": cursor, "page_size": page_size}, result_type=NamespaceList,
            with_perf=with_perf,
        )


    async def list_pipelines(self, *, with_perf: bool = False) -> PipelineList | LayerResponse[PipelineList]:
        return await self._request_json(
            "GET",
            f"/v2/pipelines",
            result_type=PipelineList,
            with_perf=with_perf,
        )


    async def list_scans(self, namespace: str, *, with_perf: bool = False) -> ScanJobList | LayerResponse[ScanJobList]:
        return await self._request_json(
            "GET",
            f"/v2/namespaces/{namespace}/scans",
            result_type=ScanJobList,
            with_perf=with_perf,
        )


    async def list_search_history(self, namespace: str, *, tags: list[str] | None = None, from_: str | None = None, to: str | None = None, before: str | None = None, limit: int | None = None, with_perf: bool = False) -> SearchHistoryListResponse | LayerResponse[SearchHistoryListResponse]:
        return await self._request_json(
            "GET",
            f"/v2/namespaces/{namespace}/search-history",
            params={"tag": tags, "from": from_, "to": to, "before": before, "limit": limit}, result_type=SearchHistoryListResponse,
            with_perf=with_perf,
        )


    async def list_snapshot_activity(self, *, since: int | None = None, limit: int | None = None, namespace: str | None = None, cursor: str | None = None, with_perf: bool = False) -> SnapshotActivityList | LayerResponse[SnapshotActivityList]:
        return await self._request_json(
            "GET",
            f"/v2/activity/snapshots",
            params={"since": since, "limit": limit, "namespace": namespace, "cursor": cursor}, result_type=SnapshotActivityList,
            with_perf=with_perf,
        )


    async def list_snapshot_jobs(self, namespace: str, *, with_perf: bool = False) -> SnapshotJobList | LayerResponse[SnapshotJobList]:
        return await self._request_json(
            "GET",
            f"/v2/namespaces/{namespace}/snapshot-jobs",
            result_type=SnapshotJobList,
            with_perf=with_perf,
        )


    async def list_turbopuffer_namespaces(self, *, cursor: str | None = None, prefix: str | None = None, page_size: int | None = None, with_perf: bool = False) -> TurbopufferNamespaceList | LayerResponse[TurbopufferNamespaceList]:
        return await self._request_json(
            "GET",
            f"/v1/namespaces",
            params={"cursor": cursor, "prefix": prefix, "page_size": page_size}, result_type=TurbopufferNamespaceList,
            with_perf=with_perf,
        )


    async def list_udfs(self, *, with_perf: bool = False) -> UdfList | LayerResponse[UdfList]:
        return await self._request_json(
            "GET",
            f"/v2/udfs",
            result_type=UdfList,
            with_perf=with_perf,
        )


    async def list_vectorstores(self, *, with_perf: bool = False) -> VectorStoreList | LayerResponse[VectorStoreList]:
        return await self._request_json(
            "GET",
            f"/v2/vectorstores",
            result_type=VectorStoreList,
            with_perf=with_perf,
        )


    async def list_warehouses(self, *, with_perf: bool = False) -> WarehouseList | LayerResponse[WarehouseList]:
        return await self._request_json(
            "GET",
            f"/v2/warehouses",
            result_type=WarehouseList,
            with_perf=with_perf,
        )


    async def list_warm_jobs(self, namespace: str, *, with_perf: bool = False) -> WarmJobList | LayerResponse[WarmJobList]:
        return await self._request_json(
            "GET",
            f"/v2/namespaces/{namespace}/warm-jobs",
            result_type=WarmJobList,
            with_perf=with_perf,
        )


    async def mint_key(self, body: MintKeyRequest | dict[str, Any], *, with_perf: bool = False) -> MintKeyResponse | LayerResponse[MintKeyResponse]:
        return await self._request_json(
            "POST",
            f"/v2/keys",
            json=body, result_type=MintKeyResponse,
            with_perf=with_perf,
        )


    async def pause_udf(self, udf_id: str, *, with_perf: bool = False) -> Udf | LayerResponse[Udf]:
        return await self._request_json(
            "POST",
            f"/v2/udfs/{udf_id}/pause",
            result_type=Udf,
            with_perf=with_perf,
        )


    async def put_blob(self, namespace: str, body: bytes, *, warm: bool | None = None, with_perf: bool = False) -> BlobPutResponse | LayerResponse[BlobPutResponse]:
        return await self._request_json(
            "PUT",
            f"/v1/namespaces/{namespace}/blobs",
            params={"warm": warm}, content=body, content_type="application/octet-stream", result_type=BlobPutResponse,
            with_perf=with_perf,
        )


    async def put_pipeline_document_chunks(self, pipeline_id: str, doc_id: str, body: PutChunksRequest | dict[str, Any], *, with_perf: bool = False) -> StageDocumentResponse | LayerResponse[StageDocumentResponse]:
        return await self._request_json(
            "PUT",
            f"/v2/pipelines/{pipeline_id}/documents/{doc_id}",
            json=body, result_type=StageDocumentResponse,
            with_perf=with_perf,
        )


    async def put_pipeline_document_vectors(self, pipeline_id: str, doc_id: str, body: PutVectorsRequest | dict[str, Any], *, with_perf: bool = False) -> StatusResponse | LayerResponse[StatusResponse]:
        return await self._request_json(
            "PUT",
            f"/v2/pipelines/{pipeline_id}/documents/{doc_id}/vectors",
            json=body, result_type=StatusResponse,
            with_perf=with_perf,
        )


    async def put_snapshot_policy(self, namespace: str, body: SnapshotPolicy | dict[str, Any], *, with_perf: bool = False) -> SnapshotPolicy | LayerResponse[SnapshotPolicy]:
        return await self._request_json(
            "PUT",
            f"/v2/namespaces/{namespace}/snapshot-policy",
            json=body, result_type=SnapshotPolicy,
            with_perf=with_perf,
        )


    async def query(self, body: FederatedQueryRequest | dict[str, Any], *, with_perf: bool = False) -> FederatedQueryResponse | LayerResponse[FederatedQueryResponse]:
        return await self._request_json(
            "POST",
            f"/v2/query",
            json=body, result_type=FederatedQueryResponse,
            with_perf=with_perf,
        )


    async def query_agent(self, name: str, body: AgentQueryRequest | dict[str, Any], *, with_perf: bool = False) -> AgentQueryResponse | LayerResponse[AgentQueryResponse]:
        return await self._request_json(
            "POST",
            f"/v2/agents/{name}/query",
            json=body, result_type=AgentQueryResponse,
            with_perf=with_perf,
        )


    async def query_metrics(self, *, query: str | None = None, time: str | None = None, timeout: str | None = None, with_perf: bool = False) -> PrometheusResponse | LayerResponse[PrometheusResponse]:
        return await self._request_json(
            "GET",
            f"/v2/metrics/query",
            params={"query": query, "time": time, "timeout": timeout}, result_type=PrometheusResponse,
            with_perf=with_perf,
        )


    async def query_metrics_api_v1(self, *, query: str | None = None, time: str | None = None, timeout: str | None = None, with_perf: bool = False) -> PrometheusResponse | LayerResponse[PrometheusResponse]:
        return await self._request_json(
            "GET",
            f"/v2/metrics/api/v1/query",
            params={"query": query, "time": time, "timeout": timeout}, result_type=PrometheusResponse,
            with_perf=with_perf,
        )


    async def query_metrics_range(self, *, query: str | None = None, start: str | None = None, end: str | None = None, step: str | None = None, timeout: str | None = None, with_perf: bool = False) -> PrometheusResponse | LayerResponse[PrometheusResponse]:
        return await self._request_json(
            "GET",
            f"/v2/metrics/query_range",
            params={"query": query, "start": start, "end": end, "step": step, "timeout": timeout}, result_type=PrometheusResponse,
            with_perf=with_perf,
        )


    async def query_metrics_range_api_v1(self, *, query: str | None = None, start: str | None = None, end: str | None = None, step: str | None = None, timeout: str | None = None, with_perf: bool = False) -> PrometheusResponse | LayerResponse[PrometheusResponse]:
        return await self._request_json(
            "GET",
            f"/v2/metrics/api/v1/query_range",
            params={"query": query, "start": start, "end": end, "step": step, "timeout": timeout}, result_type=PrometheusResponse,
            with_perf=with_perf,
        )


    async def query_namespace(self, namespace: str, body: QueryRequest | dict[str, Any], *, raw_query: str | None = None, tags: list[str] | None = None, with_perf: bool = False) -> QueryResponse | LayerResponse[QueryResponse]:
        return await self._request_json(
            "POST",
            f"/v2/namespaces/{namespace}/query",
            json=body, headers=self._search_history_headers(raw_query, tags), result_type=QueryResponse,
            with_perf=with_perf,
        )


    async def query_turbopuffer_namespace(self, namespace: str, body: TurbopufferQueryRequest | dict[str, Any], *, with_perf: bool = False) -> TurbopufferQueryResponse | LayerResponse[TurbopufferQueryResponse]:
        return await self._request_json(
            "POST",
            f"/v1/namespaces/{namespace}/query",
            json=body, result_type=TurbopufferQueryResponse,
            with_perf=with_perf,
        )


    async def reset_failed_udf(self, udf_id: str, *, with_perf: bool = False) -> UdfItemsResponse | LayerResponse[UdfItemsResponse]:
        return await self._request_json(
            "POST",
            f"/v2/udfs/{udf_id}/reset-failed",
            result_type=UdfItemsResponse,
            with_perf=with_perf,
        )


    async def resume_udf(self, udf_id: str, *, with_perf: bool = False) -> Udf | LayerResponse[Udf]:
        return await self._request_json(
            "POST",
            f"/v2/udfs/{udf_id}/resume",
            result_type=Udf,
            with_perf=with_perf,
        )


    async def revoke_key(self, keyId: str, *, with_perf: bool = False) -> ApiKey | LayerResponse[ApiKey]:
        return await self._request_json(
            "POST",
            f"/v2/keys/{keyId}/revoke",
            result_type=ApiKey,
            with_perf=with_perf,
        )


    async def set_documents_stage(self, pipeline_id: str, body: SetDocumentsStageRequest | dict[str, Any], *, with_perf: bool = False) -> DocumentsStageResponse | LayerResponse[DocumentsStageResponse]:
        return await self._request_json(
            "POST",
            f"/v2/pipelines/{pipeline_id}/documents/stage",
            json=body, result_type=DocumentsStageResponse,
            with_perf=with_perf,
        )


    async def update_turbopuffer_namespace_metadata(self, namespace: str, body: TurbopufferMetadataPatch | dict[str, Any], *, with_perf: bool = False) -> NamespaceMetadata | LayerResponse[NamespaceMetadata]:
        return await self._request_json(
            "PATCH",
            f"/v1/namespaces/{namespace}/metadata",
            json=body, result_type=NamespaceMetadata,
            with_perf=with_perf,
        )


    async def update_turbopuffer_namespace_schema(self, namespace: str, body: TurbopufferSchema | dict[str, Any], *, with_perf: bool = False) -> TurbopufferSchema | LayerResponse[TurbopufferSchema]:
        return await self._request_json(
            "POST",
            f"/v1/namespaces/{namespace}/schema",
            json=body, result_type=TurbopufferSchema,
            with_perf=with_perf,
        )


    async def upsert_udf(self, udf_id: str, body: UpdateUdfRequest | dict[str, Any], *, with_perf: bool = False) -> Udf | LayerResponse[Udf]:
        return await self._request_json(
            "PUT",
            f"/v2/udfs/{udf_id}",
            json=body, result_type=Udf,
            with_perf=with_perf,
        )


    async def warm_cache(self, namespace: str, *, page_size: int | None = None, with_perf: bool = False) -> WarmJob | LayerResponse[WarmJob]:
        return await self._request_json(
            "POST",
            f"/v2/namespaces/{namespace}/warm",
            params={"page_size": page_size}, result_type=WarmJob,
            with_perf=with_perf,
        )


    async def write_namespace(self, namespace: str, body: TurbopufferWriteRequest | dict[str, Any], *, with_perf: bool = False) -> TurbopufferWriteResponse | LayerResponse[TurbopufferWriteResponse]:
        return await self._request_json(
            "POST",
            f"/v2/namespaces/{namespace}",
            json=body, result_type=TurbopufferWriteResponse,
            with_perf=with_perf,
        )


    async def ensure_pipeline(self, body: CreatePipelineRequest | dict[str, Any]) -> Pipeline:
        try:
            return await self.create_pipeline(body)
        except HevlayerError as exc:
            if exc.status_code != 409:
                raise
            pipeline_id = body.id if isinstance(body, CreatePipelineRequest) else body["id"]
            pipelines = await self.list_pipelines()
            for pipeline in pipelines.pipelines:
                if pipeline.id == str(pipeline_id):
                    return pipeline
            raise

    async def release_documents(
        self,
        pipeline_id: str,
        document_ids: list[str],
        *,
        from_stage: str | None = None,
        worker_id: str | None = None,
        with_perf: bool = False,
    ) -> DocumentsStageResponse | LayerResponse[DocumentsStageResponse]:
        return await self.set_documents_stage(
            pipeline_id,
            SetDocumentsStageRequest(
                document_ids=document_ids,
                stage="pending",
                from_stage=from_stage,
                worker_id=worker_id,
            ),
            with_perf=with_perf,
        )

    async def fail_documents(
        self,
        pipeline_id: str,
        document_ids: list[str],
        *,
        from_stage: str | None = None,
        worker_id: str | None = None,
        with_perf: bool = False,
    ) -> DocumentsStageResponse | LayerResponse[DocumentsStageResponse]:
        return await self.set_documents_stage(
            pipeline_id,
            SetDocumentsStageRequest(
                document_ids=document_ids,
                stage="failed",
                from_stage=from_stage,
                worker_id=worker_id,
            ),
            with_perf=with_perf,
        )

    async def complete_documents(
        self,
        pipeline_id: str,
        document_ids: list[str],
        *,
        from_stage: str | None = None,
        worker_id: str | None = None,
        with_perf: bool = False,
    ) -> DocumentsStageResponse | LayerResponse[DocumentsStageResponse]:
        return await self.set_documents_stage(
            pipeline_id,
            SetDocumentsStageRequest(
                document_ids=document_ids,
                stage="indexed",
                from_stage=from_stage,
                worker_id=worker_id,
            ),
            with_perf=with_perf,
        )

    async def write_single_vector(
        self,
        pipeline_id: str,
        doc_id: str,
        vector: VectorEntry | dict[str, Any],
        *,
        with_perf: bool = False,
    ) -> StatusResponse | LayerResponse[StatusResponse]:
        entry = vector if isinstance(vector, VectorEntry) else VectorEntry(**vector)
        return await self.put_pipeline_document_vectors(
            pipeline_id,
            doc_id,
            PutVectorsRequest(vectors=[entry]),
            with_perf=with_perf,
        )

    async def write_single_multivector(
        self,
        pipeline_id: str,
        doc_id: str,
        id: str,
        vectors: list[list[float]],
        *,
        attributes: dict[str, Any] | None = None,
        with_perf: bool = False,
    ) -> StatusResponse | LayerResponse[StatusResponse]:
        return await self.put_pipeline_document_vectors(
            pipeline_id,
            doc_id,
            PutVectorsRequest(vectors=[VectorEntry(id=id, vectors=vectors, attributes=attributes)]),
            with_perf=with_perf,
        )

    async def wait_for_scan(
        self,
        namespace: str,
        scan_id: str,
        *,
        initial_delay: float = 0.05,
        max_delay: float = 2.0,
        timeout: float | None = None,
    ) -> ScanJob:
        started = time.perf_counter()
        delay = initial_delay
        while True:
            scan = await self.get_scan(namespace, scan_id)
            status = scan.get("status") if isinstance(scan, dict) else scan.status
            if status in {"completed", "failed"}:
                return scan
            if timeout is not None and time.perf_counter() - started >= timeout:
                raise TimeoutError(f"scan {scan_id!r} did not finish within {timeout} seconds")
            await asyncio.sleep(delay)
            delay = min(delay * 2, max_delay)

    async def scan(
        self,
        namespace: str,
        body: CreateScanRequest | dict[str, Any],
        *,
        initial_delay: float = 0.05,
        max_delay: float = 2.0,
        timeout: float | None = None,
    ) -> ScanJob:
        created = await self.create_scan(namespace, body)
        scan_id = created.get("id") if isinstance(created, dict) else created.id
        return await self.wait_for_scan(
            namespace,
            scan_id,
            initial_delay=initial_delay,
            max_delay=max_delay,
            timeout=timeout,
        )

    async def warm_namespace(
        self,
        namespace: str,
        *,
        page_size: int | None = None,
        with_perf: bool = False,
    ) -> WarmJob | LayerResponse[WarmJob]:
        return await self.warm_cache(namespace, page_size=page_size, with_perf=with_perf)

    async def patch_columns(
        self,
        namespace: str,
        ids: list[str],
        attrs: dict[str, list[Any]],
        *,
        with_perf: bool = False,
    ) -> TurbopufferWriteResponse | LayerResponse[TurbopufferWriteResponse]:
        if not ids:
            raise ValueError("patch_columns requires at least one id")
        columns: dict[str, list[Any]] = {"id": list(ids)}
        for name, values in attrs.items():
            if name == "id":
                raise ValueError("patch_columns attrs must not include id")
            values_list = list(values)
            if len(values_list) != len(ids):
                raise ValueError(
                    f"patch_columns attr {name!r} has {len(values_list)} values for {len(ids)} ids"
                )
            columns[name] = values_list
        return await self.write_namespace(
            namespace,
            {"patch_columns": columns},
            with_perf=with_perf,
        )

    async def _request_json(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any = None,
        content: bytes | None = None,
        content_type: str | None = None,
        headers: dict[str, str] | None = None,
        result_type: Any = None,
        with_perf: bool = False,
    ) -> Any:
        started = time.perf_counter()
        request_body = self._json_body(json)
        clean_params = self._clean_params(params)
        request_headers = dict(headers or {})
        if content is not None and content_type:
            request_headers["content-type"] = content_type
        response = await self._client.request(
            method,
            path,
            params=clean_params,
            json=None if content is not None else request_body,
            content=content,
            headers=request_headers or None,
        )
        latency_ms = (time.perf_counter() - started) * 1000
        cache_status = response.headers.get("x-layer-cache")

        if response.is_error:
            error, message = self._error_payload(response)
            raise HevlayerError(response.status_code, message, error=error, response=response)

        raw = None if response.status_code == 204 or not response.content else response.json()
        data = self._parse_data(raw, result_type)
        self._apply_layer_headers(data, response.headers)
        if with_perf:
            return LayerResponse(data=data, perf=LayerPerf(latency_ms=latency_ms, cache_status=cache_status))
        return data

    async def _request_bytes(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any = None,
        content: bytes | None = None,
        content_type: str | None = None,
        with_perf: bool = False,
    ) -> Any:
        started = time.perf_counter()
        headers = {"content-type": content_type} if content is not None and content_type else None
        response = await self._client.request(
            method,
            path,
            params=self._clean_params(params),
            json=None if content is not None else self._json_body(json),
            content=content,
            headers=headers,
        )
        latency_ms = (time.perf_counter() - started) * 1000
        cache_status = response.headers.get("x-layer-cache")

        if response.is_error:
            error, message = self._error_payload(response)
            raise HevlayerError(response.status_code, message, error=error, response=response)

        data = response.content
        if with_perf:
            return LayerResponse(data=data, perf=LayerPerf(latency_ms=latency_ms, cache_status=cache_status))
        return data

    def _apply_layer_headers(self, data: Any, headers: httpx.Headers) -> None:
        stable = headers.get("x-layer-stable-as-of")
        if stable and hasattr(data, "stable_as_of"):
            try:
                data.stable_as_of = int(stable)
            except ValueError:
                pass
        next_cursor = headers.get("x-layer-next-cursor")
        if next_cursor and hasattr(data, "next_cursor"):
            data.next_cursor = next_cursor

    def _search_history_headers(
        self,
        raw_query: str | None,
        tags: list[str] | None,
    ) -> dict[str, str] | None:
        headers: dict[str, str] = {}
        if raw_query is not None:
            query = str(raw_query).strip()
            if query:
                headers["x-hevlayer-search-query"] = query

        if tags:
            clean_tags = self._clean_history_tags(tags)
            if clean_tags:
                headers["x-hevlayer-tags"] = ",".join(clean_tags)

        return headers or None

    def _clean_history_tags(self, tags: list[Any]) -> list[str]:
        clean_tags: list[str] = []
        for raw_tag in tags:
            tag = str(raw_tag).strip()
            if not tag:
                continue
            if len(tag.encode("utf-8")) > _SEARCH_HISTORY_MAX_TAG_LENGTH:
                raise ValueError(
                    f"search-history tag {tag!r} exceeds {_SEARCH_HISTORY_MAX_TAG_LENGTH} bytes"
                )
            if not all(ch.isascii() and (ch.isalnum() or ch in _SEARCH_HISTORY_TAG_CHARS) for ch in tag):
                raise ValueError(
                    "search-history tags may contain only ASCII letters, digits, ':', '_', '-', '.', '/', '=', or '+'; commas separate tags and cannot be escaped"
                )
            clean_tags.append(tag)
        clean_tags = sorted(set(clean_tags))
        if len(clean_tags) > _SEARCH_HISTORY_MAX_TAGS:
            raise ValueError(
                f"search-history tags are limited to {_SEARCH_HISTORY_MAX_TAGS} unique tags"
            )
        return clean_tags

    def _json_body(self, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, BaseModel):
            return value.model_dump(mode="json", by_alias=True, exclude_none=True)
        return value

    def _clean_token(self, value: str | None) -> str | None:
        if value is None:
            return None
        token = value.strip()
        return token or None

    def _clean_params(self, params: dict[str, Any] | None) -> dict[str, Any] | None:
        if not params:
            return None
        cleaned: dict[str, Any] = {}
        for key, value in params.items():
            if value is None:
                continue
            if isinstance(value, list):
                if key == "tag":
                    cleaned[key] = ",".join(self._clean_history_tags(value))
                else:
                    cleaned[key] = ",".join(str(item) for item in value)
            else:
                cleaned[key] = value
        return cleaned or None

    def _parse_data(self, value: Any, result_type: Any) -> Any:
        if result_type is None:
            return None
        if isinstance(result_type, tuple):
            for candidate in result_type:
                try:
                    return self._parse_data(value, candidate)
                except Exception:
                    pass
            return value
        origin = get_origin(result_type)
        if origin is list:
            (inner_type,) = get_args(result_type)
            return [self._parse_data(item, inner_type) for item in (value or [])]
        if isinstance(result_type, type) and issubclass(result_type, BaseModel):
            return result_type.model_validate(value)
        return value

    def _error_payload(self, response: httpx.Response) -> tuple[str | None, str]:
        try:
            body = response.json()
        except ValueError:
            body = None
        if isinstance(body, dict):
            error = body.get("error")
            message = body.get("message") or response.text or response.reason_phrase
            return str(error) if error is not None else None, str(message)
        return None, response.text or response.reason_phrase
