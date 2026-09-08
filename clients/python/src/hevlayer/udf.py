from __future__ import annotations

import asyncio
import inspect
import os
import uuid
from typing import Any, Callable

from .client import AsyncHevlayer


class TransientError(Exception):
    pass


class PermanentError(Exception):
    pass


class TpufClient:
    def __init__(self, client: AsyncHevlayer) -> None:
        self._client = client

    async def fetch(self, namespace: str, doc_id: str, *, include_attributes: list[str] | None = None) -> Any:
        return await self._client.fetch_document(namespace, doc_id, include_attributes=include_attributes)

    async def query(self, namespace: str, body: dict[str, Any]) -> Any:
        return await self._client.query_namespace(namespace, body)

    async def list_ids(self, namespace: str, filters: Any, *, page_size: int = 10_000) -> list[str]:
        scan = await self._client.scan(namespace, {"source": "origin", "filters": filters, "page_size": page_size})
        scan_id = scan.get("id") if isinstance(scan, dict) else scan.id
        results = await self._client.get_scan_results(namespace, scan_id)
        ids = results.get("ids") if isinstance(results, dict) else results.ids
        return list(ids)

    async def patch_columns(self, namespace: str, ids: list[str], attrs: dict[str, list[Any]]) -> Any:
        return await self._client.patch_columns(namespace, ids, attrs)


def udf(**metadata: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    def decorate(fn: Callable[..., Any]) -> Callable[..., Any]:
        setattr(fn, "__hevlayer_udf__", dict(metadata))
        return fn

    return decorate


async def run_udf_worker(
    fn: Callable[..., Any],
    *,
    udf_id: str | None = None,
    api_key: str | None = None,
    base_url: str | None = None,
    worker_id: str | None = None,
    limit: int | None = None,
    poll_interval: float = 2.0,
    once: bool = False,
) -> None:
    metadata = getattr(fn, "__hevlayer_udf__", {})
    resolved_udf_id = udf_id or metadata.get("id") or os.environ.get("HEVLAYER_UDF_ID")
    if not resolved_udf_id:
        raise ValueError("udf_id is required")

    resolved_worker_id = worker_id or os.environ.get("HEVLAYER_WORKER_ID") or f"{resolved_udf_id}-{uuid.uuid4().hex[:8]}"
    resolved_limit = limit or int(os.environ.get("HEVLAYER_UDF_BATCH_SIZE", metadata.get("batch_size", 32)))
    resolved_base_url = base_url or os.environ.get("HEVLAYER_BASE_URL", "http://localhost:8080")
    signature = inspect.signature(fn)
    wants_tpuf = "tpuf" in signature.parameters
    output_attr = metadata.get("output")
    output_kind = metadata.get("kind")
    if output_attr is not None:
        if not isinstance(output_attr, str) or not output_attr.strip():
            raise ValueError("udf output metadata must be a non-empty attribute name")
        if output_attr.startswith("_hevlayer_"):
            raise ValueError("udf output metadata must not use reserved _hevlayer_* attributes")

    async with AsyncHevlayer(api_key=api_key or os.environ.get("HEVLAYER_API_KEY") or os.environ.get("LAYER_GATEWAY_API_KEY"), base_url=resolved_base_url) as client:
        tpuf = TpufClient(client)
        while True:
            claimed = await client.claim_udf_items(
                resolved_udf_id,
                {"worker_id": resolved_worker_id, "limit": resolved_limit},
            )
            if not claimed.items:
                if once:
                    return
                await asyncio.sleep(poll_interval)
                continue

            complete_items: list[dict[str, Any]] = []
            failed_items: list[dict[str, Any]] = []
            for item in claimed.items:
                kwargs = dict(item.input)
                if wants_tpuf:
                    kwargs["tpuf"] = tpuf
                try:
                    output = fn(**kwargs)
                    if inspect.isawaitable(output):
                        output = await output
                    complete_item: dict[str, Any] = {"namespace": item.namespace, "id": item.id}
                    if output_kind == "embedding":
                        complete_item["vector"] = output
                    elif output_attr:
                        complete_item["attributes"] = {output_attr: output}
                    elif output is not None:
                        raise PermanentError("UDF returned a value but @udf(output=...) is not set")
                    complete_items.append(complete_item)
                except TransientError as exc:
                    failed_items.append({"namespace": item.namespace, "id": item.id, "kind": "transient", "message": str(exc)})
                except PermanentError as exc:
                    failed_items.append({"namespace": item.namespace, "id": item.id, "kind": "permanent", "message": str(exc)})
                except Exception as exc:
                    failed_items.append({"namespace": item.namespace, "id": item.id, "kind": "transient", "message": str(exc)})

            if complete_items:
                await client.complete_udf_items(
                    resolved_udf_id,
                    {"worker_id": resolved_worker_id, "items": complete_items},
                )
            if failed_items:
                await client.fail_udf_items(
                    resolved_udf_id,
                    {"worker_id": resolved_worker_id, "items": failed_items},
                )

            if once:
                return
