"""Aggregate query responses omit `rows` on the wire (LYR-164)."""

import asyncio
import json

import httpx

from hevlayer import AsyncHevlayer
from hevlayer.models import QueryResponse

# Bodies as the gateway returns them for aggregate queries: no `rows` key.
AGGREGATE_ONLY = '{"aggregations":{"n":9769},"billing":{"billable_logical_bytes_queried":0,"billable_logical_bytes_returned":0},"performance":{"cache_hit_ratio":1.0,"cache_temperature":"hot","server_total_ms":3},"next_cursor":null}'
GROUPED = '{"aggregation_groups":[{"category":"boots","n":412},{"category":"sandals","n":97}],"billing":{"billable_logical_bytes_queried":0,"billable_logical_bytes_returned":0},"performance":{"cache_hit_ratio":1.0,"cache_temperature":"hot","server_total_ms":4},"next_cursor":null}'
QUERY = {"aggregate_by": {"n": ["Count"]}}


def _handler(body: str):
    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=body, headers={"content-type": "application/json", "x-layer-stable-as-of": "42"})

    return handle


def test_model_accepts_missing_rows():
    parsed = QueryResponse.model_validate(json.loads(AGGREGATE_ONLY))
    assert parsed.rows is None
    assert parsed.aggregations == {"n": 9769}


def test_async_aggregate_only():
    async def run():
        http = httpx.AsyncClient(base_url="http://gw", transport=httpx.MockTransport(_handler(AGGREGATE_ONLY)))
        async with AsyncHevlayer(base_url="http://gw", http_client=http) as client:
            return await client.query_namespace("ns", QUERY)

    result = asyncio.run(run())
    assert result.rows is None
    assert result.aggregations == {"n": 9769}


def test_async_grouped_aggregate():
    async def run():
        http = httpx.AsyncClient(base_url="http://gw", transport=httpx.MockTransport(_handler(GROUPED)))
        async with AsyncHevlayer(base_url="http://gw", http_client=http) as client:
            return await client.query_namespace("ns", QUERY)

    result = asyncio.run(run())
    assert result.rows is None
    assert result.aggregation_groups[0] == {"category": "boots", "n": 412}

