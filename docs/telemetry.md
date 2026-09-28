# Telemetry

The standalone gateway sends anonymous usage telemetry by default. It is
fire-and-forget, never on the request path, and never blocks startup.

Disable it with either environment variable:

```sh
LAYER_TELEMETRY=off
DO_NOT_TRACK=1
```

Telemetry events are sent to `https://telemetry.hevlayer.com`.

## Events

- `gateway_started`: gateway version, anonymous instance ID, and the backend
  kinds the gateway serves (`pgvector`, `turbopuffer`, `search`).
- `gateway_heartbeat`: the same fields, plus:
  - `featureTouches`: counters since startup for feature families, such as
    automatic routing, hybrid/RRF, fuzzy surfacing, facets, scans, federated
    query, and multi-store routing.
  - `usage`: counts since the previous heartbeat of successful queries,
    writes, and rows upserted, patched and deleted.

The gateway checks hourly and sends a heartbeat when there is usage to
report, or once a day when idle.

## Distribution

A tool that starts the gateway can identify itself:

```sh
LAYER_TELEMETRY_SOURCE=kit            # lowercase [a-z0-9-], up to 32 chars
LAYER_TELEMETRY_SOURCE_VERSION=0.3.3  # [0-9A-Za-z.+-], up to 32 chars
```

They are sent as `distribution` and `distributionVersion`. A value outside
that shape is not sent.

The gateway never sends query text, vectors, namespace names, document IDs,
bearer tokens, upstream API keys, or document contents.
