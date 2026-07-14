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

- `gateway_started`: gateway version, anonymous instance ID, and configured backend kinds.
- `gateway_heartbeat`: daily aggregate counters for feature families, such as
  automatic routing, hybrid/RRF, fuzzy surfacing, facets, scans, federated query,
  and multi-store routing.

The gateway never sends query text, vectors, namespace names, document IDs,
bearer tokens, upstream API keys, or document contents.
