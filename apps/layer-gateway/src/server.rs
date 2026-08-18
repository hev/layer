use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use dashmap::DashMap;
use tracing::{info, warn};

use crate::clients::aerospike::{AerospikeClient, AerospikeRuntime};
use crate::clients::s3::{AwsS3Client, NoopS3Client, S3Client};
use crate::clients::search::HttpSearchClient;
use crate::clients::turbopuffer::{
    HttpTurbopufferClient, RoutingTurbopufferClient, TurbopufferClient,
};
use crate::config::Config;
use crate::consistency::ConsistencyWatcher;
use crate::cost::AwsCostConfig;
use crate::index_config::{IndexConfigSource, StaticIndexConfigSource};
use crate::metrics::LayerMetrics;
use crate::telemetry::{Telemetry, TelemetryCounters};
use crate::vector_store::{resolve_vector_stores_from_yaml, ResolvedVectorStoreKind};
use crate::{build_router, AppState, RestoreRunState};

#[derive(Debug, Clone, Copy)]
pub struct ServerOptions {
    pub managed_platform: bool,
    pub document_cache: bool,
    pub transform_runtime: bool,
    pub agentic: bool,
    pub minted_keys: bool,
    pub cost_sampler: bool,
}

impl ServerOptions {
    pub fn open() -> Self {
        Self {
            managed_platform: false,
            document_cache: false,
            transform_runtime: false,
            agentic: false,
            minted_keys: false,
            cost_sampler: false,
        }
    }

    pub fn pro() -> Self {
        Self::open()
    }
}

pub async fn run() {
    run_open().await;
}

pub async fn run_open() {
    run_with_options(ServerOptions::open()).await;
}

pub async fn run_pro() {
    warn!("Pro composition is not included in the public gateway mirror; starting open gateway");
    run_open().await;
}

pub async fn run_with_options(options: ServerOptions) {
    let _ = rustls::crypto::ring::default_provider().install_default();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "hevlayer_gateway=info,tower_http=info".into()),
        )
        .init();

    let config = Config::from_env();
    let addr = SocketAddr::from(([0, 0, 0, 0], config.port));
    let metrics = Arc::new(LayerMetrics::new());
    let telemetry_counters = Arc::new(TelemetryCounters::default());

    let stores_json = config
        .stores_json
        .as_deref()
        .unwrap_or_else(|| panic!("open gateway requires LAYER_STORE_FILE or LAYER_STORE_JSON"));
    let resolved_stores = resolve_vector_stores_from_yaml(
        stores_json,
        "LAYER_STORE_FILE or LAYER_STORE_JSON",
        &config.vector_store_namespace,
    )
        .await
        .unwrap_or_else(|err| panic!("failed to resolve standalone VectorStore config: {err}"));

    let default_store_kind = resolved_stores
        .stores
        .get(&resolved_stores.default_store)
        .map(|store| match store.kind {
            ResolvedVectorStoreKind::Turbopuffer => "turbopuffer",
            ResolvedVectorStoreKind::Search => "search",
        })
        .unwrap_or("turbopuffer");
    metrics.set_store_kind(default_store_kind);

    let telemetry_backend_kinds = resolved_stores.stores.values().map(|store| match store.kind {
        ResolvedVectorStoreKind::Turbopuffer => "turbopuffer".to_string(),
        ResolvedVectorStoreKind::Search => "search".to_string(),
    });
    if config.telemetry_enabled {
        if let Some(telemetry) = Telemetry::new(
            config.telemetry_endpoint.clone(),
            config.telemetry_state_path.clone(),
            telemetry_backend_kinds,
            Arc::clone(&telemetry_counters),
        ) {
            telemetry.spawn();
            info!("Anonymous gateway telemetry enabled");
        }
    } else {
        info!("Anonymous gateway telemetry disabled");
    }

    info!(
        namespace = %config.vector_store_namespace,
        vector_store = %resolved_stores.default_store,
        "Inbound API key required from default VectorStore"
    );

    if options.managed_platform
        || options.document_cache
        || options.transform_runtime
        || options.agentic
        || options.minted_keys
        || options.cost_sampler
    {
        warn!("Pro server options were requested but are not included in the public gateway mirror");
    }

    let namespace_store_refs = Arc::new(RwLock::new(HashMap::new()));
    let mut upstream_clients = HashMap::new();
    let mut stores = resolved_stores.stores.values().collect::<Vec<_>>();
    stores.sort_by(|left, right| left.name.cmp(&right.name));
    let mut embedding_provider_client = None;
    for store in stores {
        let inner: Arc<dyn TurbopufferClient> = match store.kind {
            ResolvedVectorStoreKind::Turbopuffer => {
                let client: Arc<dyn TurbopufferClient> = Arc::new(HttpTurbopufferClient::new(
                    store.upstream_api_key.as_deref().unwrap_or_default(),
                    &store.endpoint_url,
                ));
                if embedding_provider_client.is_none() {
                    embedding_provider_client = Some(Arc::clone(&client));
                }
                client
            }
            ResolvedVectorStoreKind::Search => Arc::new(HttpSearchClient::new(
                store.upstream_api_key.as_deref(),
                &store.endpoint_url,
            )),
        };
        upstream_clients.insert(store.name.clone(), inner);
        info!(
            vector_store = %store.name,
            kind = ?store.kind,
            endpoint = %store.endpoint_url,
            "VectorStore client initialized"
        );
    }
    let routed: Arc<dyn TurbopufferClient> = Arc::new(RoutingTurbopufferClient::new(
        resolved_stores.default_store.clone(),
        upstream_clients,
        Arc::clone(&namespace_store_refs),
    ));
    let turbopuffer: Option<Arc<dyn TurbopufferClient>> =
        Some(metrics.instrument_turbopuffer(routed));
    let embedding_provider = embedding_provider_client.map(|client| {
        Arc::new(crate::embedding::TurbopufferEmbeddingProvider::new(client))
            as Arc<dyn crate::embedding::EmbeddingProvider>
    });
    let lattice_embedding_provider = config.lattice_model_path.as_deref().map(|path| {
        let provider =
            crate::embedding::LatticeEmbeddingProvider::load(path).unwrap_or_else(|error| {
                panic!("failed to configure Lattice embedding provider: {error}")
            });
        info!(model = %path.display(), "Lattice embedding provider initialized");
        Arc::new(provider) as Arc<dyn crate::embedding::EmbeddingProvider>
    });
    let local_clip_embedding_provider = config.local_clip_model_path.as_deref().map(|path| {
        let provider = crate::embedding::LocalClipEmbeddingProvider::load(path)
            .unwrap_or_else(|error| {
                panic!("failed to configure local CLIP embedding provider: {error}")
            });
        info!(model = %path.display(), "Local CLIP embedding provider initialized");
        Arc::new(provider) as Arc<dyn crate::embedding::EmbeddingProvider>
    });

    let aerospike_runtime = Arc::new(AerospikeRuntime::new(None));
    metrics.set_aerospike_connection_state(false);
    info!("Document cache disabled in public gateway composition");
    let aerospike: Arc<dyn AerospikeClient> = aerospike_runtime.clone();

    let s3_inner: Arc<dyn S3Client> = match config.s3_bucket.as_deref() {
        Some(bucket) => Arc::new(
            AwsS3Client::new(bucket, &config.s3_region, config.s3_endpoint.as_deref()).await,
        ),
        None => {
            if config.s3_endpoint.is_some() {
                warn!(
                    "S3_ENDPOINT is set but S3_BUCKET is not; ignoring the endpoint and \
                     running without an object store"
                );
            }
            info!(
                "S3_BUCKET is unset; object store disabled — snapshots, search history, \
                 checkpoints, and blobs degrade or report \"object store not configured\""
            );
            Arc::new(NoopS3Client)
        }
    };
    let s3: Arc<dyn S3Client> = metrics.instrument_s3(s3_inner);

    let facet_override_enabled = !config.facet_fields.is_empty();
    let snapshot_min_interval_ms = config.snapshot_min_interval_ms;
    let facet_fields = if facet_override_enabled {
        match StaticIndexConfigSource::new(config.facet_fields.clone())
            .load_facet_fields()
            .await
        {
            Ok(facet_fields) => facet_fields,
            Err(error) => {
                warn!(
                    error = %error,
                    "LAYER_FACET_FIELDS is invalid; starting with an empty facet map"
                );
                HashMap::new()
            }
        }
    } else {
        HashMap::new()
    };

    let consistency = Arc::new(ConsistencyWatcher::new());
    if facet_override_enabled {
        for namespace in facet_fields.keys() {
            consistency.register(namespace);
        }
    }

    let agents = crate::agent::registry_from_json(None)
        .unwrap_or_else(|err| panic!("failed to load empty agent registry: {err}"));
    let agent_provider: Arc<dyn crate::agent::AgentInferenceProvider> =
        Arc::new(crate::agent::DisabledAgentProvider);

    let state = Arc::new(AppState {
        draining: Arc::new(AtomicBool::new(false)),
        drain_marker_path: config.drain_marker_path.clone(),
        metrics: Arc::clone(&metrics),
        telemetry: Arc::clone(&telemetry_counters),
        turbopuffer: turbopuffer.clone(),
        embedding_provider,
        lattice_embedding_provider,
        local_clip_embedding_provider,
        embedding_cache: Arc::new(DashMap::new()),
        embedding_cache_ttl: std::time::Duration::from_millis(config.embedding_cache_ttl_ms),
        wire_embedding_profiles: Arc::new(DashMap::new()),
        aerospike,
        aerospike_runtime,
        s3,
        index_deleter: None,
        jobs: Arc::new(DashMap::new()),
        restore_runs: Arc::new(DashMap::<String, RestoreRunState>::new()),
        aerospike_set_prefix: config.aerospike_set_prefix,
        pipeline_store: None,
        udf_store: None,
        write_trigger: None,
        metrics_backend_url: config
            .metrics_backend_url
            .map(|url| url.trim_end_matches('/').to_string()),
        aws_cost_config: AwsCostConfig {
            enabled: false,
            region: config.aws_cost_region.clone(),
            tag_key: config.aws_cost_tag_key.clone(),
            tag_value: config.aws_cost_tag_value.clone(),
            site: config.aws_cost_site.clone(),
            cache_ttl_seconds: config.aws_cost_cache_ttl_seconds,
        },
        pipeline_status_cache: Arc::new(DashMap::new()),
        pipeline_status_cache_ttl: std::time::Duration::from_millis(
            config.pipeline_status_cache_ttl_ms,
        ),
        pipeline_status_inflight: Arc::new(DashMap::new()),
        udf_status_cache: Arc::new(DashMap::new()),
        udf_status_inflight: Arc::new(DashMap::new()),
        consistency: Arc::clone(&consistency),
        cache_warmed_through: Arc::new(DashMap::new()),
        cache_namespaces: Arc::new(DashMap::new()),
        warm_inflight: Arc::new(DashMap::new()),
        reactive_warm_generations: Arc::new(DashMap::new()),
        facet_fields: Arc::new(RwLock::new(facet_fields)),
        scan_threads: Arc::new(RwLock::new(HashMap::new())),
        snapshot_min_interval_ms,
        snapshot_interval_ms: Arc::new(RwLock::new(HashMap::new())),
        snapshot_retention: Arc::new(RwLock::new(HashMap::new())),
        blob_reference_attributes: Arc::new(RwLock::new(HashMap::new())),
        blob_store_enabled: false,
        managed_platform_enabled: false,
        namespace_store_refs,
        embedding_profiles: Arc::new(RwLock::new(HashMap::new())),
        last_snapshot_at: Arc::new(DashMap::new()),
        snapshot_inflight: Arc::new(DashMap::new()),
        inbound_auth: resolved_stores.inbound_auth.clone(),
        minted_key_verifier: None,
        key_store: None,
        keys_namespace: config.keys_namespace.clone(),
        vector_store_namespace: config.vector_store_namespace.clone(),
        turbopuffer_dashboard_base_url: config
            .turbopuffer_dashboard_base_url
            .trim_end_matches('/')
            .to_string(),
        default_store: resolved_stores.default_store.clone(),
        resolved_vectorstores: Arc::new(resolved_stores.stores.clone()),
        shard_count: config.shard_count,
        federated_query_max_namespaces: config.federated_query_max_namespaces,
        federated_query_namespace_threads: config.federated_query_namespace_threads,
        sharded_namespaces: Arc::new(DashMap::new()),
        init_tasks: Arc::new(DashMap::new()),
        init_backfill_batch_size: config.init_backfill_batch_size,
        init_backfill_rps: config.init_backfill_rps,
        namespace_list_cache: Arc::new(DashMap::new()),
        namespace_list_cache_ttl: std::time::Duration::from_millis(
            config.namespace_list_cache_ttl_ms,
        ),
        agents,
        agentic_enabled: false,
        agent_provider,
        search_kind_stores: resolved_stores
            .stores
            .values()
            .filter(|store| store.kind == ResolvedVectorStoreKind::Search)
            .map(|store| store.name.clone())
            .collect(),
    });

    if let Some(tpuf) = turbopuffer {
        let watcher = Arc::clone(&consistency);
        let poll_interval = Duration::from_millis(config.consistency_poll_interval_ms);
        let stable_poll_interval =
            Duration::from_millis(config.consistency_stable_poll_interval_ms);
        let safety_margin = Duration::from_millis(config.consistency_safety_margin_ms);
        tokio::spawn(async move {
            watcher
                .run(tpuf, poll_interval, stable_poll_interval, safety_margin, None)
                .await;
        });
    }

    let app = build_router(state);

    info!(addr = %addr, "Hevlayer gateway starting");
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
