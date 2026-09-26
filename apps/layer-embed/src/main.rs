use layer_embed::{
    registry::Registry,
    service::{router, Service},
};
use std::{
    path::PathBuf,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

fn main() -> anyhow::Result<()> {
    // Set before creating any threads. CPU-only Candle and tokenizers share this
    // bounded budget; no BLAS/Accelerate/MKL or GPU features are enabled.
    std::env::set_var("RAYON_NUM_THREADS", "1");
    std::env::set_var("CANDLE_NUM_THREADS", "1");
    std::env::set_var("TOKENIZERS_PARALLELISM", "false");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(2)
        .enable_all()
        .build()?;
    let result = runtime.block_on(run());
    runtime.shutdown_timeout(Duration::from_secs(1));
    result
}
async fn run() -> anyhow::Result<()> {
    let baked = std::env::var_os("LAYER_EMBED_BAKED_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/opt/layer-embed/models"));
    let mounted = std::env::var_os("LAYER_EMBED_MODELS_DIR").map(PathBuf::from);
    let address = std::env::var("LAYER_EMBED_BIND").unwrap_or_else(|_| "0.0.0.0:8081".to_owned());
    let service = Service::new();
    let listener = tokio::net::TcpListener::bind(address).await?;
    let capacity = service.admitted.available_permits() as u32;
    let loading_service = service.clone();
    let mut loader =
        tokio::task::spawn_blocking(move || Registry::load(&baked, mounted.as_deref()));
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let server = axum::serve(
        layer_embed::connection::TrackingListener(listener),
        router(service.clone())
            .into_make_service_with_connect_info::<layer_embed::connection::Disconnect>(),
    )
    .with_graceful_shutdown(async {
        let _ = stop_rx.await;
    });
    let mut server = Box::pin(std::future::IntoFuture::into_future(server));
    let mut termination = Box::pin(terminate());
    let mut loaded = false;
    loop {
        tokio::select! {
            result = &mut server => { result?; return Ok(()); }
            result = &mut loader, if !loaded => {
                match result? {
                    Ok(registry) => { *loading_service.registry.write().unwrap() = Some(Arc::new(registry)); loaded = true; }
                    Err(error) => {
                        service.invalid_artifacts.store(true,Ordering::Release);
                        eprintln!("invalid artifacts: {error}");
                        let _ = stop_tx.send(());
                        return Err(anyhow::anyhow!("artifact initialization failed"));
                    }
                }
            }
            _ = &mut termination => {
                service.shutting_down.store(true,Ordering::Release);
                service.active.close();
                let _ = stop_tx.send(());
                let drain = async {
                    let (_, _) = tokio::join!(&mut server, service.admitted.clone().acquire_many_owned(capacity));
                };
                let _ = tokio::time::timeout(Duration::from_secs(29), drain).await;
                return Ok(());
            }
        }
    }
}
async fn terminate() {
    #[cfg(unix)]
    {
        let mut signal = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("SIGTERM handler");
        tokio::select! { _ = signal.recv() => {}, _ = tokio::signal::ctrl_c() => {} }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
