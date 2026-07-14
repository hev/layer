#[tokio::main]
async fn main() {
    hevlayer_gateway::server::run_open().await;
}
