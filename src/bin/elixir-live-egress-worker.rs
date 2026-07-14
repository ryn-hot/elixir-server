#[tokio::main]
async fn main() -> anyhow::Result<()> {
    elixir_server::live::egress::run_live_egress_worker_from_environment().await
}
