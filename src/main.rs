use std::process::ExitCode;
use std::time::Duration;

use quire::config::AppConfig;
use quire::db;
use quire::web::{AppState, create_router};
use tracing::{error, info};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, fmt};

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();

    if args.get(1).map(String::as_str) == Some("healthcheck") {
        return run_cli_healthcheck().await;
    }

    let config = match AppConfig::from_env() {
        Ok(cfg) => cfg,
        Err(err) => {
            eprintln!("Failed to load configuration: {err}");
            return ExitCode::FAILURE;
        }
    };

    init_logging(&config.log_level);

    info!(
        version = quire::web::APP_VERSION,
        host = %config.host,
        port = config.port,
        data_dir = %config.data_dir.display(),
        timezone = %config.timezone,
        "Starting Quire"
    );

    let pool = match db::init_pool(&config.database_url, &config.data_dir).await {
        Ok(p) => p,
        Err(err) => {
            error!(error = ?err, "Failed to initialize SQLite database");
            return ExitCode::FAILURE;
        }
    };

    let bind_addr = match config.bind_socket_addr() {
        Ok(addr) => addr,
        Err(err) => {
            error!(error = ?err, "Invalid bind socket address");
            return ExitCode::FAILURE;
        }
    };

    let state = AppState::new(pool.clone(), config);
    if let Err(err) = state.coordinator.recover_startup_state().await {
        error!(error = ?err, "Failed to recover interrupted ingestion state on startup");
        return ExitCode::FAILURE;
    }

    let coordinator = state.coordinator.clone();
    let scheduler_handle = coordinator.spawn_background_loop(Duration::from_secs(30));

    let app = create_router(state);

    let listener = match tokio::net::TcpListener::bind(bind_addr).await {
        Ok(l) => l,
        Err(err) => {
            error!(addr = %bind_addr, error = ?err, "Failed to bind TCP listener");
            coordinator.signal_shutdown();
            let _ = scheduler_handle.await;
            return ExitCode::FAILURE;
        }
    };

    info!(addr = %bind_addr, "Quire HTTP server listening");

    let serve_res = axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown_signal().await;
            coordinator.signal_shutdown();
        })
        .await;

    // Wait up to 10 seconds for the background scheduler loop to finish active work
    let _ = tokio::time::timeout(Duration::from_secs(10), scheduler_handle).await;
    pool.close().await;

    if let Err(err) = serve_res {
        error!(error = ?err, "HTTP server exited with error");
        return ExitCode::FAILURE;
    }

    info!("Quire shut down cleanly");
    ExitCode::SUCCESS
}

fn init_logging(default_level: &str) {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(format!("quire={default_level},tower_http=info")))
        .unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(fmt::layer().with_target(true))
        .with(filter)
        .init();
}

async fn run_cli_healthcheck() -> ExitCode {
    let config = match AppConfig::from_env() {
        Ok(cfg) => cfg,
        Err(err) => {
            eprintln!("Healthcheck configuration error: {err}");
            return ExitCode::FAILURE;
        }
    };

    let probe_host = match config.host.as_str() {
        "0.0.0.0" | "::" | "[::]" => "127.0.0.1",
        other => other,
    };
    let url = format!("http://{probe_host}:{}/health", config.port);

    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(4))
        .build()
    {
        Ok(c) => c,
        Err(err) => {
            eprintln!("Healthcheck client build failed: {err}");
            return ExitCode::FAILURE;
        }
    };

    match client.get(&url).send().await {
        Ok(resp) if resp.status().is_success() => ExitCode::SUCCESS,
        Ok(resp) => {
            eprintln!("Healthcheck failed with status {}", resp.status());
            ExitCode::FAILURE
        }
        Err(err) => {
            eprintln!("Healthcheck request failed: {err}");
            ExitCode::FAILURE
        }
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut sigterm) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            sigterm.recv().await;
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
}
