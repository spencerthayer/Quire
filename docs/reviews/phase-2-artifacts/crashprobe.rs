//! Reviewer crash probe: runs the real IngestionCoordinator background loop
//! against a local fixture (resolver maps every host to the fixture).
use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use quire::config::AppConfig;
use quire::db;
use quire::error::AppResult;
use quire::feeds::{self, NewFeedInput};
use quire::feeds::validation::HostResolver;
use quire::web::AppState;

struct AnyHost(SocketAddr);
impl HostResolver for AnyHost {
    fn resolve<'a>(&'a self, _h: &'a str, _p: u16) -> Pin<Box<dyn Future<Output = AppResult<Vec<SocketAddr>>> + Send + 'a>> {
        let a = self.0;
        Box::pin(async move { Ok(vec![a]) })
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().with_env_filter("quire=info").init();
    let a: Vec<String> = std::env::args().collect();
    let (dir, port) = (a[1].clone(), a[2].parse::<u16>().unwrap());
    let mut env = HashMap::new();
    env.insert("QUIRE_DATA_DIR".to_string(), dir.clone());
    env.insert("QUIRE_DATABASE_URL".to_string(), format!("sqlite://{dir}/q.db"));
    env.insert("QUIRE_EXTRACTION_CONCURRENCY".to_string(), "4".to_string());
    env.insert("QUIRE_ARTICLE_REQUEST_TIMEOUT_SECONDS".to_string(), "60".to_string());
    let cfg = AppConfig::from_map(&env).unwrap();
    let pool = db::init_pool(&cfg.database_url, &cfg.data_dir).await.unwrap();
    let st = AppState::with_resolver(pool.clone(), cfg, Arc::new(AnyHost(SocketAddr::from(([127, 0, 0, 1], port)))));
    st.coordinator.recover_startup_state().await.unwrap();
    if feeds::list_feeds(&pool).await.unwrap().is_empty() {
        feeds::create_feed(&pool, NewFeedInput { name: "crash".into(), feed_url: format!("http://feeds.example.com:{port}/feed.xml"), enabled: true, ..Default::default() }).await.unwrap();
    }
    let h = st.coordinator.spawn_background_loop(Duration::from_secs(1));
    tokio::signal::ctrl_c().await.ok();
    st.coordinator.signal_shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(10), h).await;
    pool.close().await;
    eprintln!("probe exited cleanly");
}
