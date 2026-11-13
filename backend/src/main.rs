mod cities;
mod db;
mod grpc_client;
mod invite;
mod place;
mod post;
mod profile;
mod user;

use std::{env, time::Duration};

use axum::{
    Router,
    extract::{FromRef, Request},
    routing::{get, post, put},
};
use grpc_client::{GrpcClient, GrpcClientConfig};
use mysql_async::{OptsBuilder, Pool};
use tower_http::trace::{DefaultOnResponse, TraceLayer};
use tracing::Level;
use tracing_subscriber::EnvFilter;

fn required_env(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("{name} is not set"))
}

fn mysql_pool() -> Pool {
    // host:port, the same format as MYSQL_URL in the cluster's config map
    let url = required_env("MYSQL_URL");
    let (host, port) = url.split_once(':').expect("MYSQL_URL must be host:port");
    let port = port
        .parse()
        .expect("the port of MYSQL_URL must be a number");

    Pool::new(
        OptsBuilder::default()
            .ip_or_hostname(host)
            .tcp_port(port)
            .user(Some(required_env("MYSQL_USERNAME")))
            .pass(Some(required_env("MYSQL_PASSWORD")))
            .db_name(Some(required_env("MYSQL_DATABASE"))),
    )
}

/// What the handlers share: most only need the database, sending an invite asks chat too
#[derive(Clone)]
pub struct AppState {
    pool: Pool,
    grpc_client: GrpcClient,
}

impl FromRef<AppState> for Pool {
    fn from_ref(state: &AppState) -> Self {
        state.pool.clone()
    }
}

impl FromRef<AppState> for GrpcClient {
    fn from_ref(state: &AppState) -> Self {
        state.grpc_client.clone()
    }
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .json()
        .init();
}

#[tokio::main]
async fn main() {
    init_tracing();

    // the gateway sends everything under /api here and keeps the prefix in the path
    let api: Router<AppState> = Router::new()
        .route("/profile", put(profile::save).get(profile::get_my_brief))
        .nest(
            "/post",
            Router::new()
                .route("/", post(post::create))
                .route("/list", get(post::list)),
        )
        .nest(
            "/invite",
            Router::new()
                .route("/", post(invite::send))
                .route("/list", get(invite::list))
                .route("/{id}/accept", post(invite::accept))
                .route("/{id}/decline", post(invite::decline)),
        );
    let app = Router::new()
        .nest("/api", api)
        // one line per request, with what it was and how it went
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(|request: &Request| {
                    let user_id = request
                        .headers()
                        .get("X-User-Id")
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or("");
                    // the path and not the whole uri: a query string could carry what shouldn't be in logs
                    tracing::info_span!(
                        "request",
                        method = %request.method(),
                        path = request.uri().path(),
                        user_id,
                    )
                })
                // tower-http logs these at debug, info makes them show with the default filter
                .on_response(DefaultOnResponse::new().level(Level::INFO)),
        )
        .with_state(AppState {
            pool: mysql_pool(),
            grpc_client: GrpcClient::connect_lazy(GrpcClientConfig {
                // chat in this namespace, its grpc port: http://chat.holidayfriend.svc.cluster.local:50051
                chat_url: required_env("CHAT_GRPC_URL"),
                timeout: Duration::from_secs(3),
            }),
        });

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();
    tracing::info!("listening on 0.0.0.0:8080");
    axum::serve(listener, app).await.unwrap();
}
