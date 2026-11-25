mod cities;
mod db;
mod embedding_client;
mod grpc_client;
mod id;
mod invite;
mod kafka;
mod milvus_client;
mod moderation_client;
mod place;
mod post;
mod profile;

use std::{env, time::Duration};

use axum::{
    Router,
    extract::{FromRef, Request},
    routing::{get, post},
};
use embedding_client::EmbeddingClient;
use grpc_client::{GrpcClient, GrpcClientConfig};
use kafka::producer::Producer;
use milvus_client::MilvusClient;
use moderation_client::ModerationClient;
use mysql_async::Pool;
use tokio::signal::unix::{SignalKind, signal};
use tokio_util::sync::CancellationToken;
use tower_http::trace::{DefaultOnResponse, TraceLayer};
use tracing::Level;
use tracing_subscriber::EnvFilter;

fn required_env(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("{name} is not set"))
}

/// What the handlers share: most only need the database, sending an invite asks chat too, reporting a post kafka.
/// the report consumer gets it too, as a handler would
#[derive(Clone)]
pub struct AppState {
    pool: Pool,
    grpc_client: GrpcClient,
    producer: Producer,
    moderation: ModerationClient,
    embedding: EmbeddingClient,
    milvus: MilvusClient,
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

impl FromRef<AppState> for Producer {
    fn from_ref(state: &AppState) -> Self {
        state.producer.clone()
    }
}

impl FromRef<AppState> for ModerationClient {
    fn from_ref(state: &AppState) -> Self {
        state.moderation.clone()
    }
}

impl FromRef<AppState> for MilvusClient {
    fn from_ref(state: &AppState) -> Self {
        state.milvus.clone()
    }
}

fn init_tracing() {
    // confluent-kafka logs every fetch at info under fetch_diag, twice a second even when there's nothing
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,fetch_diag=warn"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .json()
        .init();
}

#[tokio::main]
async fn main() {
    init_tracing();

    let state = AppState {
        pool: db::pool(),
        grpc_client: GrpcClient::connect_lazy(GrpcClientConfig {
            // chat in this namespace, its grpc port: http://chat.holidayfriend.svc.cluster.local:50051
            chat_url: required_env("CHAT_GRPC_URL"),
            timeout: Duration::from_secs(3),
        }),
        producer: kafka::producer::new("holidayfriend-server"),
        moderation: ModerationClient::from_env(),
        embedding: EmbeddingClient::from_env(),
        // a server without milvus doesn't start, the pod restarts until it's there
        milvus: MilvusClient::connect(&required_env("MILVUS_URL"))
            .await
            .expect("fail to connect to milvus"),
    };

    // SIGTERM from kubernetes or ctrl-c: the server stops taking requests and finishes its own, the consumer leaves
    // its group
    let shutdown = CancellationToken::new();
    tokio::spawn(cancel_on_signal(shutdown.clone()));

    // in this process, as onlineconversation's consumer is in its server: the reports are few, and it shares the
    // state with the handlers, the producer hands a failed report to the retry job
    let consumer = tokio::spawn(kafka::consumer::consume(state.clone(), shutdown.clone()));
    // it returns at shutdown. before it, it panicked or got a kafka error that won't pass, and the server shouldn't go
    // on without it: the pod restarts instead
    let consumer = {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            let result = consumer.await;
            if result.is_err() || !shutdown.is_cancelled() {
                tracing::error!(error = ?result.err(), "consumer stopped");
                std::process::exit(1);
            }
        })
    };

    // the gateway sends everything under /api here and keeps the prefix in the path
    let api: Router<AppState> = Router::new()
        .route("/profile", get(profile::get).put(profile::save))
        .route("/profile/brief/my", get(profile::get_my_brief))
        .route("/profile/report", post(profile::report))
        .nest(
            "/post",
            Router::new()
                .route(
                    "/",
                    get(post::get)
                        .post(post::create)
                        .put(post::update)
                        .delete(post::delete),
                )
                .route("/list", get(post::list))
                .route("/user", get(post::list_by_user))
                .route("/related", get(post::list_related))
                .route("/report", post(post::report))
                .route(
                    "/not-interested",
                    get(post::list_not_interested).post(post::not_interested),
                ),
        )
        .nest(
            "/invite",
            Router::new()
                .route("/", post(invite::send))
                .route("/list", get(invite::list))
                .route("/accept", post(invite::accept))
                .route("/decline", post(invite::decline))
                .route("/report", post(invite::report)),
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
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();
    tracing::info!("listening on 0.0.0.0:8080");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown.cancelled_owned())
        .await
        .unwrap();
    // waits for the consumer to leave its group and commit what its workers finished
    let _ = consumer.await;
}

async fn cancel_on_signal(shutdown: CancellationToken) {
    let mut terminate = signal(SignalKind::terminate()).expect("fail to listen for SIGTERM");
    tokio::select! {
        _ = terminate.recv() => {}
        _ = tokio::signal::ctrl_c() => {}
    }
    tracing::info!("shutting down");
    shutdown.cancel();
}
