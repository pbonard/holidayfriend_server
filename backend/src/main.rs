mod profile;

use std::env;

use axum::{Router, routing::put};
use mysql_async::{OptsBuilder, Pool};

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

#[tokio::main]
async fn main() {
    // the gateway sends everything under /api here and keeps the prefix in the path
    let api = Router::new().route("/profile", put(profile::save).get(profile::get_brief));
    let app = Router::new().nest("/api", api).with_state(mysql_pool());

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
