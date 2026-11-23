//! The sql of every table, and the pool they're run on. a function takes the connection, or the transaction when
//! the service does several writes together, and returns what it read: the service decides what's one transaction
pub mod invite;
pub mod outbox;
pub mod post;
pub mod profile;
pub mod verdict;

use mysql_async::{OptsBuilder, Pool};

use crate::required_env;

/// The pool of connections to vtgate, from MYSQL_URL and the mysql secret's credentials
pub fn pool() -> Pool {
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
