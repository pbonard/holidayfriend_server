//! The kafka cluster's clients, over mTLS with the kafka-user and cluster ca the reflector copies to this namespace
pub mod consumer;
pub mod producer;

use std::{collections::HashMap, fs};

use crate::required_env;

/// holidayfriend-server's tasks, each record says which in its "taskType" header. my-infra's
/// k8s/bootstrap/kafka-topics.yaml
pub const TOPIC: &str = "holidayfriend";
/// The header that says which task a record of TOPIC is
pub const TASK_TYPE_HEADER: &str = "taskType";
/// A post report, keyed by the post's id
pub const REPORT_POST_TASK: &str = "report-post";

/// The settings every client has: the broker, and the PEM files of the cluster ca and the user's certificate.
/// the crate reads a keystore file as the certificate and the key together, strimzi gives them apart, so they go
/// inline instead
fn client_props(client_id: &str) -> HashMap<String, String> {
    let read = |name: &str| {
        let path = required_env(name);
        fs::read_to_string(&path).unwrap_or_else(|err| panic!("fail to read {name} {path}: {err}"))
    };
    HashMap::from([
        ("bootstrap.servers".into(), required_env("KAFKA_ADDRESS")),
        ("client.id".into(), client_id.into()),
        ("security.protocol".into(), "SSL".into()),
        ("ssl.truststore.type".into(), "PEM".into()),
        (
            "ssl.truststore.location".into(),
            required_env("KAFKA_CA_CERT_PATH"),
        ),
        ("ssl.keystore.type".into(), "PEM".into()),
        (
            "ssl.keystore.certificate.chain".into(),
            read("KAFKA_USER_CERT_PATH"),
        ),
        ("ssl.keystore.key".into(), read("KAFKA_USER_KEY_PATH")),
    ])
}
