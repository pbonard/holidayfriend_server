//! The topic's consumer, as onlineconversation's consumer package: what each record means, by its "taskType"
//! header. a report is evaluated, a changed post indexed, and either is handed to the retry job when it can't be. the
//! polling, the workers and the commits are consumer_group's
use std::{collections::HashMap, time::SystemTime};

use axum::extract::FromRef;
use base64::{Engine, prelude::BASE64_STANDARD};
use confluent_kafka::{common::header::Header, consumer::ConsumerRecord};
use mysql_async::Pool;
use serde::Serialize;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    INDEX_POST_TASK, REPORT_INVITE_TASK, REPORT_POST_TASK, REPORT_PROFILE_TASK, TASK_TYPE_HEADER,
    TOPIC,
    consumer_group::{self, ConsumerGroupHandler},
    producer::{self, Producer},
};
use crate::{AppState, invite::InviteReport};

/// The consumer group, and the prefix of the retry job's partitionId of this group's failed records
const GROUP_ID: &str = "holidayfriend";
/// The topic of the exponential-backoff-retry job (my-server), shared with the other apps: it sends a failed record
/// back to its topic later, with the "partitionId" header, and to dlq after the last try
const RETRY_TOPIC: &str = "exponential-backoff-retry";
/// The header the retry job sets on a record it sends again
const PARTITION_ID_HEADER: &str = "partitionId";

/// Consumes the topic until `shutdown`, as onlineconversation's GetMessage
pub async fn consume(state: AppState, shutdown: CancellationToken) {
    consumer_group::run(GROUP_ID, TOPIC, Tasks { state }, shutdown).await;
}

/// onlineconversation's Consumer, the ConsumerGroupHandler: the app's state, as a handler's
struct Tasks {
    state: AppState,
}

impl ConsumerGroupHandler for Tasks {
    async fn handle(&self, record: &ConsumerRecord<Vec<u8>, Vec<u8>>) -> Result<(), String> {
        handle(&self.state, record).await
    }
}

/// One record, by its "taskType" header: its task is done, or it's handed to the retry job when it couldn't be. an
/// error only when neither worked
async fn handle(state: &AppState, record: &ConsumerRecord<Vec<u8>, Vec<u8>>) -> Result<(), String> {
    // as a handler's State extractors take them
    let producer = Producer::from_ref(state);
    let retry_partition_id = record
        .headers()
        .into_iter()
        .find(|header| header.key() == PARTITION_ID_HEADER)
        .and_then(|header| header.value())
        .map(|value| String::from_utf8_lossy(value).into_owned());
    // the retry job's record for another group, should one ever share the topic
    if let Some(partition_id) = &retry_partition_id
        && !partition_id.starts_with(&format!("{GROUP_ID}:"))
    {
        return Ok(());
    }
    let task_type = record
        .headers()
        .into_iter()
        .find(|header| header.key() == TASK_TYPE_HEADER)
        .and_then(|header| header.value())
        .map(String::from_utf8_lossy);
    // a task's error is why it failed, the record goes to the retry job with it
    let result = match task_type.as_deref() {
        // its key is the post's id. a key that isn't one is dropped, retrying won't fix it
        Some(REPORT_POST_TASK) => {
            let Some(post_id) = record.key().and_then(|key| Uuid::from_slice(key).ok()) else {
                tracing::error!(key = ?record.key(), "fail to parse report");
                return Ok(());
            };
            // as a handler's State extractors take them. the moderation client is borrowed, a clone would copy its
            // policy
            crate::post::manage_report(&Pool::from_ref(state), &state.moderation, post_id).await
        }
        Some(REPORT_INVITE_TASK) => {
            let id = record.key().and_then(|key| Uuid::from_slice(key).ok());
            let report = record
                .value()
                .and_then(|value| serde_json::from_slice::<InviteReport>(value).ok());
            let (Some(id), Some(report)) = (id, report) else {
                tracing::error!(key = ?record.key(), "fail to parse invite report");
                return Ok(());
            };
            crate::invite::manage_report(
                &Pool::from_ref(state),
                &state.moderation,
                report.to_user_id,
                id,
            )
            .await
        }
        Some(REPORT_PROFILE_TASK) => {
            let Some(profile_id) = record.key().and_then(|key| Uuid::from_slice(key).ok()) else {
                tracing::error!(key = ?record.key(), "fail to parse profile report");
                return Ok(());
            };
            crate::profile::manage_report(&Pool::from_ref(state), &state.moderation, profile_id)
                .await
        }
        // only the key: the post is read as it is now, and a deleted one's value is null
        Some(INDEX_POST_TASK) => {
            let Some(post_id) = record.key().and_then(|key| Uuid::from_slice(key).ok()) else {
                tracing::error!(key = ?record.key(), "fail to parse post to index");
                return Ok(());
            };
            crate::post::index(
                &Pool::from_ref(state),
                &state.embedding,
                &state.milvus,
                post_id,
            )
            .await
        }
        task_type => {
            tracing::warn!(task_type, "skip a record of an unknown task type");
            return Ok(());
        }
    };
    let Err(reason) = result else {
        return Ok(());
    };
    tracing::warn!(
        topic = record.topic(),
        partition = record.partition(),
        offset = record.offset(),
        reason,
        "record goes to the retry job"
    );
    let event = match retry_partition_id {
        // a retried record keeps the id of its first failure, the job counts its tries by it
        Some(partition_id) => RetryEvent {
            partition_id,
            reason,
            ..Default::default()
        },
        None => RetryEvent {
            partition_id: format!(
                "{GROUP_ID}:{}:{}:{}",
                record.topic(),
                record.partition(),
                record.offset()
            ),
            reason,
            backoff: Some(add_jitter(2_000)),
            multiplier: Some(2),
            cap: Some(15_000_000),
            max_failure: Some(5),
            topic: Some(record.topic().to_string()),
            key: record.key().map(|key| base64(key)),
            headers: Some(
                record
                    .headers()
                    .into_iter()
                    .filter(|header| header.key() != PARTITION_ID_HEADER)
                    .map(|header| {
                        (
                            header.key().to_string(),
                            base64(header.value().unwrap_or_default()),
                        )
                    })
                    .collect(),
            ),
            value: record.value().map(|value| base64(value)),
        },
    };
    let value = serde_json::to_vec(&event).map_err(|err| err.to_string())?;
    // keyed by partition id so the events of one retry stay ordered in the job
    producer::send(
        &producer,
        RETRY_TOPIC,
        event.partition_id.as_bytes(),
        Some(&value),
        vec![],
    )
    .await
    .map_err(|err| err.to_string())
}

/// `RetryEvent` of howwasread-server's common package, `IncomingEvent` of the retry job. its byte arrays are
/// jackson's, base64. the first failure of a record carries how to retry it and the record itself, a later one only
/// its partitionId and reason
#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct RetryEvent {
    /// "<group>:<topic>:<partition>:<offset>" of the first failed record, the job's state key
    partition_id: String,
    reason: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    backoff: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    multiplier: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cap: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_failure: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    topic: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    key: Option<String>,
    /// the record's headers but "partitionId"
    #[serde(skip_serializing_if = "Option::is_none")]
    headers: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<String>,
}

fn base64(bytes: &[u8]) -> String {
    BASE64_STANDARD.encode(bytes)
}

/// base ± 10%, so records that failed together aren't retried together. the clock's nanoseconds are random enough
/// for that
fn add_jitter(base: i64) -> i64 {
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|now| now.subsec_nanos())
        .unwrap_or_default();
    // -1.0 to 1.0
    let multiplier = f64::from(nanos % 2_000_001) / 1_000_000.0 - 1.0;
    base + (base as f64 * 0.10 * multiplier) as i64
}
