//! Consumes the topic as sarama's consumer group does, a worker per partition: a post report is evaluated, and
//! handed to the retry job when it can't be
use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use axum::extract::FromRef;
use base64::{Engine, prelude::BASE64_STANDARD};
use confluent_kafka::{
    common::{Error, TopicPartition, header::Header, serialization::ByteArrayDeserializer},
    consumer::{
        Consumer, ConsumerConfig, ConsumerHandle, ConsumerRebalanceListener, ConsumerRecord,
        KafkaConsumer, OffsetAndMetadata,
    },
};
use mysql_async::Pool;
use serde::Serialize;
use tokio::{sync::mpsc, task::JoinHandle};
use uuid::Uuid;

use super::{
    REPORT_POST_TASK, TASK_TYPE_HEADER, TOPIC, client_props,
    producer::{self, Producer},
};
use crate::AppState;

/// The consumer group, and the prefix of the retry job's partitionId of this group's failed records
const GROUP_ID: &str = "holidayfriend";
/// The topic of the exponential-backoff-retry job (my-server), shared with the other apps: it sends a failed record
/// back to its topic later, with the "partitionId" header, and to dlq after the last try
const RETRY_TOPIC: &str = "exponential-backoff-retry";
/// The header the retry job sets on a record it sends again
const PARTITION_ID_HEADER: &str = "partitionId";

/// A consumer of `group_id` that commits by itself, after a record is handled, and starts a new group from the
/// oldest record so what came while it was down isn't skipped
fn new(group_id: &str) -> KafkaConsumer<Vec<u8>, Vec<u8>> {
    let mut props = client_props(group_id);
    props.extend([
        ("group.id".into(), group_id.into()),
        // the only protocol the crate has, KIP-848. the crate's default is classic, which it refuses
        ("group.protocol".into(), "consumer".into()),
        ("enable.auto.commit".into(), "false".into()),
        ("auto.offset.reset".into(), "earliest".into()),
        ("max.poll.records".into(), MAX_POLL_RECORDS.to_string()),
    ]);
    let config = ConsumerConfig::new(&props).expect("invalid kafka consumer config");
    KafkaConsumer::new(
        config,
        ByteArrayDeserializer::new(),
        ByteArrayDeserializer::new(),
    )
    .expect("fail to create kafka consumer")
}

/// Records a poll gives at most. a worker's queue is twice it, and a partition is paused while its queue has less
/// room than it, so a poll never fills a queue
const MAX_POLL_RECORDS: usize = 100;

/// The worker of one partition: it takes the partition's records in order, as sarama's ConsumeClaim goroutine does
struct Worker {
    /// tells which worker a stuck report is from: a partition revoked and assigned again has a new one
    id: u64,
    records: mpsc::Sender<ConsumerRecord<Vec<u8>, Vec<u8>>>,
    /// set when the partition is revoked: the record being handled is finished, the queued ones are dropped
    stop: Arc<AtomicBool>,
    task: JoinHandle<()>,
}

/// A worker gave up on the record at `offset`: it couldn't be handled or handed to the retry job
struct Stuck {
    partition: TopicPartition,
    worker_id: u64,
    offset: i64,
}

/// What the polling task, the workers and the rebalance listener share. never locked across an await
#[derive(Default)]
struct Shared {
    workers: Mutex<HashMap<TopicPartition, Worker>>,
    /// the next offset of each partition, after the records its worker finished, not committed yet
    done: Mutex<HashMap<TopicPartition, i64>>,
}

/// Consumes the topic until the process ends, as sarama's consumer group does: this task polls, and hands each
/// partition's records to its own worker, so the partitions are handled at once and each in order. a record is
/// committed once it's handled or handed to the retry job, a record neither could take is read again from its offset
pub async fn consume(state: AppState) {
    let (group_id, topic) = (GROUP_ID, TOPIC);
    let mut consumer = new(group_id);
    let shared = Arc::new(Shared::default());
    let listener = Arc::new(RebalanceListener {
        consumer: consumer.handle(),
        shared: shared.clone(),
    });
    consumer
        .subscribe_with_topics_listener(vec![topic.to_string()], listener)
        .await
        .unwrap_or_else(|err| panic!("fail to subscribe to {topic}: {err}"));
    tracing::info!(topic, group_id, "kafka consumer up and running");

    let (stuck_sender, mut stuck_receiver) = mpsc::unbounded_channel::<Stuck>();
    let mut next_worker_id = 0;
    let mut paused: HashSet<TopicPartition> = HashSet::new();
    loop {
        // a stuck worker's partition is read again from the record it gave up on, by a new worker
        while let Ok(Stuck {
            partition,
            worker_id,
            offset,
        }) = stuck_receiver.try_recv()
        {
            let removed = {
                let mut workers = shared.workers.lock().unwrap();
                match workers.get(&partition) {
                    Some(worker) if worker.id == worker_id => workers.remove(&partition),
                    // revoked meanwhile, its new owner reads it from the last commit
                    _ => None,
                }
            };
            if removed.is_none() {
                continue;
            }
            if let Err(err) = consumer.seek_with_offset(partition.clone(), offset).await {
                tracing::error!(error = %err, topic, "fail to seek back to the record");
            }
            if paused.remove(&partition)
                && let Err(err) = consumer.resume(std::slice::from_ref(&partition)).await
            {
                tracing::error!(error = %err, topic, "fail to resume a partition");
            }
        }

        // a partition whose worker couldn't take another poll waits for it, as sarama stops fetching a partition
        // whose channel is full
        let (to_pause, to_resume) = {
            let workers = shared.workers.lock().unwrap();
            // a revoked partition isn't paused any more
            paused.retain(|partition| workers.contains_key(partition));
            let mut to_pause = vec![];
            let mut to_resume = vec![];
            for (partition, worker) in workers.iter() {
                let full = worker.records.capacity() < MAX_POLL_RECORDS;
                if full && !paused.contains(partition) {
                    to_pause.push(partition.clone());
                } else if !full && paused.contains(partition) {
                    to_resume.push(partition.clone());
                }
            }
            (to_pause, to_resume)
        };
        if !to_pause.is_empty() {
            match consumer.pause(&to_pause).await {
                Ok(()) => paused.extend(to_pause),
                Err(err) => tracing::error!(error = %err, topic, "fail to pause partitions"),
            }
        }
        if !to_resume.is_empty() {
            match consumer.resume(&to_resume).await {
                Ok(()) => to_resume.iter().for_each(|partition| {
                    paused.remove(partition);
                }),
                Err(err) => tracing::error!(error = %err, topic, "fail to resume partitions"),
            }
        }

        // the rebalance listener runs in here, when the assignment changes
        let records = match consumer.poll(Duration::from_secs(1)).await {
            Ok(records) => records,
            Err(err) => {
                tracing::error!(error = %err, topic, "fail to poll");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        for record in records {
            let partition = TopicPartition::new(record.topic(), record.partition());
            let sender = {
                let mut workers = shared.workers.lock().unwrap();
                workers
                    .entry(partition.clone())
                    .or_insert_with(|| {
                        next_worker_id += 1;
                        spawn_worker(&state, &shared, &stuck_sender, partition, next_worker_id)
                    })
                    .records
                    .clone()
            };
            // never waits, the partition was paused before its queue could fill. a worker that gave up has closed
            // its queue, its partition is sought back to the record it gave up on
            let _ = sender.send(record).await;
        }

        // what the workers finished, as sarama's auto-commit does every second
        let offsets: HashMap<TopicPartition, i64> =
            std::mem::take(&mut *shared.done.lock().unwrap());
        if !offsets.is_empty()
            && let Err(err) = consumer
                .commit_sync_with_offsets(offset_and_metadata(&offsets))
                .await
        {
            // committed with the next, unless revoked meanwhile: read again and handled again then, so a task has
            // to take a record twice
            tracing::error!(error = %err, topic, "fail to commit");
            let workers = shared.workers.lock().unwrap();
            let mut done = shared.done.lock().unwrap();
            for (partition, offset) in offsets {
                if workers.contains_key(&partition) {
                    let next = done.entry(partition).or_insert(offset);
                    *next = (*next).max(offset);
                }
            }
        }
    }
}

fn spawn_worker(
    state: &AppState,
    shared: &Arc<Shared>,
    stuck: &mpsc::UnboundedSender<Stuck>,
    partition: TopicPartition,
    id: u64,
) -> Worker {
    let (records, receiver) = mpsc::channel(MAX_POLL_RECORDS * 2);
    let stop = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn(work(
        state.clone(),
        shared.clone(),
        stuck.clone(),
        partition,
        id,
        receiver,
        stop.clone(),
    ));
    Worker {
        id,
        records,
        stop,
        task,
    }
}

/// A partition's records, in order, until its queue is closed or the partition is revoked
async fn work(
    state: AppState,
    shared: Arc<Shared>,
    stuck: mpsc::UnboundedSender<Stuck>,
    partition: TopicPartition,
    id: u64,
    mut records: mpsc::Receiver<ConsumerRecord<Vec<u8>, Vec<u8>>>,
    stop: Arc<AtomicBool>,
) {
    while let Some(record) = records.recv().await {
        if stop.load(Ordering::Relaxed) {
            return;
        }
        if let Err(err) = handle(&state, &record).await {
            tracing::error!(
                error = %err,
                topic = record.topic(),
                partition = record.partition(),
                offset = record.offset(),
                "fail to hand a record to the retry job, it's read again"
            );
            // the retry topic or the broker is down, no use asking again right away
            tokio::time::sleep(Duration::from_secs(5)).await;
            let _ = stuck.send(Stuck {
                partition,
                worker_id: id,
                offset: record.offset(),
            });
            return;
        }
        shared
            .done
            .lock()
            .unwrap()
            .insert(partition.clone(), record.offset() + 1);
    }
}

fn offset_and_metadata(
    offsets: &HashMap<TopicPartition, i64>,
) -> HashMap<TopicPartition, OffsetAndMetadata> {
    offsets
        .iter()
        .map(|(partition, offset)| {
            (
                partition.clone(),
                OffsetAndMetadata::new(*offset).expect("an offset is never negative"),
            )
        })
        .collect()
}

/// What sarama does around a rebalance: a revoked partition's worker finishes the record it's on, and what it
/// finished is committed before the partition goes, so its next owner starts after it. it runs in the polling task,
/// inside poll, and reaches the consumer through its handle
struct RebalanceListener {
    consumer: ConsumerHandle,
    shared: Arc<Shared>,
}

impl RebalanceListener {
    fn take_workers(&self, partitions: &[TopicPartition]) -> Vec<Worker> {
        let mut workers = self.shared.workers.lock().unwrap();
        partitions
            .iter()
            .filter_map(|partition| workers.remove(partition))
            .collect()
    }

    fn take_done(&self, partitions: &[TopicPartition]) -> HashMap<TopicPartition, i64> {
        let mut done = self.shared.done.lock().unwrap();
        partitions
            .iter()
            .filter_map(|partition| done.remove_entry(partition))
            .collect()
    }
}

#[async_trait]
impl ConsumerRebalanceListener for RebalanceListener {
    async fn on_partitions_revoked(&self, partitions: &[TopicPartition]) -> Result<(), Error> {
        for Worker {
            records,
            stop,
            task,
            ..
        } in self.take_workers(partitions)
        {
            stop.store(true, Ordering::Relaxed);
            // closes the queue, the worker ends after the record it's on
            drop(records);
            let _ = task.await;
        }
        let offsets = self.take_done(partitions);
        if !offsets.is_empty()
            && let Err(err) = self
                .consumer
                .commit_sync_with_offsets(offset_and_metadata(&offsets))
                .await
        {
            // its next owner reads them again from the last commit
            tracing::error!(error = %err, "fail to commit revoked partitions");
        }
        Ok(())
    }

    async fn on_partitions_assigned(&self, _partitions: &[TopicPartition]) -> Result<(), Error> {
        // a worker starts with its partition's first record
        Ok(())
    }

    async fn on_partitions_lost(&self, partitions: &[TopicPartition]) -> Result<(), Error> {
        // already someone else's, nothing can be committed: the workers are stopped where they are, a transaction
        // they're in rolls back
        for worker in self.take_workers(partitions) {
            worker.task.abort();
        }
        self.take_done(partitions);
        Ok(())
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
