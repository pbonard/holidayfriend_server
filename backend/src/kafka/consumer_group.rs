//! The counterpart of sarama's consumer group, which confluent-kafka doesn't have: one consumer polls, and each
//! partition's records go to its own worker in order, as sarama's ConsumeClaim goroutine per partition. what the
//! workers finished is committed every second, a full worker's partition is paused, a revoked partition's worker is
//! cancelled and what it finished committed, and shutdown leaves the group. what a record means is the handler's
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use confluent_kafka::{
    common::{Error, TopicPartition, serialization::ByteArrayDeserializer},
    consumer::{
        CloseOptions, Consumer, ConsumerConfig, ConsumerHandle, ConsumerRebalanceListener,
        ConsumerRecord, KafkaConsumer, OffsetAndMetadata,
    },
};
use tokio::{
    sync::mpsc::{self, error::TrySendError},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use super::client_props;

/// What a record means, as sarama's ConsumerGroupHandler: `run` gives it each record of a partition, in order
pub trait ConsumerGroupHandler: Send + Sync + 'static {
    /// One record. an error means it could neither be handled nor put aside (e.g. to a retry topic): its partition
    /// is read again from it after STUCK_BACKOFF. it can be dropped at any await by a rebalance or shutdown, and a
    /// record can come twice, so what it does has to be safe to do again
    fn handle(
        &self,
        record: &ConsumerRecord<Vec<u8>, Vec<u8>>,
    ) -> impl Future<Output = Result<(), String>> + Send;
}

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
/// room than it, so a poll doesn't fill a queue
const MAX_POLL_RECORDS: usize = 100;
/// How often what the workers finished is committed, as sarama's auto-commit interval
const COMMIT_INTERVAL: Duration = Duration::from_secs(1);
/// How long a worker whose handler gave an error waits before its partition is read again
const STUCK_BACKOFF: Duration = Duration::from_secs(5);
/// How long a commit may take, retries included. the crate's default is 60 seconds, longer than the pod's 30 seconds of
/// grace, and a poll loop in a commit doesn't see the shutdown
const COMMIT_TIMEOUT: Duration = Duration::from_secs(5);
/// How long leaving the group may take at shutdown, after the last commits, within the pod's 30 seconds of grace
const CLOSE_TIMEOUT: Duration = Duration::from_secs(10);

/// The worker of one partition: it takes the partition's records in order, as sarama's ConsumeClaim goroutine does
struct Worker {
    /// tells which worker a stuck record is from: a partition revoked and assigned again has a new one
    id: u64,
    records: mpsc::Sender<ConsumerRecord<Vec<u8>, Vec<u8>>>,
    /// cancelled when the partition is revoked or at shutdown, as sarama cancels the session's context: the record
    /// being handled is given up and read again by the partition's next owner, the queued ones are dropped
    cancel: CancellationToken,
    task: JoinHandle<()>,
}

impl Worker {
    /// It couldn't take another poll's records, or it gave up and closed its queue
    fn is_busy(&self) -> bool {
        self.records.is_closed() || self.records.capacity() < MAX_POLL_RECORDS
    }
}

/// A worker gave up on the record at `offset`: the handler gave an error for it
struct Stuck {
    partition: TopicPartition,
    worker_id: u64,
    offset: i64,
}

/// What the polling task, the workers and the rebalance listener share. never locked across an await, and the
/// polling task takes workers before paused when it needs both
#[derive(Default)]
struct Shared {
    workers: Mutex<HashMap<TopicPartition, Worker>>,
    /// the next offset of each partition, after the records its worker finished, not committed yet
    done: Mutex<HashMap<TopicPartition, i64>>,
    /// the partitions this consumer paused. kafka forgets a revoked partition's pause, the listener does too
    paused: Mutex<HashSet<TopicPartition>>,
}

/// Consumes `topic` as `group_id` until `shutdown`, as sarama's ConsumerGroup.Consume in its loop: this task polls,
/// and hands each partition's records to its own worker, which gives them to `handler` in order, so the partitions
/// are handled at once. a record is committed once `handler` took it, a record it gave an error for is read again
/// from its offset. at shutdown the workers stop, what they finished is committed and the group is left, as sarama's
/// Close. it returns before `shutdown` when a poll's error won't pass: the caller has to take that as a failure
pub async fn run<H: ConsumerGroupHandler>(
    group_id: &'static str,
    topic: &'static str,
    handler: H,
    shutdown: CancellationToken,
) {
    let handler = Arc::new(handler);
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
    // a stuck partition whose seek failed, tried again on the next round. it stays paused meanwhile
    let mut unsought: Vec<Stuck> = vec![];
    let mut next_worker_id = 0;
    let mut last_commit = Instant::now();
    while !shutdown.is_cancelled() {
        // a stuck worker's partition is read again from the record it gave up on. the worker stays, and its closed
        // queue keeps the partition paused, until the seek worked: a new worker takes it from there
        while let Ok(stuck) = stuck_receiver.try_recv() {
            unsought.push(stuck);
        }
        for stuck in std::mem::take(&mut unsought) {
            let current = shared
                .workers
                .lock()
                .unwrap()
                .get(&stuck.partition)
                .is_some_and(|worker| worker.id == stuck.worker_id);
            // revoked meanwhile, its next owner reads it from the last commit
            if !current {
                continue;
            }
            match consumer
                .seek_with_offset(stuck.partition.clone(), stuck.offset)
                .await
            {
                Ok(()) => {
                    shared.workers.lock().unwrap().remove(&stuck.partition);
                }
                Err(err) => {
                    tracing::error!(error = %err, topic, "fail to seek back to the record, tried again");
                    unsought.push(stuck);
                }
            }
        }

        // a partition whose worker couldn't take another poll waits for it, as sarama stops fetching a partition
        // whose channel is full. a pause or resume that failed is tried again on the next round
        let assignment = consumer.assignment();
        let (to_pause, to_resume) = {
            let workers = shared.workers.lock().unwrap();
            let mut paused = shared.paused.lock().unwrap();
            paused.retain(|partition| assignment.contains(partition));
            let mut to_pause = vec![];
            let mut to_resume = vec![];
            for partition in &assignment {
                let busy = workers.get(partition).is_some_and(Worker::is_busy);
                if busy && !paused.contains(partition) {
                    to_pause.push(partition.clone());
                } else if !busy && paused.contains(partition) {
                    to_resume.push(partition.clone());
                }
            }
            (to_pause, to_resume)
        };
        if !to_pause.is_empty() {
            match consumer.pause(&to_pause).await {
                Ok(()) => shared.paused.lock().unwrap().extend(to_pause),
                Err(err) => tracing::error!(error = %err, topic, "fail to pause partitions"),
            }
        }
        if !to_resume.is_empty() {
            match consumer.resume(&to_resume).await {
                Ok(()) => {
                    let mut paused = shared.paused.lock().unwrap();
                    to_resume.iter().for_each(|partition| {
                        paused.remove(partition);
                    });
                }
                Err(err) => tracing::error!(error = %err, topic, "fail to resume partitions"),
            }
        }

        // the rebalance listener runs in here, when the assignment changes
        let records = match consumer.poll(Duration::from_secs(1)).await {
            Ok(records) => records,
            Err(err) if may_recover(&err) => {
                tracing::error!(error = %err, topic, "fail to poll, tried again");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
            // a member that got a fatal error is out of the group for good: the error comes once, and the polls after
            // it are only empty
            Err(err) => {
                tracing::error!(error = %err, topic, "fail to poll, the consumer stops");
                break;
            }
        };
        // a partition whose records this round can't go to its worker: the rest of them are read again
        let mut held_back: HashSet<TopicPartition> = HashSet::new();
        for record in records {
            let partition = TopicPartition::new(record.topic(), record.partition());
            if held_back.contains(&partition) {
                continue;
            }
            let sender = {
                let mut workers = shared.workers.lock().unwrap();
                workers
                    .entry(partition.clone())
                    .or_insert_with(|| {
                        next_worker_id += 1;
                        spawn_worker(
                            &handler,
                            &shared,
                            &stuck_sender,
                            partition.clone(),
                            next_worker_id,
                        )
                    })
                    .records
                    .clone()
            };
            // the poll loop doesn't wait on a worker, the other partitions would wait with it
            match sender.try_send(record) {
                Ok(()) => {}
                // the pause didn't take: read again from this record once the worker has room. waited for only
                // when even the seek failed, as a record can't be dropped
                Err(TrySendError::Full(record)) => {
                    match consumer
                        .seek_with_offset(partition.clone(), record.offset())
                        .await
                    {
                        Ok(()) => {
                            held_back.insert(partition);
                        }
                        Err(err) => {
                            tracing::error!(error = %err, topic, "fail to seek back to a record a worker had no room for");
                            let _ = sender.send(record).await;
                        }
                    }
                }
                // a worker that gave up has closed its queue, its partition is sought back to the record it gave up
                // on
                Err(TrySendError::Closed(_)) => {
                    held_back.insert(partition);
                }
            }
        }

        // what the workers finished, as sarama's auto-commit does every second
        if last_commit.elapsed() >= COMMIT_INTERVAL {
            last_commit = Instant::now();
            commit_done(&mut consumer, &shared).await;
        }
    }

    // as sarama's Close: the workers stop, what they finished is committed, and the group is left so the partitions
    // go to the other members at once instead of after the session times out
    tracing::info!(topic, group_id, "kafka consumer shutting down");
    let workers: Vec<Worker> = shared
        .workers
        .lock()
        .unwrap()
        .drain()
        .map(|(_, worker)| worker)
        .collect();
    stop_workers(workers).await;
    commit_done(&mut consumer, &shared).await;
    if let Err(err) = consumer
        .close_with_options(CloseOptions::new_with_timeout(CLOSE_TIMEOUT))
        .await
    {
        tracing::error!(error = %err, topic, "fail to close the kafka consumer");
    }
}

/// Whether a later poll can work after `err`. a retriable one, and those that aren't but pass anyway: a member the
/// coordinator doesn't know or whose epoch is old joins again by itself, and the crate keeps the member when the topic
/// isn't authorized, for an ACL that comes later
fn may_recover(err: &Error) -> bool {
    err.is_retriable_error()
        || err.is_unknown_member_id_error()
        || err.is_stale_member_epoch_error()
        || err.is_topic_authorization_error()
}

/// Commits what the workers finished. a failed commit is kept for the next one while the partition is still this
/// consumer's, even when it has no worker for a moment (a stuck one was just replaced). a revoked one's is dropped:
/// read again and handled again then, so a task has to take a record twice
async fn commit_done(consumer: &mut KafkaConsumer<Vec<u8>, Vec<u8>>, shared: &Shared) {
    let offsets: HashMap<TopicPartition, i64> = std::mem::take(&mut *shared.done.lock().unwrap());
    if offsets.is_empty() {
        return;
    }
    if let Err(err) = consumer
        .commit_sync_with_offsets_timeout(offset_and_metadata(&offsets), COMMIT_TIMEOUT)
        .await
    {
        tracing::error!(error = %err, "fail to commit");
        let assignment = consumer.assignment();
        let mut done = shared.done.lock().unwrap();
        for (partition, offset) in offsets {
            if assignment.contains(&partition) {
                let next = done.entry(partition).or_insert(offset);
                *next = (*next).max(offset);
            }
        }
    }
}

/// Cancels every worker first and then waits for them, so each gives up what it's on at once instead of one after
/// another
async fn stop_workers(workers: Vec<Worker>) {
    for worker in &workers {
        worker.cancel.cancel();
    }
    for worker in workers {
        let _ = worker.task.await;
    }
}

fn spawn_worker<H: ConsumerGroupHandler>(
    handler: &Arc<H>,
    shared: &Arc<Shared>,
    stuck: &mpsc::UnboundedSender<Stuck>,
    partition: TopicPartition,
    id: u64,
) -> Worker {
    let (records, receiver) = mpsc::channel(MAX_POLL_RECORDS * 2);
    let cancel = CancellationToken::new();
    let work = tokio::spawn(work(
        handler.clone(),
        shared.clone(),
        stuck.clone(),
        partition.clone(),
        id,
        receiver,
        cancel.clone(),
    ));
    // a panic ends the process, as one in a ConsumeClaim goroutine does in go: its partition would be read by nobody,
    // its records dropped at its closed queue. the pod restarts and the partition is read from the last commit
    let task = tokio::spawn(async move {
        if let Err(err) = work.await
            && err.is_panic()
        {
            tracing::error!(error = %err, partition = partition.partition(), "kafka worker panicked");
            std::process::exit(1);
        }
    });
    Worker {
        id,
        records,
        cancel,
        task,
    }
}

/// A partition's records, in order, until it's cancelled or gives up on one
async fn work<H: ConsumerGroupHandler>(
    handler: Arc<H>,
    shared: Arc<Shared>,
    stuck: mpsc::UnboundedSender<Stuck>,
    partition: TopicPartition,
    id: u64,
    mut records: mpsc::Receiver<ConsumerRecord<Vec<u8>, Vec<u8>>>,
    cancel: CancellationToken,
) {
    loop {
        let record = tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            record = records.recv() => match record {
                Some(record) => record,
                None => return,
            },
        };
        // a cancelled handler is dropped at whichever await it's on, unlike sarama, which waits for the handler to
        // return on its cancelled context: a transaction it's in rolls back, an http call ends. so its side effects
        // have to be one transaction, or each one safe to do again: a step outside it can be cut off
        // halfway, and the record is handled again
        let result = tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            result = handler.handle(&record) => result,
        };
        if let Err(err) = result {
            tracing::error!(
                error = %err,
                topic = record.topic(),
                partition = record.partition(),
                offset = record.offset(),
                "fail to handle a record, its partition is read again from it"
            );
            // closed at once, so the partition is paused while this waits and nothing more is fetched for it
            drop(records);
            // whatever failed won't be back right away. a revoke doesn't wait for it
            tokio::select! {
                () = cancel.cancelled() => return,
                () = tokio::time::sleep(STUCK_BACKOFF) => {}
            }
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

/// What sarama does around a rebalance: a revoked partition's worker is cancelled, and what it finished is
/// committed before the partition goes, so its next owner starts after it. it runs in the polling task, inside poll,
/// and reaches the consumer through its handle
struct RebalanceListener {
    consumer: ConsumerHandle,
    shared: Arc<Shared>,
}

impl RebalanceListener {
    /// The partitions' workers, which no longer belong here. kafka forgets their pause, so this does too
    fn take_workers(&self, partitions: &[TopicPartition]) -> Vec<Worker> {
        let workers = {
            let mut workers = self.shared.workers.lock().unwrap();
            partitions
                .iter()
                .filter_map(|partition| workers.remove(partition))
                .collect()
        };
        self.shared
            .paused
            .lock()
            .unwrap()
            .retain(|partition| !partitions.contains(partition));
        workers
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
        stop_workers(self.take_workers(partitions)).await;
        // after the workers stopped, so nothing finishes after it's taken
        let offsets = self.take_done(partitions);
        if !offsets.is_empty()
            && let Err(err) = self
                .consumer
                .commit_sync_with_offsets_timeout(offset_and_metadata(&offsets), COMMIT_TIMEOUT)
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
        // already someone else's, nothing can be committed: the workers are only stopped
        stop_workers(self.take_workers(partitions)).await;
        self.take_done(partitions);
        Ok(())
    }
}
