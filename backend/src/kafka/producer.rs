//! Sends to the topics: the post reports, and the failed records to the retry job
use std::sync::Arc;

use confluent_kafka::{
    common::{
        Error,
        header::{RecordHeader, RecordHeaders},
        serialization::ByteArraySerializer,
    },
    producer::{KafkaProducer, ProducerConfig, ProducerRecord},
};

use super::client_props;

pub type Producer = Arc<KafkaProducer<Vec<u8>, Vec<u8>>>;

pub fn new(client_id: &str) -> Producer {
    let mut props = client_props(client_id);
    // a report is sent once and waited for, so the broker has it before the request answers
    props.insert("acks".into(), "all".into());
    let config = ProducerConfig::new(&props).expect("invalid kafka producer config");
    Arc::new(
        KafkaProducer::new(
            config,
            ByteArraySerializer::new(),
            ByteArraySerializer::new(),
        )
        .expect("fail to create kafka producer"),
    )
}

/// Sends one record and waits until the broker has it
pub async fn send(
    producer: &KafkaProducer<Vec<u8>, Vec<u8>>,
    topic: &str,
    key: &[u8],
    value: Option<&[u8]>,
    headers: Vec<(String, Vec<u8>)>,
) -> Result<(), Error> {
    let headers = RecordHeaders::with_header_iter(
        headers
            .into_iter()
            .map(|(key, value)| RecordHeader::new(key, Some(value))),
    );
    let record = ProducerRecord::with_partition_key_headers(
        topic.to_string(),
        None,
        Some(key),
        value,
        headers,
    )
    .map_err(|err| Error::local_illegal_argument(err.to_string()))?;
    producer.send(record, None).await?.get().await?;
    Ok(())
}
