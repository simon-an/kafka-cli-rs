use apache_avro::{Reader, Schema};
use kafka_config::SchemaRegistryConfig;
use log::{error, info, warn};
use opentelemetry::{
    global,
    trace::{Span, Tracer},
    Context,
};
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::*;
use rdkafka::error::KafkaResult;
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::Message;
use schema_registry::schema_from_file;
use schema_registry_converter::async_impl::{
    avro::AvroDecoder,
    schema_registry::{SrSettings, SrSettingsBuilder},
};

use std::time::Duration;

pub struct SchemaRegistry<'a> {
    // url: url::Url,
    // subject_name_strategy: SubjectNameStrategy,
    // schema_id: u32,
    avro_decoder: AvroDecoder<'a>,
    // sr_settings: SrSettings,
    // schema: Schema,
}

pub struct KafkaConsumer<'a> {
    // client: ClientConfig,
    topic: String,
    partition: Option<i32>,
    offset: Option<i64>,
    // key_file: Option<PathBuf>,
    // consumer_group_id: String,
    // consumer_group_instance_id: Option<String>,
    schema_registry: Option<SchemaRegistry<'a>>,
    // key_schema: Option<Schema>,
    value_schema: Option<Schema>,
    consumer: LoggingConsumer,
}

impl KafkaConsumer<'_> {
    pub fn new(
        config: &kafka_config::KafkaConfig,
        consumer_config: kafka_config::consumer::ConsumerConfig,
    ) -> Self {
        let client: ClientConfig = config.clone().into();
        let context = CustomContext;
        log::info!(
            "consuming with group.id: {}",
            consumer_config.consumer_group_id.clone()
        );
        let consumer: LoggingConsumer = client
            .clone()
            .set("group.id", consumer_config.consumer_group_id.clone())
            .set("client.id", consumer_config.consumer_group_id.clone())
            .set(
                "group.instance.id",
                consumer_config
                    .consumer_group_instance_id
                    .clone()
                    .clone()
                    .unwrap_or(uuid::Uuid::new_v4().to_string()),
            )
            // .set("bootstrap.servers", brokers)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "true") // TODO do we want to commit?
            .set("enable.auto.offset.store", "true") // TODO do we want to commit?
            // .set("delivery.timeout.ms", "1000") // THIS IS THE DEFAULT IN KafkaConfig
            //.set("statistics.interval.ms", "30000")
            .set("auto.offset.reset", "latest")
            // .set("auto.offset.reset", "earliest")
            .set_log_level(RDKafkaLogLevel::Debug)
            .create_with_context(context)
            .expect("Consumer creation failed");

        Self {
            consumer,
            topic: consumer_config.topic.clone(),
            offset: consumer_config.offset.clone(),
            // client: config.clone().into(),
            // key_file: key_file.clone(),
            // consumer_group_id: consumer_config.consumer_group_id.clone(),
            // consumer_group_instance_id: consumer_config.consumer_group_instance_id.clone(),
            partition: consumer_config.partition.clone(),
            value_schema: consumer_config
                .value_schema_file
                .clone()
                .map(|path| schema_from_file(path)),
            // key_schema: consumer_config
            //     .key_schema_file
            //     .clone()
            //     .map(|path| schema_from_file(path)),
            schema_registry: config
                .schema_registry
                .clone()
                .map(|src: SchemaRegistryConfig| {
                    let sr_settings: SrSettings = SrSettingsBuilder::from(src)
                        .build()
                        .expect("Failed to build schema registry settings");
                    let avro_decoder = AvroDecoder::new(sr_settings.clone());
                    // let schema = std::fs::read_to_string("resources/msg.avro")
                    //     .expect("Should have been able to read the file");
                    // let schema: Schema = Schema::parse_str(&schema).unwrap();
                    // let subject_name_strategy =
                    //     SubjectNameStrategy::TopicNameStrategy(topic.clone(), false);
                    // upload schema!
                    // let subject_name_strategy = SubjectNameStrategy::TopicNameStrategyWithSchema(
                    //     topic.clone(),
                    //     true,
                    //     get_supplied_schema(&schema),
                    // );

                    SchemaRegistry {
                        // url: endpoint.clone(),
                        // schema,
                        // sr_settings,
                        // schema_id: id,
                        // subject_name_strategy,
                        avro_decoder,
                    }
                }),
        }
    }
    pub async fn get_commited_offset(&self) -> anyhow::Result<i64> {
        let mut l = TopicPartitionList::new();
        l.add_partition(&self.topic, 0);
        let dur = Duration::from_secs(30);
        let tpl = if let Ok(tpl) = self.consumer.committed_offsets(l, dur) {
            tpl
        } else {
            warn!("No committed offsets found, returning 0");
            return Ok(0);
        };
        Ok(tpl
            .find_partition(&self.topic, 0) // TODO allow for multiple partitions
            .expect("Partition 0 not found. Please reconfigure topic or tests.")
            .offset()
            .to_raw()
            .unwrap_or(0))
    }
    pub async fn get_current_offset(&self) -> anyhow::Result<i64> {
        let mut l = TopicPartitionList::new();
        l.add_partition(&self.topic, 0);
        let dur = Duration::from_secs(30);
        self.consumer.assign(&l).expect("assign must work");
        tokio::time::sleep(Duration::from_secs(2)).await;
        let (earliest, latest) = self
            .consumer
            .fetch_watermarks(&self.topic, 0, dur)
            .expect("seek must work");
        info!("watermarks: {earliest} {latest}");
        Ok(latest)
    }
    pub async fn consume(&self, partition_offset: Option<(i32, i64)>) -> anyhow::Result<Vec<u8>> {
        let dur = Duration::from_secs(30);
        use tokio::time::timeout;

        if let Some((partition, offset)) = partition_offset {
            let mut l = TopicPartitionList::new();
            l.add_partition(&self.topic, partition);
            self.consumer.assign(&l).expect("assign must work");
            tokio::time::sleep(Duration::from_millis(10000)).await;
            self.consumer
                .seek(
                    self.topic.as_str(),
                    partition,
                    rdkafka::Offset::Offset(offset),
                    dur,
                )
                .expect("seek must work");
        } else {
            let mut l = TopicPartitionList::new();
            if let Some(partition) = &self.partition {
                l.add_partition(&self.topic, *partition);
            } else {
                l.add_partition(&self.topic, 0);
            }
            let offset = if let Some(offset) = &self.offset {
                info!(
                    "assign the consumer to consume from the offset provided from config: {offset}"
                );
                rdkafka::Offset::Offset(*offset)
            } else {
                info!("assign the consumer to consume from latest offset from high watermark");
                let offset = self.get_current_offset().await.expect("Without any offset this will not work, because any other options (Beging, End, Stored) make no sense for tests");
                rdkafka::Offset::Offset(offset)
            };
            let _ = l
                .set_partition_offset(&self.topic, 0, offset)
                .expect("set offset must work");
            info!("set offset to {:?}", offset);
            self.consumer.assign(&l).expect("assign must work");
        }

        let future = timeout(dur, self.consumer.recv());
        match future.await.expect("timeout waiting for a result for 10s") {
            Err(e) => panic!("Kafka error: {}", e),
            Ok(message) => {
                let context = if let Some(headers) = message.headers() {
                    global::get_text_map_propagator(|propagator| {
                        propagator.extract(&schema_registry::HeaderExtractor(&headers))
                    })
                } else {
                    Context::current()
                };

                let mut span: global::BoxedSpan =
                    global::tracer("consumer").start_with_context("consume_payload", &context);

                log::info!(
                    "Recieved message at offset {} in partition {}, with key {:?}",
                    message.offset(),
                    message.partition(),
                    message.key()
                );

                let payload: serde_json::Value = if let Some(sr) = &self.schema_registry {
                    info!("Using avro encoder");
                    let value_result = match sr.avro_decoder.decode(message.payload()).await {
                        Ok(v) => v.value,
                        Err(e) => {
                            error!("Error getting value while decoding avro: {}", e);
                            panic!("Error getting value while decoding avro: {}", e);
                        }
                    };
                    value_result.try_into().unwrap()
                } else {
                    if let Some(schema) = self.value_schema.clone() {
                        info!("Using avro reader to decode message");
                        let reader = Reader::with_schema(&schema, &message.payload().unwrap()[..])
                            .expect("cannot create reader with value schema");
                        let value = reader
                            .into_iter()
                            .next()
                            .unwrap()
                            .expect("cannot get value from reader");
                        match apache_avro::from_value::<String>(&value) {
                            Ok(v) => v.try_into().unwrap(),
                            Err(e) => {
                                error!("Error getting value while decoding avro: {}", e);
                                panic!("Error getting value while decoding avro: {}", e);
                            }
                        }
                    } else {
                        info!("Using json decoder");
                        serde_json::from_slice(message.payload().unwrap()).unwrap()
                    }
                };
                // self.consumer
                //     .commit_message(&message, CommitMode::Async)
                //     .unwrap();
                span.end();

                Ok(payload.to_string().into_bytes())
            }
        }
    }
}

struct CustomContext;

impl ClientContext for CustomContext {
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = false;
}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        info!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        info!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        info!("Committing offsets: {:?}", result);
    }
}

// A type alias with your custom consumer can be created for convenience.
type LoggingConsumer = StreamConsumer<CustomContext>;
