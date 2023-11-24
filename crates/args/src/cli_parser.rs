use std::path::PathBuf;

use clap::{Args, Parser, ValueEnum};

use serde::Deserialize;

#[derive(Parser, Debug)]
#[command(name = "Kafka Cli")]
#[command(author)]
#[command(version)]
#[command(propagate_version = true)]
#[command(about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub action: Action,
    #[clap(value_enum, default_value_t=LogOutput::StdOut)]
    #[arg(short, long)]
    pub log_output: LogOutput,
}

#[derive(ValueEnum, Clone, Debug)]
pub enum LogOutput {
    StdOut,
    StdErr,
}

#[derive(clap::Subcommand, Debug)]
pub enum Action {
    Consume(ConsumerConfig),
    Produce(ProducerConfig),
    #[clap(alias = "test")]
    IntegrationTest(ClapOnlyTestConfig),
    #[clap(alias = "test-folder")]
    IntegrationTestFolder(TestFolder),
    #[clap(alias = "test-files")]
    IntegrationTestFiles(TestFiles),
    UploadSchema(SchemaUploadConfig),
}

#[derive(Args, Debug, Clone)]
pub struct TestFolder {
    pub folder: PathBuf,
    #[arg(short, long)]
    pub assertion_value_file: Option<PathBuf>,
}

#[derive(Args, Debug, Clone)]
pub struct TestFiles {
    #[arg(short, long)]
    pub consumer_config_file: PathBuf,
    #[arg(short, long)]
    pub producer_config_file: PathBuf,
    #[arg(short, long)]
    pub assertion_value_file: PathBuf,
}

#[derive(Args, Debug, Clone, Deserialize)]
pub struct ClapOnlyTestConfig {
    #[arg(short, long)]
    pub consumer_topic: String,
    #[arg(short, long)]
    pub producer_topic: String,
    // #[arg(short, long)]
    // pub consumer_key_file: PathBuf,
    // #[arg(short, long)]
    // pub consumer_value_file: PathBuf,
    #[arg(short, long)]
    pub producer_key_file: PathBuf,
    #[arg(short, long)]
    pub producer_value_file: PathBuf,

    #[arg(short, long)]
    pub assertion_value_file: Option<PathBuf>,
    //
    #[arg(short, long)]
    pub consumer_group_id: String,
    #[arg(short, long)]
    pub consumer_group_instance_id: Option<String>,

    #[arg(short, long)]
    pub consumer_partition: Option<i32>,
    #[arg(short, long)]
    pub consumer_key_schema_file: Option<PathBuf>,
    #[arg(short, long)]
    pub consumer_value_schema_file: Option<PathBuf>,
    #[arg(short, long)]
    pub consumer_key_schema_id: Option<u32>,
    #[arg(short, long)]
    pub consumer_value_schema_id: Option<u32>,
    #[arg(short, long)]
    pub producer_key_schema_file: Option<PathBuf>,
    #[arg(short, long)]
    pub producer_value_schema_file: Option<PathBuf>,
    #[arg(short, long)]
    pub producer_key_schema_id: Option<u32>,
    #[arg(short, long)]
    pub producer_value_schema_id: Option<u32>,
}
#[derive(Args, Debug, Deserialize)]
pub struct ConsumerConfig {
    #[arg(short, long)]
    pub topic: String,
    #[arg(short, long)]
    pub consumer_group_id: String,
    #[arg(short, long)]
    pub consumer_group_instance_id: Option<String>,
    #[arg(short, long)]
    pub partition: Option<i32>,
    #[arg(short, long)]
    pub offset: Option<i64>,
    #[arg(short, long)]
    pub key_schema_id: Option<u32>,
    #[arg(short, long)]
    pub value_schema_id: Option<u32>,
    #[arg(short, long)]
    pub key_schema_file: Option<PathBuf>,
    #[arg(short, long)]
    pub value_schema_file: Option<PathBuf>,
}
#[derive(Args, Debug, Deserialize)]
pub struct ProducerConfig {
    #[arg(short, long)]
    pub topic: String,
    #[arg(short, long)]
    pub key_file: PathBuf,
    #[arg(short, long)]
    pub value_file: PathBuf,
    #[arg(short, long)]
    pub key_schema_file: Option<PathBuf>,
    #[arg(short, long)]
    pub value_schema_file: Option<PathBuf>,
    #[arg(short, long)]
    pub key_schema_id: Option<u32>,
    #[arg(short, long)]
    pub value_schema_id: Option<u32>,
}
#[derive(Args, Debug)]
pub struct SchemaUploadConfig {
    #[arg(short, long)]
    pub subject: String,
    #[arg(short, long)]
    pub key_schema_file: Option<PathBuf>,
    #[arg(short, long)]
    pub value_schema_file: PathBuf,
}

impl From<ConsumerConfig> for kafka_config::consumer::ConsumerConfig {
    fn from(config: ConsumerConfig) -> Self {
        Self {
            topic: config.topic,
            consumer_group_id: config.consumer_group_id,
            partition: config.partition,
            key_schema_file: config.key_schema_file,
            value_schema_file: config.value_schema_file,
            key_schema_id: config.key_schema_id,
            value_schema_id: config.value_schema_id,
            consumer_group_instance_id: config.consumer_group_instance_id,
            offset: config.offset,
        }
    }
}

impl From<ProducerConfig> for kafka_config::producer::ProducerConfig {
    fn from(config: ProducerConfig) -> Self {
        Self {
            topic: config.topic,
            key_file: config.key_file,
            value_file: config.value_file,
            key_schema_file: config.key_schema_file,
            value_schema_file: config.value_schema_file,
            key_schema_id: config.key_schema_id,
            value_schema_id: config.value_schema_id,
        }
    }
}

impl From<TestConfig> for kafka_config::producer::ProducerConfig {
    fn from(config: TestConfig) -> Self {
        Self {
            ..config.producer.into()
        }
    }
}

impl From<TestConfig> for kafka_config::consumer::ConsumerConfig {
    fn from(config: TestConfig) -> Self {
        Self {
            ..config.consumer.into()
        }
    }
}

pub struct TestConfig {
    pub consumer: ConsumerConfig,
    pub producer: ProducerConfig,
    pub assertion_value_file: PathBuf,
}

impl TestConfig {
    pub fn new(
        consumer: ConsumerConfig,
        producer: ProducerConfig,
        assertion_value_file: PathBuf,
    ) -> Self {
        Self {
            consumer,
            producer,
            assertion_value_file,
        }
    }
}

impl From<ClapOnlyTestConfig> for TestConfig {
    fn from(value: ClapOnlyTestConfig) -> Self {
        Self {
            consumer: ConsumerConfig {
                topic: value.consumer_topic,
                consumer_group_id: value.consumer_group_id,
                partition: value.consumer_partition,
                offset: None,
                key_schema_id: value.consumer_key_schema_id,
                value_schema_id: value.consumer_value_schema_id,
                key_schema_file: value.consumer_key_schema_file,
                value_schema_file: value.consumer_value_schema_file,
                consumer_group_instance_id: value.consumer_group_instance_id,
            },
            producer: ProducerConfig {
                topic: value.producer_topic,
                key_file: value.producer_key_file,
                value_file: value.producer_value_file,
                key_schema_id: value.producer_key_schema_id,
                value_schema_id: value.producer_value_schema_id,
                key_schema_file: value.producer_key_schema_file,
                value_schema_file: value.producer_value_schema_file,
            },
            assertion_value_file: value
                .assertion_value_file
                .unwrap_or("assertion_value.json".into()),
        }
    }
}

impl From<TestFolder> for TestConfig {
    fn from(folder: TestFolder) -> Self {
        let consumer = ConsumerConfig::from_file(Some(folder.folder.clone()));
        let producer = ProducerConfig::from_file(Some(folder.folder.clone()));
        let assertion_value_file = folder
            .assertion_value_file
            .map(|f| folder.folder.join(f))
            .unwrap_or(folder.folder.join("assertion_value.json"));
        if !assertion_value_file.exists() {
            panic!(
                "Assertion value file does not exist: {:?}. Path must be relative to folder",
                assertion_value_file
            );
        }
        Self::new(consumer, producer, assertion_value_file)
    }
}
impl From<TestFiles> for TestConfig {
    fn from(c: TestFiles) -> Self {
        let TestFiles {
            consumer_config_file,
            producer_config_file,
            assertion_value_file,
        } = c;
        if !assertion_value_file.exists() {
            panic!("Assertion value file does not exist");
        }
        if !consumer_config_file.exists() {
            panic!("Assertion value file does not exist");
        }
        if !producer_config_file.exists() {
            panic!("Assertion value file does not exist");
        }
        let consumer = ConsumerConfig::from_file(Some(consumer_config_file));
        let producer = ProducerConfig::from_file(Some(producer_config_file));
        Self::new(consumer, producer, assertion_value_file)
    }
}
