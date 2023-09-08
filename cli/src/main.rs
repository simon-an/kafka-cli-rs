use std::{io::BufReader, time::Duration};

use apache_avro::Schema;
use args::{SchemaUploadConfig, TestConfig};
use clap::Parser;
use env_logger::{Builder, Target};
use kafka_config::{consumer::ConsumerConfig, producer::ProducerConfig, KafkaConfig};
use schema_registry::register_schema;
use serde_json::Value;

#[tokio::main]
async fn main() {
    let args = args::Cli::parse();

    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }

    let mut builder = Builder::from_default_env();
    match args.log_output {
        args::LogOutput::StdOut => {
            builder.target(Target::Stdout);
        }
        args::LogOutput::StdErr => {
            builder.target(Target::Stderr);
        }
    }
    builder.init();

    let kafka_config = kafka_config::KafkaConfig::from_env().unwrap();

    match args.action {
        args::Action::UploadSchema(SchemaUploadConfig {
            value_schema_file,
            key_schema_file,
            subject,
        }) => {
            if let Some(file) = key_schema_file {
                let schema =
                    std::fs::read_to_string(file).expect("Should have been able to read the file");
                let schema: Schema = Schema::parse_str(&schema).unwrap();
                let id = register_schema(
                    kafka_config
                        .schema_registry
                        .clone()
                        .expect("schema-registry-config must exist to upload schemas")
                        .endpoint
                        .to_string(),
                    format!("{}-key", subject),
                    schema,
                )
                .await
                .expect("Upload key schema failed");
                log::info!("New key schema id: {}", id.id);
            }
            let schema = std::fs::read_to_string(value_schema_file)
                .expect("Should have been able to read the file");
            let value_schema: Schema = Schema::parse_str(&schema).unwrap();
            let id = register_schema(
                kafka_config
                    .schema_registry
                    .expect("schema-registry-config must exist to upload schemas")
                    .endpoint
                    .to_string(),
                format!("{}-value", subject),
                value_schema,
            )
            .await
            .expect("schema upload failed");
            log::info!("New value schema id: {}", id.id);
        }
        args::Action::Consume(args) => {
            let consumer = kafka_consumer::KafkaConsumer::new(&kafka_config, args.into());
            match consumer.consume(None).await {
                Ok(_) => {
                    log::info!("Integration test passed");
                }
                Err(e) => {
                    log::error!("Integration test failed: {:?}", e);
                    panic!("Integration test failed: {:?}", e);
                }
            }
        }
        args::Action::Produce(args) => {
            let producer = kafka_producer::KafkaProducer::new(&kafka_config, args.into());
            match producer.produce().await {
                Ok(_) => {
                    log::info!("Message produced");
                }
                Err(e) => {
                    log::error!("Message not produced: {:?}", e);
                    panic!("Message not produced: {:?}", e);
                }
            }
        }
        args::Action::IntegrationTest(config) => {
            log::info!("IntegrationTest: {:?}", config);
            let integration_test_config: TestConfig = config.into();
            test(integration_test_config, kafka_config).await;
        }
        args::Action::IntegrationTestFiles(config) => {
            log::info!("IntegrationTestFiles: {:?}", config);
            let integration_test_config: TestConfig = config.into();
            test(integration_test_config, kafka_config).await;
        }
        args::Action::IntegrationTestFolder(config) => {
            log::info!("IntegrationTestFolder: {:?}", config);
            let integration_test_config: TestConfig = config.into();
            test(integration_test_config, kafka_config).await;
        }
    }
}

async fn test(integration_test_config: TestConfig, kafka_config: KafkaConfig) {
    let pc: ProducerConfig = integration_test_config.producer.into();
    let cc: ConsumerConfig = integration_test_config.consumer.into();
    let use_offset = cc.topic == pc.topic;
    let producer = kafka_producer::KafkaProducer::new(&kafka_config, pc);
    let consumer = kafka_consumer::KafkaConsumer::new(&kafka_config, cc);
    match producer.produce().await {
        Ok((partition, offset)) => {
            log::info!("Message produced at partiton {partition} and offset {offset}");
            tokio::time::sleep(Duration::from_millis(500)).await;
            let offset = if use_offset == true {
                Some((partition, offset))
            } else {
                None
            };
            match consumer.consume(offset).await {
                Ok(msg) => {
                    let expect = serde_json::from_slice::<Value>(
                        std::fs::read(integration_test_config.assertion_value_file)
                            .unwrap()
                            .as_slice(),
                    )
                    .unwrap();
                    let msg = if let Ok(test) = serde_json::from_slice::<Value>(msg.as_slice()) {
                        log::info!("json result {test}");
                        test
                    } else {
                        let reader = BufReader::new(msg.as_slice());
                        // let mut reader = Reader::with_schema(&schema1, reader).unwrap();
                        let mut reader: apache_avro::Reader<'_, BufReader<_>> =
                            apache_avro::Reader::new(reader).unwrap();
                        let value = reader.next().unwrap().unwrap();
                        let value: Value = value.try_into().expect("cannot convert avro to json");
                        log::info!("msg {value}");
                        value
                    };
                    log::info!("expect {expect}");
                    assert_eq!(msg, expect);
                    log::info!("Integration test passed");
                }
                Err(e) => {
                    log::error!("Integration test failed: {:?}", e);
                    panic!("Integration test failed: {:?}", e);
                }
            }
        }
        Err(e) => {
            log::error!("Message not produced: {:?}", e);
            panic!("Message not produced: {:?}", e);
        }
    }
}
