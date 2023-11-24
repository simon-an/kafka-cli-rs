use apache_avro::types::Value;
use apache_avro::{Reader, Schema, Writer};
use core::panic;
use kafka_config::producer::ProducerConfig;
use kafka_config::KafkaConfig;
use opentelemetry::trace::{Span, TraceContextExt, Tracer};
use opentelemetry::{global, Context, Key, KeyValue, StringValue};
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::FutureProducer;
use rdkafka::producer::FutureRecord;
use rdkafka::util::Timeout;
use rdkafka::ClientConfig;
use schema_registry::schema_from_file;
use schema_registry_converter::async_impl::avro::AvroEncoder;
use schema_registry_converter::async_impl::schema_registry::{
    get_schema_by_id, get_schema_by_subject, SrSettings, SrSettingsBuilder,
};
use schema_registry_converter::schema_registry_common::{SchemaType, SubjectNameStrategy};
use std::fs;
use std::io::BufReader;
use std::path::PathBuf;
use tracing::{error, info, warn};

pub struct KafkaProducer<'a> {
    // client: ClientConfig,
    topic: String,
    value_file: PathBuf,
    key_file: PathBuf,
    schema_registry: Option<SchemaRegistry<'a>>,
    producer: FutureProducer,
    key_schema: Option<Schema>,
    value_schema: Option<Schema>,
}

pub struct SchemaRegistry<'a> {
    // url: url::Url,
    subject_name_strategy_value: SubjectNameStrategy,
    subject_name_strategy_key: SubjectNameStrategy,
    value_schema_id: Option<u32>,
    key_schema_id: Option<u32>,
    avro_encoder: AvroEncoder<'a>,
    sr_settings: SrSettings,
}

impl KafkaProducer<'_> {
    pub fn new(kafka_config: &KafkaConfig, producer_config: ProducerConfig) -> Self {
        let client: ClientConfig = kafka_config.clone().into();

        Self {
            topic: producer_config.topic.clone(),
            producer: client
                .clone()
                .set("client.id", "integration-tests")
                .create()
                .expect("Producer creation error"),
            // client,
            value_file: producer_config.value_file.clone(),
            key_file: producer_config.key_file.clone(),
            key_schema: producer_config
                .key_schema_file
                .clone()
                .map(|path| schema_from_file(path)),
            value_schema: producer_config
                .value_schema_file
                .clone()
                .map(|path| schema_from_file(path)),

            schema_registry: kafka_config.schema_registry.clone().map(|c| {
                let sr_settings: SrSettings = SrSettingsBuilder::from(c)
                    .build()
                    .expect("Failed to build schema registry settings");
                let avro_encoder = AvroEncoder::new(sr_settings.clone());

                SchemaRegistry {
                    // url: endpoint.clone(),
                    sr_settings,
                    value_schema_id: producer_config.value_schema_id.clone(),
                    key_schema_id: producer_config.key_schema_id.clone(),
                    subject_name_strategy_key: SubjectNameStrategy::TopicNameStrategy(
                        producer_config.topic.clone(),
                        true,
                    ),
                    subject_name_strategy_value: SubjectNameStrategy::TopicNameStrategy(
                        producer_config.topic.clone(),
                        false,
                    ),
                    avro_encoder,
                }
            }),
        }
    }
    pub async fn produce(&self) -> anyhow::Result<(i32, i64)> {
        let mut span = global::tracer("producer").start("produce_to_kafka");
        span.set_attribute(KeyValue {
            key: Key::new("topic"),
            value: opentelemetry::Value::String(StringValue::from(self.topic.clone())),
        });
        // Values might be sensitive, so we don't want to log them
        // span.set_attribute(KeyValue {
        //     key: Key::new("payload"),
        //     value: opentelemetry::Value::String(StringValue::from(
        //         serde_json::to_string(&payload).expect("Failed to serialize payload"),
        //     )),
        // });

        let context = Context::current_with_span(span);
        let mut headers = OwnedHeaders::new().insert(Header {
            key: "key",
            value: Some("value"),
        });
        global::get_text_map_propagator(|propagator| {
            propagator.inject_context(&context, &mut schema_registry::HeaderInjector(&mut headers))
        });

        let payload = self.encode_payload().await?;
        let key = self.encode_key().await?;

        let delivery_status = self
            .producer
            .send(
                FutureRecord::to(&self.topic)
                    .headers(headers)
                    .key(&key)
                    .payload(&payload),
                Timeout::After(std::time::Duration::from_secs(5)),
            )
            .await;

        match delivery_status {
            Ok(status) => {
                info!("message delivered");
                Ok(status)
            }
            Err(e) => {
                error!("{}", e.0.to_string());
                panic!("{}", e.0.to_string());
            }
        }
    }

    async fn encode_key(&self) -> anyhow::Result<Vec<u8>> {
        if let Some(schema) = &self.key_schema {
            if self.schema_registry.is_some() {
                warn!(
                    "schema registry is ignored, since a schema file is provided and will be used"
                );
            }
            let mut writer = Writer::new(&schema, Vec::new());
            let value: Value =
                avro_value_from_file(&self.key_file, &schema).expect("Failed to read value file");
            let value = ensure_is_record_if_required(value, &schema);
            writer.append_value_ref(&value).unwrap();

            let encoded = writer.into_inner().unwrap();
            Ok(encoded)
        } else if let Some(ref schema_registry) = self.schema_registry {
            encode_value_or_key(
                &schema_registry.sr_settings,
                &self.key_file,
                &schema_registry.subject_name_strategy_key,
                &schema_registry.key_schema_id,
                &schema_registry.avro_encoder,
            )
            .await
        } else {
            warn!("no schema used, we will use the key as is");
            let value =
                fs::read_to_string(&self.key_file).expect("Should have been able to read the file");
            Ok(value.into_bytes())
        }
    }
    async fn encode_payload(&self) -> anyhow::Result<Vec<u8>> {
        if let Some(schema) = &self.value_schema {
            if self.schema_registry.is_some() {
                warn!(
                    "schema registry is ignored, since a schema file is provided and will be used"
                );
            }
            let mut writer = Writer::new(&schema, Vec::new());
            let value =
                avro_value_from_file(&self.value_file, &schema).expect("Failed to read value file");
            writer.append_value_ref(&value).unwrap();

            let encoded = writer.into_inner().unwrap();
            Ok(encoded)
        } else if let Some(ref schema_registry) = self.schema_registry {
            encode_value_or_key(
                &schema_registry.sr_settings,
                &self.value_file,
                &schema_registry.subject_name_strategy_value,
                &schema_registry.value_schema_id,
                &schema_registry.avro_encoder,
            )
            .await
        } else if let Some(schema) = &self.value_schema {
            let mut writer = Writer::new(&schema, Vec::new());
            let value =
                avro_value_from_file(&self.value_file, &schema).expect("Failed to read value file");
            writer.append_value_ref(&value).unwrap();

            let encoded = writer.into_inner().unwrap();
            Ok(encoded)
        } else {
            warn!("no schema used, we will use the value as is");
            let value = fs::read_to_string(&self.value_file)
                .expect("Should have been able to read the file");
            Ok(value.into_bytes())
        }
    }
}

async fn encode_value_or_key(
    sr_settings: &SrSettings,
    data_file: &PathBuf,
    subject_name_strategy: &SubjectNameStrategy,
    schema_id: &Option<u32>,
    avro_encoder: &AvroEncoder<'_>,
) -> anyhow::Result<Vec<u8>> {
    info!("Using schema registry & avro encoder");

    let registered_schema = if let Some(schema_id) = schema_id {
        // get_schema_by_id_and_type(id, sr_settings, schema_type)
        get_schema_by_id(*schema_id, &sr_settings)
            .await
            .expect("schema not found")
    } else {
        get_schema_by_subject(&sr_settings, &subject_name_strategy)
            .await
            .expect("schema not found")
    };

    info!(
        "schmema loaded from schema registry: {:?}",
        registered_schema
    );

    let value_schema = if registered_schema.schema_type == SchemaType::Avro {
        Schema::parse_str(&registered_schema.schema).unwrap()
    } else if registered_schema.schema_type == SchemaType::Json {
        unimplemented!("Json Schemas are not supported yet");
        // let schema = serde_json::from_str::<serde_json::Value>(&s.schema).unwrap();
        // let schema = Schema::parse(&schema).unwrap();
        // schema
    } else if registered_schema.schema_type == SchemaType::Protobuf {
        unimplemented!("Protobuf Schemas are not supported yet");
    } else {
        unimplemented!("Only Topics with Schema are supported");
    };

    let value: Value =
        avro_value_from_file(&data_file, &value_schema).expect("Failed to read value file");
    info!("value: {:?}", value);

    let value: Vec<(&str, Value)> = match value {
        Value::Map(ref record) => {
            let map: Vec<(&str, Value)> = record
                .iter()
                .map(|(k, v)| (k.as_str(), v.clone()))
                .collect();
            map
        }
        Value::Record(ref record) => {
            let map: Vec<(&str, Value)> = record
                .iter()
                .map(|(k, v)| (k.as_str(), v.clone()))
                .collect();
            map
        }
        _ => panic!("Unsupported value type {:?}", value),
    };

    let payload = match avro_encoder
        .encode(value, subject_name_strategy.clone())
        .await
    {
        Ok(v) => v,
        Err(e) => panic!("Error encoding avro payload: {}", e),
    };

    Ok(payload)
}

fn avro_value_from_file(file: &PathBuf, schema: &Schema) -> anyhow::Result<Value> {
    let f = fs::File::open(file.clone()).unwrap();
    let extension = file.extension().expect("message_file must have extension");

    let map = if extension == "avro" {
        let reader = BufReader::new(f);
        let mut reader = Reader::with_schema(&schema, reader).unwrap();
        // let mut reader: Reader<'_, BufReader<fs::File>> = Reader::new(reader).unwrap();
        let value = reader.next().unwrap().unwrap();
        value
    } else if extension == "json" {
        // let reader = Cursor::new(file);
        let text = fs::read_to_string(file.clone()).unwrap();
        let value: serde_json::Value = serde_json::from_str::<serde_json::Value>(&text).unwrap();
        info!("json value: {:?}", value);
        let value: Value = value.into();
        info!("avro value: {:?}", value);
        if !value.validate(&schema) {
            panic!(
                "Invalid value! Json message {:?} is not a valid schema: {:?}",
                value, &schema
            );
        }
        value
    } else {
        panic!("Unsupported file extension {:?}", extension)
    };

    Ok(map)
}

fn ensure_is_record_if_required(value: Value, schema: &Schema) -> Value {
    match value.clone() {
        Value::Map(map) => {
            match schema.clone() {
                // Schema::Map(_) => {
                //     Value::Map(map)
                // },
                Schema::Record(fields) => {
                    let values = fields
                        .fields
                        .iter()
                        .map(|field| {
                            let value = map.get(&field.name).unwrap();
                            (field.name.clone(), value.clone())
                        })
                        .collect();
                    Value::Record(values)
                }
                _ => value,
            }
        }
        any => any,
    }
}

#[cfg(test)]
mod tests {
    use apache_avro::schema::Schema;
    use apache_avro::types::Record;
    use apache_avro::*;
    use serde::Deserialize;
    use serde::Serialize;
    use uuid::Uuid;

    use crate::ensure_is_record_if_required;

    #[derive(Debug, Serialize, Deserialize)]
    struct Key {
        a: Uuid,
        b: String,
    }

    #[test]
    pub fn avro_sample_serde() {
        // This test makes sure avro + serde works (which is not the case for version 0.13)

        let raw_schema = r#"
            {
                "type": "record",
                "name": "Key",
                "fields": [
                    {"name": "a", "type": "string", "logicalType": "uuid"},
                    {"name": "b", "type": "string"}
                ]
            }
        "#;

        // if the schema is not valid, this function will return an error
        let schema = Schema::parse_str(raw_schema).unwrap();

        // a writer needs a schema and something to write to
        let mut writer = Writer::new(&schema, Vec::new());

        // the structure models our Record schema
        let test = Key {
            a: Uuid::parse_str("a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8").unwrap(),
            b: "foo".to_owned(),
        };
        // schema validation happens here
        writer.append_ser(test).unwrap();

        let mut record = Record::new(writer.schema()).unwrap();
        record.put("a", "a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8");
        record.put("b", "bar");
        writer.append(record).unwrap();

        let json: serde_json::Value =
            serde_json::from_str(r#"{"a":"a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8","b":"foobar"}"#)
                .unwrap();
        use apache_avro::types::Value;
        let value: Value = json.into();

        let value: Value = ensure_is_record_if_required(value, &schema);
        writer.append_value_ref(&value).unwrap();
        // this is how to get back the resulting avro bytecode
        // this performs a flush operation to make sure data is written, so it can fail
        // you can also call `writer.flush()` yourself without consuming the writer
        let encoded = writer.into_inner().unwrap();
        // println!("encoded: {:?}", encoded);
        use apache_avro::from_value;
        use apache_avro::Reader;
        let reader = Reader::with_schema(&schema, &encoded[..]).unwrap();

        let values: Vec<Key> = reader
            .into_iter()
            .map(|value| from_value::<Key>(&value.unwrap()).unwrap())
            .collect();

        let v = values.get(0).unwrap();
        assert_eq!(
            v.a,
            Uuid::parse_str("a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8").unwrap()
        );
        assert_eq!(v.b, "foo");
        let v = values.get(1).unwrap();
        assert_eq!(
            v.a,
            Uuid::parse_str("a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8").unwrap()
        );
        assert_eq!(v.b, "bar");
        let v = values.get(2).unwrap();
        assert_eq!(
            v.a,
            Uuid::parse_str("a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8").unwrap()
        );
        assert_eq!(v.b, "foobar");
    }

    //     use crate::async_impl::easy_avro::{EasyAvroDecoder, EasyAvroEncoder};
    //     use crate::async_impl::schema_registry::SrSettings;
    //     use crate::avro_common::get_supplied_schema;
    //     use crate::schema_registry_common::SubjectNameStrategy;
    //     use apache_avro::types::Value;
    //     use apache_avro::{from_value, Schema};
    //     use mockito::{mock, server_address};
    //     use test_utils::Heartbeat;

    //     #[tokio::test]
    //     async fn test_decoder_default() {
    //         let _m = mock("GET", "/schemas/ids/1?deleted=true")
    //             .with_status(200)
    //             .with_header("content-type", "application/vnd.schemaregistry.v1+json")
    //             .with_body(r#"{"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
    //             .create();

    //         let sr_settings = SrSettings::new(format!("http://{}", server_address()));
    //         let decoder = EasyAvroDecoder::new(sr_settings);
    //         let heartbeat = decoder
    //             .decode(Some(&[0, 0, 0, 0, 1, 6]))
    //             .await
    //             .unwrap()
    //             .value;

    //         assert_eq!(
    //             heartbeat,
    //             Value::Record(vec![("beat".to_string(), Value::Long(3))])
    //         );

    //         let item = match from_value::<Heartbeat>(&heartbeat) {
    //             Ok(h) => h,
    //             Err(_) => unreachable!(),
    //         };
    //         assert_eq!(item.beat, 3i64);
    //     }

    //     #[tokio::test]
    //     async fn test_decode_with_schema_default() {
    //         let _m = mock("GET", "/schemas/ids/1?deleted=true")
    //             .with_status(200)
    //             .with_header("content-type", "application/vnd.schemaregistry.v1+json")
    //             .with_body(r#"{"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
    //             .create();

    //         let sr_settings = SrSettings::new(format!("http://{}", server_address()));
    //         let decoder = EasyAvroDecoder::new(sr_settings);
    //         let heartbeat = decoder
    //             .decode_with_schema(Some(&[0, 0, 0, 0, 1, 6]))
    //             .await
    //             .unwrap()
    //             .unwrap()
    //             .value;

    //         assert_eq!(
    //             heartbeat,
    //             Value::Record(vec![("beat".to_string(), Value::Long(3))])
    //         );

    //         let item = match from_value::<Heartbeat>(&heartbeat) {
    //             Ok(h) => h,
    //             Err(_) => unreachable!(),
    //         };
    //         assert_eq!(item.beat, 3i64);
    //     }

    //     #[tokio::test]
    //     async fn test_encode_value() {
    //         let _m = mock("GET", "/subjects/heartbeat-value/versions/latest")
    //             .with_status(200)
    //             .with_header("content-type", "application/vnd.schemaregistry.v1+json")
    //             .with_body(r#"{"subject":"heartbeat-value","version":1,"id":3,"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
    //             .create();

    //         let sr_settings = SrSettings::new(format!("http://{}", server_address()));
    //         let encoder = EasyAvroEncoder::new(sr_settings);

    //         let value_strategy =
    //             SubjectNameStrategy::TopicNameStrategy(String::from("heartbeat"), false);
    //         let bytes = encoder
    //             .encode(vec![("beat", Value::Long(3))], value_strategy)
    //             .await
    //             .unwrap();

    //         assert_eq!(bytes, vec![0, 0, 0, 0, 3, 6])
    //     }

    //     #[tokio::test]
    //     async fn test_primitive_schema() {
    //         let sr_settings = SrSettings::new(format!("http://{}", server_address()));
    //         let encoder = EasyAvroEncoder::new(sr_settings);

    //         let _n = mock("POST", "/subjects/heartbeat-key/versions")
    //             .with_status(200)
    //             .with_header("content-type", "application/vnd.schemaregistry.v1+json")
    //             .with_body(r#"{"id":4}"#)
    //             .create();

    //         let primitive_schema_strategy = SubjectNameStrategy::TopicNameStrategyWithSchema(
    //             String::from("heartbeat"),
    //             true,
    //             get_supplied_schema(&Schema::String),
    //         );
    //         let bytes = encoder
    //             .encode_struct("key-value", &primitive_schema_strategy)
    //             .await;

    //         assert_eq!(
    //             bytes,
    //             Ok(vec![
    //                 0, 0, 0, 0, 4, 18, 107, 101, 121, 45, 118, 97, 108, 117, 101
    //             ])
    //         );
    //     }
}
