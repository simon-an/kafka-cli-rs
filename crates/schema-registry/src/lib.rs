use std::path::PathBuf;

use apache_avro::Schema;
use kafka_config::SchemaRegistryConfig;
use opentelemetry::propagation::{Extractor, Injector};
use rdkafka::message::{BorrowedHeaders, Headers, OwnedHeaders};
use reqwest::ClientBuilder;
use schema_registry_converter::{
    async_impl::schema_registry::{post_schema, SrSettingsBuilder},
    avro_common::get_supplied_schema,
    error::SRCError,
    schema_registry_common::{RegisteredSchema, SuppliedSchema},
};
pub struct HeaderInjector<'a>(pub &'a mut OwnedHeaders);

impl<'a> Injector for HeaderInjector<'a> {
    fn set(&mut self, key: &str, value: String) {
        let mut new = OwnedHeaders::new().insert(rdkafka::message::Header {
            key,
            value: Some(&value),
        });

        for header in self.0.iter() {
            let s = String::from_utf8(header.value.unwrap().to_vec()).unwrap();
            new = new.insert(rdkafka::message::Header {
                key: header.key,
                value: Some(&s),
            });
        }

        self.0.clone_from(&new);
    }
}

pub struct HeaderExtractor<'a>(pub &'a BorrowedHeaders);

impl<'a> Extractor for HeaderExtractor<'a> {
    fn get(&self, key: &str) -> Option<&str> {
        for i in 0..self.0.count() {
            if let Ok(val) = self.0.get_as::<str>(i) {
                if val.key == key {
                    return val.value;
                }
            }
        }
        None
    }

    fn keys(&self) -> Vec<&str> {
        self.0.iter().map(|kv| kv.key).collect::<Vec<_>>()
    }
}

pub async fn register_schema(
    schema_registry: SchemaRegistryConfig,
    subject: String,
    schema: Schema,
) -> Result<RegisteredSchema, SRCError> {
    let sr_settings = SrSettingsBuilder::from(schema_registry)
        .build_with(ClientBuilder::default().connection_verbose(true))
        .unwrap();
    let supplied_schema: SuppliedSchema = get_supplied_schema(&schema);
    post_schema(&sr_settings, subject, supplied_schema).await
}

pub fn schema_from_file(path: PathBuf) -> Schema {
    let schema = std::fs::read_to_string(path).expect("Should have been able to read the file");
    let schema: Schema = Schema::parse_str(&schema).unwrap();
    schema
}
pub fn supplied_schema_from_file(path: PathBuf) -> SuppliedSchema {
    let schema = std::fs::read_to_string(path).expect("Should have been able to read the file");
    let schema: Schema = Schema::parse_str(&schema).unwrap();
    get_supplied_schema(&schema)
}
