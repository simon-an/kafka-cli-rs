use config::Config;
use config::File;
use rdkafka::ClientConfig;
use serde::Deserialize;
use serde::Serialize;
use url::Url;

pub mod consumer;
pub mod producer;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Sasl {
    pub username: String,
    pub password: String,
    pub mechanisms: String,
}
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SchemaRegistryConfig {
    pub username: String,
    pub password: String,
    pub endpoint: Url,
}

impl Default for Sasl {
    fn default() -> Self {
        Sasl {
            mechanisms: "PLAIN".to_string(),
            username: "".to_string(),
            password: "".to_string(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct KafkaConfig {
    pub securityprotocol: String,
    pub endpoint: String,
    pub sasl: Option<Sasl>,
    #[serde(rename = "schemaregistry")]
    pub schema_registry: Option<SchemaRegistryConfig>,
}

impl KafkaConfig {
    // pub fn sasl(endpoint: String, sasl: Sasl) -> Self {
    //     Self {
    //         endpoint,
    //         securityprotocol: "SASL_SSL".to_string(),
    //         sasl: Some(sasl),
    //     }
    // }
    #[cfg(test)]
    pub fn unsecure(endpoint: String) -> Self {
        Self {
            endpoint,
            securityprotocol: "plaintext".to_string(),
            sasl: None,
            schema_registry: None,
        }
    }
    pub fn from_env() -> anyhow::Result<KafkaConfig> {
        let mut builder = Config::builder();
        builder = builder.add_source(Config::try_from(&KafkaConfig::default())?);
        builder = builder
            .add_source(File::new(".kafka.config.yaml", config::FileFormat::Yaml).required(false));
        builder = builder
            .add_source(File::new(".kafka.config.json", config::FileFormat::Yaml).required(false));
        builder = builder.add_source(config::Environment::with_prefix("KAFKA").separator("_"));
        let kafka_config: KafkaConfig = builder.build()?.try_deserialize()?;
        Ok(kafka_config)
    }
}

impl Default for KafkaConfig {
    fn default() -> Self {
        KafkaConfig {
            endpoint: "localhost:9092".to_string(),
            securityprotocol: "SASL_SSL".to_string(),
            sasl: Some(Sasl::default()),
            schema_registry: None,
        }
    }
}

impl From<KafkaConfig> for ClientConfig {
    fn from(config: KafkaConfig) -> Self {
        let x: Option<&str> = None;
        assert_eq!(x.ok_or(0), Err(0));

        let mut client_config = ClientConfig::new();

        client_config.set("bootstrap.servers", config.endpoint.clone());
        client_config.set("security.protocol", config.securityprotocol.clone());
        if let Some(sasl) = config.sasl {
            client_config.set("sasl.mechanisms", sasl.mechanisms);
            client_config.set("sasl.username", sasl.username);
            client_config.set("sasl.password", sasl.password);
            client_config.set("delivery.timeout.ms", "2000");
        }

        client_config
    }
}

#[test]
fn test_create_kafka_config() {
    std::env::set_var("KAFKA_ENDPOINT", "endpoint:9092");
    std::env::set_var("KAFKA_SASL_USERNAME", "username");
    std::env::set_var("KAFKA_SASL_PASSWORD", "password");
    std::env::set_var("KAFKA_SCHEMAREGISTRY_ENDPOINT", "otherendpoint:8081");
    std::env::set_var("KAFKA_SCHEMAREGISTRY_USERNAME", "username2");
    std::env::set_var("KAFKA_SCHEMAREGISTRY_PASSWORD", "password2");

    let cfg: KafkaConfig = KafkaConfig::from_env().unwrap();
    let sasl = cfg.sasl.unwrap();
    let username: String = sasl.username;
    let password: String = sasl.password;
    let endpoint: String = cfg.endpoint;

    assert_eq!(username, "username");
    assert_eq!(password, "password");
    assert_eq!(endpoint, "endpoint:9092");
    assert_eq!(
        cfg.schema_registry.clone().unwrap().endpoint,
        Url::parse("otherendpoint:8081").unwrap()
    );
    assert_eq!(cfg.schema_registry.clone().unwrap().password, "password2");
    assert_eq!(cfg.schema_registry.clone().unwrap().username, "username2");

    std::env::remove_var("KAFKA_ENDPOINT");
    std::env::remove_var("KAFKA_SASL_USERNAME");
    std::env::remove_var("KAFKA_SASL_PASSWORD");
    std::env::remove_var("KAFKA_SCHEMAREGISTRY_ENDPOINT");
    std::env::remove_var("KAFKA_SCHEMAREGISTRY_USERNAME");
    std::env::remove_var("KAFKA_SCHEMAREGISTRY_PASSWORD");
}
