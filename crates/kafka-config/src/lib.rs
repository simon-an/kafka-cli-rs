use config::Config;
use config::File;
use rdkafka::ClientConfig;
use serde::Deserialize;
use serde::Serialize;
use url::Url;

pub mod consumer;
mod into;
pub mod producer;
pub use into::*;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Sasl {
    pub username: String,
    pub password: String,
    pub mechanisms: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SchemaRegistryConfig {
    #[serde(flatten)]
    pub auth: SchemaRegistryAuth,
    pub endpoint: Url,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(untagged)]
pub enum SchemaRegistryAuth {
    Basic(SchemaRegistryBasicAuth),
    Bearer(SchemaRegistryBearerAuth),
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SchemaRegistryBasicAuth {
    pub username: String,
    pub password: String,
}
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SchemaRegistryBearerAuth {
    pub token: String,
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

        let path_from_env = std::env::var("KAFKA_CONFIG_PATH");
        builder = if let Ok(path_from_env) = path_from_env {
            log::info!("Loading config from: {path_from_env}");
            if path_from_env.ends_with("yaml") {
                builder = builder
                    .add_source(File::new(&path_from_env, config::FileFormat::Yaml).required(true));
                builder
            } else if path_from_env.ends_with("json") {
                builder = builder
                    .add_source(File::new(&path_from_env, config::FileFormat::Json).required(true));
                builder
            } else {
                log::warn!("File type not supported. Ignore file: {path_from_env}");
                builder
            }
        } else {
            builder
        };

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

#[cfg(test)]
mod tests {
    use crate::{
        KafkaConfig, SchemaRegistryAuth, SchemaRegistryBasicAuth, SchemaRegistryBearerAuth,
    };
    use serial_test::serial;
    use url::Url;

    #[test]
    #[serial]
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
        if let SchemaRegistryAuth::Basic(auth) = cfg.schema_registry.clone().unwrap().auth {
            assert_eq!(auth.username, "username2");
            assert_eq!(auth.password, "password2");
        } else {
            panic!("Should have been basic auth");
        }

        std::env::remove_var("KAFKA_ENDPOINT");
        std::env::remove_var("KAFKA_SASL_USERNAME");
        std::env::remove_var("KAFKA_SASL_PASSWORD");
        std::env::remove_var("KAFKA_SCHEMAREGISTRY_ENDPOINT");
        std::env::remove_var("KAFKA_SCHEMAREGISTRY_USERNAME");
        std::env::remove_var("KAFKA_SCHEMAREGISTRY_PASSWORD");
    }

    #[test]
    #[serial]
    fn test_create_kafka_config_with_token() {
        std::env::set_var("KAFKA_ENDPOINT", "endpoint:9092");
        std::env::set_var("KAFKA_SASL_USERNAME", "username"); // TODO: replace with token
        std::env::set_var("KAFKA_SASL_PASSWORD", "password");
        std::env::set_var("KAFKA_SCHEMAREGISTRY_ENDPOINT", "otherendpoint:8081");
        std::env::set_var("KAFKA_SCHEMAREGISTRY_TOKEN", "token");

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
        if let SchemaRegistryAuth::Bearer(auth) = cfg.schema_registry.clone().unwrap().auth {
            assert_eq!(auth.token, "token");
        } else {
            panic!("Should have been Bearer auth");
        }

        std::env::remove_var("KAFKA_ENDPOINT");
        std::env::remove_var("KAFKA_SASL_USERNAME");
        std::env::remove_var("KAFKA_SASL_PASSWORD");
        std::env::remove_var("KAFKA_SCHEMAREGISTRY_ENDPOINT");
        std::env::remove_var("KAFKA_SCHEMAREGISTRY_TOKEN");
    }

    #[test]
    #[serial]
    fn test_topics_config_from_file_using_env_path() {
        std::env::set_var("KAFKA_CONFIG_PATH", "resources/.kafka.example.yaml");
        let cfg: KafkaConfig = KafkaConfig::from_env().unwrap();

        assert_eq!(cfg.sasl.unwrap().username, "MyApiKey");
        if let SchemaRegistryAuth::Basic(SchemaRegistryBasicAuth { username, .. }) =
            cfg.schema_registry.unwrap().auth
        {
            assert_eq!(username, "MyApiKeyForSchemaRegistry");
        } else {
            panic!("Should have been SchemaRegistryBasicAuth auth");
        }

        std::env::remove_var("KAFKA_CONFIG_PATH");
    }
    #[test]
    #[serial]
    fn test_topics_config_from_file_using_env_path_with_token() {
        std::env::set_var("KAFKA_CONFIG_PATH", "resources/.kafka.example.token.yaml");
        let cfg: KafkaConfig = KafkaConfig::from_env().unwrap();

        if let SchemaRegistryAuth::Bearer(SchemaRegistryBearerAuth { token }) =
            cfg.schema_registry.unwrap().auth
        {
            assert_eq!(token, "MyToken");
        } else {
            panic!("Should have been Bearer auth");
        }

        std::env::remove_var("KAFKA_CONFIG_PATH");
    }
}
