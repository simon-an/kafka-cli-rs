use schema_registry_converter::async_impl::schema_registry::{SrSettings, SrSettingsBuilder};

use crate::{
    SchemaRegistryAuth, SchemaRegistryBasicAuth, SchemaRegistryBearerAuth, SchemaRegistryConfig,
};

impl From<SchemaRegistryConfig> for SrSettingsBuilder {
    fn from(config: SchemaRegistryConfig) -> Self {
        let mut builder = SrSettings::new_builder(config.endpoint.to_string());
        match config.auth {
            SchemaRegistryAuth::Basic(SchemaRegistryBasicAuth { username, password }) => {
                builder.set_basic_authorization(&username, Some(&password));
            }
            SchemaRegistryAuth::Bearer(SchemaRegistryBearerAuth { token }) => {
                builder.set_token_authorization(&token);
            }
        }
        builder
    }
}
