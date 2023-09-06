use std::path::PathBuf;

pub struct ConsumerConfig {
    pub topic: String,
    pub consumer_group_id: String,
    pub partition: Option<u32>,
    pub key_schema_file: Option<PathBuf>,
    pub value_schema_file: Option<PathBuf>,
    pub key_schema_id: Option<u32>,
    pub value_schema_id: Option<u32>,
}
