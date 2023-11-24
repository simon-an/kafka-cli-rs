use std::path::PathBuf;

pub struct ConsumerConfig {
    pub topic: String,
    pub consumer_group_id: String,
    pub partition: Option<i32>,
    pub key_schema_file: Option<PathBuf>,
    pub value_schema_file: Option<PathBuf>,
    pub key_schema_id: Option<u32>,
    pub value_schema_id: Option<u32>,
    pub consumer_group_instance_id: Option<String>,
    pub offset: Option<i64>,
}
