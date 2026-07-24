use std::collections::HashMap;
use crate::core::domain::types::{MediaMetadata, VideoMetadata};

impl VideoMetadata {
    pub fn new(id: String, duration_seconds: f64) -> Self {
        Self {
            base: MediaMetadata {
                id,
                original_name: None,
                total_size_bytes: 0,
                content_type: Some("application/octet-stream".to_string()),
                checksum: None,
                created_at: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                attributes: HashMap::new(),
            },
            duration_seconds,
            width: None,
            height: None,
            fps: None,
            timeline_indices: Vec::new(),
        }
    }

    pub fn with_content_type(mut self, content_type: impl Into<String>) -> Self {
        self.base.content_type = Some(content_type.into());
        self
    }

    pub fn add_bytes(&mut self, size: u64) {
        self.base.total_size_bytes += size;
    }

    pub fn set_checksum(&mut self, hash: String) {
        self.base.checksum = Some(hash);
    }

    pub fn add_partition_index(&mut self, index: u32) {
        self.timeline_indices.push(index);
    }
}