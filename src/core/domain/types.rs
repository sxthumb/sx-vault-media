use std::collections::HashMap;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct MediaMetadata {
    pub id: String,
    pub original_name: Option<String>,
    pub total_size_bytes: u64,
    pub content_type: Option<String>,
    pub checksum: Option<String>,
    pub created_at: u64,
    pub attributes: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct VideoMetadata {
    pub base: MediaMetadata,
    pub duration_seconds: f64,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub fps: Option<f32>,
    pub timeline_indices: Vec<u32>,
}