use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

pub const MAX_BODY: usize = 1_048_576;
pub const MAX_ITEMS: usize = 32;
pub const MAX_TOKENS: usize = 4096;
pub const MAX_INPUT: usize = 65536;
pub const MAX_TIMEOUT: u64 = 120000;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Purpose {
    Document,
    Query,
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Modality {
    Text,
    Image,
}
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub model: String,
    pub artifact_sha256: String,
    pub dimensions: usize,
    pub purpose: Purpose,
    pub modality: Modality,
    pub inputs: Vec<String>,
    pub timeout_ms: u64,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Output {
    pub model: String,
    pub artifact_sha256: String,
    pub dimensions: usize,
    pub vectors: Vec<Vec<f32>>,
    pub usage: Usage,
    pub timing: Timing,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Usage {
    pub input_tokens: usize,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Timing {
    pub inference_ms: f64,
}
#[derive(Debug, Clone)]
pub struct Error {
    pub status: StatusCode,
    pub code: &'static str,
    pub message: &'static str,
    pub index: Option<usize>,
}
impl Error {
    pub fn new(code: &'static str) -> Self {
        let (status, message) = match code {
            "invalid_input" => (400, "input is blank or missing its required prefix"),
            "model_not_found" => (404, "model is not registered"),
            "not_found" => (404, "unknown path"),
            "method_not_allowed" => (405, "method not allowed"),
            "unsupported_media_type" => (415, "application/json required"),
            "artifact_mismatch" => (409, "artifact fingerprint differs; re-index required"),
            "body_too_large" => (413, "body exceeds byte limit"),
            "batch_too_large" => (413, "batch exceeds item limit"),
            "input_too_long" => (422, "input exceeds model token or byte limit"),
            "dimension_mismatch" => (422, "requested dimensions differ from model"),
            "unsupported_modality" => (422, "model supports text only"),
            "batch_token_limit" => (422, "batch exceeds token limit"),
            "overloaded" => (429, "inference queue is full"),
            "not_ready" => (503, "models are not ready"),
            "deadline_exceeded" => (504, "request deadline exceeded"),
            "invalid_output" => (500, "model produced invalid output"),
            "inference_failed" => (500, "model inference failed"),
            _ => (400, "invalid request"),
        };
        Self {
            status: StatusCode::from_u16(status).unwrap(),
            code,
            message,
            index: None,
        }
    }
    pub fn at(mut self, index: usize) -> Self {
        self.index = Some(index);
        self
    }
}
impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let retryable = matches!(self.code, "overloaded" | "not_ready" | "deadline_exceeded");
        let mut response = (self.status, Json(json!({"error": {"code":self.code,"message":self.message,"index":self.index,"retryable":retryable}}))).into_response();
        if matches!(self.code, "overloaded" | "not_ready") {
            response
                .headers_mut()
                .insert("retry-after", "1".parse().unwrap());
        }
        response
    }
}
