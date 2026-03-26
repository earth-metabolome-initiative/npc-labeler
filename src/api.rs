use std::thread;
use std::time::Duration;

use serde::Deserialize;

const API_URL: &str = "https://npclassifier.gnps2.org/classify";

#[derive(Debug, Deserialize)]
pub struct ApiResponse {
    pub class_results: Vec<String>,
    pub superclass_results: Vec<String>,
    pub pathway_results: Vec<String>,
    pub isglycoside: bool,
}

#[derive(Debug)]
pub enum ClassifyError {
    InvalidSmiles,
    RateLimit,
    ServerError(u16),
    ParseError(String),
    NetworkError(String),
}

pub fn classify(agent: &ureq::Agent, smiles: &str) -> Result<ApiResponse, ClassifyError> {
    match agent.get(API_URL).query("smiles", smiles).call() {
        Ok(response) => response
            .into_json::<ApiResponse>()
            .map_err(|e| ClassifyError::ParseError(e.to_string())),
        Err(ureq::Error::Status(429, _)) => {
            eprintln!("[api] rate limited, sleeping 30s");
            thread::sleep(Duration::from_secs(30));
            Err(ClassifyError::RateLimit)
        }
        Err(ureq::Error::Status(500, _)) => Err(ClassifyError::InvalidSmiles),
        Err(ureq::Error::Status(code, _)) => {
            eprintln!("[api] server error {code}, sleeping 10s");
            thread::sleep(Duration::from_secs(10));
            Err(ClassifyError::ServerError(code))
        }
        Err(ureq::Error::Transport(transport)) => {
            eprintln!("[api] transport error: {transport}, sleeping 10s");
            thread::sleep(Duration::from_secs(10));
            Err(ClassifyError::NetworkError(transport.to_string()))
        }
    }
}
