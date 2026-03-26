use std::fmt;
#[cfg(test)]
use std::io::{Read, Write};
#[cfg(test)]
use std::net::TcpListener;
#[cfg(test)]
use std::thread;

use serde::{Deserialize, Serialize};

const API_URL: &str = "https://npclassifier.gnps2.org/classify";
pub const DEFAULT_API_URL: &str = API_URL;

#[derive(Debug, Deserialize, Serialize)]
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

impl ClassifyError {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::InvalidSmiles => "invalid_smiles",
            Self::RateLimit => "rate_limit",
            Self::ServerError(_) => "server_error",
            Self::ParseError(_) => "parse_error",
            Self::NetworkError(_) => "network_error",
        }
    }

    pub fn message(&self) -> String {
        match self {
            Self::InvalidSmiles => "Invalid SMILES (HTTP 500)".to_string(),
            Self::RateLimit => "Rate limited (HTTP 429)".to_string(),
            Self::ServerError(code) => format!("Server error (HTTP {code})"),
            Self::ParseError(detail) => format!("JSON parse error: {detail}"),
            Self::NetworkError(detail) => format!("Network error: {detail}"),
        }
    }
}

impl fmt::Display for ClassifyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message())
    }
}

pub fn classify(agent: &ureq::Agent, smiles: &str) -> Result<ApiResponse, ClassifyError> {
    classify_at(agent, API_URL, smiles)
}

pub(crate) fn classify_at(
    agent: &ureq::Agent,
    api_url: &str,
    smiles: &str,
) -> Result<ApiResponse, ClassifyError> {
    match agent.get(api_url).query("smiles", smiles).call() {
        Ok(response) => response
            .into_json::<ApiResponse>()
            .map_err(|e| ClassifyError::ParseError(e.to_string())),
        Err(ureq::Error::Status(429, _)) => Err(ClassifyError::RateLimit),
        Err(ureq::Error::Status(500, _)) => Err(ClassifyError::InvalidSmiles),
        Err(ureq::Error::Status(code, _)) => Err(ClassifyError::ServerError(code)),
        Err(ureq::Error::Transport(transport)) => {
            Err(ClassifyError::NetworkError(transport.to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_RESPONSE: &str = r#"{"class_results":["lipid"],"superclass_results":["fatty acids"],"pathway_results":["acetate"],"isglycoside":false}"#;

    #[test]
    fn classify_maps_success_response() {
        let server = MockServer::spawn(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n".to_string() + TEST_RESPONSE,
        );
        let agent = ureq::AgentBuilder::new().build();
        let result = classify_at(&agent, &server.url(), "CCO").expect("successful response");
        assert_eq!(result.class_results, vec!["lipid"]);
        assert_eq!(result.superclass_results, vec!["fatty acids"]);
        assert_eq!(result.pathway_results, vec!["acetate"]);
        assert!(!result.isglycoside);
    }

    #[test]
    fn classify_maps_rate_limit() {
        let server = MockServer::spawn(
            "HTTP/1.1 429 Too Many Requests\r\nContent-Length: 0\r\n\r\n".to_string(),
        );
        let agent = ureq::AgentBuilder::new().build();
        let result = classify_at(&agent, &server.url(), "CCO").expect_err("rate limit error");
        assert!(matches!(result, ClassifyError::RateLimit));
    }

    #[test]
    fn classify_maps_invalid_smiles() {
        let server = MockServer::spawn(
            "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n".to_string(),
        );
        let agent = ureq::AgentBuilder::new().build();
        let result = classify_at(&agent, &server.url(), "CCO").expect_err("invalid smiles");
        assert!(matches!(result, ClassifyError::InvalidSmiles));
    }

    #[test]
    fn classify_maps_other_server_errors() {
        let server = MockServer::spawn(
            "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n".to_string(),
        );
        let agent = ureq::AgentBuilder::new().build();
        let result = classify_at(&agent, &server.url(), "CCO").expect_err("server error");
        assert!(matches!(result, ClassifyError::ServerError(503)));
    }

    #[test]
    fn classify_maps_parse_errors() {
        let server = MockServer::spawn(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{\"class_results\":"
                .to_string(),
        );
        let agent = ureq::AgentBuilder::new().build();
        let result = classify_at(&agent, &server.url(), "CCO").expect_err("parse error");
        assert!(matches!(result, ClassifyError::ParseError(_)));
    }

    #[test]
    fn classify_maps_network_errors() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind unused port");
        let address = listener.local_addr().expect("listener address");
        drop(listener);

        let agent = ureq::AgentBuilder::new().build();
        let result = classify_at(&agent, &format!("http://{address}/classify"), "CCO")
            .expect_err("network error");
        assert!(matches!(result, ClassifyError::NetworkError(_)));
    }

    #[test]
    fn error_helpers_return_expected_strings() {
        let cases = [
            (
                ClassifyError::InvalidSmiles,
                "invalid_smiles",
                "Invalid SMILES (HTTP 500)",
            ),
            (
                ClassifyError::RateLimit,
                "rate_limit",
                "Rate limited (HTTP 429)",
            ),
            (
                ClassifyError::ServerError(503),
                "server_error",
                "Server error (HTTP 503)",
            ),
            (
                ClassifyError::ParseError("bad".to_string()),
                "parse_error",
                "JSON parse error: bad",
            ),
            (
                ClassifyError::NetworkError("offline".to_string()),
                "network_error",
                "Network error: offline",
            ),
        ];

        for (error, kind, message) in cases {
            assert_eq!(error.kind(), kind);
            assert_eq!(error.message(), message);
            assert_eq!(error.to_string(), message);
        }
    }

    struct MockServer {
        url: String,
        join_handle: Option<thread::JoinHandle<()>>,
    }

    impl MockServer {
        fn spawn(response: String) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock server");
            let address = listener.local_addr().expect("mock server address");
            let join_handle = thread::spawn(move || {
                let (mut stream, _) = listener.accept().expect("accept request");
                let mut request = [0_u8; 1024];
                let _ = stream.read(&mut request);
                stream
                    .write_all(response.as_bytes())
                    .expect("write mock response");
                stream.flush().expect("flush mock response");
            });
            Self {
                url: format!("http://{address}/classify"),
                join_handle: Some(join_handle),
            }
        }

        fn url(&self) -> String {
            self.url.clone()
        }
    }

    impl Drop for MockServer {
        fn drop(&mut self) {
            if let Some(join_handle) = self.join_handle.take() {
                join_handle.join().expect("join mock server");
            }
        }
    }
}
