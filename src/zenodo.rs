use std::fs::File;
use std::path::Path;

const ZENODO_API: &str = "https://zenodo.org/api";
const DEPOSIT_ID: &str = "14040990";

#[derive(Clone)]
struct PublishConfig {
    api_base: String,
    deposit_id: String,
}

impl PublishConfig {
    fn production() -> Self {
        Self {
            api_base: ZENODO_API.to_string(),
            deposit_id: DEPOSIT_ID.to_string(),
        }
    }
}

fn build_metadata(successful_rows: u64, invalid_rows: u64, failed_rows: u64) -> serde_json::Value {
    let today = chrono::Local::now().format("%Y-%m-%d").to_string();
    let excluded_rows = invalid_rows + failed_rows;

    serde_json::json!({
        "metadata": {
            "title": format!("NPClassifier PubChem Classifications ({today})"),
            "upload_type": "dataset",
            "publication_date": today,
            "description": format!(
                "<p>Open dataset of <a href=\"https://npclassifier.gnps2.org/\">NPClassifier</a> \
                 classifications for PubChem compounds.</p>\
                 <p>This snapshot contains {successful_rows} successful NPClassifier responses. \
                 Rows that ended invalid ({invalid_rows}) or failed after bounded inline retry \
                 ({failed_rows}) are excluded from the release artifact.</p>\
                 <p>The release contains a merged <code>completed.jsonl.zst</code> dataset and a \
                 machine-readable <code>manifest.json</code> describing the chunk set used to build it.</p>\
                 <p>Each completed row contains the PubChem CID, SMILES, the NPClassifier pathway, \
                 superclass, and class labels as JSON arrays, plus the glycoside flag.</p>\
                 <p>Format: JSON Lines compressed with Zstandard.</p>\
                 <p>Excluded rows in this snapshot: {excluded_rows}.</p>\
                 <p>Source code: \
                 <a href=\"https://github.com/earth-metabolome-initiative/npc-labeler\">npc-labeler</a>.</p>"
            ),
            "creators": [
                {
                    "name": "Cappelletti, Luca",
                    "orcid": "0000-0002-1269-2038"
                }
            ],
            "keywords": [
                "natural products",
                "NPClassifier",
                "PubChem",
                "cheminformatics",
                "SMILES",
                "chemical classification",
                "open data",
                "machine learning dataset"
            ],
            "license": "MIT",
            "access_right": "open",
            "related_identifiers": [
                {
                    "identifier": "https://github.com/earth-metabolome-initiative/npc-labeler",
                    "relation": "isCompiledBy",
                    "resource_type": "software",
                    "scheme": "url"
                },
                {
                    "identifier": "https://npclassifier.gnps2.org/",
                    "relation": "isDerivedFrom",
                    "scheme": "url"
                }
            ],
            "notes": format!(
                "Snapshot: {today}. Successful rows: {successful_rows}. Invalid: {invalid_rows}. Failed: {failed_rows}."
            )
        }
    })
}

fn zenodo_agent() -> ureq::Agent {
    ureq::AgentBuilder::new()
        .timeout_read(std::time::Duration::from_mins(5))
        .timeout_write(std::time::Duration::from_mins(5))
        .timeout_connect(std::time::Duration::from_secs(30))
        .build()
}

pub fn publish(
    token: &str,
    output_path: &Path,
    manifest_path: &Path,
    successful_rows: u64,
    invalid_rows: u64,
    failed_rows: u64,
) -> Result<String, String> {
    publish_with_config(
        token,
        output_path,
        manifest_path,
        successful_rows,
        invalid_rows,
        failed_rows,
        &PublishConfig::production(),
    )
}

fn publish_with_config(
    token: &str,
    output_path: &Path,
    manifest_path: &Path,
    successful_rows: u64,
    invalid_rows: u64,
    failed_rows: u64,
    config: &PublishConfig,
) -> Result<String, String> {
    let agent = zenodo_agent();

    eprintln!("[zenodo] creating new version...");

    let response = agent
        .post(&format!(
            "{}/deposit/depositions/{}/actions/newversion",
            config.api_base, config.deposit_id
        ))
        .set("Authorization", &format!("Bearer {token}"))
        .call()
        .map_err(|error| format!("failed to create new version: {error}"))?;

    let body: serde_json::Value = response
        .into_json()
        .map_err(|error| format!("failed to parse new version response: {error}"))?;

    let draft_url = body["links"]["latest_draft"]
        .as_str()
        .ok_or("missing latest_draft link in response")?
        .to_string();

    let draft: serde_json::Value = agent
        .get(&draft_url)
        .set("Authorization", &format!("Bearer {token}"))
        .call()
        .map_err(|error| format!("failed to get draft: {error}"))?
        .into_json()
        .map_err(|error| format!("failed to parse draft: {error}"))?;

    let bucket_url = draft["links"]["bucket"]
        .as_str()
        .ok_or("missing bucket link in draft")?
        .to_string();
    let draft_id = draft["id"].as_u64().ok_or("missing id in draft")?;

    if let Some(files) = draft["files"].as_array() {
        for file in files {
            if let Some(file_id) = file["id"].as_str() {
                let _ = agent
                    .delete(&format!(
                        "{}/deposit/depositions/{draft_id}/files/{file_id}",
                        config.api_base
                    ))
                    .set("Authorization", &format!("Bearer {token}"))
                    .call();
            }
        }
    }

    let metadata = build_metadata(successful_rows, invalid_rows, failed_rows);
    agent
        .put(&format!(
            "{}/deposit/depositions/{draft_id}",
            config.api_base
        ))
        .set("Authorization", &format!("Bearer {token}"))
        .set("Content-Type", "application/json")
        .send_json(&metadata)
        .map_err(|error| format!("failed to update metadata: {error}"))?;

    upload_file(&agent, token, &bucket_url, output_path)?;
    upload_file(&agent, token, &bucket_url, manifest_path)?;

    let publish_response = agent
        .post(&format!(
            "{}/deposit/depositions/{draft_id}/actions/publish",
            config.api_base
        ))
        .set("Authorization", &format!("Bearer {token}"))
        .call()
        .map_err(|error| format!("failed to publish: {error}"))?;

    let publish_body: serde_json::Value = publish_response
        .into_json()
        .map_err(|error| format!("failed to parse publish response: {error}"))?;

    Ok(publish_body["doi"]
        .as_str()
        .unwrap_or("unknown")
        .to_string())
}

fn upload_file(
    agent: &ureq::Agent,
    token: &str,
    bucket_url: &str,
    path: &Path,
) -> Result<(), String> {
    let filename = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("invalid filename for {}", path.display()))?;
    let file_size = std::fs::metadata(path)
        .map_err(|error| format!("cannot stat {}: {error}", path.display()))?
        .len();
    let file =
        File::open(path).map_err(|error| format!("failed to open {}: {error}", path.display()))?;

    eprintln!(
        "[zenodo] uploading {filename} ({:.1} MB)...",
        file_size as f64 / 1_048_576.0
    );

    agent
        .put(&format!("{bucket_url}/{filename}"))
        .set("Authorization", &format!("Bearer {token}"))
        .set("Content-Type", "application/octet-stream")
        .set("Content-Length", &file_size.to_string())
        .send(file)
        .map_err(|error| format!("failed to upload {filename}: {error}"))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{MockHttpServer, MockResponse, TestDir};

    #[test]
    fn config_and_metadata_helpers_have_expected_defaults() {
        let config = PublishConfig::production();
        assert_eq!(config.api_base, ZENODO_API);
        assert_eq!(config.deposit_id, DEPOSIT_ID);

        let metadata = build_metadata(7, 2, 1);
        assert_eq!(metadata["metadata"]["upload_type"], "dataset");
        assert!(
            metadata["metadata"]["description"]
                .as_str()
                .expect("description")
                .contains("Excluded rows in this snapshot: 3")
        );
        assert!(
            metadata["metadata"]["notes"]
                .as_str()
                .expect("notes")
                .contains("Successful rows: 7. Invalid: 2. Failed: 1.")
        );
    }

    #[test]
    fn publishes_output_and_manifest_to_mock_zenodo() {
        let temp_dir = TestDir::new("zenodo");
        let output_path = temp_dir.path().join("completed.jsonl.zst");
        let manifest_path = temp_dir.path().join("manifest.json");
        std::fs::write(&output_path, b"output-bytes").expect("write output artifact");
        std::fs::write(&manifest_path, b"{\"manifest_version\":1}").expect("write manifest");

        let server = MockHttpServer::spawn_with_builder(|base| {
            vec![
                MockResponse::json(
                    "201 Created",
                    &format!(r#"{{"links":{{"latest_draft":"{base}/draft"}}}}"#),
                ),
                MockResponse::json(
                    "200 OK",
                    &format!(
                        r#"{{"links":{{"bucket":"{base}/bucket"}},"id":123,"files":[{{"id":"old-file"}}]}}"#
                    ),
                ),
                MockResponse::json("204 No Content", r#"{}"#),
                MockResponse::json("200 OK", r#"{"updated":true}"#),
                MockResponse::json("200 OK", r#"{"uploaded":"completed"}"#),
                MockResponse::json("200 OK", r#"{"uploaded":"manifest"}"#),
                MockResponse::json("200 OK", r#"{"doi":"10.1234/mock-doi"}"#),
            ]
        });

        let config = PublishConfig {
            api_base: server.url("/api"),
            deposit_id: "999".to_string(),
        };
        publish_with_config("token", &output_path, &manifest_path, 7, 2, 1, &config)
            .expect("publish succeeds");

        let requests = server.requests();
        let request_paths: Vec<_> = requests
            .iter()
            .map(|request| format!("{} {}", request.method, request.path))
            .collect();
        assert_eq!(
            request_paths,
            vec![
                "POST /api/deposit/depositions/999/actions/newversion",
                "GET /draft",
                "DELETE /api/deposit/depositions/123/files/old-file",
                "PUT /api/deposit/depositions/123",
                "PUT /bucket/completed.jsonl.zst",
                "PUT /bucket/manifest.json",
                "POST /api/deposit/depositions/123/actions/publish",
            ]
        );

        let metadata: serde_json::Value =
            serde_json::from_slice(&requests[3].body).expect("metadata payload");
        assert_eq!(metadata["metadata"]["upload_type"], "dataset");
        assert!(
            metadata["metadata"]["description"]
                .as_str()
                .expect("description")
                .contains("completed.jsonl.zst")
        );
        assert_eq!(requests[4].body, b"output-bytes");
        assert_eq!(requests[5].body, b"{\"manifest_version\":1}");
    }

    #[test]
    fn publish_returns_unknown_when_publish_response_has_no_doi() {
        let temp_dir = TestDir::new("zenodo-unknown-doi");
        let output_path = temp_dir.path().join("completed.jsonl.zst");
        let manifest_path = temp_dir.path().join("manifest.json");
        std::fs::write(&output_path, b"output-bytes").expect("write output artifact");
        std::fs::write(&manifest_path, b"{\"manifest_version\":1}").expect("write manifest");

        let server = MockHttpServer::spawn_with_builder(|base| {
            vec![
                MockResponse::json(
                    "201 Created",
                    &format!(r#"{{"links":{{"latest_draft":"{base}/draft"}}}}"#),
                ),
                MockResponse::json(
                    "200 OK",
                    &format!(r#"{{"links":{{"bucket":"{base}/bucket"}},"id":123,"files":[]}}"#),
                ),
                MockResponse::json("200 OK", r#"{"updated":true}"#),
                MockResponse::json("200 OK", r#"{"uploaded":"completed"}"#),
                MockResponse::json("200 OK", r#"{"uploaded":"manifest"}"#),
                MockResponse::json("200 OK", r#"{}"#),
            ]
        });

        let config = PublishConfig {
            api_base: server.url("/api"),
            deposit_id: "999".to_string(),
        };
        let doi = publish_with_config("token", &output_path, &manifest_path, 7, 2, 1, &config)
            .expect("publish succeeds");
        assert_eq!(doi, "unknown");
    }

    #[test]
    fn upload_file_reports_missing_input() {
        let agent = zenodo_agent();
        let path = Path::new("/tmp/does-not-exist.jsonl.zst");
        let error = upload_file(&agent, "token", "http://127.0.0.1:9/bucket", path)
            .expect_err("missing file should fail");
        assert!(error.contains("cannot stat"));
    }

    #[test]
    fn upload_file_rejects_invalid_filename() {
        let agent = zenodo_agent();
        let error = upload_file(&agent, "token", "http://127.0.0.1:9/bucket", Path::new(""))
            .expect_err("invalid filename should fail");
        assert!(error.contains("invalid filename"));
    }

    #[test]
    fn publish_with_config_reports_missing_latest_draft() {
        let temp_dir = TestDir::new("zenodo-missing-draft");
        let output_path = temp_dir.path().join("completed.jsonl.zst");
        let manifest_path = temp_dir.path().join("manifest.json");
        std::fs::write(&output_path, b"output-bytes").expect("write output artifact");
        std::fs::write(&manifest_path, b"{\"manifest_version\":1}").expect("write manifest");

        let server = MockHttpServer::spawn(vec![MockResponse::json("201 Created", r#"{}"#)]);
        let config = PublishConfig {
            api_base: server.url("/api"),
            deposit_id: "999".to_string(),
        };

        let error = publish_with_config("token", &output_path, &manifest_path, 7, 2, 1, &config)
            .expect_err("missing latest_draft should fail");
        assert!(error.contains("missing latest_draft"));
    }
}
