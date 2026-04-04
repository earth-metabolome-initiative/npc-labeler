use std::path::{Path, PathBuf};
use std::time::Duration;

use zenodo_rs::{
    AccessRight, Auth, Creator, DepositMetadataUpdate, DepositionId, Endpoint, FileReplacePolicy,
    PollOptions, RelatedIdentifier, UploadSpec, UploadType, ZenodoClient,
};

const ROOT_DEPOSITION_ID: u64 = 14_040_990;

#[derive(Clone, Debug)]
struct PublishConfig {
    endpoint: Endpoint,
    deposition_id: DepositionId,
}

impl PublishConfig {
    fn production() -> Self {
        Self {
            endpoint: Endpoint::Production,
            deposition_id: DepositionId(ROOT_DEPOSITION_ID),
        }
    }
}

fn build_metadata(
    successful_rows: u64,
    invalid_rows: u64,
    failed_rows: u64,
) -> Result<DepositMetadataUpdate, String> {
    let today = chrono::Local::now().date_naive();
    let excluded_rows = invalid_rows + failed_rows;
    let creator = Creator::builder()
        .name("Cappelletti, Luca")
        .orcid("0000-0002-1269-2038")
        .build()
        .map_err(|error| format!("failed to build creator metadata: {error}"))?;

    DepositMetadataUpdate::builder()
        .title(format!("NPClassifier PubChem Classifications ({today})"))
        .upload_type(UploadType::Dataset)
        .publication_date(today)
        .description_html(format!(
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
        ))
        .creator(creator)
        .keyword("natural products")
        .keyword("NPClassifier")
        .keyword("PubChem")
        .keyword("cheminformatics")
        .keyword("SMILES")
        .keyword("chemical classification")
        .keyword("open data")
        .keyword("machine learning dataset")
        .access_right(AccessRight::Open)
        .license("MIT")
        .related_identifier(related_identifier(
            "https://github.com/earth-metabolome-initiative/npc-labeler",
            "isCompiledBy",
            Some("software"),
        )?)
        .related_identifier(related_identifier(
            "https://npclassifier.gnps2.org/",
            "isDerivedFrom",
            None,
        )?)
        .notes(format!(
            "Snapshot: {today}. Successful rows: {successful_rows}. Invalid: {invalid_rows}. Failed: {failed_rows}."
        ))
        .build()
        .map_err(|error| format!("failed to build deposit metadata: {error}"))
}

fn related_identifier(
    identifier: &str,
    relation: &str,
    resource_type: Option<&str>,
) -> Result<RelatedIdentifier, String> {
    let builder = RelatedIdentifier::builder()
        .identifier(identifier)
        .relation(relation)
        .scheme("url");
    let builder = if let Some(resource_type) = resource_type {
        builder.resource_type(resource_type)
    } else {
        builder
    };

    builder
        .build()
        .map_err(|error| format!("failed to build related identifier metadata: {error}"))
}

fn zenodo_client(token: &str, config: &PublishConfig) -> Result<ZenodoClient, String> {
    ZenodoClient::builder(Auth::new(token))
        .endpoint(config.endpoint.clone())
        .user_agent(format!(
            "{}/{}",
            env!("CARGO_PKG_NAME"),
            env!("CARGO_PKG_VERSION")
        ))
        .poll_options(PollOptions {
            max_wait: Duration::from_mins(2),
            initial_delay: Duration::from_millis(250),
            max_delay: Duration::from_secs(2),
        })
        .build()
        .map_err(|error| format!("failed to build Zenodo client: {error}"))
}

fn prepare_upload_spec(path: &Path) -> Result<UploadSpec, String> {
    let filename = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("invalid filename for {}", path.display()))?
        .to_string();
    let file_size = std::fs::metadata(path)
        .map_err(|error| format!("cannot stat {}: {error}", path.display()))?
        .len();

    eprintln!(
        "[zenodo] queueing {filename} ({:.1} MB)...",
        file_size as f64 / 1_048_576.0
    );

    UploadSpec::from_path(PathBuf::from(path))
        .map_err(|error| format!("failed to prepare upload {filename}: {error}"))
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
    let metadata = build_metadata(successful_rows, invalid_rows, failed_rows)?;
    let files = vec![
        prepare_upload_spec(output_path)?,
        prepare_upload_spec(manifest_path)?,
    ];

    eprintln!("[zenodo] publishing new dataset version...");

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("failed to build tokio runtime: {error}"))?;
    let client = zenodo_client(token, config)?;

    runtime.block_on(async move {
        let published = client
            .publish_dataset_with_policy(
                config.deposition_id,
                &metadata,
                FileReplacePolicy::ReplaceAll,
                files,
            )
            .await
            .map_err(|error| format!("failed to publish dataset: {error}"))?;

        Ok(published
            .record
            .doi
            .as_ref()
            .map(ToString::to_string)
            .or_else(|| published.deposition.doi.as_ref().map(ToString::to_string))
            .unwrap_or_else(|| "unknown".to_string()))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{MockHttpServer, MockResponse, TestDir};

    #[test]
    fn config_and_metadata_helpers_have_expected_defaults() {
        let config = PublishConfig::production();
        assert_eq!(
            config
                .endpoint
                .base_url()
                .expect("production api base")
                .as_str(),
            "https://zenodo.org/api/"
        );
        assert_eq!(config.deposition_id, DepositionId(ROOT_DEPOSITION_ID));

        let metadata = build_metadata(7, 2, 1).expect("metadata");
        assert_eq!(metadata.upload_type, UploadType::Dataset);
        assert_eq!(metadata.access_right, AccessRight::Open);
        assert_eq!(metadata.license.as_deref(), Some("MIT"));
        assert_eq!(metadata.keywords.len(), 8);

        let payload = serde_json::to_value(&metadata).expect("serialize metadata");
        assert!(
            payload["description"]
                .as_str()
                .expect("description")
                .contains("Excluded rows in this snapshot: 3")
        );
        assert!(
            payload["notes"]
                .as_str()
                .expect("notes")
                .contains("Successful rows: 7. Invalid: 2. Failed: 1.")
        );
        assert_eq!(
            payload["related_identifiers"]
                .as_array()
                .expect("relations")
                .len(),
            2
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
                    "200 OK",
                    r#"{"id":999,"submitted":true,"state":"done","metadata":{},"files":[],"links":{}}"#,
                ),
                MockResponse::json(
                    "201 Created",
                    &format!(
                        r#"{{"id":999,"submitted":true,"state":"done","metadata":{{}},"files":[],"links":{{"latest_draft":"{base}/api/deposit/depositions/123"}}}}"#
                    ),
                ),
                MockResponse::json(
                    "200 OK",
                    &format!(
                        r#"{{"id":123,"submitted":false,"state":"inprogress","metadata":{{}},"files":[],"links":{{"bucket":"{base}/api/files/bucket-123"}}}}"#
                    ),
                ),
                MockResponse::json(
                    "200 OK",
                    &format!(
                        r#"{{"id":123,"submitted":false,"state":"inprogress","metadata":{{}},"files":[],"links":{{"bucket":"{base}/api/files/bucket-123"}}}}"#
                    ),
                ),
                MockResponse::json(
                    "200 OK",
                    &format!(
                        r#"{{"id":123,"submitted":false,"state":"inprogress","metadata":{{}},"files":[{{"id":"old-file","filename":"stale.txt","filesize":1}}],"links":{{"bucket":"{base}/api/files/bucket-123"}}}}"#
                    ),
                ),
                MockResponse::empty("204 No Content"),
                MockResponse::json("200 OK", r#"{"key":"completed.jsonl.zst","size":12}"#),
                MockResponse::json("200 OK", r#"{"key":"manifest.json","size":22}"#),
                MockResponse::json(
                    "202 Accepted",
                    r#"{"id":123,"submitted":false,"state":"inprogress","metadata":{},"files":[],"links":{}}"#,
                ),
                MockResponse::json(
                    "200 OK",
                    r#"{"id":123,"record_id":456,"submitted":true,"state":"done","metadata":{},"files":[],"links":{}}"#,
                ),
                MockResponse::json(
                    "200 OK",
                    r#"{"id":456,"recid":456,"doi":"10.1234/mock-doi","metadata":{"title":"published"},"files":[],"links":{}}"#,
                ),
            ]
        });

        let config = PublishConfig {
            endpoint: Endpoint::Custom(server.url("/").parse().expect("endpoint url")),
            deposition_id: DepositionId(999),
        };
        let doi = publish_with_config("token", &output_path, &manifest_path, 7, 2, 1, &config)
            .expect("publish succeeds");
        assert_eq!(doi, "10.1234/mock-doi");

        let requests = server.requests();
        let request_paths: Vec<_> = requests
            .iter()
            .map(|request| format!("{} {}", request.method, request.path))
            .collect();
        assert_eq!(
            request_paths,
            vec![
                "GET /api/deposit/depositions/999",
                "POST /api/deposit/depositions/999/actions/newversion",
                "GET /api/deposit/depositions/123",
                "PUT /api/deposit/depositions/123",
                "GET /api/deposit/depositions/123",
                "DELETE /api/deposit/depositions/123/files/old-file",
                "PUT /api/files/bucket-123/completed.jsonl.zst",
                "PUT /api/files/bucket-123/manifest.json",
                "POST /api/deposit/depositions/123/actions/publish",
                "GET /api/deposit/depositions/123",
                "GET /api/records/456",
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
        assert_eq!(requests[6].body, b"output-bytes");
        assert_eq!(requests[7].body, b"{\"manifest_version\":1}");
    }

    #[test]
    fn publish_returns_unknown_when_record_has_no_doi() {
        let temp_dir = TestDir::new("zenodo-unknown-doi");
        let output_path = temp_dir.path().join("completed.jsonl.zst");
        let manifest_path = temp_dir.path().join("manifest.json");
        std::fs::write(&output_path, b"output-bytes").expect("write output artifact");
        std::fs::write(&manifest_path, b"{\"manifest_version\":1}").expect("write manifest");

        let server = MockHttpServer::spawn_with_builder(|base| {
            vec![
                MockResponse::json(
                    "200 OK",
                    r#"{"id":999,"submitted":true,"state":"done","metadata":{},"files":[],"links":{}}"#,
                ),
                MockResponse::json(
                    "201 Created",
                    &format!(
                        r#"{{"id":999,"submitted":true,"state":"done","metadata":{{}},"files":[],"links":{{"latest_draft":"{base}/api/deposit/depositions/123"}}}}"#
                    ),
                ),
                MockResponse::json(
                    "200 OK",
                    &format!(
                        r#"{{"id":123,"submitted":false,"state":"inprogress","metadata":{{}},"files":[],"links":{{"bucket":"{base}/api/files/bucket-123"}}}}"#
                    ),
                ),
                MockResponse::json(
                    "200 OK",
                    &format!(
                        r#"{{"id":123,"submitted":false,"state":"inprogress","metadata":{{}},"files":[],"links":{{"bucket":"{base}/api/files/bucket-123"}}}}"#
                    ),
                ),
                MockResponse::json(
                    "200 OK",
                    &format!(
                        r#"{{"id":123,"submitted":false,"state":"inprogress","metadata":{{}},"files":[],"links":{{"bucket":"{base}/api/files/bucket-123"}}}}"#
                    ),
                ),
                MockResponse::json("200 OK", r#"{"key":"completed.jsonl.zst","size":12}"#),
                MockResponse::json("200 OK", r#"{"key":"manifest.json","size":22}"#),
                MockResponse::json(
                    "202 Accepted",
                    r#"{"id":123,"submitted":false,"state":"inprogress","metadata":{},"files":[],"links":{}}"#,
                ),
                MockResponse::json(
                    "200 OK",
                    r#"{"id":123,"record_id":456,"submitted":true,"state":"done","metadata":{},"files":[],"links":{}}"#,
                ),
                MockResponse::json(
                    "200 OK",
                    r#"{"id":456,"recid":456,"metadata":{"title":"published"},"files":[],"links":{}}"#,
                ),
            ]
        });

        let config = PublishConfig {
            endpoint: Endpoint::Custom(server.url("/").parse().expect("endpoint url")),
            deposition_id: DepositionId(999),
        };
        let doi = publish_with_config("token", &output_path, &manifest_path, 7, 2, 1, &config)
            .expect("publish succeeds");
        assert_eq!(doi, "unknown");
    }

    #[test]
    fn prepare_upload_spec_reports_missing_input() {
        let path = Path::new("/tmp/does-not-exist.jsonl.zst");
        let error = prepare_upload_spec(path).expect_err("missing file should fail");
        assert!(error.contains("cannot stat"));
    }

    #[test]
    fn prepare_upload_spec_rejects_invalid_filename() {
        let error = prepare_upload_spec(Path::new("")).expect_err("invalid filename should fail");
        assert!(error.contains("invalid filename"));
    }

    #[test]
    fn publish_with_config_reports_missing_latest_draft() {
        let temp_dir = TestDir::new("zenodo-missing-draft");
        let output_path = temp_dir.path().join("completed.jsonl.zst");
        let manifest_path = temp_dir.path().join("manifest.json");
        std::fs::write(&output_path, b"output-bytes").expect("write output artifact");
        std::fs::write(&manifest_path, b"{\"manifest_version\":1}").expect("write manifest");

        let server = MockHttpServer::spawn(vec![
            MockResponse::json(
                "200 OK",
                r#"{"id":999,"submitted":true,"state":"done","metadata":{},"files":[],"links":{}}"#,
            ),
            MockResponse::json(
                "201 Created",
                r#"{"id":999,"submitted":true,"state":"done","metadata":{},"files":[],"links":{}}"#,
            ),
        ]);
        let config = PublishConfig {
            endpoint: Endpoint::Custom(server.url("/").parse().expect("endpoint url")),
            deposition_id: DepositionId(999),
        };

        let error = publish_with_config("token", &output_path, &manifest_path, 7, 2, 1, &config)
            .expect_err("missing latest_draft should fail");
        assert!(error.contains("latest_draft"));
    }
}
