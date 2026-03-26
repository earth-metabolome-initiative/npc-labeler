use std::fs::File;
use std::path::Path;

const ZENODO_API: &str = "https://zenodo.org/api";
const DEPOSIT_ID: &str = "14040990";

fn build_metadata(classified_count: i64, empty_count: i64, total_count: i64) -> serde_json::Value {
    let today = chrono::Local::now().format("%Y-%m-%d").to_string();
    let done = classified_count + empty_count;
    let pct = done as f64 / total_count.max(1) as f64 * 100.0;

    serde_json::json!({
        "metadata": {
            "title": format!("NPClassifier PubChem Classifications ({today})"),
            "upload_type": "dataset",
            "publication_date": today,
            "description": format!(
                "<p>Open dataset of <a href=\"https://npclassifier.gnps2.org/\">NPClassifier</a> \
                 classifications for all PubChem compounds.</p>\
                 <p>This snapshot contains {done} queried compounds out of {total_count} total \
                 PubChem entries ({pct:.1}% complete): {classified_count} with at least one \
                 classification label and {empty_count} that returned no labels.</p>\
                 <p>Each row contains the PubChem CID, SMILES, and the NPClassifier pathway, \
                 superclass, and class labels (as JSON arrays), plus a glycoside flag.</p>\
                 <p>Format: Apache Parquet with Zstandard compression.</p>\
                 <p>Updated weekly. Source code: \
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
                "Snapshot: {today}. Queried: {done}/{total_count} ({pct:.1}%). \
                 Classified: {classified_count}. Empty: {empty_count}."
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
    parquet_path: &str,
    classified_count: i64,
    empty_count: i64,
    total_count: i64,
) -> Result<String, String> {
    let agent = zenodo_agent();

    eprintln!("[zenodo] creating new version...");

    // 1. Create a new version from the existing deposit
    let resp = agent
        .post(&format!(
            "{ZENODO_API}/deposit/depositions/{DEPOSIT_ID}/actions/newversion"
        ))
        .set("Authorization", &format!("Bearer {token}"))
        .call()
        .map_err(|e| format!("failed to create new version: {e}"))?;

    let body: serde_json::Value = resp
        .into_json()
        .map_err(|e| format!("failed to parse new version response: {e}"))?;

    let draft_url = body["links"]["latest_draft"]
        .as_str()
        .ok_or("missing latest_draft link in response")?
        .to_string();

    // 2. Get the draft
    let draft: serde_json::Value = agent
        .get(&draft_url)
        .set("Authorization", &format!("Bearer {token}"))
        .call()
        .map_err(|e| format!("failed to get draft: {e}"))?
        .into_json()
        .map_err(|e| format!("failed to parse draft: {e}"))?;

    let bucket_url = draft["links"]["bucket"]
        .as_str()
        .ok_or("missing bucket link in draft")?
        .to_string();

    let draft_id = draft["id"].as_u64().ok_or("missing id in draft")?;

    // 3. Delete existing files from the draft
    if let Some(files) = draft["files"].as_array() {
        for file in files {
            if let Some(file_id) = file["id"].as_str() {
                let _ = agent
                    .delete(&format!(
                        "{ZENODO_API}/deposit/depositions/{draft_id}/files/{file_id}"
                    ))
                    .set("Authorization", &format!("Bearer {token}"))
                    .call();
            }
        }
    }

    // 4. Update metadata
    let metadata = build_metadata(classified_count, empty_count, total_count);
    agent
        .put(&format!("{ZENODO_API}/deposit/depositions/{draft_id}"))
        .set("Authorization", &format!("Bearer {token}"))
        .set("Content-Type", "application/json")
        .send_json(&metadata)
        .map_err(|e| format!("failed to update metadata: {e}"))?;

    // 5. Upload the Parquet file
    let filename = Path::new(parquet_path)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("classifications.parquet");

    let file_size = std::fs::metadata(parquet_path)
        .map_err(|e| format!("cannot stat {parquet_path}: {e}"))?
        .len();

    eprintln!(
        "[zenodo] uploading {filename} ({:.1} MB)...",
        file_size as f64 / 1_048_576.0
    );

    let file =
        File::open(parquet_path).map_err(|e| format!("failed to open {parquet_path}: {e}"))?;
    let content_length = file_size.to_string();
    agent
        .put(&format!("{bucket_url}/{filename}"))
        .set("Authorization", &format!("Bearer {token}"))
        .set("Content-Type", "application/octet-stream")
        .set("Content-Length", &content_length)
        .send(file)
        .map_err(|e| format!("failed to upload file: {e}"))?;

    // 6. Publish
    let pub_resp = agent
        .post(&format!(
            "{ZENODO_API}/deposit/depositions/{draft_id}/actions/publish"
        ))
        .set("Authorization", &format!("Bearer {token}"))
        .call()
        .map_err(|e| format!("failed to publish: {e}"))?;

    let pub_body: serde_json::Value = pub_resp
        .into_json()
        .map_err(|e| format!("failed to parse publish response: {e}"))?;

    let doi = pub_body["doi"].as_str().unwrap_or("unknown").to_string();

    eprintln!("[zenodo] published: DOI {doi}");
    Ok(doi)
}
