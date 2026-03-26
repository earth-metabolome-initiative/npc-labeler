use std::sync::Arc;

use arrow::array::{BooleanBuilder, Int32Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::db;

const PAGE_SIZE: i64 = 10_000;

pub fn export_parquet(conn: &mut diesel::SqliteConnection, output: &str) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("cid", DataType::Int32, false),
        Field::new("smiles", DataType::Utf8, false),
        Field::new("class_results", DataType::Utf8, true),
        Field::new("superclass_results", DataType::Utf8, true),
        Field::new("pathway_results", DataType::Utf8, true),
        Field::new("isglycoside", DataType::Boolean, true),
        Field::new("status", DataType::Utf8, false),
    ]));

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .build();

    let file = std::fs::File::create(output).expect("Failed to create output file");
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
        .expect("Failed to create Parquet writer");

    let mut after_cid = 0i32;
    let mut total_rows: u64 = 0;

    loop {
        let page = db::get_classified_page(conn, after_cid, PAGE_SIZE);
        if page.is_empty() {
            break;
        }

        let len = page.len();
        let mut cid_builder = Int32Builder::with_capacity(len);
        let mut smiles_builder = StringBuilder::with_capacity(len, len * 50);
        let mut class_builder = StringBuilder::with_capacity(len, len * 30);
        let mut superclass_builder = StringBuilder::with_capacity(len, len * 30);
        let mut pathway_builder = StringBuilder::with_capacity(len, len * 30);
        let mut glycoside_builder = BooleanBuilder::with_capacity(len);
        let mut status_builder = StringBuilder::with_capacity(len, len * 10);

        for (
            cid,
            smiles,
            class_results,
            superclass_results,
            pathway_results,
            isglycoside,
            status,
        ) in &page
        {
            cid_builder.append_value(*cid);
            smiles_builder.append_value(smiles);
            match class_results {
                Some(v) => class_builder.append_value(v),
                None => class_builder.append_null(),
            }
            match superclass_results {
                Some(v) => superclass_builder.append_value(v),
                None => superclass_builder.append_null(),
            }
            match pathway_results {
                Some(v) => pathway_builder.append_value(v),
                None => pathway_builder.append_null(),
            }
            match isglycoside {
                Some(v) => glycoside_builder.append_value(*v),
                None => glycoside_builder.append_null(),
            }
            status_builder.append_value(status);
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(cid_builder.finish()),
                Arc::new(smiles_builder.finish()),
                Arc::new(class_builder.finish()),
                Arc::new(superclass_builder.finish()),
                Arc::new(pathway_builder.finish()),
                Arc::new(glycoside_builder.finish()),
                Arc::new(status_builder.finish()),
            ],
        )
        .expect("Failed to create record batch");

        writer.write(&batch).expect("Failed to write batch");

        after_cid = page.last().unwrap().0;
        total_rows += len as u64;

        if total_rows.is_multiple_of(100_000) {
            eprintln!("[export] {total_rows} rows written");
        }
    }

    writer.close().expect("Failed to close Parquet writer");
    let file_size = std::fs::metadata(output).map_or(0, |m| m.len());
    eprintln!(
        "[export] done: {total_rows} rows exported to {output} ({:.1} MB)",
        file_size as f64 / 1_048_576.0
    );
}
