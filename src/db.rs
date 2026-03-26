#![allow(dead_code)]
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};

use crate::models::{ClassificationUpdate, NewClassification};
use crate::schema::classifications;

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!();

pub fn initialize(database_url: &str) -> SqliteConnection {
    let mut conn = SqliteConnection::establish(database_url)
        .unwrap_or_else(|e| panic!("Error connecting to {database_url}: {e}"));

    diesel::sql_query("PRAGMA journal_mode=WAL")
        .execute(&mut conn)
        .expect("Failed to set WAL mode");
    diesel::sql_query("PRAGMA synchronous=NORMAL")
        .execute(&mut conn)
        .expect("Failed to set synchronous mode");
    diesel::sql_query("PRAGMA cache_size=-64000")
        .execute(&mut conn)
        .expect("Failed to set cache size");

    conn.run_pending_migrations(MIGRATIONS)
        .expect("Failed to run migrations");

    conn
}

pub fn count_total(conn: &mut SqliteConnection) -> i64 {
    use diesel::dsl::count_star;
    classifications::table
        .select(count_star())
        .first::<i64>(conn)
        .unwrap_or(0)
}

pub fn count_by_status(conn: &mut SqliteConnection, status: &str) -> i64 {
    use diesel::dsl::count_star;
    classifications::table
        .filter(classifications::status.eq(status))
        .select(count_star())
        .first::<i64>(conn)
        .unwrap_or(0)
}

pub fn bulk_insert(
    conn: &mut SqliteConnection,
    rows: &[NewClassification],
) -> Result<usize, diesel::result::Error> {
    conn.transaction(|conn| {
        diesel::insert_or_ignore_into(classifications::table)
            .values(rows)
            .execute(conn)
    })
}

pub fn get_next_pending(conn: &mut SqliteConnection) -> Option<(i32, String)> {
    classifications::table
        .filter(classifications::status.eq("pending"))
        .order(classifications::cid.asc())
        .select((classifications::cid, classifications::smiles))
        .first::<(i32, String)>(conn)
        .ok()
}

#[allow(clippy::needless_pass_by_value)]
pub fn update_one(
    conn: &mut SqliteConnection,
    cid: i32,
    update: ClassificationUpdate,
    increment_attempts: bool,
) -> Result<(), diesel::result::Error> {
    conn.transaction(|conn| {
        diesel::update(classifications::table.find(cid))
            .set(&update)
            .execute(conn)?;
        if increment_attempts {
            diesel::update(classifications::table.find(cid))
                .set(classifications::attempts.eq(classifications::attempts + 1))
                .execute(conn)?;
        }
        Ok(())
    })
}

pub fn reset_failed_for_retry(conn: &mut SqliteConnection, max_attempts: i32) -> i64 {
    diesel::update(
        classifications::table
            .filter(classifications::status.eq("failed"))
            .filter(classifications::attempts.lt(max_attempts)),
    )
    .set(classifications::status.eq("pending"))
    .execute(conn)
    .unwrap_or(0) as i64
}

pub type ClassifiedRow = (
    i32,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<bool>,
    String,
);

pub fn get_classified_page(
    conn: &mut SqliteConnection,
    after_cid: i32,
    limit: i64,
) -> Vec<ClassifiedRow> {
    classifications::table
        .filter(
            classifications::status
                .eq("classified")
                .or(classifications::status.eq("empty")),
        )
        .filter(classifications::cid.gt(after_cid))
        .order(classifications::cid.asc())
        .select((
            classifications::cid,
            classifications::smiles,
            classifications::class_results,
            classifications::superclass_results,
            classifications::pathway_results,
            classifications::isglycoside,
            classifications::status,
        ))
        .limit(limit)
        .load(conn)
        .unwrap_or_default()
}

pub fn wal_checkpoint(conn: &mut SqliteConnection) {
    diesel::sql_query("PRAGMA wal_checkpoint(PASSIVE)")
        .execute(conn)
        .ok();
}

#[derive(Debug, QueryableByName)]
pub struct LabelCount {
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub label: String,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    pub cnt: i64,
}

pub fn top_labels(conn: &mut SqliteConnection, column: &str, limit: usize) -> Vec<LabelCount> {
    // json_each requires the json1 extension (compiled into SQLite by default)
    let query = format!(
        "SELECT j.value AS label, COUNT(*) AS cnt \
         FROM classifications, json_each(classifications.{column}) AS j \
         WHERE classifications.status IN ('classified', 'empty') \
           AND classifications.{column} IS NOT NULL \
           AND classifications.{column} != '[]' \
         GROUP BY j.value \
         ORDER BY cnt DESC \
         LIMIT {limit}"
    );
    diesel::sql_query(query)
        .load::<LabelCount>(conn)
        .unwrap_or_default()
}
