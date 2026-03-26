use chrono::NaiveDateTime;
use diesel::prelude::*;

use crate::schema::classifications;

#[derive(Insertable)]
#[diesel(table_name = classifications)]
pub struct NewClassification<'a> {
    pub cid: i32,
    pub smiles: &'a str,
    pub status: &'a str,
}

#[derive(Debug, AsChangeset)]
#[diesel(table_name = classifications)]
#[diesel(treat_none_as_null = true)]
pub struct ClassificationUpdate {
    pub class_results: Option<String>,
    pub superclass_results: Option<String>,
    pub pathway_results: Option<String>,
    pub isglycoside: Option<bool>,
    pub status: String,
    pub last_error: Option<String>,
    pub classified_at: Option<NaiveDateTime>,
}

impl Default for ClassificationUpdate {
    fn default() -> Self {
        Self {
            class_results: None,
            superclass_results: None,
            pathway_results: None,
            isglycoside: None,
            status: "pending".to_string(),
            last_error: None,
            classified_at: None,
        }
    }
}
