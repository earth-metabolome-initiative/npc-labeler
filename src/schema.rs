diesel::table! {
    classifications (cid) {
        cid -> Integer,
        smiles -> Text,
        class_results -> Nullable<Text>,
        superclass_results -> Nullable<Text>,
        pathway_results -> Nullable<Text>,
        isglycoside -> Nullable<Bool>,
        status -> Text,
        attempts -> Integer,
        last_error -> Nullable<Text>,
        classified_at -> Nullable<Timestamp>,
    }
}
