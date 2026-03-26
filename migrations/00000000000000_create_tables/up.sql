CREATE TABLE classifications (
    cid INTEGER PRIMARY KEY NOT NULL,
    smiles TEXT NOT NULL,
    class_results TEXT,
    superclass_results TEXT,
    pathway_results TEXT,
    isglycoside BOOLEAN,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    classified_at TIMESTAMP
);

CREATE INDEX idx_status ON classifications(status);
