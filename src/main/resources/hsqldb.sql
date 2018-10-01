DROP TABLE log_entries IF EXISTS;

CREATE TABLE log_entries  (
    log_entry_id BIGINT IDENTITY NOT NULL PRIMARY KEY,
    entry_date TIMESTAMP,
    ip VARCHAR(20),
    request VARCHAR(20),
    status VARCHAR(20)
);

DROP TABLE blocked_entries IF EXISTS;

CREATE TABLE blocked_entries  (
    blocked_entry_id BIGINT IDENTITY NOT NULL PRIMARY KEY,
    ip VARCHAR(20),
    comment VARCHAR(200)
);