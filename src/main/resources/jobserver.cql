CREATE KEYSPACE IF NOT EXISTS jobserver WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};

CREATE TABLE IF NOT EXISTS jobserver.job_info (
    job_id text,
    content text,

    PRIMARY KEY (job_id)
) ;

CREATE TABLE IF NOT EXISTS jobserver.job_config (
    job_id text,
    content text,

    PRIMARY KEY (job_id)
) ;

