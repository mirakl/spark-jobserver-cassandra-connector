# spark-jobserver-cassandra-connector

Java  Cassandra connector for Spark JobServer


# Build status

[![Build Status](https://travis-ci.org/target2sell/spark-jobserver-cassandra-connector.svg)](https://travis-ci.org/target2sell/spark-jobserver-cassandra-connector)

# Configuration

## Create cassandra schema

```
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
```

## Configure spark-jobserver

Make directory: `/usr/bin/dse hadoop dfs -mkdir cfs:///jobserver`

Modify configuration:

```
spark {
    jobserver {
        port = 8090
        jar-store-rootdir = /tmp/jobserver/jars
        jobdao = com.github.target2sell.JobCassandraDao
    
        cassandradao {
          keyspace = jobserver
          datacenter = {{ t2s_back_cassandra_dc }}
          contactsPoints = 127.0.0.1
          consistencyLevel = ONE
          jarCache = /tmp/jobserver/jars
          fsUri = "cfs://jobserver/"
        }
    }
}
```
