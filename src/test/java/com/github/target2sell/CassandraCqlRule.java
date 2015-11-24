package com.github.target2sell;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.cassandraunit.CQLDataLoader;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class CassandraCqlRule extends ExternalResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraCqlRule.class);

    private final String keyspace;
    private final String cqlFileName;

    private Cluster cluster = null;
    private Session session = null;

    public CassandraCqlRule(String cqlFileName, String keyspace) {
        this.cqlFileName = cqlFileName;
        this.keyspace = keyspace;
    }

    @Override
    protected void before() throws Throwable {
        this.cluster = Cluster.builder().withProtocolVersion(ProtocolVersion.V3).addContactPoint("localhost").build();
        this.session = cluster.connect();
        createCassandraTable();
    }

    @Override
    protected void after() {
        truncate();
        session.close();
        cluster.close();
    }

    public void truncate() {
        Collection<TableMetadata> tables = cluster.getMetadata().getKeyspace(keyspace).getTables();
        for (TableMetadata table : tables) {
            session.execute(QueryBuilder.truncate(table));
        }
    }

    public Session getSession() {
        return session;
    }

    private void createCassandraTable() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Create Cassandra table");
        }
        session.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspace + " " +
                "WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } " +
                "AND DURABLE_WRITES = false ");
        session.execute("USE " + keyspace);
        new CQLDataLoader(session).load(new ClassPathCQLDataSet(cqlFileName, false, false, keyspace));
        truncate();
    }

}
