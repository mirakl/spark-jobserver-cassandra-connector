package com.github.target2sell;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.LatencyAwarePolicy;
import org.slf4j.Logger;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.slf4j.LoggerFactory.getLogger;

public class SessionProvider {
    private final Logger logger = getLogger(SessionProvider.class);
    private static final List<InetAddress> DEFAULT_CONTACT_POINT = singletonList(InetAddress.getLoopbackAddress());
    private static Session session;

    private static Cluster cluster = null;
    private final String datacenter;
    private final String keyspace;
    private final String contactsPoints;
    private final String consistencyLevel;

    public SessionProvider(String datacenter, String keyspace, String contactsPoints, String consistencyLevel) {
        this.datacenter = datacenter;
        this.contactsPoints = contactsPoints;
        this.consistencyLevel = consistencyLevel;
        this.keyspace = keyspace;
        initCluster();
    }


    private synchronized void initCluster() {
        if (null == cluster || cluster.isClosed()) {
            LatencyAwarePolicy latencyPolicy = LatencyAwarePolicy.builder(new DCAwareRoundRobinPolicy(getDataCenter()))
                    .build();
            Cluster.Builder builder = Cluster.builder().withLoadBalancingPolicy(latencyPolicy);
            builder.withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.valueOf(consistencyLevel)));
            cluster = builder.addContactPoints(getContactsPoints()).build();
        }
    }

    public Session getInstance() {
        synchronized (this) {
            if (session == null) {
                createSession();
            }
        }
        return session;
    }

    private void createSession() {
        session = cluster.connect(keyspace);
    }

    private List<InetAddress> getContactsPoints() {
        if (null == contactsPoints) {
            return DEFAULT_CONTACT_POINT;
        }

        List<InetAddress> inetAddresses = new ArrayList<>();

        for (String property : contactsPoints.split(",")) {
            try {
                inetAddresses.add(InetAddress.getByName(property.trim()));
            } catch (UnknownHostException e) {
                logger.warn("Invalid cassandra address {} : {}", property, e.getMessage(), e);
            }
        }

        return inetAddresses.isEmpty() ? DEFAULT_CONTACT_POINT : inetAddresses;
    }

    private String getDataCenter() {
        return datacenter;
    }
}
