package com.contrastsecurity.cassandra.migration;

import com.contrastsecurity.cassandra.migration.config.Keyspace;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;

public abstract class BaseIT {
    public static final String CASSANDRA__KEYSPACE = "cassandra_migration_test";
    public static final String CASSANDRA_CONTACT_POINT = "localhost";
    public static final int CASSANDRA_PORT = 9147;
    public static final String CASSANDRA_USERNAME = "cassandra";
    public static final String CASSANDRA_PASSWORD = "cassandra";

    @ClassRule
    public static CassandraCQLUnit cassandraUnit = new CassandraCQLUnit(new ClassPathCQLDataSet("it-schema.cql",false), "cassandra-unit.yaml");

    @Before
    public void createKeyspace() {
        Statement statement = new SimpleStatement(
                "CREATE KEYSPACE if not exists " + CASSANDRA__KEYSPACE +
                        "  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };"
        );
        getSession().execute(statement);
    }

    @After
    public void dropKeyspace() {
        Statement statement = new SimpleStatement(
                "DROP KEYSPACE " + CASSANDRA__KEYSPACE + ";"
        );
        getSession().execute(statement);
    }

    protected Keyspace getKeyspace() {
        Keyspace ks = new Keyspace();
        ks.setName(CASSANDRA__KEYSPACE);
        ks.getCluster().setContactpoints(CASSANDRA_CONTACT_POINT);
        ks.getCluster().setPort(CASSANDRA_PORT);
        ks.getCluster().setUsername(CASSANDRA_USERNAME);
        ks.getCluster().setPassword(CASSANDRA_PASSWORD);
        return ks;
    }

    protected Session getSession() {
         return cassandraUnit.getSession();
    }

    protected Session connectKeyspace(String keyspace) {
        return cassandraUnit.cluster.connect(keyspace);
    }

}
