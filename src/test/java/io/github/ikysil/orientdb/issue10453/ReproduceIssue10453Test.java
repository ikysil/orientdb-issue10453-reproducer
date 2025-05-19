package io.github.ikysil.orientdb.issue10453;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.OrientDBConfigBuilder;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraphBaseFactory;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraphFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThatNoException;

class ReproduceIssue10453Test {

    public static final int DEFAULT_CLUSTERS = 4;
    private static final String dbName = "Issue10453";
    private static final String ROOT = "root";

    private final Logger logger = Logger.getLogger(ReproduceIssue10453Test.class.getName());

    @BeforeEach
    void setup() {
        connect("remote:localhost", dbName, ROOT, ROOT);
    }

    @AfterEach
    void tearDown() {
        disconnect();
        Orient.instance().shutdown();
    }

    private OrientDB orient;
    private ODatabasePool pool;

    public void disconnect() {
        if (pool != null) {
            pool.close();
        }
        if (orient != null) {
            orient.close();
        }
    }

    private void connect(
            final String serverName,
            final String dbName,
            final String userName,
            final String password
    ) {
        final OrientDBConfigBuilder poolCfg = OrientDBConfig.builder();
        poolCfg.addConfig(OGlobalConfiguration.DB_POOL_MIN, 5);
        poolCfg.addConfig(OGlobalConfiguration.DB_POOL_MAX, 100);
        final OrientDBConfig oriendDBconfig = poolCfg.build();
        if (serverName.startsWith("remote:")) {
            // remote:<host> can be called like that
            orient = new OrientDB(serverName, userName, password, oriendDBconfig);
        } else if (serverName.startsWith("embedded:")) {
            // embedded:/<path>/directory + server can be called like that
            orient = new OrientDB(serverName, OrientDBConfig.defaultConfig());
        } else {
            throw new UnsupportedOperationException(
                    "Currently only 'embedded' and 'remote' are supported.");
        }
        pool = orient.cachedPool(dbName, userName, password, oriendDBconfig);
    }

    protected OrientGraphBaseFactory getOrientGraphBaseFactory() {
        return new OrientGraphFactory(
                orient,
                dbName,
                null,
                ROOT,
                ROOT
        );
    }

    @Test
    void hasOrientGraphBaseFactory() {
        assertThatNoException().isThrownBy(this::getOrientGraphBaseFactory);
    }

    @Test
    void defineSchemaBeforeWorkaround() {
        final var runId = System.currentTimeMillis();
        var todoClasses = 16;
        final var schemaGraph = getOrientGraphBaseFactory().getNoTx();
        try (var db = schemaGraph.getRawDatabase()) {
            while (todoClasses > 0) {
                todoClasses--;

                if (todoClasses % 13 == 0) {
                    logger.info("TODO %d".formatted(todoClasses));
                }
                final var className = "TestClass_%d_%d".formatted(runId, todoClasses);
                final var defineEdge = todoClasses % 31 < 7;

                var schema = schemaGraph.getRawDatabase().getMetadata().getSchema();
                var type = schema.getClass(className);
                if (type == null) {
                    if (defineEdge) {
                        schema = schemaGraph.getRawDatabase().getMetadata().getSchema();
                    }
                    final var superClass = schema.getClass(defineEdge ? OClass.EDGE_CLASS_NAME : OClass.VERTEX_CLASS_NAME);
                    type = schema.createClass(className, DEFAULT_CLUSTERS, superClass);
                }

                for (int propIdx = 0; propIdx < 31; propIdx++) {
                    final var propName = "prop_%d_%d".formatted(runId, propIdx);
                    if (type.existsProperty(propName)) {
                        continue;
                    }
                    final var qualifiedPropName = "%s.%s".formatted(className, propName);
                    final var query = "CREATE PROPERTY %s %s UNSAFE".formatted(qualifiedPropName, OType.STRING);
                    db.command(query).close();
                }
            }
            logger.info("DONE");
        }
    }

    @Test
    void defineSchemaAfterWorkaround() {
        final var runId = System.currentTimeMillis();
        var todoClasses = 16;
        final var schemaGraph = getOrientGraphBaseFactory().getNoTx();
        try (var db = schemaGraph.getRawDatabase()) {
            var schema = schemaGraph.getRawDatabase().getMetadata().getSchema();
            while (todoClasses > 0) {
                todoClasses--;

                if (todoClasses % 13 == 0) {
                    logger.info("TODO %d".formatted(todoClasses));
                }
                final var className = "TestClass_%d_%d".formatted(runId, todoClasses);
                final var defineEdge = todoClasses % 31 < 7;

                var type = schema.getClass(className);
                if (type == null) {
                    final var superClass = schema.getClass(defineEdge ? OClass.EDGE_CLASS_NAME : OClass.VERTEX_CLASS_NAME);
                    type = schema.createClass(className, DEFAULT_CLUSTERS, superClass);
                }

                for (int propIdx = 0; propIdx < 31; propIdx++) {
                    final var propName = "prop_%d_%d".formatted(runId, propIdx);
                    if (type.existsProperty(propName)) {
                        continue;
                    }
                    final var qualifiedPropName = "%s.%s".formatted(className, propName);
                    final var query = "CREATE PROPERTY %s %s UNSAFE".formatted(qualifiedPropName, OType.STRING);
                    db.command(query).close();
                }
            }
            logger.info("DONE");
        }
    }

}
