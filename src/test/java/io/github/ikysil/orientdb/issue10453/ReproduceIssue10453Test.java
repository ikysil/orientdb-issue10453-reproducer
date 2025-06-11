package io.github.ikysil.orientdb.issue10453;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraphBaseFactory;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraphFactory;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

class ReproduceIssue10453Test {

    public static final int DEFAULT_CLUSTERS = 4;
    private static final String DB_NAME = "Issue10453";
    private static final String ROOT = "root";
    public static final int PROPERTIES_PER_CLASS = 31;
    public static final int CLASSES_PER_RUN = 61;

    private static final Supplier<ODatabaseSession> DISCONNECTED_SESSION_FACTORY = () -> {
        throw new IllegalStateException("disconnected");
    };

    private final Logger logger = Logger.getLogger(ReproduceIssue10453Test.class.getName());

    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(4);
    private final AtomicInteger checkSuccess = new AtomicInteger(0);
    private final Collection<Exception> checkFailures = Collections.synchronizedList(new ArrayList<>());
    private final AtomicReference<Supplier<ODatabaseSession>> databaseSessionFactory = new AtomicReference<>(DISCONNECTED_SESSION_FACTORY);

    @BeforeEach
    void setup() {
        Assertions.setMaxStackTraceElementsDisplayed(24);
        checkSuccess.set(0);
        checkFailures.clear();
        connect("remote:localhost", DB_NAME, ROOT, ROOT);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        assertChecks();

        disconnect();
        Orient.instance().shutdown();
    }

    private OrientDB orient;
    private ODatabasePool pool;

    public void disconnect() {
        databaseSessionFactory.set(DISCONNECTED_SESSION_FACTORY);
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
            orient = new OrientDB(serverName, oriendDBconfig);
        } else {
            throw new UnsupportedOperationException(
                    "Currently only 'embedded' and 'remote' are supported.");
        }
        pool = orient.cachedPool(dbName, userName, password, oriendDBconfig);
//        databaseSessionFactory.set(() -> pool.acquire());
        databaseSessionFactory.set(() -> orient.open(dbName, userName, password, oriendDBconfig));
    }

    protected OrientGraphBaseFactory getOrientGraphBaseFactory() {
        return new OrientGraphFactory(
                orient,
                DB_NAME,
                null,
                ROOT,
                ROOT
        );
    }

    @Test
    void hasOrientGraphBaseFactory() {
        assertThatNoException().isThrownBy(this::getOrientGraphBaseFactory);
    }

    void check(String label) {
        try (var session = databaseSessionFactory.get().get()) {
            try (var result = session.query("SELECT @class FROM V LIMIT 20")) {
                checkSuccess.incrementAndGet();
            }
        } catch (Exception e) {
            checkFailures.add(e);
            logger.warning("%s: check failed: %s %s".formatted(
                    label, e.getClass().getSimpleName(), e.getMessage())
            );
        }
    }

    void assertChecks() throws InterruptedException {
        executor.shutdown();
        assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).as("tasks finished").isTrue();

        assertThat(checkFailures).as("failed checks - not expected")
                .isEmpty();
    }

    void progress(String label, int step) {
        if (step % 8 == 0) {
            logger.info("%s: Step %d, checks succeeded %d, checks failed %d"
                    .formatted(label, step, checkSuccess.get(), checkFailures.size()));
        }
    }

    @Test
    void defineSchemaBeforeWorkaround() {
        final var runId = System.currentTimeMillis();

        executor.scheduleAtFixedRate(() -> check("beforeWorkaround"), 3, 3, TimeUnit.SECONDS);

        var todoClasses = CLASSES_PER_RUN;
        final var schemaGraph = getOrientGraphBaseFactory().getNoTx();
        try (var db = schemaGraph.getRawDatabase()) {
            while (todoClasses > 0 && checkFailures.isEmpty()) {
                progress("beforeWorkaround", todoClasses--);

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

                for (int propIdx = 0; propIdx < PROPERTIES_PER_CLASS; propIdx++) {
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

        executor.scheduleAtFixedRate(() -> check("afterWorkaround"), 3, 3, TimeUnit.SECONDS);

        var todoClasses = CLASSES_PER_RUN;
        final var schemaGraph = getOrientGraphBaseFactory().getNoTx();
        try (var db = schemaGraph.getRawDatabase()) {
            var schema = schemaGraph.getRawDatabase().getMetadata().getSchema();
            while (todoClasses > 0 && checkFailures.isEmpty()) {
                progress("afterWorkaround", todoClasses--);

                final var className = "TestClass_%d_%d".formatted(runId, todoClasses);
                final var defineEdge = todoClasses % 31 < 7;

                var type = schema.getClass(className);
                if (type == null) {
                    final var superClass = schema.getClass(defineEdge ? OClass.EDGE_CLASS_NAME : OClass.VERTEX_CLASS_NAME);
                    type = schema.createClass(className, DEFAULT_CLUSTERS, superClass);
                }

                for (int propIdx = 0; propIdx < PROPERTIES_PER_CLASS; propIdx++) {
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
