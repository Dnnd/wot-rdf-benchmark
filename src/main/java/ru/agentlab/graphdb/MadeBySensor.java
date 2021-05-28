package ru.agentlab.graphdb;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.util.Values;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.sail.config.SailRegistry;
import org.eclipse.rdf4j.sparqlbuilder.core.query.ModifyQuery;
import org.eclipse.rdf4j.sparqlbuilder.core.query.Queries;
import org.eclipse.rdf4j.sparqlbuilder.rdf.Iri;
import org.eclipse.rdf4j.sparqlbuilder.rdf.Rdf;
import org.openjdk.jmh.annotations.*;
import reactor.core.publisher.Mono;
import ru.agentlab.changetracking.filter.ChangetrackingFilter;
import ru.agentlab.changetracking.filter.Pattern;
import ru.agentlab.changetracking.sail.ChangeTrackerConnection;
import ru.agentlab.changetracking.sail.ChangeTrackingFactory;
import ru.agentlab.changetracking.sail.TransactionChanges;
import ru.agentlab.graphdb.utils.EmbeddedGraphDB;
import ru.agentlab.semantic.wot.thing.ConnectionContext;
import ru.agentlab.semantic.wot.utils.Utils;
import ru.agentlab.semantic.wot.vocabularies.SSN;

import java.io.IOException;
import java.io.InputStream;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.eclipse.rdf4j.model.util.Values.iri;
import static org.eclipse.rdf4j.sparqlbuilder.constraint.Expressions.in;
import static org.eclipse.rdf4j.sparqlbuilder.core.SparqlBuilder.var;
import static org.eclipse.rdf4j.sparqlbuilder.graphpattern.GraphPatterns.tp;
import static ru.agentlab.semantic.wot.vocabularies.Vocabularies.DESCRIBED_BY_AFFORDANCE;
import static ru.agentlab.semantic.wot.vocabularies.Vocabularies.HAS_PROPERTY_AFFORDANCE;

public class MadeBySensor {
    private static final String TEMPERATURE_PROPERTY_IRI_PREFIX = "https://example.agentlab.ru/#Temperature_";
    private static final IRI STATE_CONTEXT = iri("https://observations.agentlab.ru");
    private static final long seed = 1000;

    @State(Scope.Benchmark)
    public static class GraphDBState {
        @Param({"10", "100", "1000", "10000"})
        public int sensorsInDataset;

        @Param({"10", "20", "25"})
        public int batchSizePercents;

        public List<IRI> sensorsToUpdate;

        public RepositoryConnection connection;

        private final Random random = new Random(seed);
        private EmbeddedGraphDB graphdb;

        @Setup(Level.Trial)
        public void createGraphDBRepositories() throws IOException {
            InputStream config = MadeBySensor.class.getClassLoader().getResourceAsStream("graphdb.ttl");
            String ruleset = "/tmp/wot-rules.pie";
            graphdb = EmbeddedGraphDB.createTempRepository(config, ruleset);
            connection = graphdb.getRepository().getConnection();
            pushTurtleResource(connection, "temperature_sensors/" + sensorsInDataset + "/data.ttl");
        }

        @Setup(Level.Iteration)
        public void chooseSensorsToUpdate() {
            var batchSize = batchSizePercents * 0.01 * sensorsInDataset;
            sensorsToUpdate = random
                    .ints(Math.round(batchSize), 0, sensorsInDataset)
                    .mapToObj(sensorId -> iri(TEMPERATURE_PROPERTY_IRI_PREFIX + sensorId))
                    .collect(Collectors.toList());
        }

        @TearDown(Level.Invocation)
        public void cleanupInvocation() {
            connection.remove((Resource) null, null, null, STATE_CONTEXT);
        }

        @TearDown(Level.Trial)
        public void closeConnection() throws IOException {
            connection.close();
            graphdb.close();
        }
    }

    @State(Scope.Benchmark)
    public static class ChangetrackingState {

        @Param({"10", "100", "1000", "10000"})
        public int sensorsInDataset;

        @Param({"10", "20", "25"})
        public int batchSizePercents;

        public ChangetrackingFilter obsFilter;
        public List<IRI> sensorsToUpdate;
        public ConnectionContext context;
        private EmbeddedGraphDB embeddedGraphDB;
        private final Random random = new Random(seed);

        @Setup(Level.Trial)
        public void createGraphDBRepository() throws IOException {
            SailRegistry.getInstance().add(new ChangeTrackingFactory());
            obsFilter = createObservationsFilter();
            InputStream config =
                    MadeBySensor.class.getClassLoader().getResourceAsStream("graphdb_changetracking.ttl");
            embeddedGraphDB = EmbeddedGraphDB.createTempRepository(config, "empty");
            var conn = embeddedGraphDB.getRepository().getConnection();
            context = new ConnectionContext(Executors.newSingleThreadExecutor(), conn);
            pushTurtleResource(conn, "temperature_sensors/" + sensorsInDataset + "/data.ttl");
        }


        @TearDown(Level.Invocation)
        public void cleanupInvocation() {
            context.getConnection().remove((Resource) null, null, null, STATE_CONTEXT);
        }

        @Setup(Level.Iteration)
        public void chooseSensorsToUpdate() {
            var batchSize = batchSizePercents * 0.01 * sensorsInDataset;

            sensorsToUpdate = random
                    .ints(Math.round(batchSize), 0, sensorsInDataset)
                    .mapToObj(sensorId -> iri(TEMPERATURE_PROPERTY_IRI_PREFIX + sensorId))
                    .collect(Collectors.toList());
        }

        @TearDown(Level.Trial)
        public void closeConnection() throws IOException {
            context.close();
            embeddedGraphDB.close();
        }

        private ChangetrackingFilter createObservationsFilter() {
            return ChangetrackingFilter.builder()
                                       .setMatchingStrategy(ChangetrackingFilter.MatchingStrategy.ANY_PATTERN)
                                       .addPattern(new Pattern(
                                               null,
                                               RDF.TYPE,
                                               SSN.OBSERVATION,
                                               ChangetrackingFilter.Filtering.ADDED
                                       ))
                                       .build();
        }

    }

    private static void pushTurtleResource(RepositoryConnection conn, String resource) throws IOException {
        try (InputStream resourceStream = mustGetResource(resource)) {
            conn.add(resourceStream, "", RDFFormat.TURTLE);
        }
    }

    private static InputStream mustGetResource(String resource) {
        return Objects.requireNonNull(MadeBySensor.class.getClassLoader().getResourceAsStream(resource));
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput, Mode.AverageTime, Mode.SampleTime})
    @Measurement(iterations = 5, time = 5)
    @Fork(1)
    @Warmup(iterations = 5, time = 5)
    public void createObservationsBatchGraphDB(GraphDBState state) {
        createObservations(state.sensorsToUpdate, state.connection);
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput, Mode.AverageTime, Mode.SampleTime})
    @Measurement(iterations = 5, time = 5)
    @Fork(1)
    @Warmup(iterations = 5, time = 5)
    public void createObservationsBatch_Changetracking_filteredQuery(ChangetrackingState state) {
        var oneShotRule = registerOneshotMadeBySensorRule(state, this::createMadeBySensorLinkWithFilter);
        createObservationsAsync(state).subscribe();
        oneShotRule.block();
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput, Mode.AverageTime, Mode.SampleTime})
    @Measurement(iterations = 5, time = 5)
    @Fork(1)
    @Warmup(iterations = 5, time = 5)
    public void createObservationsBatch_Changetracking_fullQuery(ChangetrackingState state) {
        var oneShotRule = registerOneshotMadeBySensorRule(state, this::createMadeBySensorLinkWithoutFilter);
        var sub = createObservationsAsync(state).subscribe();
        oneShotRule.block();
        sub.dispose();
    }

    private Mono<Void> createObservationsAsync(ChangetrackingState state) {
        var ctx = state.context;
        return Utils.supplyAsyncWithCancel(
                () -> createObservations(state.sensorsToUpdate, ctx.getConnection()),
                ctx.getExecutor()
        );
    }

    private ModifyQuery createMadeBySensorLinkWithFilter(TransactionChanges changes) {
        var sensor = var("sensor");
        var observation = var("observation");
        var affordance = var("affordance");
        var obs = changes.getAddedStatements()
                         .stream()
                         .map(st -> Rdf.iri((IRI) st.getSubject()))
                         .toArray(Iri[]::new);
        return Queries.INSERT(tp(observation, SSN.MADE_BY_SENSOR, sensor))
                      .where(
                              tp(observation, DESCRIBED_BY_AFFORDANCE, affordance)
                                      .filter(in(observation, obs)),
                              tp(sensor, HAS_PROPERTY_AFFORDANCE, affordance)
                      );

    }

    private ModifyQuery createMadeBySensorLinkWithoutFilter(TransactionChanges changes) {
        var sensor = var("sensor");
        var observation = var("observation");
        var affordance = var("affordance");
        return Queries.INSERT(tp(observation, SSN.MADE_BY_SENSOR, sensor))
                      .where(
                              tp(observation, DESCRIBED_BY_AFFORDANCE, affordance),
                              tp(sensor, HAS_PROPERTY_AFFORDANCE, affordance)
                      );
    }

    private Mono<Void> registerOneshotMadeBySensorRule(ChangetrackingState state,
                                                       Function<TransactionChanges, ModifyQuery> rule) {
        var ctx = state.context;
        return Mono.create(sink -> {
            var sailConn = (ChangeTrackerConnection) ctx.getSailConnection();
            sailConn.events(ctx.getScheduler())
                    .take(1)
                    .map(state.obsFilter::mapMatched)
                    .map(Optional::get)
                    .subscribe(transactionChanges -> {
                        ctx.getConnection()
                           .prepareUpdate(rule.apply(transactionChanges).getQueryString())
                           .execute();
                        sink.success();
                    });
        });
    }

    private void createObservations(List<IRI> propertiesToUpdate, RepositoryConnection connection) {
        Value observationValue = Values.literal(100.);
        Value observationTime = Values.literal(
                OffsetDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME),
                XSD.DATETIME
        );
        connection.begin();

        for (var propertyToUpdate : propertiesToUpdate) {
            var obsIRI = iri("urn:uuid:" + UUID.randomUUID());
            connection.add(obsIRI, RDF.TYPE, SSN.OBSERVATION, STATE_CONTEXT);
            connection.add(obsIRI, SSN.HAS_SIMPLE_RESULT, observationValue, STATE_CONTEXT);
            connection.add(obsIRI, SSN.RESULT_TIME, observationTime, STATE_CONTEXT);
            connection.add(obsIRI, DESCRIBED_BY_AFFORDANCE, propertyToUpdate, STATE_CONTEXT);
        }
        connection.commit();
    }
}
