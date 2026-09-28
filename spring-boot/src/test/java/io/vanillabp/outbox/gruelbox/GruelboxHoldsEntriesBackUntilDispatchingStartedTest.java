package io.vanillabp.outbox.gruelbox;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.transaction.support.TransactionTemplate;

import com.gruelbox.transactionoutbox.Submitter;
import com.gruelbox.transactionoutbox.TransactionOutbox;
import com.gruelbox.transactionoutbox.TransactionOutboxListener;

import io.vanillabp.integration.adapter.migration.observability.VanillaBpMetrics;
import io.vanillabp.integration.adapter.migration.outbox.JdbcHousekeepingLease;
import io.vanillabp.integration.adapter.migration.outbox.JdbcPhaseTwoPayloadStore;
import io.vanillabp.integration.config.VanillaBpConfigurationProperties;
import io.vanillabp.integration.spi.PhaseOperation;
import io.vanillabp.integration.spi.PhaseTwoCall;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * Spring Boot opens its web server before VanillaBP deployed its models, so an
 * application answers a request while its BPMS does not hold the process yet. Gruelbox
 * would carry such a start to the BPMS right after the commit, and the answer that
 * there is no such process is a failure no repetition can fix: the entry is blocked
 * although the next attempt would have worked. The submitter therefore keeps the entry
 * until VanillaBP dispatches, and the first poll carries it.
 * <p>
 * The poll interval of these tests is an hour, so the poller flushes once when it
 * starts and never again while a test runs. Where a test needs the gate open and the
 * poller idle, it waits for the entry of that first flush before it schedules the one
 * it asks about - a dispatch arriving after that can only have come from the submitter.
 */
@ExtendWith(SuppressOutputExtension.class)
@SuppressOutputExtension.SuppressBackgroundOutput
public class GruelboxHoldsEntriesBackUntilDispatchingStartedTest {

  private static final String TABLE = GruelboxPhaseTwoOutboxAutoConfiguration.DEFAULT_OUTBOX_TABLE_NAME;

  private SingleConnectionDataSource dataSource;

  private AnnotationConfigApplicationContext context;

  /**
   * The aggregate IDs dispatched, in the order the dispatches happened.
   */
  private LinkedBlockingQueue<String> dispatches;

  private GruelboxRedispatchAwareSubmitter submitter;

  private TransactionOutbox transactionOutbox;

  private GruelboxPhaseTwoOutbox testee;

  /**
   * Where a payload would go. No call of this test carries one, and the store and the
   * dispatcher are built with it anyway: the place a payload goes belongs to both of them
   * the way gruelbox' table does.
   */
  private JdbcPhaseTwoPayloadStore payloadStore;

  private GruelboxPhaseTwoOutboxDispatcher dispatcher;

  private TransactionTemplate transactions;

  private VanillaBpConfigurationProperties properties;

  @BeforeEach
  public void anOutboxOfThisTestsOwn() {

    // one connection kept open: the in-memory database lives as long as it does
    dataSource = new SingleConnectionDataSource(
        "jdbc:h2:mem:%s;DB_CLOSE_DELAY=-1".formatted(UUID.randomUUID()), "sa", "", true);
    dataSource.setDriverClassName("org.h2.Driver");
    dispatches = new LinkedBlockingQueue<>();
    context = new AnnotationConfigApplicationContext();
    // gruelbox resolves the invocation through the context, on the thread dispatching
    // it - which workflow was dispatched is all these tests read
    context
        .registerBean(
            GruelboxPhaseTwoDispatch.class,
            () -> (
                operation,
                workflowModuleId,
                bpmnProcessId,
                workflowAggregateId,
                adapterId,
                serializedArgs) -> dispatches.add(workflowAggregateId));
    context.refresh();
    properties = new VanillaBpConfigurationProperties();
    properties
        .getOutbox()
        .setPollInterval(Duration.ofHours(1));
    submitter = new GruelboxRedispatchAwareSubmitter(Submitter.withDefaultExecutor());
    final var configuration = new GruelboxPhaseTwoOutboxAutoConfiguration();
    transactionOutbox = configuration
        .vanillaBpTransactionOutbox(
            context,
            Map.of("transactionManager", new DataSourceTransactionManager(dataSource)),
            dataSource,
            properties,
            context.getBeanProvider(VanillaBpMetrics.class),
            context.getBeanProvider(TransactionOutboxListener.class),
            submitter);
    payloadStore = configuration.vanillaBpGruelboxPhaseTwoPayloadStore(dataSource, properties);
    testee = new GruelboxPhaseTwoOutbox(transactionOutbox, dataSource, TABLE, payloadStore);
    transactions = new TransactionTemplate(new DataSourceTransactionManager(dataSource));

  }

  @AfterEach
  public void stopEverythingThisTestStarted() {

    if (dispatcher != null) {
      dispatcher.stopPolling();
      dispatcher = null;
    }
    context.close();
    dataSource.destroy();

  }

  /**
   * The claim of this outbox, on the same database the payloads lie in.
   *
   * @return The claim, with its table in place
   */
  private JdbcHousekeepingLease aHousekeepingLease() {

    final var lease = new JdbcHousekeepingLease(
        io.vanillabp.integration.outbox.jdbc.JdbcPhaseTwoOutbox.connectionsOf(dataSource), properties
            .getOutbox()
            .getJdbc()
            .housekeepingTableName());
    lease.createSchemaIfNotExists();
    return lease;

  }

  /**
   * The dispatcher of this outbox. Building it closes the submitter's gate, so a test
   * which wants an entry to wait builds it before it schedules.
   */
  private GruelboxPhaseTwoOutboxDispatcher aDispatcher() {

    dispatcher = new GruelboxPhaseTwoOutboxDispatcher(
        transactionOutbox, properties
            .getOutbox(), submitter, testee, payloadStore, aHousekeepingLease(), () -> VanillaBpMetrics.NONE);
    return dispatcher;

  }

  private void start(
      final String aggregateId) {

    final var call = PhaseTwoCall
        .of(PhaseOperation.START_WORKFLOW, "test-module", "TestProcess", aggregateId, "test-adapter", Map.of());
    assertTrue(
        Boolean.TRUE.equals(transactions.execute(status -> testee.schedule(call))),
        "the start of '%s' was not planned".formatted(aggregateId));

  }

  /**
   * @return The aggregate ID dispatched next, <code>null</code> if none was dispatched
   *         within the given time
   */
  private String dispatchedWithin(
      final Duration time) throws Exception {

    return dispatches.poll(time.toMillis(), TimeUnit.MILLISECONDS);

  }

  private long pendingEntries() throws Exception {

    try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
      final var countPending = "SELECT COUNT(*) FROM %s WHERE processed = FALSE".formatted(TABLE);
      try (var resultSet = statement.executeQuery(countPending)) {
        resultSet.next();
        return resultSet.getLong(1);
      }
    }

  }

  @Test
  @DisplayName("A workflow started before dispatching began stays in the store")
  public void anEntryScheduledBeforeDispatchingStartedWaits() throws Exception {

    aDispatcher();

    start("4711");

    assertNull(dispatchedWithin(Duration.ofSeconds(1)), "the start reached the BPMS before the models did");
    assertEquals(1, pendingEntries(), "the entry has to wait in the store, committed and due");

  }

  @Test
  @DisplayName("The entry which waited goes out as soon as dispatching started")
  public void theWaitingEntryGoesOutWithTheFirstPoll() throws Exception {

    final var itsDispatcher = aDispatcher();
    start("4712");
    assertNull(dispatchedWithin(Duration.ofMillis(500)));

    itsDispatcher.startPolling();

    assertEquals("4712", dispatchedWithin(Duration.ofSeconds(10)), "the first poll left the entry where it was");

  }

  @Test
  @DisplayName("A workflow started once dispatching runs is on its way right after the commit")
  public void anEntryScheduledAfterwardsIsDispatchedAtOnce() throws Exception {

    final var itsDispatcher = aDispatcher();
    start("4713");
    itsDispatcher.startPolling();
    // the entry of the first poll: once it is out, that poll is over and the next one
    // is an hour away, so whatever is dispatched from here on came from the submitter
    assertEquals("4713", dispatchedWithin(Duration.ofSeconds(10)));

    start("4714");

    assertEquals("4714", dispatchedWithin(Duration.ofSeconds(10)), "the entry waited for a poll instead");

  }

  @Test
  @DisplayName("An outbox nobody polls dispatches after the commit, as it did before the gate existed")
  public void anOutboxWithoutADispatcherKeepsDispatching() throws Exception {

    // no dispatcher is built here: a test or an application polling the outbox itself
    // would otherwise wait for something which never comes
    start("4715");

    assertEquals("4715", dispatchedWithin(Duration.ofSeconds(10)), "an outbox without a dispatcher lost its dispatch");

  }

}
