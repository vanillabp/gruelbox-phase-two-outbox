package io.vanillabp.outbox.gruelbox;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

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
import io.vanillabp.integration.adapter.migration.outbox.JdbcPhaseTwoPayloadStore;
import io.vanillabp.integration.config.VanillaBpConfigurationProperties;
import io.vanillabp.integration.spi.PhaseOperation;
import io.vanillabp.integration.spi.PhaseTwoCall;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * What the housekeeping of the gruelbox store removes and what it leaves: the retention
 * counts at the entry, so an entry gruelbox blocked keeps its payload however long the
 * repair takes, an entry which was dispatched takes its payload with it when gruelbox
 * deletes it, and a payload no entry names is removed by age.
 * <p>
 * The rows are put into the shape they have after the retention passed rather than
 * waited for: gruelbox writes its own timestamps, and a test which waited for seven days
 * of them would never end. Nothing is dispatched while the test runs, because the one
 * entry left waiting is blocked.
 */
@ExtendWith(SuppressOutputExtension.class)
@SuppressOutputExtension.SuppressBackgroundOutput
public class GruelboxKeepsThePayloadOfABlockedEntryTest {

  private static final String TABLE = GruelboxPhaseTwoOutboxAutoConfiguration.DEFAULT_OUTBOX_TABLE_NAME;

  private static final PhaseOperation OPERATION = PhaseOperation
      .extensionOperation("sample:NOTIFY")
      .idempotencyKey(call -> Optional.empty())
      .build();

  /**
   * Older than the default retention of seven days, so everything written here is old
   * enough to be removed - which makes the entries the only reason a payload stays.
   */
  private static final Instant LONG_BEFORE_THE_RETENTION = Instant.now().minus(Duration.ofDays(10));

  /**
   * How long the test waits for the flush which cleans up. The first flush runs when the
   * poller starts, so this is a guard against a machine which leaves the JVM without a
   * turn, not a measurement of speed.
   */
  private static final long UNTIL_THE_HOUSEKEEPING_RAN = 30_000;

  private SingleConnectionDataSource dataSource;

  private AnnotationConfigApplicationContext context;

  private GruelboxRedispatchAwareSubmitter submitter;

  private GruelboxPhaseTwoOutbox outbox;

  private GruelboxPhaseTwoOutboxDispatcher dispatcher;

  private JdbcPhaseTwoPayloadStore payloadStore;

  private TransactionTemplate transactions;

  @BeforeEach
  public void anOutboxOfThisTestsOwn() {

    // one connection kept open: the in-memory database lives as long as it does
    dataSource = new SingleConnectionDataSource(
        "jdbc:h2:mem:%s;DB_CLOSE_DELAY=-1".formatted(UUID.randomUUID()), "sa", "", true);
    dataSource.setDriverClassName("org.h2.Driver");
    context = new AnnotationConfigApplicationContext();
    // gruelbox resolves the invocation through the context; nothing is dispatched here,
    // and a bean which does nothing is what says so
    context
        .registerBean(
            GruelboxPhaseTwoDispatch.class,
            () -> (
                operation,
                workflowModuleId,
                bpmnProcessId,
                workflowAggregateId,
                adapterId,
                serializedArgs) -> {
              // nothing is dispatched in this test
            });
    context.refresh();
    final var properties = new VanillaBpConfigurationProperties();
    properties
        .getOutbox()
        .setPollInterval(Duration.ofHours(1));
    // the default housekeeping window is an hour of the night, and this test wants to
    // watch the sweep work now
    properties
        .getOutbox()
        .getHousekeeping()
        .setStart(java.time.LocalTime.MIN);
    properties
        .getOutbox()
        .getHousekeeping()
        .setEnd(java.time.LocalTime.MAX);
    submitter = new GruelboxRedispatchAwareSubmitter(Submitter.withDefaultExecutor());
    final var configuration = new GruelboxPhaseTwoOutboxAutoConfiguration();
    final TransactionOutbox transactionOutbox = configuration
        .vanillaBpTransactionOutbox(
            context,
            Map.of("transactionManager", new DataSourceTransactionManager(dataSource)),
            dataSource,
            properties,
            context.getBeanProvider(VanillaBpMetrics.class),
            context.getBeanProvider(TransactionOutboxListener.class),
            submitter);
    payloadStore = configuration.vanillaBpGruelboxPhaseTwoPayloadStore(dataSource, properties);
    outbox = new GruelboxPhaseTwoOutbox(transactionOutbox, dataSource, TABLE, payloadStore);
    // building the dispatcher closes the submitter's gate, so an entry scheduled below
    // waits for the flush this test starts instead of going out at the commit
    dispatcher = new GruelboxPhaseTwoOutboxDispatcher(
        transactionOutbox, properties.getOutbox(), submitter, outbox, payloadStore, configuration
            .vanillaBpGruelboxHousekeepingLease(dataSource, properties), () -> VanillaBpMetrics.NONE);
    transactions = new TransactionTemplate(new DataSourceTransactionManager(dataSource));

  }

  @AfterEach
  public void stopEverythingThisTestStarted() {

    dispatcher.stopPolling();
    context.close();
    dataSource.destroy();

  }

  private static PhaseTwoCall callWith(
      final String content) {

    return PhaseTwoCall
        .of(
            OPERATION, "test-module", "TestProcess", "4711", "test-adapter", Map.of(),
            content.getBytes(StandardCharsets.UTF_8));

  }

  private PhaseTwoCall scheduled(
      final String content) {

    final var call = callWith(content);
    assertTrue(
        Boolean.TRUE.equals(transactions.execute(status -> outbox.schedule(call))),
        "the call was not planned");
    return call;

  }

  private void update(
      final String statement) throws Exception {

    try (var connection = dataSource.getConnection(); var update = connection.createStatement()) {
      update.executeUpdate(statement);
    }

  }

  private long countEntries(
      final String where) throws Exception {

    try (var connection = dataSource.getConnection(); var statement = connection
        .createStatement(); var resultSet = statement
            .executeQuery("SELECT COUNT(*) FROM %s WHERE %s".formatted(TABLE, where))) {
      resultSet.next();
      return resultSet.getLong(1);

    }

  }

  @Test
  @DisplayName("A blocked entry keeps its payload, a dispatched entry takes its own with it, an orphan goes")
  public void theRetentionCountsAtTheEntry() throws Exception {

    final var blocked = scheduled("the state an operator will send once the cause is gone");
    final var dispatched = scheduled("the state a dispatch already carried");
    final var orphan = callWith("written by a transaction which never committed");
    transactions.executeWithoutResult(status -> payloadStore.write(orphan));

    // the shape the store has after the retention passed: one entry gruelbox blocked,
    // one it dispatched long enough ago to be deleted at the next flush, and three
    // payloads older than the retention
    update(
        "UPDATE %s SET blocked = TRUE WHERE invocation LIKE '%%%s%%'"
            .formatted(TABLE, blocked.payloadReference()));
    try (var connection = dataSource.getConnection(); var statement = connection
        .prepareStatement(
            "UPDATE %s SET processed = TRUE, nextAttemptTime = ? WHERE invocation LIKE ?".formatted(TABLE))) {
      statement.setTimestamp(1, Timestamp.from(LONG_BEFORE_THE_RETENTION));
      statement.setString(2, "%%%s%%".formatted(dispatched.payloadReference()));
      statement.executeUpdate();
    }
    try (var connection = dataSource.getConnection(); var statement = connection
        .prepareStatement("UPDATE %s SET CREATED_AT = ?".formatted(payloadStore.getTableName()))) {
      statement.setTimestamp(1, Timestamp.from(LONG_BEFORE_THE_RETENTION));
      statement.executeUpdate();
    }

    dispatcher.startPolling();
    // the payload sweep is the last thing a flush does, so a gone orphan says that the
    // whole housekeeping ran
    final var deadline = System.currentTimeMillis() + UNTIL_THE_HOUSEKEEPING_RAN;
    while (payloadStore.read(orphan.payloadReference()) != null) {
      assertTrue(System.currentTimeMillis() < deadline, "the housekeeping did not remove the orphaned payload");
      Thread.sleep(50);
    }

    assertEquals(1, countEntries("blocked = TRUE"), "a blocked entry waits for a person and no retention removes it");
    assertArrayEquals(
        "the state an operator will send once the cause is gone".getBytes(StandardCharsets.UTF_8),
        payloadStore.read(blocked.payloadReference()),
        "the entry is still there, so its payload has to be there as well");

    assertEquals(0, countEntries("processed = TRUE"), "a dispatched entry goes when its retention ran out");
    assertNull(payloadStore.read(dispatched.payloadReference()), "the entry took its payload with it");

  }

}
