package io.vanillabp.outbox.gruelbox;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.UUID;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.transaction.support.TransactionTemplate;

import io.vanillabp.integration.config.VanillaBpConfigurationProperties;
import io.vanillabp.integration.spi.PhaseOperation;
import io.vanillabp.integration.spi.PhaseTwoCall;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * gruelbox owns the table its unique request IDs live in, and its unique constraint
 * spans the entries it retains after a dispatch as well. Since a key deduplicates what
 * is PLANNED and not what happened, this
 * store has to release a retained entry when a new operation of the same key arrives -
 * which is what is pinned here, together with the length gruelbox accepts at all.
 * <p>
 * <strong>The rows an assertion depends on are written by this test, not scheduled.</strong>
 * gruelbox dispatches an entry right after the commit which scheduled it, on an executor
 * of its own, and writes the outcome of that dispatch back into the row. A test which
 * scheduled its own precondition and then looked at the row would be racing that thread,
 * and it would lose on a machine slower than the one it was written on. Where a schedule
 * IS the subject, nothing afterwards reads its row again.
 */
@ExtendWith(SuppressOutputExtension.class)
public class GruelboxDeduplicationWindowTest {

  private static final String TABLE = GruelboxPhaseTwoOutboxAutoConfiguration.DEFAULT_OUTBOX_TABLE_NAME;

  /**
   * One context for the whole class: gruelbox resolves the invocation it dispatches
   * through it, after the commit and on its own thread, which may be after the test
   * which scheduled it has finished.
   */
  private static AnnotationConfigApplicationContext context;

  @AfterAll
  public static void closeTheContext() {

    if (context != null) {
      context.close();
      context = null;
    }

  }

  private static SingleConnectionDataSource h2() {

    // one connection kept open: the in-memory database lives as long as it does
    final var dataSource = new SingleConnectionDataSource(
        "jdbc:h2:mem:%s;DB_CLOSE_DELAY=-1".formatted(UUID.randomUUID()), "sa", "", true);
    dataSource.setDriverClassName("org.h2.Driver");
    return dataSource;

  }

  /**
   * A store on a fresh in-memory database, with gruelbox' own migration applied.
   */
  private static GruelboxPhaseTwoOutbox outbox(
      final DataSource dataSource) {

    if (context == null) {
      context = new AnnotationConfigApplicationContext();
      // registered because gruelbox resolves the invocation through the context; what
      // it does is irrelevant here, the subject being what happens before the dispatch
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
                // nothing to dispatch in this test
              });
      context.refresh();
    }
    final var configuration = new GruelboxPhaseTwoOutboxAutoConfiguration();
    final var transactionOutbox = configuration
        .vanillaBpTransactionOutbox(
            context,
            Map.of("transactionManager", new DataSourceTransactionManager(dataSource)),
            dataSource,
            new VanillaBpConfigurationProperties(),
            context.getBeanProvider(
                io.vanillabp.integration.adapter.migration.observability.VanillaBpMetrics.class),
            context.getBeanProvider(com.gruelbox.transactionoutbox.TransactionOutboxListener.class),
            new GruelboxRedispatchAwareSubmitter(com.gruelbox.transactionoutbox.Submitter.withDefaultExecutor()));
    // no call of this test carries a payload, and the store is built with one anyway:
    // the place a payload goes belongs to the outbox the way gruelbox' table does
    return new GruelboxPhaseTwoOutbox(
        transactionOutbox, dataSource, TABLE, configuration
            .vanillaBpGruelboxPhaseTwoPayloadStore(dataSource, new VanillaBpConfigurationProperties()));

  }

  private static PhaseTwoCall correlation(
      final String aggregateId,
      final String correlationId) {

    return PhaseTwoCall
        .of(
            PhaseOperation.CORRELATE_MESSAGE, "test-module", "TestProcess", aggregateId, null, Map
                .of(
                    PhaseTwoCall.ARG_MESSAGE_NAME, "OfferRequested",
                    PhaseTwoCall.ARG_CORRELATION_ID, correlationId));

  }

  /**
   * Schedules the call in a transaction of its own - the store has to be called within
   * one, and the answer is what the contract is about.
   */
  private static boolean scheduled(
      final TransactionTemplate transactions,
      final GruelboxPhaseTwoOutbox outbox,
      final PhaseTwoCall call) {

    return Boolean.TRUE.equals(transactions.execute(status -> outbox.schedule(call)));

  }

  /**
   * Writes an entry the way gruelbox leaves one behind, so what the store makes of it
   * does not depend on gruelbox' own thread having run.
   *
   * @param processed Whether the entry counts as dispatched
   */
  private static void writeEntry(
      final DataSource dataSource,
      final String uniqueRequestId,
      final boolean processed) throws Exception {

    final var insert = """
        INSERT INTO %s (id, invocation, lastAttemptTime, nextAttemptTime, attempts, blocked, \
        processed, version, uniqueRequestId, topic, seq) \
        VALUES (?, '{}', NULL, CURRENT_TIMESTAMP, 0, FALSE, ?, 0, ?, '*', NULL)"""
        .formatted(TABLE);
    try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(insert)) {
      statement.setString(1, UUID.randomUUID().toString());
      statement.setBoolean(2, processed);
      statement.setString(3, uniqueRequestId);
      statement.executeUpdate();
    }

  }

  private static long count(
      final DataSource dataSource,
      final String where) throws Exception {

    try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
      try (var resultSet = statement.executeQuery("SELECT COUNT(*) FROM %s WHERE %s".formatted(TABLE, where))) {
        resultSet.next();
        return resultSet.getLong(1);
      }
    }

  }

  @Test
  @DisplayName("A retained entry of a dispatched operation is released for the next round")
  public void aDispatchedEntryIsReleased() throws Exception {

    final var dataSource = h2();
    final var testee = outbox(dataSource);
    final var transactions = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
    final var call = correlation("4711", "partner-42");

    // gruelbox dispatched this operation and keeps its row until the retention passes,
    // which is what the store has to see through
    writeEntry(dataSource, call.idempotencyKey().orElseThrow(), true);

    // the second round of a loop asking partner 42 again: planned, and the retained
    // entry is gone, gruelbox' table having no column to move its key into
    assertTrue(scheduled(transactions, testee, call));
    assertEquals(0, count(dataSource, "processed = TRUE"), "the released entry is gone");
    assertEquals(1, count(dataSource, "processed = FALSE"), "the new operation waits for its dispatch");

  }

  @Test
  @DisplayName("An operation still waiting for its dispatch discards the schedule")
  public void aPendingEntryDiscardsTheSchedule() throws Exception {

    final var dataSource = h2();
    final var testee = outbox(dataSource);
    final var transactions = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
    final var call = correlation("4712", "partner-42");

    writeEntry(dataSource, call.idempotencyKey().orElseThrow(), false);

    assertFalse(scheduled(transactions, testee, call));
    assertEquals(1, count(dataSource, "1 = 1"), "nothing was added, and nothing released either");

  }

  @Test
  @DisplayName("A key too long for gruelbox is hashed instead of ending the caller's transaction")
  public void anOversizedKeyIsScheduled() throws Exception {

    final var dataSource = h2();
    final var testee = outbox(dataSource);
    final var transactions = new TransactionTemplate(new DataSourceTransactionManager(dataSource));

    // gruelbox refuses a unique request ID longer than 250 characters before any
    // database sees it, so an aggregate ID a domain model legitimately uses - a
    // composite business key, a URN - used to fail the application's own transaction
    final var aggregateId = "urn:offer:"
        + "x".repeat(400);

    assertTrue(scheduled(transactions, testee, correlation(aggregateId, "partner-42")));
    assertEquals(1, count(dataSource, "uniqueRequestId LIKE 'sha256:%'"), "the key was stored as a hash");

  }

  @Test
  @DisplayName("A hashed key deduplicates like any other while its entry waits")
  public void anOversizedKeyStillDeduplicates() throws Exception {

    final var dataSource = h2();
    final var testee = outbox(dataSource);
    final var transactions = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
    final var call = correlation("urn:offer:"
        + "x".repeat(400), "partner-42");

    writeEntry(dataSource, call.idempotencyKey().orElseThrow(), false);

    assertFalse(scheduled(transactions, testee, call));

  }

  @Test
  @DisplayName("An operation without a key is never discarded")
  public void aKeylessOperationIsAlwaysScheduled() {

    final var dataSource = h2();
    final var testee = outbox(dataSource);
    final var transactions = new TransactionTemplate(new DataSourceTransactionManager(dataSource));

    final var signal = PhaseTwoCall
        .of(
            PhaseOperation.SEND_SIGNAL, "test-module", "TestProcess", null, "test-adapter", Map
                .of(PhaseTwoCall.ARG_SIGNAL_NAME, "OfferWithdrawn"));

    assertTrue(scheduled(transactions, testee, signal));
    assertTrue(scheduled(transactions, testee, signal));

  }

}
