package io.vanillabp.outbox.gruelbox;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import com.gruelbox.transactionoutbox.DefaultPersistor;
import com.gruelbox.transactionoutbox.Dialect;
import com.gruelbox.transactionoutbox.Invocation;
import com.gruelbox.transactionoutbox.TransactionOutboxEntry;
import com.gruelbox.transactionoutbox.spring.SpringTransactionManager;

import io.vanillabp.integration.adapter.migration.observability.VanillaBpMetrics;
import io.vanillabp.integration.spi.PhaseOperation;
import io.vanillabp.integration.spi.PhaseTwoRetryLater;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * Gruelbox knows one distance for the whole outbox, and a dispatch which was rejected
 * because the BPMS has not made the workflow searchable yet names a window of its own. This
 * test reads the row back to prove that the window is what the table holds, whether it is
 * the closer of the two or the farther one, and that the store's own backoff stays where a
 * dispatch says nothing about a moment.
 * <p>
 * The entry is saved and handed to the listener directly rather than scheduled, so the row
 * an assertion reads is the row this test wrote and nothing races a dispatcher thread. The
 * next attempt time it starts from is the one gruelbox writes before it calls a listener:
 * now plus <code>attempt-frequency</code>.
 */
@ExtendWith(SuppressOutputExtension.class)
public class GruelboxWritesTheDueTimeADispatchAskedForTest {

  private static final String TABLE = GruelboxPhaseTwoOutboxAutoConfiguration.DEFAULT_OUTBOX_TABLE_NAME;

  private static final Duration ATTEMPT_FREQUENCY = Duration.ofSeconds(30);

  private static final Duration WINDOW = Duration.ofSeconds(10);

  private static final Duration A_SLOW_WINDOW = Duration.ofMinutes(5);

  private SingleConnectionDataSource dataSource;

  private DefaultPersistor persistor;

  private SpringTransactionManager transactionManager;

  @BeforeEach
  public void anOutboxTableOfThisTestsOwn() throws Exception {

    // one connection kept open: the in-memory database lives as long as it does
    dataSource = new SingleConnectionDataSource(
        "jdbc:h2:mem:%s;DB_CLOSE_DELAY=-1".formatted(UUID.randomUUID()), "sa", "", true);
    dataSource.setDriverClassName("org.h2.Driver");
    persistor = DefaultPersistor
        .builder()
        .dialect(Dialect.H2)
        .migrate(true)
        .build();
    transactionManager = new SpringTransactionManager(
        new DataSourceTransactionManager(dataSource), dataSource);
    persistor.migrate(transactionManager);

  }

  @AfterEach
  public void closeTheDatabase() {

    dataSource.destroy();

  }

  private GruelboxPhaseTwoFailureListener listener() {

    return new GruelboxPhaseTwoFailureListener(
        persistor, transactionManager, () -> VanillaBpMetrics.NONE, 50);

  }

  /**
   * An entry as gruelbox holds it right after it committed a failed attempt: the attempt is
   * counted and the next one is due after the store's own distance.
   */
  private TransactionOutboxEntry anEntryWhoseAttemptFailed() throws Exception {

    final var entry = TransactionOutboxEntry
        .builder()
        .id(UUID.randomUUID().toString())
        .invocation(
            new Invocation(
                "vanillaBpGruelboxPhaseTwoDispatch", "dispatch", new Class<?>[]{
                    String.class, String.class, String.class, String.class, String.class, String.class
                }, new Object[]{
                    PhaseOperation.CORRELATE_MESSAGE.name(), "taxiride", "Ride", "4711", "camunda8", null
                }))
        .attempts(1)
        .nextAttemptTime(Instant.now().plus(ATTEMPT_FREQUENCY).truncatedTo(ChronoUnit.MILLIS))
        .build();
    transactionManager.inTransactionThrows(transaction -> persistor.save(transaction, entry));
    return entry;

  }

  private Instant dueAtInTheTable(
      final String id) throws Exception {

    try (var connection = dataSource.getConnection(); var statement = connection
        .prepareStatement("SELECT nextAttemptTime FROM %s WHERE id = ?".formatted(TABLE))) {
      statement.setString(1, id);
      try (var resultSet = statement.executeQuery()) {
        assertTrue(resultSet.next(), "the entry vanished from the table");
        return resultSet
            .getTimestamp(1)
            .toInstant();
      }
    }

  }

  @Test
  @DisplayName("A workflow which is not searchable yet makes the entry due after the window")
  public void theWindowOfTheAdapterBecomesTheDueTime() throws Exception {

    final var entry = anEntryWhoseAttemptFailed();

    // truncated like the due time the listener writes, which is a moment in milliseconds
    final var beforeTheWrite = Instant.now().truncatedTo(ChronoUnit.MILLIS);
    listener().failure(entry, new PhaseTwoRetryLater("not searchable yet", WINDOW));

    final var dueAt = dueAtInTheTable(entry.getId());
    assertFalse(
        dueAt.isBefore(beforeTheWrite.plus(WINDOW)),
        "the entry is due before the adapter said it would be: "
            + dueAt);
    assertTrue(
        dueAt.isBefore(beforeTheWrite.plus(ATTEMPT_FREQUENCY)),
        "the entry still waits for the distance of the whole outbox: "
            + dueAt);
    assertEquals(dueAt, entry.getNextAttemptTime(), "what the table holds is what the entry says");

  }

  /**
   * A window is what the adapter knows about its BPMS, so it replaces the distance of the
   * store even where it is the farther of the two. The stores VanillaBP writes itself do it
   * that way, and an adapter's window means the same thing on all of them.
   */
  @Test
  @DisplayName("A window longer than the store's own distance is written as well")
  public void aLongerWindowIsWrittenToo() throws Exception {

    final var entry = anEntryWhoseAttemptFailed();
    final var gruelboxWrote = entry.getNextAttemptTime();

    final var beforeTheWrite = Instant.now().truncatedTo(ChronoUnit.MILLIS);
    listener().failure(entry, new PhaseTwoRetryLater("this cluster is slow", A_SLOW_WINDOW));

    final var dueAt = dueAtInTheTable(entry.getId());
    assertTrue(
        dueAt.isAfter(gruelboxWrote),
        "the entry is due before the adapter said it would be: "
            + dueAt);
    assertFalse(
        dueAt.isBefore(beforeTheWrite.plus(A_SLOW_WINDOW)),
        "the entry waits less than the adapter asked for: "
            + dueAt);
    assertEquals(dueAt, entry.getNextAttemptTime(), "what the table holds is what the entry says");

  }

  @Test
  @DisplayName("A failure which says nothing about a moment keeps the store's own distance")
  public void anOrdinaryFailureKeepsTheStoresDistance() throws Exception {

    final var entry = anEntryWhoseAttemptFailed();
    final var gruelboxWrote = entry.getNextAttemptTime();

    listener().failure(entry, new IllegalStateException("the cluster is away"));

    assertEquals(gruelboxWrote, dueAtInTheTable(entry.getId()));

  }

}
