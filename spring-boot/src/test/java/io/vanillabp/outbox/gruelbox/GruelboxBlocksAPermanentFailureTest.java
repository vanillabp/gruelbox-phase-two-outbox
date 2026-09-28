package io.vanillabp.outbox.gruelbox;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import com.gruelbox.transactionoutbox.DefaultPersistor;
import com.gruelbox.transactionoutbox.Dialect;
import com.gruelbox.transactionoutbox.Invocation;
import com.gruelbox.transactionoutbox.TransactionOutboxEntry;
import com.gruelbox.transactionoutbox.TransactionOutboxListener;
import com.gruelbox.transactionoutbox.spring.SpringTransactionManager;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.vanillabp.integration.adapter.migration.observability.VanillaBpMetrics;
import io.vanillabp.integration.spi.PhaseOperation;
import io.vanillabp.integration.spi.PhaseTwoPermanentFailure;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * Gruelbox retries every failure the same way, fifty times by default, and the adapter's
 * answer that repeating cannot help used to be lost on this store - which is the default
 * of a Spring Boot application on JPA. It is not lost any more: the listener writes the
 * block itself, and this test reads the row back to prove that the write went through
 * against the version gruelbox had just committed.
 * <p>
 * The entry is saved and handed to the listener directly rather than scheduled. Nothing
 * dispatches here, so the row an assertion reads is the row this test wrote, and the test
 * is not racing a dispatcher thread.
 */
@ExtendWith(SuppressOutputExtension.class)
public class GruelboxBlocksAPermanentFailureTest {

  private static final String TABLE = GruelboxPhaseTwoOutboxAutoConfiguration.DEFAULT_OUTBOX_TABLE_NAME;

  private SingleConnectionDataSource dataSource;

  private DefaultPersistor persistor;

  private SpringTransactionManager transactionManager;

  private RecordedBlocks blocked;

  private ListAppender<ILoggingEvent> logWatcher;

  private Logger listenerLogger;

  /**
   * What the listener counted, in the order it counted it.
   */
  private static final class RecordedBlocks implements VanillaBpMetrics {

    private final List<String> entries = new ArrayList<>();

    @Override
    public void outboxEntryBlocked(
        final String store,
        final String operation,
        final boolean permanent) {

      entries.add("%s/%s/%s".formatted(store, operation, permanent));

    }

  }

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
    blocked = new RecordedBlocks();

    logWatcher = new ListAppender<>();
    logWatcher.start();
    listenerLogger = (Logger) LoggerFactory.getLogger(GruelboxPhaseTwoFailureListener.class);
    listenerLogger.addAppender(logWatcher);

  }

  @AfterEach
  public void closeTheDatabase() {

    listenerLogger.detachAppender(logWatcher);
    logWatcher.stop();
    dataSource.destroy();

  }

  private GruelboxPhaseTwoFailureListener listener() {

    return new GruelboxPhaseTwoFailureListener(persistor, transactionManager, () -> blocked, 50);

  }

  /**
   * An entry as gruelbox holds it after it committed a failed attempt: the attempt is
   * counted and the version is the one that commit wrote.
   */
  private TransactionOutboxEntry savedEntry(
      final Invocation invocation,
      final int attempts,
      final boolean alreadyBlocked) throws Exception {

    final var entry = TransactionOutboxEntry
        .builder()
        .id(UUID.randomUUID().toString())
        .invocation(invocation)
        .attempts(attempts)
        .blocked(alreadyBlocked)
        .nextAttemptTime(Instant.now())
        .build();
    transactionManager.inTransactionThrows(transaction -> persistor.save(transaction, entry));
    return entry;

  }

  private static Invocation aStartOfARide() {

    return phaseTwoDispatch(
        PhaseOperation.START_WORKFLOW.name(), "taxiride", "Ride", "4711", "camunda7");

  }

  private static Invocation phaseTwoDispatch(
      final String... arguments) {

    return new Invocation(
        "vanillaBpGruelboxPhaseTwoDispatch", "dispatch", new Class<?>[]{
            String.class, String.class, String.class, String.class, String.class, String.class
        }, new Object[]{
            arguments[0], arguments[1], arguments[2], arguments[3], arguments[4], null
        });

  }

  private boolean isBlockedInTheTable(
      final String id) throws Exception {

    try (var connection = dataSource.getConnection(); var statement = connection
        .prepareStatement("SELECT blocked FROM %s WHERE id = ?".formatted(TABLE))) {
      statement.setString(1, id);
      try (var resultSet = statement.executeQuery()) {
        assertTrue(resultSet.next(), "the entry vanished from the table");
        return resultSet.getBoolean(1);
      }
    }

  }

  private List<ILoggingEvent> errors() {

    return logWatcher.list
        .stream()
        .filter(event -> event.getLevel() == Level.ERROR)
        .toList();

  }

  @Test
  @DisplayName("A failure repeating cannot fix blocks the entry after the first attempt")
  public void aPermanentFailureBlocksTheEntry() throws Exception {

    final var entry = savedEntry(aStartOfARide(), 1, false);

    listener()
        .failure(
            entry,
            new PhaseTwoPermanentFailure("the model cannot be evaluated", new IllegalStateException()));

    assertTrue(isBlockedInTheTable(entry.getId()));
    assertEquals(
        List.of("GruelboxPhaseTwoOutbox/START_WORKFLOW/true"),
        blocked.entries,
        "a blocked entry is counted exactly once");

  }

  @Test
  @DisplayName("The error of a blocked entry names the workflow and not only the entry")
  public void theErrorNamesTheWorkflow() throws Exception {

    final var entry = savedEntry(aStartOfARide(), 1, false);

    listener()
        .failure(
            entry,
            new PhaseTwoPermanentFailure("the model cannot be evaluated", new IllegalStateException()));

    assertEquals(1, errors().size());
    final var reported = errors()
        .get(0)
        .getFormattedMessage();
    assertTrue(reported.contains("START_WORKFLOW"), reported);
    assertTrue(reported.contains("taxiride"), reported);
    assertTrue(reported.contains("Ride"), reported);
    assertTrue(reported.contains("4711"), reported);
    assertTrue(reported.contains("camunda7"), reported);
    assertTrue(reported.contains(entry.getId()), reported);

  }

  @Test
  @DisplayName("A failure which may pass leaves the entry to gruelbox' own retries")
  public void anOrdinaryFailureIsNotBlocked() throws Exception {

    final var entry = savedEntry(aStartOfARide(), 1, false);

    listener().failure(entry, new IllegalStateException("the database hiccupped"));

    assertFalse(isBlockedInTheTable(entry.getId()));
    assertTrue(blocked.entries.isEmpty());
    assertTrue(errors().isEmpty());

  }

  /**
   * Gruelbox writes the block itself once <code>block-after-attempts</code> are used up,
   * and it logs a line naming the entry id. The listener adds the workflow to it and
   * counts the entry, and it must not write a second block or a second error.
   */
  @Test
  @DisplayName("An entry gruelbox gave up on itself is reported once and not blocked again")
  public void anEntryBlockedByGruelboxIsOnlyReported() throws Exception {

    final var entry = savedEntry(aStartOfARide(), 50, true);

    listener().failure(entry, new IllegalStateException("the cluster stayed away"));

    assertEquals(
        List.of("GruelboxPhaseTwoOutbox/START_WORKFLOW/false"),
        blocked.entries);
    assertEquals(1, errors().size());
    assertTrue(errors().get(0).getFormattedMessage().contains("50 times"));

  }

  /**
   * The outbox bean is VanillaBP's, but an application may schedule work of its own on
   * it. Blocking somebody else's entry is not this listener's business.
   */
  @Test
  @DisplayName("An entry which is not a phase-two dispatch is left alone")
  public void aForeignEntryIsLeftAlone() throws Exception {

    final var entry = savedEntry(
        new Invocation("someBeanOfTheApplication", "sendTheInvoice", new Class<?>[]{
            String.class
        }, new Object[]{
            "4711"
        }),
        1,
        false);

    listener()
        .failure(
            entry,
            new PhaseTwoPermanentFailure("looks permanent but is none of ours", new IllegalStateException()));

    assertFalse(isBlockedInTheTable(entry.getId()));
    assertTrue(blocked.entries.isEmpty());
    assertTrue(errors().isEmpty());

  }

  /**
   * Gruelbox takes one listener, so an application which brings its own would lose it to
   * VanillaBP's. The auto-configuration chains them instead, and this is what the chain
   * has to deliver.
   */
  @Test
  @DisplayName("A listener the application brought keeps being called")
  public void theApplicationsOwnListenerStillHears() throws Exception {

    final var heard = new ArrayList<String>();
    final var chain = listener().andThen(new TransactionOutboxListener() {

      @Override
      public void failure(
          final TransactionOutboxEntry entry,
          final Throwable cause) {

        heard.add(entry.getId());

      }

    });
    final var entry = savedEntry(aStartOfARide(), 1, false);

    chain
        .failure(
            entry,
            new PhaseTwoPermanentFailure("the model cannot be evaluated", new IllegalStateException()));

    assertEquals(List.of(entry.getId()), heard);
    assertTrue(isBlockedInTheTable(entry.getId()));

  }

}
