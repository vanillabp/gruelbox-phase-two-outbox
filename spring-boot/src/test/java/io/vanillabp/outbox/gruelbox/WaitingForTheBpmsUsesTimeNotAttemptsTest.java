package io.vanillabp.outbox.gruelbox;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
import com.gruelbox.transactionoutbox.spring.SpringTransactionManager;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.vanillabp.integration.adapter.migration.config.PhaseTwoOutboxProperties;
import io.vanillabp.integration.adapter.migration.observability.VanillaBpMetrics;
import io.vanillabp.integration.spi.PhaseOperation;
import io.vanillabp.integration.spi.PhaseTwoRetryLater;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * A BPMS which does not report a workflow yet is no failure of the entry, so its answer "not
 * yet" must not use up the attempts of <code>vanillabp.outbox.block-after-attempts</code>.
 * What ends such a wait is <code>vanillabp.outbox.wait-for-visibility-at-most</code>, counted
 * from the moment the entry was written. This test reads the row back to prove both halves on
 * this store, where gruelbox counts the attempt before the listener is called.
 * <p>
 * Each entry is saved the way gruelbox holds it right after it committed a failed attempt,
 * and then handed to the listener. Nothing dispatches here, so the row an assertion reads is
 * the row this test wrote. How old an entry is, is set through the moment in its session,
 * so no test waits for time to pass.
 */
@ExtendWith(SuppressOutputExtension.class)
public class WaitingForTheBpmsUsesTimeNotAttemptsTest {

  private static final String TABLE = GruelboxPhaseTwoOutboxAutoConfiguration.DEFAULT_OUTBOX_TABLE_NAME;

  private static final int ATTEMPT_BUDGET = 3;

  private static final Duration WAIT_AT_MOST = Duration.ofMinutes(10);

  private static final Duration WINDOW = Duration.ofSeconds(10);

  private static final Duration ATTEMPT_FREQUENCY = Duration.ofSeconds(30);

  /**
   * The words of the ERROR which reports an entry that waited too long. The key is what
   * an operator needs to read there.
   */
  private static final String WAITED_TOO_LONG = "which is longer than '"
      + PhaseTwoOutboxProperties.WAIT_FOR_VISIBILITY_AT_MOST_PROPERTY
      + "' allows";

  private SingleConnectionDataSource dataSource;

  private DefaultPersistor persistor;

  private SpringTransactionManager transactionManager;

  private final List<String> blocks = new ArrayList<>();

  private ListAppender<ILoggingEvent> logWatcher;

  private Logger listenerLogger;

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

    final VanillaBpMetrics countsBlocks = new VanillaBpMetrics() {

      @Override
      public void outboxEntryBlocked(
          final String store,
          final String operation,
          final boolean permanent) {

        blocks.add("%s/%s/%s".formatted(store, operation, permanent));

      }

    };
    final var properties = PhaseTwoOutboxProperties
        .builder()
        .blockAfterAttempts(ATTEMPT_BUDGET)
        .build();
    properties.setWaitForVisibilityAtMost(WAIT_AT_MOST);
    return new GruelboxPhaseTwoFailureListener(persistor, transactionManager, () -> countsBlocks, properties);

  }

  /**
   * An entry as gruelbox holds it right after it committed a failed attempt.
   *
   * @param session The session the entry was written with, <code>null</code> for none
   * @param attempts The attempts gruelbox counted, this one included
   * @return The entry, saved
   */
  private TransactionOutboxEntry anEntryWhoseAttemptFailed(
      final Map<String, String> session,
      final int attempts) throws Exception {

    final var entry = TransactionOutboxEntry
        .builder()
        .id(UUID.randomUUID().toString())
        .invocation(
            new Invocation(
                "vanillaBpGruelboxPhaseTwoDispatch", "dispatch", new Class<?>[]{
                    String.class, String.class, String.class, String.class, String.class, String.class
                }, new Object[]{
                    PhaseOperation.CORRELATE_MESSAGE.name(), "taxiride", "Ride", "4711", "camunda8", null
                }, null, session))
        .attempts(attempts)
        // gruelbox blocks an entry once the attempt it just counted was the last of the budget
        .blocked(attempts >= ATTEMPT_BUDGET)
        .lastAttemptTime(Instant.now().truncatedTo(ChronoUnit.MILLIS))
        .nextAttemptTime(Instant.now().plus(ATTEMPT_FREQUENCY).truncatedTo(ChronoUnit.MILLIS))
        .build();
    transactionManager.inTransactionThrows(transaction -> persistor.save(transaction, entry));
    return entry;

  }

  private static Map<String, String> writtenAgo(
      final Duration age) {

    return Map.of(GruelboxPhaseTwoFailureListener.WRITTEN_AT, Instant.now().minus(age).toString());

  }

  /**
   * What the table says about one entry.
   */
  private record Row(
                     int attempts,
                     boolean blocked,
                     Instant dueAt) {
  }

  private Row rowOf(
      final String id) throws Exception {

    try (var connection = dataSource.getConnection(); var statement = connection
        .prepareStatement("SELECT attempts, blocked, nextAttemptTime FROM %s WHERE id = ?".formatted(TABLE))) {
      statement.setString(1, id);
      try (var resultSet = statement.executeQuery()) {
        assertTrue(resultSet.next(), "the entry vanished from the table");
        return new Row(resultSet.getInt(1), resultSet.getBoolean(2), resultSet.getTimestamp(3).toInstant());
      }
    }

  }

  private List<String> errors() {

    return logWatcher.list
        .stream()
        .filter(event -> event.getLevel() == Level.ERROR)
        .map(ILoggingEvent::getFormattedMessage)
        .toList();

  }

  @Test
  @DisplayName("The answer 'not yet' uses no attempt, and the entry is due after the window")
  public void theAnswerNotYetUsesNoAttempt() throws Exception {

    final var entry = anEntryWhoseAttemptFailed(writtenAgo(Duration.ofSeconds(1)), 1);

    final var beforeTheWrite = Instant.now().truncatedTo(ChronoUnit.MILLIS);
    listener().failure(entry, new PhaseTwoRetryLater("not searchable yet", WINDOW));

    final var row = rowOf(entry.getId());
    assertEquals(0, row.attempts(), "gruelbox counted the answer, and the listener has to take it back");
    assertFalse(row.blocked());
    assertFalse(row.dueAt().isBefore(beforeTheWrite.plus(WINDOW)), "due before the window: "
        + row.dueAt());
    assertTrue(row.dueAt().isBefore(beforeTheWrite.plus(ATTEMPT_FREQUENCY)), "due after the store's distance: "
        + row.dueAt());
    assertTrue(errors().isEmpty(), errors().toString());
    assertTrue(blocks.isEmpty(), blocks.toString());

  }

  /**
   * More answers than the budget has attempts block nothing as long as the wait is not over.
   * Every answer arrives with the attempt gruelbox just counted on top of what the row held.
   */
  @Test
  @DisplayName("More answers 'not yet' than the budget has attempts do not block the entry")
  public void moreAnswersThanTheBudgetBlockNothing() throws Exception {

    final var entry = anEntryWhoseAttemptFailed(writtenAgo(Duration.ofSeconds(1)), 1);
    final var listener = listener();

    for (var answer = 0; answer < ATTEMPT_BUDGET * 2; answer++) {
      listener.failure(entry, new PhaseTwoRetryLater("not searchable yet", WINDOW));
      // what gruelbox does at the next failed attempt: count it and push the row back
      entry.setAttempts(entry.getAttempts() + 1);
      entry.setBlocked(entry.getAttempts() >= ATTEMPT_BUDGET);
      entry.setNextAttemptTime(Instant.now().plus(ATTEMPT_FREQUENCY).truncatedTo(ChronoUnit.MILLIS));
      transactionManager.inTransactionThrows(transaction -> persistor.update(transaction, entry));
    }
    listener.failure(entry, new PhaseTwoRetryLater("not searchable yet", WINDOW));

    final var row = rowOf(entry.getId());
    assertEquals(0, row.attempts());
    assertFalse(row.blocked(), "the entry waited a second, which is far from the ten minutes it may wait");

  }

  /**
   * An entry which used up all attempts but one with real failures is blocked by gruelbox
   * at its next failure. Where that failure is the answer "not yet", the answer is looked at
   * first, so the entry stays open, as it does on the other stores.
   */
  @Test
  @DisplayName("An entry gruelbox blocked at the answer 'not yet' is opened again")
  public void anEntryGruelboxBlockedAtTheAnswerIsOpenedAgain() throws Exception {

    final var entry = anEntryWhoseAttemptFailed(writtenAgo(Duration.ofMinutes(1)), ATTEMPT_BUDGET);

    listener().failure(entry, new PhaseTwoRetryLater("not searchable yet", WINDOW));

    final var row = rowOf(entry.getId());
    assertFalse(row.blocked(), "the answer 'not yet' must not end the entry while it may still wait");
    assertEquals(ATTEMPT_BUDGET - 1, row.attempts(), "the earlier failures stay counted");
    assertTrue(errors().isEmpty(), errors().toString());

  }

  @Test
  @DisplayName("An entry which waited longer than allowed is blocked at the next answer 'not yet'")
  public void anEntryWhichWaitedTooLongIsBlocked() throws Exception {

    final var entry = anEntryWhoseAttemptFailed(writtenAgo(WAIT_AT_MOST.plusMinutes(1)), 1);

    listener().failure(entry, new PhaseTwoRetryLater("not searchable yet", WINDOW));

    final var row = rowOf(entry.getId());
    assertTrue(row.blocked());
    assertEquals(1, row.attempts(), "the block keeps the attempt gruelbox counted, as every block does");
    assertEquals(List.of("GruelboxPhaseTwoOutbox/CORRELATE_MESSAGE/false"), blocks);
    assertEquals(1, errors().size(), errors().toString());
    final var reported = errors().getFirst();
    assertTrue(reported.contains(WAITED_TOO_LONG), reported);
    assertTrue(reported.contains("taxiride"), reported);
    assertTrue(reported.contains(entry.getId()), reported);

  }

  /**
   * Where gruelbox used up the budget at the same answer at which the wait ends, the row is
   * blocked already. The listener reports it once, as an entry which waited too long.
   */
  @Test
  @DisplayName("An entry which waited too long and used up its attempts is reported once")
  public void anEntryWhichWaitedTooLongAndUsedUpItsAttemptsIsReportedOnce() throws Exception {

    final var entry = anEntryWhoseAttemptFailed(writtenAgo(WAIT_AT_MOST.plusMinutes(1)), ATTEMPT_BUDGET);

    listener().failure(entry, new PhaseTwoRetryLater("not searchable yet", WINDOW));

    final var row = rowOf(entry.getId());
    assertTrue(row.blocked());
    assertEquals(List.of("GruelboxPhaseTwoOutbox/CORRELATE_MESSAGE/false"), blocks);
    assertEquals(1, errors().size(), errors().toString());
    assertTrue(errors().getFirst().contains(WAITED_TOO_LONG), errors().getFirst());

  }

  /**
   * An entry written before this store kept the moment, or by an outbox built without the
   * listener, cannot be ended by time. It keeps the old rule, so the attempt budget still
   * ends a wait which never ends.
   */
  @Test
  @DisplayName("An entry which does not know when it was written counts the answer as an attempt")
  public void anEntryWithoutTheMomentCountsTheAnswer() throws Exception {

    final var entry = anEntryWhoseAttemptFailed(null, 1);

    listener().failure(entry, new PhaseTwoRetryLater("not searchable yet", WINDOW));

    final var row = rowOf(entry.getId());
    assertEquals(1, row.attempts());
    assertFalse(row.blocked());
    assertTrue(row.dueAt().isBefore(Instant.now().plus(ATTEMPT_FREQUENCY)), "the window is still written");
    assertTrue(errors().stream().noneMatch(error -> error.contains(WAITED_TOO_LONG)), errors().toString());

  }

  @Test
  @DisplayName("A moment which cannot be read counts as no moment")
  public void aMomentWhichCannotBeReadCountsAsNone() throws Exception {

    final var entry = anEntryWhoseAttemptFailed(Map.of(GruelboxPhaseTwoFailureListener.WRITTEN_AT, "yesterday"), 1);

    assertNull(GruelboxPhaseTwoFailureListener.writtenAtOf(entry));
    listener().failure(entry, new PhaseTwoRetryLater("not searchable yet", WINDOW));

    assertEquals(1, rowOf(entry.getId()).attempts());

  }

  @Test
  @DisplayName("A new entry carries the moment it was written")
  public void aNewEntryCarriesTheMomentItWasWritten() {

    final var before = Instant.now();
    final var session = listener().extractSession();
    final var entry = TransactionOutboxEntry
        .builder()
        .id(UUID.randomUUID().toString())
        .invocation(new Invocation("any", "dispatch", new Class<?>[0], new Object[0], null, session))
        .build();

    final var writtenAt = GruelboxPhaseTwoFailureListener.writtenAtOf(entry);
    assertNotNull(writtenAt);
    assertFalse(writtenAt.isBefore(before), writtenAt.toString());
    assertFalse(writtenAt.isAfter(Instant.now()), writtenAt.toString());

  }

}
