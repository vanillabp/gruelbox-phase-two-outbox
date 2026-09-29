package io.vanillabp.outbox.gruelbox;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
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
import org.mockito.Mockito;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import com.gruelbox.transactionoutbox.DefaultPersistor;
import com.gruelbox.transactionoutbox.Dialect;
import com.gruelbox.transactionoutbox.Invocation;
import com.gruelbox.transactionoutbox.TransactionOutboxEntry;
import com.gruelbox.transactionoutbox.spring.SpringTransactionManager;

import io.vanillabp.integration.spi.PhaseOperation;
import io.vanillabp.integration.spi.PhaseTwoPayloadStore;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * The three numbers this store reads out of gruelbox' table: how many entries still wait for
 * a dispatch, when the next thing is due, and how many dispatched entries a flush still owes
 * a delete.
 * <p>
 * Two promises hold them together. A BLOCKED entry is no moment to wake up for and nothing a
 * flush owes a delete, because it waits for a person rather than for a clock; the backlog is
 * the one number which does carry it, since gruelbox leaves such an entry unprocessed, and the
 * gauge <code>vanillabp.outbox.blocked</code> stands beside it for exactly that reason. And
 * none of the three may cost the application anything when the table cannot be read - a meter
 * reports a gap instead of a number, and the poller falls back to its cap - which is why the
 * last test of each group asks a store whose table is not there.
 * <p>
 * The rows are written with gruelbox' own persistor and then moved into the state under test
 * with plain SQL, so what an assertion reads is what this test wrote. Nothing is scheduled
 * here: a schedule hands the entry to a dispatcher thread, and the numbers would then be
 * racing it.
 */
@ExtendWith(SuppressOutputExtension.class)
public class TheNumbersThisStoreReportsTest {

  private static final String TABLE = GruelboxPhaseTwoOutboxAutoConfiguration.DEFAULT_OUTBOX_TABLE_NAME;

  /**
   * A table of a name no database of this test has. Asking it is how the answer of a store
   * which cannot read its table is measured, and it is the shape a real application meets
   * when somebody drops the table under a running node.
   */
  private static final String NO_SUCH_TABLE = "A_TABLE_WHICH_IS_NOT_THERE";

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

  /**
   * A store over gruelbox' table of this test. It is built without a transaction outbox and
   * with a payload store nobody calls, because none of the three numbers schedules anything:
   * each of them is a read of the table the store was built with.
   *
   * @param tableName The table to read, which one test deliberately gets wrong
   * @return The store
   */
  private GruelboxPhaseTwoOutbox storeReading(
      final String tableName) {

    return new GruelboxPhaseTwoOutbox(
        null, dataSource, tableName, Mockito.mock(PhaseTwoPayloadStore.class));

  }

  /**
   * An entry as gruelbox writes it when a call is scheduled: waiting, never attempted, due
   * at the moment given.
   *
   * @param dueAt When gruelbox would pick it up
   * @return The entry, already in the table
   */
  private TransactionOutboxEntry anEntryWaitingUntil(
      final Instant dueAt) throws Exception {

    final var entry = TransactionOutboxEntry
        .builder()
        .id(UUID.randomUUID().toString())
        .invocation(
            new Invocation(
                "vanillaBpGruelboxPhaseTwoDispatch", "dispatch", new Class<?>[]{
                    String.class, String.class, String.class, String.class, String.class, String.class
                }, new Object[]{
                    PhaseOperation.START_WORKFLOW.name(), "taxiride", "Ride", UUID.randomUUID()
                        .toString(), "camunda8", null
                }))
        .nextAttemptTime(dueAt.truncatedTo(ChronoUnit.MILLIS))
        .build();
    transactionManager.inTransactionThrows(transaction -> persistor.save(transaction, entry));
    return entry;

  }

  /**
   * Marks an entry the way gruelbox marks one it dispatched: processed, with the moment its
   * retention runs out in the column it keeps both moments in.
   *
   * @param entry The entry to mark
   * @param retentionRunsOutAt When a flush may delete it
   */
  private void dispatched(
      final TransactionOutboxEntry entry,
      final Instant retentionRunsOutAt) throws Exception {

    update(
        "UPDATE %s SET processed = TRUE, nextAttemptTime = ? WHERE id = ?",
        java.sql.Timestamp.from(retentionRunsOutAt.truncatedTo(ChronoUnit.MILLIS)),
        entry.getId());

  }

  /**
   * Marks an entry the way this store's failure listener marks one nobody is going to retry.
   *
   * @param entry The entry to block
   */
  private void blocked(
      final TransactionOutboxEntry entry) throws Exception {

    update("UPDATE %s SET blocked = TRUE WHERE id = ?", entry.getId());

  }

  private void update(
      final String statement,
      final Object... arguments) throws Exception {

    try (var connection = dataSource.getConnection(); var prepared = connection
        .prepareStatement(statement.formatted(TABLE))) {
      for (var argument = 0; argument < arguments.length; argument++) {
        prepared.setObject(argument + 1, arguments[argument]);
      }
      prepared.executeUpdate();
    }

  }

  @Test
  @DisplayName("The backlog counts every entry which was not dispatched, a blocked one included")
  public void theBacklogCountsWhatWaits() throws Exception {

    final var store = storeReading(TABLE);

    assertEquals(0, store.pendingCalls().orElseThrow(), "an empty table owes nothing");

    anEntryWaitingUntil(Instant.now());
    final var alsoWaiting = anEntryWaitingUntil(Instant.now());
    assertEquals(2, store.pendingCalls().orElseThrow());

    dispatched(alsoWaiting, Instant.now().plus(Duration.ofDays(7)));
    assertEquals(1, store.pendingCalls().orElseThrow(), "a dispatched entry is not a backlog");

    // a blocked entry, on the other hand, IS in this number: gruelbox leaves it
    // unprocessed, and an operation nobody carried out is still owed to the workflow
    final var givenUpOn = anEntryWaitingUntil(Instant.now());
    blocked(givenUpOn);
    assertEquals(
        2,
        store.pendingCalls().orElseThrow(),
        "gruelbox keeps a blocked entry unprocessed, so this number carries it - see the gauge "
            + "'vanillabp.outbox.blocked' beside it");

  }

  @Test
  @DisplayName("A backlog which cannot be read is a gap in the meter and not a failure")
  public void aBacklogWhichCannotBeReadIsAGap() {

    assertTrue(storeReading(NO_SUCH_TABLE).pendingCalls().isEmpty(), "a meter must not end anything");

  }

  @Test
  @DisplayName("The next moment is the earlier of a waiting entry and a retention which runs out")
  public void theNextMomentIsTheEarlierOfTheTwo() throws Exception {

    final var store = storeReading(TABLE);

    assertNull(store.earliestDueAt(), "nothing is owed, so the poller stays on its cap");

    final var inTenMinutes = Instant.now().plus(Duration.ofMinutes(10)).truncatedTo(ChronoUnit.MILLIS);
    anEntryWaitingUntil(inTenMinutes);
    assertEquals(inTenMinutes, store.earliestDueAt(), "the waiting entry alone");

    // a dispatched entry whose retention runs out sooner: the flush has something to do
    // before the attempt is due, and that is the moment to wake up for
    final var inOneMinute = Instant.now().plus(Duration.ofMinutes(1)).truncatedTo(ChronoUnit.MILLIS);
    dispatched(anEntryWaitingUntil(Instant.now()), inOneMinute);
    assertEquals(inOneMinute, store.earliestDueAt(), "the earlier of the two");

  }

  @Test
  @DisplayName("A retention which runs out is the only moment where nothing waits")
  public void aRetentionAloneIsTheMoment() throws Exception {

    final var store = storeReading(TABLE);

    final var inOneMinute = Instant.now().plus(Duration.ofMinutes(1)).truncatedTo(ChronoUnit.MILLIS);
    dispatched(anEntryWaitingUntil(Instant.now()), inOneMinute);

    assertEquals(inOneMinute, store.earliestDueAt());

  }

  @Test
  @DisplayName("A blocked entry is no moment to wake up for")
  public void aBlockedEntryIsNoMomentToWakeUpFor() throws Exception {

    final var store = storeReading(TABLE);

    blocked(anEntryWaitingUntil(Instant.now()));

    assertNull(
        store.earliestDueAt(),
        "an entry waiting for a person would keep the poller turning for as long as it sits there");

  }

  @Test
  @DisplayName("A moment which cannot be read leaves the poller on its cap")
  public void aMomentWhichCannotBeReadLeavesTheCap() {

    assertNull(storeReading(NO_SUCH_TABLE).earliestDueAt(), "a poller which stops asking is worse");

  }

  @Test
  @DisplayName("What a flush still owes counts the dispatched entries whose retention passed")
  public void whatAFlushStillOwes() throws Exception {

    final var store = storeReading(TABLE);

    assertEquals(0, store.countDispatchedEntriesPastTheirRetention().orElseThrow());

    // dispatched and past its retention: the next flush deletes it
    dispatched(anEntryWaitingUntil(Instant.now()), Instant.now().minus(Duration.ofMinutes(1)));
    assertEquals(1, store.countDispatchedEntriesPastTheirRetention().orElseThrow());

    // dispatched and kept for another week: not owed yet
    dispatched(anEntryWaitingUntil(Instant.now()), Instant.now().plus(Duration.ofDays(7)));
    assertEquals(1, store.countDispatchedEntriesPastTheirRetention().orElseThrow());

    // waiting for its dispatch: nothing a flush deletes
    anEntryWaitingUntil(Instant.now());
    assertEquals(1, store.countDispatchedEntriesPastTheirRetention().orElseThrow());

    // blocked and long past the moment in its column: gruelbox deletes no blocked entry,
    // so counting it would promise a number which never falls
    final var givenUpOn = anEntryWaitingUntil(Instant.now().minus(Duration.ofDays(30)));
    blocked(givenUpOn);
    assertEquals(1, store.countDispatchedEntriesPastTheirRetention().orElseThrow());

  }

  @Test
  @DisplayName("What a flush owes is a gap where the table cannot be read")
  public void whatAFlushOwesIsAGapWhereTheTableCannotBeRead() {

    assertTrue(storeReading(NO_SUCH_TABLE).countDispatchedEntriesPastTheirRetention().isEmpty());

  }

  @Test
  @DisplayName("The store says which table it reads, which is what the housekeeping claims it by")
  public void theStoreSaysWhichTableItReads() {

    assertEquals(TABLE, storeReading(TABLE).getTableName());
    assertFalse(TABLE.isBlank(), "a claim needs a name");

  }

}
