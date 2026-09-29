package io.vanillabp.outbox.gruelbox;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

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

import com.gruelbox.transactionoutbox.Invocation;
import com.gruelbox.transactionoutbox.Persistor;
import com.gruelbox.transactionoutbox.TransactionOutboxEntry;
import com.gruelbox.transactionoutbox.spring.SpringTransactionManager;

import io.vanillabp.integration.adapter.migration.observability.VanillaBpMetrics;
import io.vanillabp.integration.spi.PhaseOperation;
import io.vanillabp.integration.spi.PhaseTwoPermanentFailure;
import io.vanillabp.integration.spi.PhaseTwoRetryLater;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * This listener writes into the row gruelbox just committed: the window a rejected dispatch
 * asked for, and the blocked flag of a failure the adapter called permanent. Both writes can
 * fail - the database is away, or somebody else touched the row at the version it was handed
 * over at - and what the entry has to look like afterwards is the subject here.
 * <p>
 * The rule is the same for both: the entry keeps what gruelbox wrote. A due time which could
 * not be written leaves the store's own distance in the object, so the entry is retried at
 * <code>attempt-frequency</code> instead of carrying a moment the table does not have. A
 * blocked flag which could not be written leaves the entry unblocked, so it is attempted again
 * until <code>block-after-attempts</code> are used up rather than looking blocked to a reader
 * of the object while the row says otherwise.
 * <p>
 * Nothing is scheduled: the write is the subject, so the persistor is the double which refuses
 * it and the entry is the one gruelbox would hand over. The transaction manager is the real
 * one over an in-memory database, because what a refused write does to the transaction around
 * it is part of the case.
 */
@ExtendWith(SuppressOutputExtension.class)
public class AWriteWhichFailsLeavesTheEntryAsGruelboxWroteItTest {

  private static final Duration ATTEMPT_FREQUENCY = Duration.ofSeconds(30);

  private static final Duration WINDOW = Duration.ofSeconds(10);

  private static final int ATTEMPT_BUDGET = 50;

  private SingleConnectionDataSource dataSource;

  private SpringTransactionManager transactionManager;

  @BeforeEach
  public void aDatabaseOfThisTestsOwn() {

    // one connection kept open: the in-memory database lives as long as it does
    dataSource = new SingleConnectionDataSource(
        "jdbc:h2:mem:%s;DB_CLOSE_DELAY=-1".formatted(UUID.randomUUID()), "sa", "", true);
    dataSource.setDriverClassName("org.h2.Driver");
    transactionManager = new SpringTransactionManager(
        new DataSourceTransactionManager(dataSource), dataSource);

  }

  @AfterEach
  public void closeTheDatabase() {

    dataSource.destroy();

  }

  /**
   * The listener over a persistor which cannot write. Nothing migrates gruelbox' table here:
   * the persistor never reaches a database, because it refuses the update itself.
   *
   * @return The listener under test
   */
  private GruelboxPhaseTwoFailureListener listenerWhoseWritesFail() throws Exception {

    final var persistor = mock(Persistor.class);
    doThrow(new java.sql.SQLException("the database is away")).when(persistor).update(any(), any());
    return new GruelboxPhaseTwoFailureListener(
        persistor, transactionManager, () -> VanillaBpMetrics.NONE, ATTEMPT_BUDGET);

  }

  /**
   * An entry as gruelbox holds it right after it committed a failed attempt: the attempt is
   * counted and the next one is due after the store's own distance.
   *
   * @return The entry
   */
  private static TransactionOutboxEntry anEntryWhoseAttemptFailed() {

    return TransactionOutboxEntry
        .builder()
        .id(UUID.randomUUID().toString())
        .invocation(
            new Invocation(
                "vanillaBpGruelboxPhaseTwoDispatch", "dispatch", new Class<?>[]{
                    String.class, String.class, String.class, String.class, String.class, String.class
                }, new Object[]{
                    PhaseOperation.START_WORKFLOW.name(), "taxiride", "Ride", "4711", "camunda8", null
                }))
        .attempts(1)
        .nextAttemptTime(Instant.now().plus(ATTEMPT_FREQUENCY).truncatedTo(ChronoUnit.MILLIS))
        .build();

  }

  @Test
  @DisplayName("A due time which cannot be written leaves the distance gruelbox wrote")
  public void aDueTimeWhichCannotBeWrittenLeavesTheDistanceGruelboxWrote() throws Exception {

    final var entry = anEntryWhoseAttemptFailed();
    final var gruelboxWrote = entry.getNextAttemptTime();

    listenerWhoseWritesFail()
        .failure(entry, new PhaseTwoRetryLater("the workflow is not searchable yet", WINDOW));

    assertEquals(
        gruelboxWrote,
        entry.getNextAttemptTime(),
        "the row was not changed, so the object must not claim a moment of its own");
    assertFalse(entry.isBlocked(), "a rejection is not a reason to block anything");

  }

  @Test
  @DisplayName("A blocked flag which cannot be written leaves the entry to its attempt budget")
  public void aBlockedFlagWhichCannotBeWrittenLeavesTheEntryToItsBudget() throws Exception {

    final var entry = anEntryWhoseAttemptFailed();

    listenerWhoseWritesFail()
        .failure(entry, new PhaseTwoPermanentFailure("the command was refused as invalid", null));

    assertFalse(
        entry.isBlocked(),
        "the row says the entry is open, so the object has to say the same and be attempted again");
    assertTrue(
        entry.getAttempts() < ATTEMPT_BUDGET,
        "what ends such an entry is the budget, which is the only thing left once blocking failed");

  }

}
