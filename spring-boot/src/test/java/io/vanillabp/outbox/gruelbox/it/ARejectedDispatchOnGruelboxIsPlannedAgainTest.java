package io.vanillabp.outbox.gruelbox.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.support.TransactionTemplate;

import io.vanillabp.integration.spi.PhaseTwoCall;
import io.vanillabp.integration.spi.PhaseTwoOutbox;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;
import io.vanillabp.outbox.gruelbox.GruelboxPhaseTwoOutboxAutoConfiguration;
import io.vanillabp.spi.process.ProcessService;

/**
 * The two promises every VanillaBP outbox makes, measured on this store: a call the adapter
 * cannot serve yet reaches the consumer once and is ticked off, and the entry which waits
 * for its BPMS does not hold the entries behind it.
 * <p>
 * The parts of this are already tested on their own - the dispatch gives the entry back
 * after one attempt, and the listener writes the window the adapter named - but neither
 * says what an application gets out of a running outbox, which is what this test reads.
 * <p>
 * The application runs on a database of its own, so nothing another test left behind is
 * read here. Its <code>attempt-frequency</code> is the distance gruelbox writes onto a row
 * it failed, and it is set to five minutes here because the second test needs one entry to
 * stay away for as long as that test runs (see
 * {@link #LONGER_THAN_THIS_TEST_CAN_TAKE}).
 */
@ExtendWith(SuppressOutputExtension.class)
@SuppressOutputExtension.SuppressBackgroundOutput
@SpringBootTest(
    classes = TestApplication.class,
    properties = {
        "spring.datasource.url=jdbc:h2:mem:outbox-gruelbox-rejected;DB_CLOSE_DELAY=-1", "vanillabp.outbox.attempt-frequency=PT5M"
    })
public class ARejectedDispatchOnGruelboxIsPlannedAgainTest {

  /**
   * The table gruelbox writes, taken from the class which configures it.
   */
  private static final String TABLE = GruelboxPhaseTwoOutboxAutoConfiguration.DEFAULT_OUTBOX_TABLE_NAME;

  /**
   * The BPMN process the workflow service serves. It names none, so the convention applies
   * and the class name is the process id.
   */
  private static final String PROCESS = SampleWorkflowService.class.getSimpleName();

  /**
   * How long an assertion waits for something the outbox does on its own thread. Long
   * enough for a loaded build machine, and it is a deadline rather than a pause: a test
   * which is right stops waiting as soon as the entry moved.
   */
  private static final long PATIENCE = 20000L;

  /**
   * The window a rejected dispatch names in the second test. It says how long the entry
   * stays away. gruelbox first writes its own distance onto the row, and the listener then
   * writes this window over it, whether it is shorter or longer. The test sets
   * <code>vanillabp.outbox.attempt-frequency</code> to the same five minutes, so the entry
   * stays away even where the write of the window fails. The entry comes back when the test
   * makes it due, not when this passes.
   */
  private static final Duration LONGER_THAN_THIS_TEST_CAN_TAKE = Duration.ofMinutes(5);

  /**
   * The window the first test names. Short, because that test waits for the entry to come
   * back on its own.
   */
  private static final Duration UNTIL_THE_BPMS_CAUGHT_UP = Duration.ofMillis(500);

  @Autowired
  private ProcessService<Aggregate> processService;

  @Autowired
  private TransactionTemplate transactionTemplate;

  @Autowired
  private PhaseTwoOutbox outbox;

  @Autowired
  private SampleExtension extension;

  @Autowired
  private AggregateRepository aggregates;

  @Autowired
  private DataSource dataSource;

  @BeforeEach
  public void resetExtension() {

    extension.reset();

  }

  private Aggregate startWorkflowAndSchedule(
      final String content,
      final String event) {

    return transactionTemplate.execute(status -> {
      final var aggregate = new Aggregate();
      aggregate.setContent(content);
      final var attached = processService.startWorkflow(aggregate);
      outbox
          .schedule(
              SampleExtension.call("test-module", PROCESS, attached.getId().toString(), event));
      return attached;
    });

  }

  /**
   * The key the extension's operation derives. Gruelbox keeps it as the unique request id
   * of the entry, which is the one thing that finds the row of one aggregate among
   * everything this table holds.
   */
  private static String idempotencyKeyOf(
      final Aggregate aggregate,
      final String event) {

    return "%s|%s|%s|%s".formatted("test-module", PROCESS, aggregate.getId(), event);

  }

  /**
   * Whether the entry of that operation was ticked off. Gruelbox marks an entry processed
   * once the dispatch returned, and it keeps the row until the retention runs out, so this
   * one read is the whole answer.
   */
  private boolean isTickedOff(
      final String idempotencyKey) throws SQLException {

    try (var connection = dataSource.getConnection(); var statement = connection
        .prepareStatement("SELECT processed FROM %s WHERE uniqueRequestId = ?".formatted(TABLE))) {
      statement.setString(1, idempotencyKey);
      try (var resultSet = statement.executeQuery()) {
        return resultSet.next() && resultSet.getBoolean(1);
      }
    }

  }

  /**
   * Waits until the entry of that operation is ticked off for good.
   */
  private void awaitTickedOff(
      final String idempotencyKey) throws Exception {

    final var deadline = System.currentTimeMillis() + PATIENCE;
    while (!isTickedOff(idempotencyKey)) {
      if (System.currentTimeMillis() > deadline) {
        throw new AssertionError(
            "The outbox entry of '%s' was never ticked off".formatted(idempotencyKey));
      }
      Thread.sleep(50);
    }

  }

  /**
   * Whether gruelbox wrote down an attempt on the entry of that operation. It stamps the
   * time of the last attempt when the attempt ENDED. The count of attempts says nothing
   * here, because the answer "not yet" is taken back from it.
   */
  private boolean wasAttempted(
      final String idempotencyKey) throws SQLException {

    try (var connection = dataSource.getConnection(); var statement = connection
        .prepareStatement("SELECT lastAttemptTime FROM %s WHERE uniqueRequestId = ?".formatted(TABLE))) {
      statement.setString(1, idempotencyKey);
      try (var resultSet = statement.executeQuery()) {
        return resultSet.next() && (resultSet.getTimestamp(1) != null);
      }
    }

  }

  /**
   * Waits until gruelbox wrote down an attempt on that entry, which is what says that a
   * rejection was used up and which entry used it.
   */
  private void awaitAttempted(
      final String idempotencyKey) throws Exception {

    final var deadline = System.currentTimeMillis() + PATIENCE;
    while (!wasAttempted(idempotencyKey)) {
      if (System.currentTimeMillis() > deadline) {
        throw new AssertionError(
            "The outbox entry of '%s' was never attempted".formatted(idempotencyKey));
      }
      Thread.sleep(50);
    }

  }

  /**
   * Makes the entry of that operation due now, whatever due time it carries.
   */
  private void makeDueNow(
      final String idempotencyKey) throws SQLException {

    try (var connection = dataSource.getConnection(); var statement = connection
        .prepareStatement("UPDATE %s SET nextAttemptTime = ? WHERE uniqueRequestId = ?".formatted(TABLE))) {
      statement.setTimestamp(1, Timestamp.from(Instant.now()));
      statement.setString(2, idempotencyKey);
      statement.executeUpdate();
    }

  }

  @Test
  @DisplayName("A call rejected once reaches the consumer exactly once and the entry is ticked off")
  public void aRejectedCallIsDispatchedOnceAndTheEntryIsDone() throws Exception {

    extension.rejectNextDispatches(1, UNTIL_THE_BPMS_CAUGHT_UP);
    extension.writeWhileDispatching();

    final var aggregate = startWorkflowAndSchedule("rejected-once", "created");
    assertNotNull(aggregate);
    final var key = idempotencyKeyOf(aggregate, "created");

    extension.awaitDispatched(1, PATIENCE);
    // an entry marked processed is one no flush takes again, so this is what makes "once"
    // an answer of the store rather than of a pause in this test
    awaitTickedOff(key);

    assertEquals(
        1,
        extension.getDispatched().size(),
        "the call reached the consumer twice: the first dispatch lost its transaction");
    assertEquals(
        2,
        extension.getAttempts(),
        "one rejected attempt and one which went through is what this case costs");
    assertEquals(
        SampleExtension.REPORTED_BY_THE_HANDLER,
        aggregates
            .findById(aggregate.getId())
            .orElseThrow()
            .getReported(),
        "what was written while the call went out has to stand with that call");

  }

  /**
   * The other half of ending a rejected attempt: an entry which waited for its BPMS must
   * not hold every other workflow's entry with it.
   * <p>
   * The rejected entry is given a window which outlasts the test, so it cannot come back
   * while the test runs. The order below rests on that and not on the speed of the
   * machine. A store which ends the rejected attempt dispatches the entry behind it at
   * once, and a store which waits for the rejected one dispatches nothing, however long
   * anybody waits for it. The test makes the rejected entry due itself at the end, so
   * nothing of it is left standing for the classes which follow.
   */
  @Test
  @DisplayName("A workflow which is not searchable yet does not hold the entries behind it")
  public void anEntryWhichIsNotDueYetLetsTheOthersPass() throws Exception {

    extension.rejectNextDispatches(1, LONGER_THAN_THIS_TEST_CAN_TAKE);

    final var waiting = startWorkflowAndSchedule("not-searchable-yet", "created");
    // the rejection belongs to this workflow, and the store writing down the attempt is
    // what says so. Scheduling the second workflow before that could hand the rejection to it
    awaitAttempted(idempotencyKeyOf(waiting, "created"));
    final var passing = startWorkflowAndSchedule("searchable", "created");

    final var dispatched = extension.awaitDispatched(1, PATIENCE);

    assertEquals(
        List
            .of(
                passing
                    .getId()
                    .toString()),
        dispatched
            .stream()
            .map(PhaseTwoCall::workflowAggregateId)
            .toList(),
        "the rejected entry was waited for, so everything behind it waited too");
    awaitTickedOff(idempotencyKeyOf(passing, "created"));

    makeDueNow(idempotencyKeyOf(waiting, "created"));
    awaitTickedOff(idempotencyKeyOf(waiting, "created"));

  }

}
