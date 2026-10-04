package io.vanillabp.outbox.gruelbox.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.time.Duration;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.support.TransactionTemplate;

import io.vanillabp.integration.adapter.migration.config.PhaseTwoOutboxProperties;
import io.vanillabp.integration.spi.PhaseTwoOutbox;
import io.vanillabp.integration.test.utils.CapturedOutput;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;
import io.vanillabp.outbox.gruelbox.GruelboxPhaseTwoOutboxAutoConfiguration;
import io.vanillabp.spi.process.ProcessService;

/**
 * The rule of every VanillaBP outbox, measured on a running application with this store: a
 * BPMS which keeps answering "not yet" uses no attempts, and the entry is blocked once
 * <code>vanillabp.outbox.wait-for-visibility-at-most</code> passed since it was written.
 * <p>
 * The budget is two attempts and the wait is six seconds. A store which counted the answers
 * would block the entry at the second one and dispatch it no more, so the five answers this
 * test waits for are only reached by a store which does not count them. The application runs
 * on a database of its own, so nothing another test left behind is read here.
 */
@ExtendWith(SuppressOutputExtension.class)
@SuppressOutputExtension.SuppressBackgroundOutput
@SpringBootTest(
    classes = TestApplication.class,
    properties = {
        "spring.datasource.url=jdbc:h2:mem:outbox-gruelbox-waits-by-time;DB_CLOSE_DELAY=-1", "vanillabp.outbox.block-after-attempts="
            + WaitingForTheBpmsOnGruelboxEndsByTimeTest.ATTEMPT_BUDGET, "vanillabp.outbox.wait-for-visibility-at-most=PT6S"
    })
public class WaitingForTheBpmsOnGruelboxEndsByTimeTest {

  static final int ATTEMPT_BUDGET = 2;

  /**
   * More answers than the budget has attempts, so reaching them proves that none was
   * counted.
   */
  private static final int MORE_ANSWERS_THAN_THE_BUDGET = 5;

  /**
   * How long an assertion waits for something the outbox does on its own thread. It is a
   * deadline and not a pause, and it is longer than the six seconds the entry may wait.
   */
  private static final long PATIENCE = 30000L;

  private static final String TABLE = GruelboxPhaseTwoOutboxAutoConfiguration.DEFAULT_OUTBOX_TABLE_NAME;

  private static final String PROCESS = SampleWorkflowService.class.getSimpleName();

  /**
   * The words of the ERROR which reports an entry that waited too long.
   */
  private static final String WAITED_TOO_LONG = "which is longer than '"
      + PhaseTwoOutboxProperties.WAIT_FOR_VISIBILITY_AT_MOST_PROPERTY
      + "' allows";

  @Autowired
  private ProcessService<Aggregate> processService;

  @Autowired
  private TransactionTemplate transactionTemplate;

  @Autowired
  private PhaseTwoOutbox outbox;

  @Autowired
  private SampleExtension extension;

  @Autowired
  private DataSource dataSource;

  @AfterEach
  public void stopRejecting() {

    extension.reset();

  }

  /**
   * What the table says about the entry of one operation.
   */
  private record Row(
                     int attempts,
                     boolean blocked) {
  }

  private Row rowOf(
      final String idempotencyKey) throws SQLException {

    try (var connection = dataSource.getConnection(); var statement = connection
        .prepareStatement("SELECT attempts, blocked FROM %s WHERE uniqueRequestId = ?".formatted(TABLE))) {
      statement.setString(1, idempotencyKey);
      try (var resultSet = statement.executeQuery()) {
        assertTrue(resultSet.next(), "the entry of '%s' is not in the table".formatted(idempotencyKey));
        return new Row(resultSet.getInt(1), resultSet.getBoolean(2));
      }
    }

  }

  @Test
  @DisplayName("Answers 'not yet' use no attempts, and the entry is blocked once it waited too long")
  public void waitingEndsByTimeAndNotByAttempts(
      final CapturedOutput output) throws Exception {

    extension.rejectNextDispatches(Integer.MAX_VALUE, Duration.ofMillis(200));

    final var aggregate = transactionTemplate.execute(status -> {
      final var created = new Aggregate();
      created.setContent("never-searchable");
      final var attached = processService.startWorkflow(created);
      outbox.schedule(SampleExtension.call("test-module", PROCESS, attached.getId().toString(), "created"));
      return attached;
    });
    final var key = "%s|%s|%s|%s".formatted("test-module", PROCESS, aggregate.getId(), "created");

    final var deadline = System.currentTimeMillis() + PATIENCE;
    while (extension.getAttempts() < MORE_ANSWERS_THAN_THE_BUDGET) {
      if (System.currentTimeMillis() > deadline) {
        throw new AssertionError(
            "The entry was dispatched %d times and then no more, so the answers used up its %d attempts"
                .formatted(extension.getAttempts(), ATTEMPT_BUDGET));
      }
      Thread.sleep(50);
    }
    final var whileWaiting = rowOf(key);
    assertFalse(whileWaiting.blocked(), "more answers than attempts, and the entry is blocked");
    // a read may fall between gruelbox counting the attempt and the listener taking it back
    assertTrue(whileWaiting.attempts() <= 1, "the answers were counted: "
        + whileWaiting.attempts());

    while (!rowOf(key).blocked()) {
      if (System.currentTimeMillis() > deadline) {
        throw new AssertionError("The entry was never blocked, although it may wait six seconds only");
      }
      Thread.sleep(50);
    }
    assertEquals(1, rowOf(key).attempts(), "the block counts one attempt, the answers none");
    assertTrue(output.getAll().contains(WAITED_TOO_LONG), output.getAll());

  }

}
