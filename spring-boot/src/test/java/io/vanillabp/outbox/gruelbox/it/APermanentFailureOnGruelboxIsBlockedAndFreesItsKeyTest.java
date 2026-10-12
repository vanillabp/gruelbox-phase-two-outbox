package io.vanillabp.outbox.gruelbox.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.support.TransactionTemplate;

import com.gruelbox.transactionoutbox.TransactionOutbox;

import io.vanillabp.integration.spi.PhaseTwoOutbox;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;
import io.vanillabp.outbox.gruelbox.GruelboxPhaseTwoOutboxAutoConfiguration;
import io.vanillabp.spi.process.ProcessService;

/**
 * Holds what this store does with a failure which the extension or the adapter calls permanent
 * ({@link io.vanillabp.integration.spi.PhaseTwoPermanentFailure}), compared with the stores
 * VanillaBP writes itself. The entry is attempted once, it is blocked, and the next request of the
 * same operation gets an entry of its own.
 * <p>
 * Gruelbox did not keep the last promise by itself. Its unique constraint covers a blocked row as
 * well. So the operation which failed could not be planned again until somebody removed the row.
 * Now the store frees the key of a blocked row when the same key is planned again. The blocked row
 * stays where an operator finds it.
 * <p>
 * The application runs on a database of its own, so nothing which another test left behind is
 * read here.
 */
@ExtendWith(SuppressOutputExtension.class)
@SuppressOutputExtension.SuppressBackgroundOutput
@SpringBootTest(
    classes = TestApplication.class,
    properties = "spring.datasource.url=jdbc:h2:mem:outbox-gruelbox-permanent;DB_CLOSE_DELAY=-1")
public class APermanentFailureOnGruelboxIsBlockedAndFreesItsKeyTest {

  private static final String TABLE = GruelboxPhaseTwoOutboxAutoConfiguration.DEFAULT_OUTBOX_TABLE_NAME;

  private static final String PROCESS = SampleWorkflowService.class.getSimpleName();

  /**
   * How long an assertion waits for something which the outbox does on its own thread. It is a
   * deadline, not a pause.
   */
  private static final long PATIENCE = 20000L;

  /**
   * How long the first test gives the outbox to repeat a blocked entry, which it must not do. The
   * test application flushes every half second and retries after half a second. So the outbox gets
   * four chances to do it wrong.
   */
  private static final long FOUR_FLUSHES = 2000L;

  @Autowired
  private ProcessService<Aggregate> processService;

  @Autowired
  private TransactionTemplate transactionTemplate;

  @Autowired
  private PhaseTwoOutbox outbox;

  @Autowired
  private TransactionOutbox gruelbox;

  @Autowired
  private SampleExtension extension;

  @Autowired
  private DataSource dataSource;

  @BeforeEach
  public void resetExtension() {

    extension.reset();

  }

  @AfterEach
  public void leaveNoFailureForTheNextTest() {

    extension.reset();

  }

  /**
   * One row of the gruelbox table, as far as this test reads it.
   *
   * @param id The id gruelbox gave the entry
   * @param uniqueRequestId The key, <code>null</code> if it was freed
   * @param attempts How many attempts gruelbox counted
   * @param blocked Whether the entry is blocked
   * @param processed Whether the entry was dispatched
   */
  private record Row(String id, String uniqueRequestId, int attempts, boolean blocked, boolean processed) {
  }

  private Aggregate startWorkflow(
      final String content) {

    return transactionTemplate.execute(status -> {
      final var aggregate = new Aggregate();
      aggregate.setContent(content);
      return processService.startWorkflow(aggregate);
    });

  }

  private boolean schedule(
      final Aggregate aggregate,
      final String event) {

    return Boolean.TRUE.equals(transactionTemplate
        .execute(status -> outbox
            .schedule(SampleExtension.call("test-module", PROCESS, aggregate.getId().toString(), event))));

  }

  private static String idempotencyKeyOf(
      final Aggregate aggregate,
      final String event) {

    return "%s|%s|%s|%s".formatted("test-module", PROCESS, aggregate.getId(), event);

  }

  /**
   * The rows of one operation: the one which carries its key, and the blocked one if its key was
   * freed. A row which gave its key away is found by its id.
   */
  private List<Row> rowsOf(
      final String idempotencyKey,
      final String blockedId) throws SQLException {

    final var rows = new ArrayList<Row>();
    try (var connection = dataSource.getConnection(); var statement = connection
        .prepareStatement(
            "SELECT id, uniqueRequestId, attempts, blocked, processed FROM %s WHERE uniqueRequestId = ? OR id = ?"
                .formatted(TABLE))) {
      statement.setString(1, idempotencyKey);
      statement.setString(2, blockedId);
      try (var resultSet = statement.executeQuery()) {
        while (resultSet.next()) {
          rows.add(new Row(
              resultSet.getString(1), resultSet.getString(2), resultSet.getInt(3), resultSet.getBoolean(4), resultSet
                  .getBoolean(5)));
        }
      }
    }
    return rows;

  }

  private Row awaitBlocked(
      final String idempotencyKey) throws Exception {

    final var deadline = System.currentTimeMillis() + PATIENCE;
    while (true) {
      final var blocked = rowsOf(idempotencyKey, "")
          .stream()
          .filter(Row::blocked)
          .findFirst();
      if (blocked.isPresent()) {
        return blocked.get();
      }
      if (System.currentTimeMillis() > deadline) {
        throw new AssertionError("The entry of '%s' was never blocked".formatted(idempotencyKey));
      }
      Thread.sleep(50);
    }

  }

  private void awaitProcessed(
      final String idempotencyKey) throws Exception {

    final var deadline = System.currentTimeMillis() + PATIENCE;
    while (rowsOf(idempotencyKey, "")
        .stream()
        .filter(Row::processed)
        .findAny()
        .isEmpty()) {
      if (System.currentTimeMillis() > deadline) {
        throw new AssertionError("The entry of '%s' was never dispatched".formatted(idempotencyKey));
      }
      Thread.sleep(50);
    }

  }

  @Test
  @DisplayName("A permanent failure is attempted once and the entry is blocked")
  public void aPermanentFailureIsAttemptedOnceAndBlocked() throws Exception {

    extension.failNextDispatchesPermanently(1);
    final var aggregate = startWorkflow("permanent-once");
    assertTrue(schedule(aggregate, "created"));

    final var key = idempotencyKeyOf(aggregate, "created");
    final var blocked = awaitBlocked(key);
    // no flush reads a blocked entry again. If one did, gruelbox would try four more
    // times in this time
    Thread.sleep(FOUR_FLUSHES);

    assertEquals(1, extension.getAttempts(), "a failure repeating cannot fix was repeated");
    assertEquals(
        List.of(new Row(blocked.id(), key, 1, true, false)),
        rowsOf(key, blocked.id()),
        "one row, attempted once, blocked and not dispatched");

  }

  /**
   * The case of the Business Cockpit. A change whose workflow has no user task after ten minutes is
   * given up, and the next change of the same kind has to get through. The stores VanillaBP writes
   * itself manage that, because blocking frees the key. This store frees it when the key is
   * planned again.
   */
  @Test
  @DisplayName("The next request of the same key gets an entry of its own and the blocked one stays")
  public void theNextRequestOfTheSameKeyGetsThrough() throws Exception {

    extension.failNextDispatchesPermanently(1);
    final var aggregate = startWorkflow("permanent-then-again");
    final var key = idempotencyKeyOf(aggregate, "created");
    assertTrue(schedule(aggregate, "created"));
    final var blocked = awaitBlocked(key);

    assertTrue(schedule(aggregate, "created"), "the blocked entry still holds its key");
    extension.awaitDispatched(1, PATIENCE);
    awaitProcessed(key);

    final var rows = rowsOf(key, blocked.id());
    assertEquals(2, rows.size(), "the blocked row and the new one: "
        + rows);
    final var stillBlocked = rows
        .stream()
        .filter(row -> row.id().equals(blocked.id()))
        .findFirst()
        .orElseThrow();
    assertTrue(stillBlocked.blocked(), "the blocked row is where an operator finds it");
    assertEquals(null, stillBlocked.uniqueRequestId(), "the blocked row gave its key away");
    assertEquals(2, extension.getAttempts(), "the failed attempt and the one of the new entry");

  }

  /**
   * The way back which the README names: the <code>unblock</code> of gruelbox, the same statement
   * an operator runs by hand. The next flush dispatches the entry.
   */
  @Test
  @DisplayName("An entry opened again is dispatched by the next flush")
  public void anUnblockedEntryIsDispatched() throws Exception {

    extension.failNextDispatchesPermanently(1);
    final var aggregate = startWorkflow("permanent-then-unblocked");
    final var key = idempotencyKeyOf(aggregate, "created");
    assertTrue(schedule(aggregate, "created"));
    final var blocked = awaitBlocked(key);

    assertTrue(Boolean.TRUE.equals(transactionTemplate.execute(status -> gruelbox.unblock(blocked.id()))));
    extension.awaitDispatched(1, PATIENCE);
    awaitProcessed(key);

    assertEquals(
        List.of(blocked.id()),
        rowsOf(key, blocked.id())
            .stream()
            .map(Row::id)
            .toList(),
        "the entry opened again is the one which was dispatched, and there is no second one");

  }

}
