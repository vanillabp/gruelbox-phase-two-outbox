package io.vanillabp.outbox.gruelbox.it;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicReference;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.support.TransactionTemplate;

import io.vanillabp.integration.spi.PhaseTwoOutbox;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;
import io.vanillabp.outbox.gruelbox.GruelboxPhaseTwoOutboxAutoConfiguration;
import io.vanillabp.spi.process.ProcessService;

/**
 * Holds the promise every VanillaBP outbox makes, on this store. A call which says that it replaces
 * what still waits takes the place of the waiting entry. The handler is called once, with the
 * younger state.
 * <p>
 * The way there differs from the stores VanillaBP writes itself. That is why this test exists, and
 * not just a second run of their tests. Gruelbox has no replace in its API. So the waiting row is
 * DELETED, and the younger call is scheduled again under the same unique request id. Two things
 * decide whether that is allowed: the <code>version</code> column of gruelbox, and the register of
 * the submitter. If either says that a dispatch has the entry, the younger call becomes a SECOND
 * entry, with no unique request id at all.
 * <p>
 * The application runs on a database of its own, so nothing which another test left behind is
 * read here.
 */
@ExtendWith(SuppressOutputExtension.class)
@SuppressOutputExtension.SuppressBackgroundOutput
@SpringBootTest(
    classes = TestApplication.class,
    properties = {
        "spring.datasource.url=jdbc:h2:mem:outbox-gruelbox-replace;DB_CLOSE_DELAY=-1"
    })
public class AYoungerCallReplacesTheWaitingOneOnGruelboxTest {

  /**
   * The table gruelbox writes, taken from the class which configures it.
   */
  private static final String TABLE = GruelboxPhaseTwoOutboxAutoConfiguration.DEFAULT_OUTBOX_TABLE_NAME;

  /**
   * How long an assertion waits for something which the outbox does on its own thread. It is a
   * deadline, not a pause. A test which is right stops waiting as soon as the entry moved.
   */
  private static final long PATIENCE = 20000L;

  /**
   * How long a test waits before it says that nothing more came. It is three of the half-second
   * windows these tests dispatch in. It is a guard, not a measurement of speed.
   */
  private static final long UNTIL_NOTHING_MORE_CAN_COME = 1500L;

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

  @BeforeEach
  public void resetExtension() {

    extension.reset();

  }

  private static byte[] payloadOf(
      final String content) {

    return content.getBytes(StandardCharsets.UTF_8);

  }

  /**
   * How many rows of the gruelbox table carry that unique request id. The store deduplicates on
   * this id, so a replacement must not raise this number.
   */
  private long countEntriesOf(
      final String idempotencyKey) throws SQLException {

    try (var connection = dataSource.getConnection(); var statement = connection
        .prepareStatement("SELECT count(*) FROM %s WHERE uniqueRequestId = ?".formatted(TABLE))) {
      statement.setString(1, idempotencyKey);
      try (var resultSet = statement.executeQuery()) {
        resultSet.next();
        return resultSet.getLong(1);
      }
    }

  }

  /**
   * How many rows gruelbox holds at all, whether they carry a unique request id or not.
   * The younger call beside a claimed entry is one of those without.
   */
  private long countAllEntries() throws SQLException {

    try (var connection = dataSource.getConnection(); var statement = connection
        .prepareStatement("SELECT count(*) FROM %s".formatted(TABLE))) {
      try (var resultSet = statement.executeQuery()) {
        resultSet.next();
        return resultSet.getLong(1);
      }
    }

  }

  @Test
  @DisplayName("The younger report takes the waiting row, and only it is dispatched")
  public void theYoungerReportReplacesTheWaitingOne() throws Exception {

    final var younger = new AtomicReference<String>();
    final var key = new AtomicReference<String>();

    // both use ONE transaction on purpose. Gruelbox hands an entry to its submitter the
    // moment the scheduling transaction commits. So before that commit, the first entry
    // surely still waits
    final var aggregate = transactionTemplate.execute(status -> {
      final var newAggregate = new Aggregate();
      newAggregate.setContent("gruelbox-replace-waiting");
      final var attached = processService.startWorkflow(newAggregate);

      final var first = SampleExtension
          .call("test-module", "dummy", attached.getId().toString(), "reported", payloadOf("{\"amount\":1}"));
      key.set(first.idempotencyKey().orElseThrow());
      assertTrue(outbox.scheduleReplacingWhatIsStillWaiting(first));

      final var second = SampleExtension
          .call("test-module", "dummy", attached.getId().toString(), "reported", payloadOf("{\"amount\":2}"));
      younger.set(second.payloadReference());
      assertTrue(outbox.scheduleReplacingWhatIsStillWaiting(second));
      return attached;
    });
    assertNotNull(aggregate);

    // one row under that key: the younger call took the row of the waiting one, and does
    // not stand next to it
    assertEquals(1L, countEntriesOf(key.get()));

    final var dispatched = extension.awaitDispatched(1, PATIENCE);
    assertArrayEquals(payloadOf("{\"amount\":2}"), dispatched.getFirst().payload());
    assertEquals(younger.get(), dispatched.getFirst().payloadReference());

    // the one which was dispatched is the only one there ever was
    Thread.sleep(UNTIL_NOTHING_MORE_CAN_COME);
    assertEquals(1, extension.getDispatched().size());

  }

  @Test
  @DisplayName("An entry a dispatch has taken is not replaced - the younger call becomes a second one")
  public void anEntryADispatchHasTakenIsNotReplaced() throws Exception {

    extension.holdNextDispatch();
    try {
      final var aggregate = transactionTemplate.execute(status -> {
        final var newAggregate = new Aggregate();
        newAggregate.setContent("gruelbox-replace-claimed");
        final var attached = processService.startWorkflow(newAggregate);
        assertTrue(
            outbox
                .scheduleReplacingWhatIsStillWaiting(
                    SampleExtension
                        .call(
                            "test-module",
                            "dummy",
                            attached.getId().toString(),
                            "reported",
                            payloadOf("{\"amount\":1}"))));
        return attached;
      });
      assertNotNull(aggregate);

      // the dispatch waits inside the handler. So the register of the submitter names
      // this entry, and the store must not take its row away
      extension.awaitHeldDispatchEntered(PATIENCE);

      final var scheduled = transactionTemplate
          .execute(status -> outbox
              .scheduleReplacingWhatIsStillWaiting(
                  SampleExtension
                      .call(
                          "test-module",
                          "dummy",
                          aggregate.getId().toString(),
                          "reported",
                          payloadOf("{\"amount\":2}"))));
      assertTrue(Boolean.TRUE.equals(scheduled));

      // two rows: the one on its way, which keeps the key, and the one this call became.
      // That one carries no key, because the key belongs to the entry being dispatched
      assertTrue(countAllEntries() >= 2, "the younger call has to become a second entry");
    } finally {
      extension.releaseHeldDispatch();
    }

    // both reach the handler. Which one comes first is up to the dispatcher
    final var payloads = extension
        .awaitDispatched(2, PATIENCE)
        .stream()
        .map(call -> new String(call.payload(), StandardCharsets.UTF_8))
        .toList();
    assertTrue(payloads.contains("{\"amount\":1}"), payloads.toString());
    assertTrue(payloads.contains("{\"amount\":2}"), payloads.toString());

  }

}
