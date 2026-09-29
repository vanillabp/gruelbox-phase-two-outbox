package io.vanillabp.outbox.gruelbox;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.transaction.support.TransactionTemplate;

import com.gruelbox.transactionoutbox.TransactionOutbox;

import io.vanillabp.integration.spi.PhaseOperation;
import io.vanillabp.integration.spi.PhaseTwoCall;
import io.vanillabp.integration.spi.PhaseTwoPayloadStore;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * Every call which carries an idempotency key is read against gruelbox' table before it is
 * scheduled: that read is how the key of a dispatched entry is freed and how a younger call
 * takes the place of one which is still waiting. A store which cannot do that read must not
 * schedule anything.
 * <p>
 * Swallowing the failure would be the dangerous answer. The schedule would then go in without
 * the key, so the operation would be planned a second time beside an entry which is still
 * waiting for it, and nobody would learn about the table until a workflow was carried out
 * twice. So the store refuses, in the transaction of the caller, and the message names the
 * workflow and the table it could not read.
 * <p>
 * The table of this test's database is never created, which is the shape an application meets
 * when a schema was handed over and this one table forgotten - or when somebody drops it under
 * a running node.
 */
@ExtendWith(SuppressOutputExtension.class)
public class AStoreWhichCannotReadItsTableRefusesTheScheduleTest {

  private static final String TABLE = GruelboxPhaseTwoOutboxAutoConfiguration.DEFAULT_OUTBOX_TABLE_NAME;

  private SingleConnectionDataSource dataSource;

  private TransactionTemplate transactions;

  /**
   * What gruelbox would have been asked to schedule. It records nothing, because a schedule
   * must not reach it: the read comes first, and this test is about the read failing.
   */
  private TransactionOutbox gruelbox;

  @BeforeEach
  public void aDatabaseWithoutGruelboxTable() {

    // one connection kept open: the in-memory database lives as long as it does. Nothing
    // migrates gruelbox' schema here, which is the point of this test
    dataSource = new SingleConnectionDataSource(
        "jdbc:h2:mem:%s;DB_CLOSE_DELAY=-1".formatted(UUID.randomUUID()), "sa", "", true);
    dataSource.setDriverClassName("org.h2.Driver");
    transactions = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
    gruelbox = Mockito.mock(TransactionOutbox.class);

  }

  @AfterEach
  public void closeTheDatabase() {

    dataSource.destroy();

  }

  /**
   * The store over that database.
   *
   * @return The store
   */
  private GruelboxPhaseTwoOutbox storeWithoutItsTable() {

    return new GruelboxPhaseTwoOutbox(
        gruelbox, dataSource, TABLE, Mockito.mock(PhaseTwoPayloadStore.class));

  }

  @Test
  @DisplayName("A schedule whose key cannot be read is refused, naming the workflow and the table")
  public void aScheduleWhoseKeyCannotBeReadIsRefused() {

    final var store = storeWithoutItsTable();
    final var call = PhaseTwoCall
        .of(
            PhaseOperation.CORRELATE_MESSAGE, "taxiride", "Ride", "4711", null, Map
                .of(
                    PhaseTwoCall.ARG_MESSAGE_NAME, "OfferRequested",
                    PhaseTwoCall.ARG_CORRELATION_ID, "offer-1"));
    assertTrue(call.idempotencyKey().isPresent(), "a call without a key would not read the table at all");

    final var refused = assertThrows(
        IllegalStateException.class,
        () -> transactions.execute(status -> store.schedule(call)));

    final var message = refused.getMessage();
    assertTrue(message.contains("Could not look up the phase-two outbox entry"), message);
    assertTrue(message.contains("'Ride'"), message);
    assertTrue(message.contains("'taxiride'"), message);
    assertTrue(message.contains("'%s'".formatted(TABLE)), message);
    assertInstanceOf(
        SQLException.class,
        refused.getCause(),
        "what the database said belongs in the trail, because it names the table and the schema");

    // and nothing was handed to gruelbox: a schedule without its key is exactly the
    // duplicate this refusal is here to prevent
    Mockito.verifyNoInteractions(gruelbox);

  }

}
