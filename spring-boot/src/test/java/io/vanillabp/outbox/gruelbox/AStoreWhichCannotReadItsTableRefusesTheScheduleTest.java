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
 * Holds that a store which cannot read its table schedules nothing. Every call which carries an
 * idempotency key is checked against the gruelbox table before it is scheduled. That read frees the
 * key of a dispatched entry, and it lets a younger call take the place of one which still waits.
 * <p>
 * Swallowing the failure would be the dangerous answer. The schedule would then go in without the
 * key. So the operation would be planned a second time, next to an entry which still waits for
 * it. Nobody would learn about the table until a workflow ran twice. So the store refuses, in the
 * transaction of the caller, and the message names the workflow and the table it could not read.
 * <p>
 * The database of this test never gets the table. An application meets that when a schema was
 * handed over and this one table was forgotten, or when somebody drops it under a running node.
 */
@ExtendWith(SuppressOutputExtension.class)
public class AStoreWhichCannotReadItsTableRefusesTheScheduleTest {

  private static final String TABLE = GruelboxPhaseTwoOutboxAutoConfiguration.DEFAULT_OUTBOX_TABLE_NAME;

  private SingleConnectionDataSource dataSource;

  private TransactionTemplate transactions;

  /**
   * What gruelbox would have been asked to schedule. It records nothing, because no schedule may
   * reach it. The read comes first, and this test is about the read failing.
   */
  private TransactionOutbox gruelbox;

  @BeforeEach
  public void aDatabaseWithoutGruelboxTable() {

    // one connection kept open: the in-memory database lives as long as it does. Nothing
    // creates the gruelbox schema here, and that is the point of this test
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

    // nothing was handed to gruelbox: a schedule without its key is exactly the
    // duplicate this refusal prevents
    Mockito.verifyNoInteractions(gruelbox);

  }

}
