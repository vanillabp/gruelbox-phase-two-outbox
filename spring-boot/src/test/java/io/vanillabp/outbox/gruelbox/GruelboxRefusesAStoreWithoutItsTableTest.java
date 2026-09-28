package io.vanillabp.outbox.gruelbox;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import javax.sql.DataSource;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

import com.gruelbox.transactionoutbox.TransactionOutbox;

import io.vanillabp.integration.spi.PhaseTwoPayloadStore;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * An application which builds the gruelbox store itself and leaves out one of the places
 * that store works on.
 * <p>
 * Without the data source or the name of gruelbox' table it hands entries to gruelbox and
 * can do nothing else: it cannot free the key of an entry which was dispatched, cannot let
 * a younger call take the place of a waiting one, and answers the payload housekeeping that
 * every payload is still named, which is the answer that keeps the payload table growing
 * for good. Without a payload store it loses what a call carries. None of that shows up as
 * an error later on, so each of them is refused where the store is built.
 */
@ExtendWith(SuppressOutputExtension.class)
public class GruelboxRefusesAStoreWithoutItsTableTest {

  private static final String TABLE = "TXNO_OUTBOX";

  @Test
  @DisplayName("A store without a data source is refused, naming what it would cost")
  public void aStoreWithoutADataSourceIsRefused() {

    final var refused = assertThrows(
        IllegalArgumentException.class,
        () -> new GruelboxPhaseTwoOutbox(
            Mockito.mock(TransactionOutbox.class), null, TABLE, Mockito.mock(PhaseTwoPayloadStore.class)));

    final var message = refused.getMessage();
    assertTrue(message.contains("without a data source"), message);
    // what it would cost, in both of its shapes
    assertTrue(message.contains("discards the next operation"), message);
    assertTrue(message.contains("removes no payload"), message);
    // and the two ways to a complete store
    assertTrue(message.contains("Pass the data source"), message);
    assertTrue(message.contains("GruelboxPhaseTwoOutboxAutoConfiguration"), message);

  }

  @Test
  @DisplayName("A store without the table name is refused naming that half alone")
  public void aStoreWithoutTheTableNameIsRefused() {

    final var refused = assertThrows(
        IllegalArgumentException.class,
        () -> new GruelboxPhaseTwoOutbox(
            Mockito.mock(TransactionOutbox.class), Mockito.mock(DataSource.class), null, Mockito
                .mock(PhaseTwoPayloadStore.class)));

    final var message = refused.getMessage();
    assertTrue(
        message.contains("without the name of gruelbox' table"),
        "the message names what is missing, not both halves: "
            + message);

  }

  @Test
  @DisplayName("A store missing both halves names both of them")
  public void aStoreMissingBothHalvesNamesBoth() {

    final var refused = assertThrows(
        IllegalArgumentException.class,
        () -> new GruelboxPhaseTwoOutbox(
            Mockito.mock(TransactionOutbox.class), null, null, Mockito.mock(PhaseTwoPayloadStore.class)));

    final var message = refused.getMessage();
    assertTrue(message.contains("without a data source and the name of gruelbox' table"), message);

  }

  @Test
  @DisplayName("A store without a payload store is refused, naming what a call would lose")
  public void aStoreWithoutAPayloadStoreIsRefused() {

    final var refused = assertThrows(
        IllegalArgumentException.class,
        () -> new GruelboxPhaseTwoOutbox(
            Mockito.mock(TransactionOutbox.class), Mockito.mock(DataSource.class), TABLE, null));

    final var message = refused.getMessage();
    assertTrue(message.contains("without a payload store"), message);
    // what it would cost
    assertTrue(message.contains("reach the BPMS without what it carries"), message);
    // and the two ways to a complete store
    assertTrue(message.contains("Pass a PhaseTwoPayloadStore"), message);
    assertTrue(message.contains("GruelboxPhaseTwoOutboxAutoConfiguration"), message);

  }

  @Test
  @DisplayName("A store with all three places is built")
  public void aStoreWithEveryPlaceIsBuilt() {

    assertDoesNotThrow(
        () -> new GruelboxPhaseTwoOutbox(
            Mockito.mock(TransactionOutbox.class), Mockito.mock(DataSource.class), TABLE, Mockito
                .mock(PhaseTwoPayloadStore.class)));

  }

}
