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

import io.vanillabp.integration.adapter.migration.config.PhaseTwoOutboxProperties;
import io.vanillabp.integration.adapter.migration.observability.VanillaBpMetrics;
import io.vanillabp.integration.adapter.migration.outbox.JdbcHousekeepingLease;
import io.vanillabp.integration.spi.PhaseTwoPayloadStore;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * An application which builds the gruelbox dispatcher itself and leaves out one of the
 * stores it works on.
 * <p>
 * Without the store it polls, the dispatcher asks at the configured cap and answers its own
 * housekeeping nothing, so no payload is ever removed. Without the payload store it removes
 * none either. Both leave the payload table growing for as long as the application runs, and
 * nothing shows up as an error, so both are refused where the dispatcher is built.
 */
@ExtendWith(SuppressOutputExtension.class)
public class GruelboxRefusesADispatcherWithoutItsStoresTest {

  private static final String TABLE = "TXNO_OUTBOX";

  private static GruelboxPhaseTwoOutbox aStore() {

    return new GruelboxPhaseTwoOutbox(
        Mockito.mock(TransactionOutbox.class), Mockito.mock(DataSource.class), TABLE, Mockito
            .mock(PhaseTwoPayloadStore.class));

  }

  @Test
  @DisplayName("A dispatcher without the store it polls is refused, naming what it would cost")
  public void aDispatcherWithoutTheStoreItPollsIsRefused() {

    final var refused = assertThrows(
        IllegalArgumentException.class,
        () -> new GruelboxPhaseTwoOutboxDispatcher(
            Mockito.mock(TransactionOutbox.class), new PhaseTwoOutboxProperties(), null, null, Mockito
                .mock(PhaseTwoPayloadStore.class), Mockito
                    .mock(JdbcHousekeepingLease.class), () -> VanillaBpMetrics.NONE));

    final var message = refused.getMessage();
    assertTrue(message.contains("without the store it polls"), message);
    // what it would cost, in both of its shapes
    assertTrue(message.contains("asking at the configured cap"), message);
    assertTrue(message.contains("has not deleted yet"), message);
    // and the two ways to a complete dispatcher
    assertTrue(message.contains("Pass a GruelboxPhaseTwoOutbox"), message);
    assertTrue(message.contains("GruelboxPhaseTwoOutboxAutoConfiguration"), message);

  }

  @Test
  @DisplayName("A dispatcher without a payload store is refused, naming what stays behind")
  public void aDispatcherWithoutAPayloadStoreIsRefused() {

    final var refused = assertThrows(
        IllegalArgumentException.class,
        () -> new GruelboxPhaseTwoOutboxDispatcher(
            Mockito.mock(TransactionOutbox.class), new PhaseTwoOutboxProperties(), null, aStore(), null, Mockito
                .mock(JdbcHousekeepingLease.class), () -> VanillaBpMetrics.NONE));

    final var message = refused.getMessage();
    assertTrue(message.contains("without a payload store"), message);
    // what it would cost
    assertTrue(message.contains("payload table grows"), message);
    // and the two ways to a complete dispatcher
    assertTrue(message.contains("Pass a PhaseTwoPayloadStore"), message);
    assertTrue(message.contains("GruelboxPhaseTwoOutboxAutoConfiguration"), message);

  }

  @Test
  @DisplayName("A dispatcher with both stores is built")
  public void aDispatcherWithBothStoresIsBuilt() {

    assertDoesNotThrow(
        () -> new GruelboxPhaseTwoOutboxDispatcher(
            Mockito.mock(TransactionOutbox.class), new PhaseTwoOutboxProperties(), null, aStore(), Mockito
                .mock(PhaseTwoPayloadStore.class), Mockito
                    .mock(JdbcHousekeepingLease.class), () -> VanillaBpMetrics.NONE));

  }

}
