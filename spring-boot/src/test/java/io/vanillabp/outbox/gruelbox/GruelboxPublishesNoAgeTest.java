package io.vanillabp.outbox.gruelbox;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

import com.gruelbox.transactionoutbox.TransactionOutbox;

import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * Holds that this is the one store which answers nothing when it is asked how long its oldest
 * waiting entry has waited.
 * <p>
 * Gruelbox keeps the moment an entry was written in the column of its next attempt. It overwrites
 * it the first time a flush picks the entry up. So the entries which are most likely old are
 * exactly the ones which cannot say. An answer from the untouched ones would report a young age,
 * while old entries stand next to them. That reads like an outbox which is up to date. The stores
 * VanillaBP writes itself keep that moment in a column of their own, and they do answer.
 * <p>
 * The answer needs no database, so the store here is built on mocks.
 */
@ExtendWith(SuppressOutputExtension.class)
public class GruelboxPublishesNoAgeTest {

  @Test
  @DisplayName("Gruelbox says nothing about the age of its oldest waiting entry")
  public void gruelboxSaysNothingAboutTheAgeOfItsOldestEntry() {

    final var outbox = new GruelboxPhaseTwoOutbox(
        Mockito.mock(TransactionOutbox.class), Mockito.mock(javax.sql.DataSource.class), "TXNO_OUTBOX", Mockito
            .mock(io.vanillabp.integration.spi.PhaseTwoPayloadStore.class));

    assertTrue(
        outbox
            .ageOfOldestPendingCall()
            .isEmpty(),
        "an age which could only ever look young is not published at all");

  }

}
