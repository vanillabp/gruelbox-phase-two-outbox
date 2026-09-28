package io.vanillabp.outbox.gruelbox;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

import com.gruelbox.transactionoutbox.TransactionOutbox;

import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * The one store which answers nothing when it is asked how long its oldest waiting entry
 * has been waiting.
 * <p>
 * Gruelbox keeps the moment an entry was written in the column its next attempt lives in
 * and overwrites it the first time a flush picks the entry up, so the entries most likely
 * to be old are exactly the ones which could not say. Answering from the untouched ones
 * would report a young age while old entries stand next to them, which reads as an outbox
 * that is up to date. The stores VanillaBP writes itself keep that moment in a column of
 * their own and do answer.
 * <p>
 * No database is needed for the answer, which is why the store here is built over mocks.
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
