package io.vanillabp.outbox.gruelbox;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vanillabp.integration.adapter.migration.processservice.PhaseTwoRouter;
import io.vanillabp.integration.spi.PhaseTwoRetryLater;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * Holds what this store does with a workflow which its BPMS has not made searchable yet. It gives
 * the entry back after one attempt, and it does not wait.
 * <p>
 * It used to wait here and ask again. The transaction is the reason why it cannot. Gruelbox opened
 * a transaction around this call. The rejected attempt marked it rollback-only. A second attempt
 * inside it reached the BPMS and then lost its commit. Holding the entry back is now the job of
 * {@link GruelboxPhaseTwoFailureListener}, which writes the window the adapter named onto the row.
 */
@ExtendWith(SuppressOutputExtension.class)
public class GruelboxGivesBackWhatItCannotDispatchYetTest {

  private static final String ARGS = null;

  private static PhaseTwoRetryLater notVisibleYet() {

    return new PhaseTwoRetryLater("not searchable yet", Duration.ofSeconds(10));

  }

  @Test
  @DisplayName("A workflow which is not searchable yet is attempted once and travels on")
  public void aRejectedDispatchEndsWithTheFirstAttempt() {

    final var attempts = new AtomicInteger();
    final var rejection = notVisibleYet();
    final var router = mock(PhaseTwoRouter.class);
    doAnswer(invocation -> {
      attempts.incrementAndGet();
      throw rejection;
    })
        .when(router)
        .dispatch(any(), anyBoolean());

    final var thrown = assertThrows(
        PhaseTwoRetryLater.class,
        () -> new GruelboxPhaseTwoDispatchBean(router)
            .dispatch("CORRELATE_MESSAGE", "loan-approval", "loan_approval", "4711", null, ARGS));

    // waiting here would mean dispatching again. So one attempt shows that the thread
    // gave the entry back. The time the call took would not show it. A machine which
    // runs several builds stops this JVM for seconds at a time, and a wall clock cannot
    // tell that from a thread which waited
    assertEquals(1, attempts.get(), "the entry goes back instead of being attempted again in here");
    assertSame(rejection, thrown, "the window the adapter named has to reach the store unchanged");

  }

  @Test
  @DisplayName("Any other failure travels on untouched")
  public void anOrdinaryFailureIsNotRepeatedEither() {

    final var attempts = new AtomicInteger();
    final var router = mock(PhaseTwoRouter.class);
    doAnswer(invocation -> {
      attempts.incrementAndGet();
      throw new IllegalStateException("the BPMS refused this");
    })
        .when(router)
        .dispatch(any(), anyBoolean());

    assertThrows(
        IllegalStateException.class,
        () -> new GruelboxPhaseTwoDispatchBean(router)
            .dispatch("CORRELATE_MESSAGE", "loan-approval", "loan_approval", "4711", null, ARGS));

    assertEquals(1, attempts.get(), "gruelbox counts the attempt and decides when to ask again");

  }

  @Test
  @DisplayName("A dispatch which works is routed once")
  public void aWorkingDispatchIsRoutedOnce() {

    final var attempts = new AtomicInteger();
    final var router = mock(PhaseTwoRouter.class);
    doAnswer(invocation -> {
      attempts.incrementAndGet();
      return null;
    })
        .when(router)
        .dispatch(any(), anyBoolean());

    new GruelboxPhaseTwoDispatchBean(router)
        .dispatch("CORRELATE_MESSAGE", "loan-approval", "loan_approval", "4711", null, ARGS);

    assertEquals(1, attempts.get());

  }

}
