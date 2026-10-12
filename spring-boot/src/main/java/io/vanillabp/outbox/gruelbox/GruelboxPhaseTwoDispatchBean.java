package io.vanillabp.outbox.gruelbox;

import java.time.Instant;

import io.vanillabp.integration.adapter.migration.observability.VanillaBpMetrics;
import io.vanillabp.integration.adapter.migration.observability.VanillaBpMetrics.DispatchOutcome;
import io.vanillabp.integration.adapter.migration.processservice.PhaseTwoRouter;
import io.vanillabp.integration.spi.PhaseTwoCall;
import io.vanillabp.integration.spi.PhaseTwoOutbox;
import io.vanillabp.integration.spi.PhaseTwoPayloadStore;
import io.vanillabp.integration.spi.PhaseTwoRetryLater;

/**
 * What gruelbox calls once the scheduling transaction committed. It builds the
 * {@link PhaseTwoCall} again and hands it to the {@link PhaseTwoRouter} of the core.
 * <p>
 * There is one attempt per call, and a failure is passed on. That includes a workflow which the
 * BPMS has not made searchable yet ({@link PhaseTwoRetryLater}). On Camunda 8 that is normal
 * while its exporter catches up. The entry goes back to the store and is dispatched again. It is
 * not repeated here.
 * <p>
 * This class used to repeat it here, every half second. The transaction is the reason why it
 * cannot. Gruelbox opened a transaction around this call, and the dispatch JOINED it
 * (<code>TransactionRunner#requireTransaction</code>). That keeps the entry and everything the
 * application wrote in one unit of work. A rejected attempt rolls that transaction back. Spring
 * then marks a joined transaction as rollback-only for good, and there is no way back. So a second
 * attempt inside it reached the BPMS and then lost its commit. The report was out, but the entry
 * stayed open. What a handler had written while reporting was rolled back with the transaction.
 * The entry came again afterwards, so the consumer got the same report twice. And gruelbox logged
 * what it saw (<code>Failed to update attempt count</code>): the update which rolled back had
 * already counted up the row it wanted to write. See
 * <code>ARejectedDispatchOnGruelboxIsPlannedAgainTest</code> of this repository.
 * <p>
 * Instead, the entry costs one due time. Gruelbox has one distance for the whole outbox. So
 * {@link GruelboxPhaseTwoFailureListener} writes the window the adapter named onto the row, once
 * the failed attempt is committed. It blocks a permanent failure the same way. The listener takes
 * back the attempt gruelbox counted, because waiting for a BPMS is no failure of the entry. A
 * workflow which never becomes visible is ended by
 * <code>vanillabp.outbox.wait-for-visibility-at-most</code>, counted from the moment the entry was
 * written.
 * <p>
 * This class does NOT ask the BPMS again on the spot. The attempt is not alone in its
 * transaction. A second try inside it would either repeat work the first one had already written,
 * or throw away what the first one did, and neither would say so. Giving the entry back keeps both
 * halves visible.
 */
public class GruelboxPhaseTwoDispatchBean implements GruelboxPhaseTwoDispatch {

  private final PhaseTwoRouter phaseTwoRouter;

  /**
   * Where the bytes of a call with a payload are read from, and where they are removed once the
   * call was dispatched. It is <code>null</code> for a caller which brings none. An entry which
   * names a payload is then dispatched without it, and the handler of the router sees a call
   * which carries nothing.
   */
  private final PhaseTwoPayloadStore payloadStore;

  /**
   * Where the wait of a dispatched entry is reported to. Micrometer is optional, and an
   * application without it uses this.
   */
  private final VanillaBpMetrics metrics;

  /**
   * The dispatch of an outbox whose calls carry no payload, and whose waiting time nobody
   * measures. Here an entry which names a payload is dispatched without it.
   *
   * @param phaseTwoRouter The router the rebuilt call is handed to
   */
  public GruelboxPhaseTwoDispatchBean(
      final PhaseTwoRouter phaseTwoRouter) {

    this(phaseTwoRouter, null);

  }

  /**
   * The dispatch of an outbox whose calls may carry a payload, and whose waiting time
   * nobody measures.
   *
   * @param phaseTwoRouter The router the rebuilt call is handed to
   * @param payloadStore Where the payload of an entry which names one is read from
   */
  public GruelboxPhaseTwoDispatchBean(
      final PhaseTwoRouter phaseTwoRouter,
      final PhaseTwoPayloadStore payloadStore) {

    this(phaseTwoRouter, payloadStore, VanillaBpMetrics.NONE);

  }

  /**
   * The full form. {@link GruelboxPhaseTwoOutboxAutoConfiguration} builds it again for every entry
   * gruelbox hands over.
   *
   * @param phaseTwoRouter The router the rebuilt call is handed to
   * @param payloadStore Where the payload of an entry which names one is read from
   * @param metrics What the wait of the dispatched entry is reported to
   */
  public GruelboxPhaseTwoDispatchBean(
      final PhaseTwoRouter phaseTwoRouter,
      final PhaseTwoPayloadStore payloadStore,
      final VanillaBpMetrics metrics) {

    this.phaseTwoRouter = phaseTwoRouter;
    this.payloadStore = payloadStore;
    this.metrics = metrics;

  }

  @Override
  public void dispatch(
      final String operation,
      final String workflowModuleId,
      final String bpmnProcessId,
      final String workflowAggregateId,
      final String adapterId,
      final String serializedArgs) {

    final var args = PhaseTwoCall.deserializeArgs(serializedArgs);
    // the one extra read this form costs, and only for an entry which names a payload:
    // a lookup by primary key, once per dispatch attempt
    final var payloadReference = args.get(PhaseTwoCall.ARG_PAYLOAD_REFERENCE);
    final var payload = (payloadReference == null) || (payloadStore == null)
        ? null
        : payloadStore.read(payloadReference);
    final var call = PhaseTwoCall
        .forDispatch(
            operation,
            workflowModuleId,
            bpmnProcessId,
            workflowAggregateId,
            adapterId,
            args,
            payload);
    // the submitter wrapper set this on this thread. A retried entry runs the START
    // re-dispatch mitigation
    final var previouslyAttempted = GruelboxRedispatchAwareSubmitter.isPreviouslyAttempted();

    // the submitter wrapper set this as well. It is null for an entry gruelbox has
    // already picked up once, because gruelbox has no column for the moment an entry
    // was written
    final var writtenAt = GruelboxRedispatchAwareSubmitter.whenTheEntryWasWritten();
    try {
      phaseTwoRouter.dispatch(call, previouslyAttempted);
    } catch (final RuntimeException e) {
      reportWait(writtenAt, DispatchOutcome.FAILED);
      throw e;
    }
    reportWait(writtenAt, DispatchOutcome.SUCCEEDED);

    // the bytes have done their work. This runs in the transaction gruelbox opened
    // around the invocation, the same one in which it marks the entry as processed. So
    // the two end together. If the dispatch threw, nothing is removed: the entry comes
    // again
    if ((payloadReference != null) && (payloadStore != null)) {
      payloadStore.remove(payloadReference);
    }

  }

  /**
   * Reports how long the entry waited for this attempt, counted from the moment it was
   * written.
   *
   * @param writtenAt When the entry was written, or <code>null</code> if gruelbox cannot say.
   *          The attempt is then not measured, instead of being measured from now
   * @param outcome How the attempt ended
   */
  private void reportWait(
      final Instant writtenAt,
      final DispatchOutcome outcome) {

    if (writtenAt == null) {
      return;
    }
    metrics
        .outboxDispatchEnded(
            GruelboxPhaseTwoOutbox.class.getSimpleName(),
            outcome,
            PhaseTwoOutbox
                .waitedSince(writtenAt)
                .toNanos());

  }

}
