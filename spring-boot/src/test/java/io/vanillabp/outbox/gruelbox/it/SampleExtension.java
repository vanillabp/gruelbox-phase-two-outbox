package io.vanillabp.outbox.gruelbox.it;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import io.vanillabp.integration.spi.PhaseOperation;
import io.vanillabp.integration.spi.PhaseOperationRegistry;
import io.vanillabp.integration.spi.PhaseTwoCall;
import io.vanillabp.integration.spi.PhaseTwoPermanentFailure;
import io.vanillabp.integration.spi.PhaseTwoRetryLater;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;

/**
 * Stands for a real VanillaBP extension (e.g. the Business Cockpit) which uses the outbox for its
 * own work after the commit, safe against crashes. It registers an operation of its own in the
 * {@link PhaseOperationRegistry}, and it records the calls dispatched to it.
 * <p>
 * It uses everything an extension needs: an operation name with a namespace, an idempotency key
 * of its own, arguments which travel with the call, and a dispatch which does NOT go through the
 * election of an adapter for the aggregate, as the core operations do.
 */
@RequiredArgsConstructor
public class SampleExtension {

  public static final String OPERATION_NAME = "sample-extension:NOTIFY";

  /**
   * What a dispatch writes into the workflow aggregate if a test asked for it. It stands for
   * everything a handler of an extension writes while it reports. That write has to be committed
   * with the dispatch which carried it, and rolled back with a dispatch which failed.
   */
  public static final String REPORTED_BY_THE_HANDLER = "reported-by-the-handler";

  public static final String ARG_EVENT = "event";

  private final PhaseOperationRegistry registry;

  /**
   * Where the write of a dispatch goes. The handler of a real extension writes through VanillaBP,
   * which runs it inside the dispatch. This double writes the same row in the same place, because
   * these tests read the transaction the write was part of.
   */
  private final AggregateRepository aggregates;

  private final List<PhaseTwoCall> dispatched = new CopyOnWriteArrayList<>();

  private volatile int failNextDispatches;

  /**
   * How many of the next dispatches fail the way an adapter fails an operation which
   * repeating cannot fix.
   */
  private volatile int failNextDispatchesPermanently;

  /**
   * How many of the next dispatches are rejected the way an adapter rejects a workflow which its
   * BPMS has not made searchable yet.
   */
  private volatile int rejectNextDispatches;

  /**
   * The window such a rejection names.
   */
  private volatile Duration rejectionWindow = Duration.ofSeconds(1);

  /**
   * Whether a dispatch which goes through first writes into the workflow aggregate, the way a
   * provider notes down what it reported.
   */
  private volatile boolean writeWhileDispatching;

  /**
   * Counts down when a held dispatch entered the handler, so a test knows that the entry is
   * claimed.
   */
  private volatile java.util.concurrent.CountDownLatch dispatchEntered;

  /**
   * What a held dispatch waits for. A test which plans against an entry which a dispatch has
   * ALREADY taken needs that dispatch to stand still while it plans. There is no other way to be
   * sure that the claim happened.
   */
  private volatile java.util.concurrent.CountDownLatch releaseDispatch;

  /**
   * Whether a dispatch is still to be held. Only the FIRST dispatch after
   * {@link #holdNextDispatch()} waits, and this flag releases the ones which follow.
   */
  private final java.util.concurrent.atomic.AtomicBoolean holdArmed = new java.util.concurrent.atomic.AtomicBoolean();

  private final java.util.concurrent.atomic.AtomicInteger attempts = new java.util.concurrent.atomic.AtomicInteger();

  /**
   * The operation of this extension. It is deduplicated per workflow aggregate AND event. So the
   * same event is published at most once, while different events of the same workflow are all
   * published.
   */
  public static final PhaseOperation OPERATION = PhaseOperation
      .extensionOperation(OPERATION_NAME)
      .idempotencyKey(
          call -> Optional
              .of(
                  "%s|%s|%s|%s".formatted(
                      call.workflowModuleId(),
                      call.bpmnProcessId(),
                      call.workflowAggregateId(),
                      call.args().get(ARG_EVENT))))
      .describedAs(args -> "notifying about event '%s'".formatted(args.get(ARG_EVENT)))
      .build();

  @PostConstruct
  public void registerOperation() {

    registry
        .register(
            OPERATION,
            (
                call,
                previouslyAttempted) -> {
              attempts.incrementAndGet();
              holdWhereATestAskedForIt();
              if (rejectNextDispatches > 0) {
                rejectNextDispatches--;
                throw new PhaseTwoRetryLater(
                    "test rejection: the workflow is not searchable yet", rejectionWindow);
              }
              if (failNextDispatchesPermanently > 0) {
                failNextDispatchesPermanently--;
                throw new PhaseTwoPermanentFailure(
                    "test failure: repeating cannot fix this", new IllegalStateException("the model is broken"));
              }
              if (failNextDispatches > 0) {
                failNextDispatches--;
                throw new RuntimeException("test dispatch failure");
              }
              if (writeWhileDispatching) {
                writeIntoTheAggregate(call.workflowAggregateId());
              }
              dispatched.add(call);
            });

  }

  /**
   * Builds a call of the operation of this extension. The extension would schedule it inside the
   * business transaction.
   *
   * @param workflowModuleId The workflow module ID
   * @param bpmnProcessId The BPMN process ID
   * @param workflowAggregateId The ID of the aggregate in serialized form
   * @param event The event to be published
   * @return The call to be scheduled
   */
  public static PhaseTwoCall call(
      final String workflowModuleId,
      final String bpmnProcessId,
      final String workflowAggregateId,
      final String event) {

    return call(workflowModuleId, bpmnProcessId, workflowAggregateId, event, null);

  }

  /**
   * Builds a call of the operation of this extension which carries the state the extension saw
   * when it planned the call. A sync to the Business Cockpit passes such a state.
   *
   * @param workflowModuleId The workflow module ID
   * @param bpmnProcessId The BPMN process ID
   * @param workflowAggregateId The ID of the aggregate in serialized form
   * @param event The event to be published
   * @param payload The bytes to carry, or <code>null</code>
   * @return The call to be scheduled
   */
  public static PhaseTwoCall call(
      final String workflowModuleId,
      final String bpmnProcessId,
      final String workflowAggregateId,
      final String event,
      final byte[] payload) {

    return PhaseTwoCall
        .of(
            OPERATION, workflowModuleId, bpmnProcessId, workflowAggregateId, null, Map
                .of(ARG_EVENT, event),
            payload);

  }

  /**
   * Writes into the workflow aggregate the way a handler of an extension does while it reports. It
   * runs inside the dispatch. So the row it writes is committed when the entry is ticked off, and
   * it is gone if the dispatch failed.
   *
   * @param workflowAggregateId The ID of the aggregate in serialized form
   */
  private void writeIntoTheAggregate(
      final String workflowAggregateId) {

    aggregates
        .findById(Long.valueOf(workflowAggregateId))
        .ifPresent(aggregate -> {
          aggregate.setReported(REPORTED_BY_THE_HANDLER);
          aggregates.save(aggregate);
        });

  }

  /**
   * How often the dispatch of the operation of this extension was entered, including the attempts
   * which threw. It tells a failed attempt from one which never happened.
   *
   * @return The number of attempts since the last reset
   */
  public int getAttempts() {

    return attempts.get();

  }

  public List<PhaseTwoCall> getDispatched() {

    return dispatched;

  }

  public void reset() {

    dispatched.clear();
    failNextDispatches = 0;
    failNextDispatchesPermanently = 0;
    rejectNextDispatches = 0;
    writeWhileDispatching = false;
    holdArmed.set(false);
    attempts.set(0);

  }

  /**
   * Makes the next dispatch stop inside the handler until {@link #releaseHeldDispatch()} lets it
   * go. The entry is claimed by then, and a test about a claimed entry needs that state.
   */
  public void holdNextDispatch() {

    dispatchEntered = new java.util.concurrent.CountDownLatch(1);
    releaseDispatch = new java.util.concurrent.CountDownLatch(1);
    holdArmed.set(true);

  }

  /**
   * Waits until the held dispatch entered the handler.
   *
   * @param timeoutMillis The maximum time to wait
   * @throws InterruptedException If interrupted while waiting
   */
  public void awaitHeldDispatchEntered(
      final long timeoutMillis) throws InterruptedException {

    if (!dispatchEntered.await(timeoutMillis, java.util.concurrent.TimeUnit.MILLISECONDS)) {
      throw new AssertionError("no dispatch entered the handler");
    }

  }

  /**
   * Lets the held dispatch finish.
   */
  public void releaseHeldDispatch() {

    holdArmed.set(false);
    final var release = releaseDispatch;
    if (release != null) {
      release.countDown();
    }

  }

  /**
   * Stops the dispatch which entered first, if a test asked for it.
   */
  private void holdWhereATestAskedForIt() {

    if (!holdArmed.compareAndSet(true, false)) {
      return;
    }
    final var release = releaseDispatch;
    dispatchEntered.countDown();
    try {
      release.await(30, java.util.concurrent.TimeUnit.SECONDS);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }

  }

  public void failNextDispatches(
      final int count) {

    failNextDispatches = count;

  }

  /**
   * Makes the next dispatches fail with {@link PhaseTwoPermanentFailure}. An extension throws it
   * when waiting longer cannot help, for example for a workflow which still has no user task after
   * ten minutes.
   *
   * @param count The number of dispatches to fail that way
   */
  public void failNextDispatchesPermanently(
      final int count) {

    failNextDispatchesPermanently = count;

  }

  /**
   * Makes the next dispatches be rejected with the window an adapter names while its BPMS has not
   * made the workflow searchable yet. That is the normal case on Camunda 8.
   *
   * @param count The number of dispatches to reject that way
   * @param window The window the rejection names
   */
  public void rejectNextDispatches(
      final int count,
      final Duration window) {

    rejectionWindow = window;
    rejectNextDispatches = count;

  }

  /**
   * Makes a dispatch which goes through write into the workflow aggregate before it records the
   * call, the way a provider notes down what it reported.
   */
  public void writeWhileDispatching() {

    writeWhileDispatching = true;

  }

  /**
   * Waits until the given number of calls was dispatched.
   *
   * @param count The number of calls awaited
   * @param timeoutMillis The maximum time to wait
   * @return The dispatched calls
   * @throws InterruptedException If interrupted while waiting
   */
  public List<PhaseTwoCall> awaitDispatched(
      final int count,
      final long timeoutMillis) throws InterruptedException {

    final var deadline = System.currentTimeMillis() + timeoutMillis;
    while (dispatched.size() < count) {
      if (System.currentTimeMillis() > deadline) {
        throw new AssertionError(
            "Only %d of %d expected extension-operation dispatches happened".formatted(
                dispatched.size(),
                count));
      }
      Thread.sleep(50);
    }
    return List.copyOf(dispatched);

  }

}
