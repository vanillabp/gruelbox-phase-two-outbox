package io.vanillabp.outbox.gruelbox.it;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import io.vanillabp.integration.spi.PhaseOperation;
import io.vanillabp.integration.spi.PhaseOperationRegistry;
import io.vanillabp.integration.spi.PhaseTwoCall;
import io.vanillabp.integration.spi.PhaseTwoRetryLater;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;

/**
 * Stands in for a real VanillaBP extension (e.g. the Business Cockpit) using the
 * outbox for its own crash-safe after-commit work: it registers an operation of its
 * own in the {@link PhaseOperationRegistry} and records the calls dispatched to
 * it.
 * <p>
 * Everything an extension needs is used here: a namespaced operation name, an
 * idempotency key of its own making, arguments travelling with the call and a
 * dispatch which is NOT routed through the aggregate-to-adapter election of the
 * core operations.
 */
@RequiredArgsConstructor
public class SampleExtension {

  public static final String OPERATION_NAME = "sample-extension:NOTIFY";

  /**
   * What a dispatch writes into the workflow aggregate where a test asked it to. It stands
   * for everything a handler of an extension writes while it reports: that write has to be
   * committed with the dispatch which carried it, and rolled back with a dispatch which
   * failed.
   */
  public static final String REPORTED_BY_THE_HANDLER = "reported-by-the-handler";

  public static final String ARG_EVENT = "event";

  private final PhaseOperationRegistry registry;

  /**
   * Where the write of a dispatch goes. The handler of a real extension writes through
   * VanillaBP, which runs it inside the dispatch; this double writes the same row in the
   * same place, because what these tests read is the transaction the write rode in.
   */
  private final AggregateRepository aggregates;

  private final List<PhaseTwoCall> dispatched = new CopyOnWriteArrayList<>();

  private volatile int failNextDispatches;

  /**
   * How many of the next dispatches are rejected the way an adapter rejects a workflow its
   * BPMS has not made searchable yet.
   */
  private volatile int rejectNextDispatches;

  /**
   * The window such a rejection names.
   */
  private volatile Duration rejectionWindow = Duration.ofSeconds(1);

  /**
   * Whether a dispatch which goes through writes into the workflow aggregate first, the way
   * a provider notes down what it reported.
   */
  private volatile boolean writeWhileDispatching;

  /**
   * Counts down when a held dispatch entered the handler, so a test knows the entry is
   * claimed.
   */
  private volatile java.util.concurrent.CountDownLatch dispatchEntered;

  /**
   * What a held dispatch waits for. A test which plans against an entry a dispatch has
   * ALREADY taken needs that dispatch to stand still while it plans, and there is no
   * other way to be sure the claim happened.
   */
  private volatile java.util.concurrent.CountDownLatch releaseDispatch;

  /**
   * Whether a dispatch is still to be held. Only the FIRST dispatch after
   * {@link #holdNextDispatch()} waits, and this is what takes the hold away from the
   * ones which follow.
   */
  private final java.util.concurrent.atomic.AtomicBoolean holdArmed = new java.util.concurrent.atomic.AtomicBoolean();

  private final java.util.concurrent.atomic.AtomicInteger attempts = new java.util.concurrent.atomic.AtomicInteger();

  /**
   * The operation contributed by this extension: deduplicated per workflow
   * aggregate AND event, so the same event is published at most once while
   * different events of the same workflow are all published.
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
   * Builds a call of this extension's operation - what the extension would
   * schedule inside the business transaction.
   *
   * @param workflowModuleId The workflow module ID
   * @param bpmnProcessId The BPMN process ID
   * @param workflowAggregateId The aggregate's ID in serialized form
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
   * Builds a call of this extension's operation which carries the state the extension
   * saw when it planned the call - what a sync to the Business Cockpit passes.
   *
   * @param workflowModuleId The workflow module ID
   * @param bpmnProcessId The BPMN process ID
   * @param workflowAggregateId The aggregate's ID in serialized form
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
   * Writes into the workflow aggregate the way a handler of an extension does while it
   * reports. It runs inside the dispatch, so the row it writes is committed with the entry
   * being ticked off and gone where the dispatch failed.
   *
   * @param workflowAggregateId The aggregate's ID in serialized form
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
   * How often the dispatch of this extension's operation was entered, the attempts which
   * threw included - what tells a failed attempt apart from one which never happened.
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
    rejectNextDispatches = 0;
    writeWhileDispatching = false;
    holdArmed.set(false);
    attempts.set(0);

  }

  /**
   * Makes the next dispatch stop inside the handler until
   * {@link #releaseHeldDispatch()} lets it go. The entry is claimed by then, which is
   * the state a test about a claimed entry needs.
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
   * Stops the dispatch which entered first, where a test asked for it.
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
   * Lets the next dispatches be rejected with the window an adapter names while its BPMS has
   * not made the workflow searchable yet - the ordinary case on Camunda 8.
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
   * Lets a dispatch which goes through write into the workflow aggregate before it records
   * the call, the way a provider notes down what it reported.
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
