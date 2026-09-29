package io.vanillabp.outbox.gruelbox.it;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import io.vanillabp.bpmsdouble.DummyPhaseTwoListener;

/**
 * Records phase-two invocations of the dummy adapter and optionally fails a
 * configurable number of dispatches (to test retry behavior of the outbox).
 */
public class RecordingPhaseTwoListener implements DummyPhaseTwoListener {

  private final List<Object> invocations = new CopyOnWriteArrayList<>();

  private final AtomicInteger failuresRemaining = new AtomicInteger(0);

  /**
   * Whether the failures this listener raises are the kind the dummy adapter reports as
   * permanent.
   */
  private volatile boolean failPermanently = false;

  @Override
  public void startedWorkflowPhaseTwo(
      final Object workflowAggregateId) {

    invocations.add(workflowAggregateId);
    if (failuresRemaining.getAndUpdate(remaining -> remaining > 0 ? remaining - 1 : 0) > 0) {
      if (failPermanently) {
        throw new io.vanillabp.bpmsdouble.DummyPermanentFailure(
            "phase two failed permanently for testing purposes");
      }
      throw new RuntimeException("phase two failed for testing purposes");
    }

  }

  /**
   * Recorded phase-two task completions as "aggregateId:taskId".
   */
  private final List<String> completedTasks = new CopyOnWriteArrayList<>();

  /**
   * Recorded phase-two task cancellations as "aggregateId:taskId:errorCode".
   */
  private final List<String> canceledTasks = new CopyOnWriteArrayList<>();

  @Override
  public void completedTaskPhaseTwo(
      final Object workflowAggregateId,
      final String taskId) {

    completedTasks.add(workflowAggregateId
        + ":"
        + taskId);
    if (failuresRemaining.getAndUpdate(remaining -> remaining > 0 ? remaining - 1 : 0) > 0) {
      throw new RuntimeException("phase two failed for testing purposes");
    }

  }

  @Override
  public void canceledTaskPhaseTwo(
      final Object workflowAggregateId,
      final String taskId,
      final String bpmnErrorCode) {

    canceledTasks.add(workflowAggregateId
        + ":"
        + taskId
        + ":"
        + bpmnErrorCode);

  }

  private final List<String> completedUserTasks = new CopyOnWriteArrayList<>();

  private final List<String> canceledUserTasks = new CopyOnWriteArrayList<>();

  @Override
  public void completedUserTaskPhaseTwo(
      final Object workflowAggregateId,
      final String taskId) {

    completedUserTasks.add(workflowAggregateId
        + ":"
        + taskId);

  }

  @Override
  public void canceledUserTaskPhaseTwo(
      final Object workflowAggregateId,
      final String taskId,
      final String bpmnErrorCode) {

    canceledUserTasks.add(workflowAggregateId
        + ":"
        + taskId
        + ":"
        + bpmnErrorCode);

  }

  public List<String> getCompletedUserTasks() {

    return List.copyOf(completedUserTasks);

  }

  public List<String> getCanceledUserTasks() {

    return List.copyOf(canceledUserTasks);

  }

  private final List<String> correlatedMessages = new CopyOnWriteArrayList<>();

  private final List<String> startedByMessage = new CopyOnWriteArrayList<>();

  @Override
  public void correlatedMessagePhaseTwo(
      final Object workflowAggregateId,
      final String messageName,
      final String correlationId) {

    correlatedMessages.add(workflowAggregateId
        + ":"
        + messageName
        + ":"
        + correlationId);

  }

  @Override
  public void startedWorkflowByMessagePhaseTwo(
      final Object workflowAggregateId,
      final String messageName) {

    startedByMessage.add(workflowAggregateId
        + ":"
        + messageName);

  }

  public List<String> getCorrelatedMessages() {

    return List.copyOf(correlatedMessages);

  }

  public List<String> getStartedByMessage() {

    return List.copyOf(startedByMessage);

  }

  public List<String> getCompletedTasks() {

    return List.copyOf(completedTasks);

  }

  public List<String> getCanceledTasks() {

    return List.copyOf(canceledTasks);

  }

  /**
   * Waits until at least the given number of phase-two task completions were
   * recorded.
   * <p>
   * Same caveat as {@link #awaitInvocations}: the entry is not done yet when this
   * returns.
   *
   * @param numberOfCompletions The number of completions to wait for
   * @param timeoutMillis How long to wait at most
   * @return All completions recorded so far
   */
  public List<String> awaitCompletedTasks(
      final int numberOfCompletions,
      final long timeoutMillis) throws InterruptedException {

    final var deadline = System.currentTimeMillis() + timeoutMillis;
    while (completedTasks.size() < numberOfCompletions) {
      if (System.currentTimeMillis() > deadline) {
        throw new AssertionError(
            "Expected at least %d phase-two task completion(s) within %dms but got %d!"
                .formatted(numberOfCompletions, timeoutMillis, completedTasks.size()));
      }
      Thread.sleep(50);
    }
    return getCompletedTasks();

  }

  /**
   * Waits until at least the given number of phase-two task cancellations were
   * recorded.
   * <p>
   * Same caveat as {@link #awaitInvocations}: the entry is not done yet when this
   * returns.
   *
   * @param numberOfCancellations The number of cancellations to wait for
   * @param timeoutMillis How long to wait at most
   * @return All cancellations recorded so far
   */
  public List<String> awaitCanceledTasks(
      final int numberOfCancellations,
      final long timeoutMillis) throws InterruptedException {

    final var deadline = System.currentTimeMillis() + timeoutMillis;
    while (canceledTasks.size() < numberOfCancellations) {
      if (System.currentTimeMillis() > deadline) {
        throw new AssertionError(
            "Expected at least %d phase-two task cancellation(s) within %dms but got %d!"
                .formatted(numberOfCancellations, timeoutMillis, canceledTasks.size()));
      }
      Thread.sleep(50);
    }
    return getCanceledTasks();

  }

  /**
   * Lets the next dispatches fail with a failure the adapter reports as PERMANENT - the
   * store has to block such an entry right away.
   *
   * @param numberOfFailures How many dispatches fail
   */
  public void failNextDispatchesPermanently(
      final int numberOfFailures) {

    failPermanently = true;
    failNextDispatches(numberOfFailures);

  }

  public void failNextDispatches(
      final int numberOfFailures) {

    failuresRemaining.set(numberOfFailures);

  }

  public List<Object> getInvocations() {

    return List.copyOf(invocations);

  }

  public void reset() {

    invocations.clear();
    completedTasks.clear();
    canceledTasks.clear();
    completedUserTasks.clear();
    canceledUserTasks.clear();
    correlatedMessages.clear();
    startedByMessage.clear();
    failuresRemaining.set(0);
    failPermanently = false;

  }

  /**
   * Waits until at least the given number of phase-two invocations were recorded.
   * <p>
   * This says that the ADAPTER was called, not that the operation is over: the listener
   * runs inside the dispatch, and the dispatcher marks the outbox entry DONE - which is
   * what frees its idempotency key - only after the dispatch returned. On return the
   * entry may still be waiting to be marked, so a repetition of the same operation
   * planned right here can be discarded as a duplicate. A test which needs the key to
   * be free has to wait for the ENTRY, in the store.
   *
   * @param numberOfInvocations The number of invocations to wait for
   * @param timeoutMillis How long to wait at most
   * @return All invocations recorded so far
   * @throws AssertionError If the invocations did not happen within the timeout
   */
  public List<Object> awaitInvocations(
      final int numberOfInvocations,
      final long timeoutMillis) throws InterruptedException {

    final var deadline = System.currentTimeMillis() + timeoutMillis;
    while (invocations.size() < numberOfInvocations) {
      if (System.currentTimeMillis() > deadline) {
        throw new AssertionError(
            "Expected at least %d phase-two invocation(s) within %dms but got %d!"
                .formatted(numberOfInvocations, timeoutMillis, invocations.size()));
      }
      Thread.sleep(50);
    }
    return getInvocations();

  }

}
