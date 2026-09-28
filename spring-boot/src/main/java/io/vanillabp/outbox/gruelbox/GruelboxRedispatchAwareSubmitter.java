package io.vanillabp.outbox.gruelbox;

import java.time.Instant;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import com.gruelbox.transactionoutbox.Submitter;
import com.gruelbox.transactionoutbox.TransactionOutboxEntry;

import lombok.extern.slf4j.Slf4j;

/**
 * Bridges gruelbox's per-entry state to the dispatch bean: gruelbox invokes the
 * scheduled method with the persisted arguments only - the
 * {@link TransactionOutboxEntry} (and with it the attempts count) is not visible
 * at the invocation. This {@link Submitter} wrapper is the one gruelbox hook
 * carrying the entry BEFORE its invocation: it records &quot;this entry was
 * dispatched before&quot; in a {@link ThreadLocal} read by
 * {@link GruelboxPhaseTwoOutboxAutoConfiguration}'s dispatch bean on the same
 * thread, feeding the START re-dispatch mitigation of the core.
 * <p>
 * Known limitation (accepted): gruelbox increments the attempts count only when
 * an attempt FAILS - after a hard crash between a successful BPMS call and
 * committing the processed flag, the recovered entry still carries
 * <code>attempts == 0</code> and is re-dispatched without the mitigation probe
 * (the documented at-least-once residual). Failure-retries - including the
 * classic &quot;BPMS call succeeded but recording the completion failed&quot; -
 * are detected, also across restarts (the count is persisted).
 * <p>
 * <strong>It is also the gate holding entries back until VanillaBP dispatches
 * them.</strong> Gruelbox submits an entry as soon as the scheduling transaction
 * commits, while Spring Boot answers requests before VanillaBP deployed its
 * models. A workflow started in that window would be carried to a BPMS which does
 * not hold its process yet, and the BPMS saying so is a failure no repetition can
 * fix. So nothing is submitted while the gate is closed, which gruelbox treats
 * like an executor refusing the work: the entry stays committed and due, and the
 * first {@link com.gruelbox.transactionoutbox.TransactionOutbox#flush()} carries
 * it. Such an entry waits for the moment the dispatcher starts polling, which is
 * once per application start, and it reaches the dispatch bean as a repetition,
 * because gruelbox stamps the attempt time of every entry a flush picks up.
 * <p>
 * Nothing closes the gate unless something can open it again. Building a
 * {@link GruelboxPhaseTwoOutboxDispatcher} for this submitter closes it, and that
 * dispatcher opens it for good when it starts polling. An outbox built without a
 * dispatcher (a test, or an application flushing the outbox itself) keeps
 * dispatching right after the commit, as it did before this gate existed: a gate
 * nobody opens would hold every entry of such an application forever. See
 * {@code GruelboxHoldsEntriesBackUntilDispatchingStartedTest}.
 */
@Slf4j
public final class GruelboxRedispatchAwareSubmitter implements Submitter {

  private static final ThreadLocal<Boolean> PREVIOUSLY_ATTEMPTED = ThreadLocal.withInitial(() -> Boolean.FALSE);

  /**
   * When the entry dispatched on this thread was written, for the wait the dispatch
   * reports. It travels the same way the flag above does, and for the same reason:
   * gruelbox invokes the scheduled method with the persisted arguments only.
   */
  private static final ThreadLocal<Instant> WRITTEN_AT = new ThreadLocal<>();

  /**
   * The entries this application is dispatching right now. gruelbox locks the row of
   * an entry it dispatches (<code>SELECT ... FOR UPDATE</code>) and keeps that lock
   * until the handler returned, so any write to such a row waits for a remote call to
   * come back - which is what an outbox exists to keep out of a business transaction.
   * The store therefore asks HERE before it replaces an entry, and this is the one
   * hook which knows: gruelbox hands an entry to its submitter before it invokes
   * anything.
   * <p>
   * It answers for this application only, and that is enough for the case it is
   * needed in: gruelbox submits an entry directly on the instance whose transaction
   * committed it, and every OTHER way into a dispatch goes through a flush, which
   * counts the entry's version up and is therefore visible to every instance. See
   * {@code GruelboxPhaseTwoOutbox} for the residual this leaves.
   */
  private static final Set<String> BEING_DISPATCHED = ConcurrentHashMap.newKeySet();

  private final Submitter delegate;

  /**
   * Whether entries are kept for the first flush instead of being submitted. Read on
   * the thread which committed the scheduling transaction and written on the thread
   * starting the dispatcher, so it is volatile.
   */
  private volatile boolean holdingBack;

  /**
   * The wrapper gruelbox is built with. The gate is open until a
   * {@link GruelboxPhaseTwoOutboxDispatcher} is built for this submitter, so an outbox
   * without one dispatches right after the commit.
   *
   * @param delegate What gruelbox would submit with by itself, and what runs the entry once
   *          this wrapper lets it through
   */
  public GruelboxRedispatchAwareSubmitter(
      final Submitter delegate) {

    this.delegate = delegate;

  }

  /**
   * Whether the entry being dispatched on this thread had an attempt before, which is what
   * the core's START re-dispatch mitigation asks for. Read on the dispatching thread only,
   * because that is where gruelbox left the answer.
   *
   * @return Whether the entry dispatched on the current thread was attempted
   *         before (a retried entry)
   */
  public static boolean isPreviouslyAttempted() {

    return PREVIOUSLY_ATTEMPTED.get();

  }

  /**
   * When the entry dispatched on the current thread was written, or <code>null</code>
   * where gruelbox cannot say any more.
   * <p>
   * gruelbox has no column for the moment of writing. It puts that moment into
   * <code>nextAttemptTime</code> when it schedules an entry and overwrites it the first
   * time a flush picks the entry up, so only an entry which was submitted right after
   * its transaction committed still carries it. Every other entry answers
   * <code>null</code> here and its wait stays unmeasured, which is what a store says
   * instead of reporting a number it cannot back.
   *
   * @return The moment or <code>null</code>
   */
  public static Instant whenTheEntryWasWritten() {

    return WRITTEN_AT.get();

  }

  /**
   * Whether this application is dispatching the entry of that id right now.
   *
   * @param entryId The id of the entry, as gruelbox stores it
   * @return Whether a dispatch holds it
   */
  public static boolean isBeingDispatched(
      final String entryId) {

    return BEING_DISPATCHED.contains(entryId);

  }

  /**
   * Closes the gate. Called by the {@link GruelboxPhaseTwoOutboxDispatcher} built for
   * this submitter, which is the one thing able to open it again.
   */
  void holdBackUntilDispatchingStarted() {

    holdingBack = true;

  }

  /**
   * Opens the gate, for the rest of this application's life. Shutdown does not close
   * it again: an entry which cannot be submitted while the application goes down is
   * dispatched by the next instance anyway, and a gate closed on the way out would
   * only add a second reason for an entry not to move.
   */
  void dispatchingStarted() {

    holdingBack = false;

  }

  @Override
  public void submit(
      final TransactionOutboxEntry entry,
      final Consumer<TransactionOutboxEntry> localExecutor) {

    if (holdingBack) {
      log.debug(
          "Keeping {} for the first poll: VanillaBP has not started dispatching yet",
          entry.description());
      return;
    }

    delegate.submit(
        entry,
        entryOnWorkerThread -> {
          final var attemptedBefore = (entryOnWorkerThread.getAttempts() > 0) || (entryOnWorkerThread
              .getLastAttemptTime() != null);
          PREVIOUSLY_ATTEMPTED.set(attemptedBefore);
          WRITTEN_AT.set(
              attemptedBefore
                  ? null
                  : entryOnWorkerThread.getNextAttemptTime());
          BEING_DISPATCHED.add(entryOnWorkerThread.getId());
          try {
            localExecutor.accept(entryOnWorkerThread);
          } finally {
            BEING_DISPATCHED.remove(entryOnWorkerThread.getId());
            PREVIOUSLY_ATTEMPTED.remove();
            WRITTEN_AT.remove();
          }
        });

  }

}
