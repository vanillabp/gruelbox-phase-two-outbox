package io.vanillabp.outbox.gruelbox;

import java.time.Instant;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import com.gruelbox.transactionoutbox.Submitter;
import com.gruelbox.transactionoutbox.TransactionOutboxEntry;

import lombok.extern.slf4j.Slf4j;

/**
 * Passes the state which gruelbox keeps per entry on to the dispatch bean. Gruelbox calls the
 * scheduled method with the stored arguments only. The {@link TransactionOutboxEntry}, and with it
 * the count of attempts, is not visible there. This {@link Submitter} wrapper is the one hook of
 * gruelbox which gets the entry BEFORE the call. It notes &quot;this entry was dispatched
 * before&quot; in a {@link ThreadLocal}. The dispatch bean of
 * {@link GruelboxPhaseTwoOutboxAutoConfiguration} reads it on the same thread, for the START
 * re-dispatch mitigation of the core.
 * <p>
 * Gruelbox counts an attempt only when it FAILED. That count alone would miss a crash between a
 * successful BPMS call and the commit which ticks the entry off. So the time of the last attempt
 * is read as well. A flush stamps it on every entry it picks up, before the dispatch runs, and only
 * a flush brings back an entry after a crash. So such an entry counts as attempted, too. A failed
 * attempt is seen either way, also across restarts, because both values are stored in the row.
 * Only an entry which gruelbox submits right after its own commit counts as new.
 * <p>
 * <strong>It is also the gate which holds entries back until VanillaBP dispatches
 * them.</strong> Gruelbox submits an entry as soon as the scheduling transaction commits. But
 * Spring Boot answers requests before VanillaBP deployed its models. A workflow started in that
 * window would be carried to a BPMS which does not hold its process yet. When the BPMS says so,
 * no repetition can fix it. So nothing is submitted while the gate is closed. Gruelbox treats that
 * like an executor which refuses the work: the entry stays committed and due, and the first
 * {@link com.gruelbox.transactionoutbox.TransactionOutbox#flush()} carries it. Such an entry waits
 * until the dispatcher starts polling, which happens once per application start. It reaches the
 * dispatch bean as a repetition, because gruelbox stamps the attempt time of every entry a flush
 * picks up.
 * <p>
 * Nothing closes the gate unless something can open it again. Building a
 * {@link GruelboxPhaseTwoOutboxDispatcher} for this submitter closes it, and that dispatcher opens
 * it for good when it starts polling. An outbox built without a dispatcher keeps dispatching right
 * after the commit, as it did before this gate existed. That is a test, or an application which
 * flushes the outbox itself. A gate which nobody opens would hold every entry of such an
 * application forever. See {@code GruelboxHoldsEntriesBackUntilDispatchingStartedTest}.
 */
@Slf4j
public final class GruelboxRedispatchAwareSubmitter implements Submitter {

  private static final ThreadLocal<Boolean> PREVIOUSLY_ATTEMPTED = ThreadLocal.withInitial(() -> Boolean.FALSE);

  /**
   * When the entry dispatched on this thread was written, for the wait the dispatch reports. It
   * travels the same way as the flag above, and for the same reason: gruelbox calls the scheduled
   * method with the stored arguments only.
   */
  private static final ThreadLocal<Instant> WRITTEN_AT = new ThreadLocal<>();

  /**
   * The entries this application dispatches right now. Gruelbox locks the row of an entry it
   * dispatches (<code>SELECT ... FOR UPDATE</code>), and it keeps that lock until the handler
   * returned. So any write to such a row waits for a remote call to come back. Keeping that out of
   * a business transaction is why an outbox exists. So the store asks HERE before it replaces an
   * entry. This is the one hook which knows, because gruelbox hands an entry to its submitter
   * before it calls anything.
   * <p>
   * It answers for this application only. That is enough for the case it is needed in. Gruelbox
   * submits an entry directly on the instance whose transaction committed it. Every OTHER way
   * into a dispatch goes through a flush. A flush counts up the version of the entry, so every
   * instance sees it. See {@code GruelboxPhaseTwoOutbox} for the gap this leaves.
   */
  private static final Set<String> BEING_DISPATCHED = ConcurrentHashMap.newKeySet();

  private final Submitter delegate;

  /**
   * Whether entries are kept for the first flush instead of being submitted. It is read on the
   * thread which committed the scheduling transaction, and written on the thread which starts the
   * dispatcher. So it is volatile.
   */
  private volatile boolean holdingBack;

  /**
   * The wrapper gruelbox is built with. The gate is open until a
   * {@link GruelboxPhaseTwoOutboxDispatcher} is built for this submitter. So an outbox without one
   * dispatches right after the commit.
   *
   * @param delegate What gruelbox would submit with by itself, and what runs the entry once
   *          this wrapper lets it through
   */
  public GruelboxRedispatchAwareSubmitter(
      final Submitter delegate) {

    this.delegate = delegate;

  }

  /**
   * Whether the entry dispatched on this thread had an attempt before. The START re-dispatch
   * mitigation of the core asks for this. Read it on the dispatching thread only, because that is
   * where gruelbox left the answer.
   *
   * @return Whether the entry dispatched on the current thread was attempted
   *         before (a retried entry)
   */
  public static boolean isPreviouslyAttempted() {

    return PREVIOUSLY_ATTEMPTED.get();

  }

  /**
   * When the entry dispatched on the current thread was written, or <code>null</code> if gruelbox
   * cannot say any more.
   * <p>
   * Gruelbox has no column for the moment of writing. It puts that moment into
   * <code>nextAttemptTime</code> when it schedules an entry, and it overwrites it the first time a
   * flush picks the entry up. So only an entry which was submitted right after its transaction
   * committed still carries it. Every other entry answers <code>null</code> here, and its wait is
   * not measured. A store says that, instead of reporting a number it cannot back up.
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
   * Closes the gate. The {@link GruelboxPhaseTwoOutboxDispatcher} built for this submitter calls
   * it. That dispatcher is the one thing which can open the gate again.
   */
  void holdBackUntilDispatchingStarted() {

    holdingBack = true;

  }

  /**
   * Opens the gate for the rest of the life of this application. Shutdown does not close it
   * again. An entry which cannot be submitted while the application goes down is dispatched by the
   * next instance anyway. A gate closed on the way out would only add a second reason for an entry
   * not to move.
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
