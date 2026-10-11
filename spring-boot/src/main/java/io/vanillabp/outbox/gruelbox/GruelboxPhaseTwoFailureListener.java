package io.vanillabp.outbox.gruelbox;

import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Supplier;

import com.gruelbox.transactionoutbox.Persistor;
import com.gruelbox.transactionoutbox.TransactionManager;
import com.gruelbox.transactionoutbox.TransactionOutboxEntry;
import com.gruelbox.transactionoutbox.TransactionOutboxListener;

import io.vanillabp.integration.adapter.migration.config.PhaseTwoOutboxProperties;
import io.vanillabp.integration.adapter.migration.observability.VanillaBpMetrics;
import io.vanillabp.integration.spi.PhaseTwoPermanentFailure;
import io.vanillabp.integration.spi.PhaseTwoRetryLater;
import lombok.extern.slf4j.Slf4j;

/**
 * Makes the gruelbox store keep the four promises which the stores VanillaBP writes itself keep.
 * An entry whose failure the adapter called permanent is blocked after the first attempt. The
 * ERROR which reports a blocked entry says which workflow was lost. An entry which cannot run YET
 * is due when the adapter said it would be. And waiting for that uses time, not attempts.
 * <p>
 * Gruelbox counts attempts, and it blocks an entry once
 * <code>vanillabp.outbox.block-after-attempts</code> of them are used up. It knows nothing about
 * how VanillaBP sorts failures. Repeating a failure which the BPMS answers the same way every time
 * gives a long row of attempts which teach nobody anything. So this listener writes the block
 * itself. Gruelbox calls it AFTER the failed attempt was committed. The entry it hands over
 * carries the version which that commit wrote.
 * {@link Persistor#update(com.gruelbox.transactionoutbox.Transaction, TransactionOutboxEntry)}
 * needs that version to write the blocked flag on top of it.
 * <p>
 * An entry which arrives here already blocked is one which gruelbox gave up on itself. That case
 * is only reported, never blocked again. So an entry ends in exactly one ERROR from VanillaBP,
 * however it got there. Gruelbox writes a line of its own next to it, which names the id of the
 * entry and not the workflow.
 * <p>
 * The due time is written the same way. It answers the one thing gruelbox cannot express. A
 * dispatch may say that asking again soon helps ({@link PhaseTwoRetryLater}, for a workflow the
 * BPMS has not made searchable yet). Without this listener it would wait for
 * <code>vanillabp.outbox.attempt-frequency</code>, thirty seconds by default, while a Camunda 8
 * cluster asked for ten. Gruelbox has just written its own distance onto the row. This listener
 * writes the window of the adapter over it, whether that window is shorter or longer. The adapter
 * knows something about its BPMS which no store knows. So its window means the same thing on
 * every store VanillaBP ships (see decision 4 in the repository's DECISIONS.md). No thread waits
 * for it during the dispatch. That is the whole difference to how this store used to answer this
 * case (see {@link GruelboxPhaseTwoDispatchBean}).
 * <p>
 * Waiting for a BPMS which does not report the workflow yet is no failure of the entry. So it
 * must not use up <code>vanillabp.outbox.block-after-attempts</code>. Gruelbox has already counted
 * the attempt when it calls here, so this listener takes it back, in the same write which sets the
 * window. Time ends a wait which would never end. Once
 * <code>vanillabp.outbox.wait-for-visibility-at-most</code> has passed since the entry was
 * written, the next answer "not yet" blocks the entry. That block keeps the one attempt gruelbox
 * counted. {@link PhaseTwoOutboxProperties#hasWaitedForVisibilityLongEnough} is the rule, the same
 * one the stores VanillaBP writes itself ask. See decision 4 in the repository's DECISIONS.md.
 * <p>
 * Gruelbox has no column for the moment an entry was written. So this listener puts that moment
 * into the session of the stored invocation when the entry is written ({@link #extractSession()}).
 * That map is where gruelbox lets an add-on store things next to a call, so no column is added. An
 * entry which carries no moment was written before this listener wrote one, or by an outbox built
 * without it. Such an entry keeps the old rule: the answer counts as an attempt, and the attempt
 * budget ends the wait. Nothing is lost either way. The wait just ends by the other budget.
 * <p>
 * Gruelbox may block the entry because the attempt it just counted was the last one of the
 * budget. If the wait is not over yet, this listener opens the entry again, because the answer is
 * looked at before the budget, as on the other stores. Gruelbox decided about the block before it
 * called here. So it still writes its own ERROR line "Blocking failing entry", and it calls the
 * <code>blocked</code> method of the listeners after this one. The row says what is true: the
 * entry is open. This only happens when earlier failures used up all attempts but one.
 * <p>
 * An entry which is not a phase-two dispatch is left alone. The outbox bean belongs to VanillaBP,
 * but an application may schedule work of its own on it. Blocking the entry of somebody else is
 * not the business of this listener.
 */
@Slf4j
public class GruelboxPhaseTwoFailureListener implements TransactionOutboxListener {

  /**
   * The method of a phase-two dispatch. It tells an entry of VanillaBP from anything else the
   * outbox carries.
   */
  private static final Method PHASE_TWO_DISPATCH = phaseTwoDispatchMethod();

  /**
   * The name this store reports itself under. Its gauge of waiting entries uses the same name.
   */
  private static final String STORE = GruelboxPhaseTwoOutbox.class.getSimpleName();

  private final Persistor persistor;

  private final TransactionManager transactionManager;

  /**
   * Read for each blocked entry, not injected once. The outbox is built while the application
   * context still starts, and the metrics bean may not exist yet. It may not exist at all, because
   * Micrometer is optional.
   */
  private final Supplier<VanillaBpMetrics> metrics;

  /**
   * The key of the session entry which carries the moment an entry was written. The session of a
   * gruelbox invocation is a map of strings. So the moment is stored in the ISO-8601 form of
   * {@link Instant#toString()}.
   */
  public static final String WRITTEN_AT = "vanillabp.writtenAt";

  /**
   * Where an operator reads how to find a blocked entry of this store, open it again or delete it.
   * It is the section of the README of this repository about the table of gruelbox. Each ERROR
   * which reports a blocked entry ends with this address. The tests which read those lines take it
   * from here.
   */
  public static final String BLOCKED_ENTRIES_GUIDE = "https://github.com/vanillabp/gruelbox-phase-two-outbox#a-blocked-entry-and-the-way-back";

  /**
   * The settings of <code>vanillabp.outbox</code>: the attempt budget, which the line about a
   * repeated entry names, and how long an entry may wait for its BPMS.
   */
  private final PhaseTwoOutboxProperties properties;

  /**
   * The listener of ONE gruelbox outbox. It writes into the table of that outbox, through the
   * transaction manager of that outbox. So an application which runs a second gruelbox instance
   * needs a second listener, not this one.
   *
   * @param persistor The persistor of the outbox this listener belongs to, used to write
   *          the blocked flag
   * @param transactionManager The transaction manager of that outbox, giving the
   *          transaction the write runs in
   * @param metrics What a blocked entry is counted into
   * @param properties The settings of <code>vanillabp.outbox</code>, which give the attempt
   *          budget of this outbox and how long an entry may wait for its BPMS
   */
  public GruelboxPhaseTwoFailureListener(
      final Persistor persistor,
      final TransactionManager transactionManager,
      final Supplier<VanillaBpMetrics> metrics,
      final PhaseTwoOutboxProperties properties) {

    this.persistor = persistor;
    this.transactionManager = transactionManager;
    this.metrics = metrics;
    this.properties = properties;

  }

  /**
   * Stores the moment an entry is written with the entry. Gruelbox asks for this map when it
   * builds a new entry, on the thread which schedules it. So the moment is the moment of the
   * schedule. A younger call which replaces a waiting entry is a new entry. It gets a moment of its
   * own, so its wait starts again.
   * <p>
   * Gruelbox asks this for every entry of the outbox, also for work an application schedules on
   * it. The key belongs to VanillaBP and nothing else reads it. So such an entry carries it along
   * unused.
   *
   * @return The session to store with the new entry
   */
  @Override
  public Map<String, String> extractSession() {

    return Map.of(WRITTEN_AT, Instant.now().toString());

  }

  @Override
  public void failure(
      final TransactionOutboxEntry entry,
      final Throwable cause) {

    if (!isPhaseTwoDispatch(entry)) {
      return;
    }

    final var permanent = PhaseTwoPermanentFailure.isPermanent(cause);
    final var retryAfter = permanent ? null : PhaseTwoRetryLater.retryAfter(cause);
    final var writtenAt = writtenAtOf(entry);

    // looked at before the attempt budget. So an entry which used up its attempts
    // earlier is not blocked by an answer "not yet"
    if ((retryAfter != null) && (writtenAt != null)) {
      waitForTheBpms(entry, cause, retryAfter, writtenAt);
      return;
    }

    if (entry.isBlocked()) {
      // gruelbox used up 'vanillabp.outbox.block-after-attempts' and wrote the block
      // before it called here. All that is left is to say which workflow it was
      reportAttemptsUsedUp(entry, cause, permanent);
      return;
    }

    if (!permanent) {
      dueWhenTheDispatchAskedFor(entry, cause);
      return;
    }

    if (blockNow(
        entry,
        "the adapter said that repeating cannot fix its failure - it is attempted again until '%s' are used up"
            .formatted(PhaseTwoOutboxProperties.BLOCK_AFTER_ATTEMPTS_PROPERTY))) {
      reportPermanentFailure(entry, cause);
    }

  }

  /**
   * Handles the answer "not yet" for an entry which knows when it was written. Either the entry is
   * due again after the window the adapter named, and the attempt gruelbox counted is taken back.
   * Or it is blocked, because it has waited long enough.
   *
   * @param entry The entry whose attempt was answered with "not yet"
   * @param cause What the dispatch was rejected with
   * @param retryAfter The window the adapter named
   * @param writtenAt When the entry was written
   */
  private void waitForTheBpms(
      final TransactionOutboxEntry entry,
      final Throwable cause,
      final Duration retryAfter,
      final Instant writtenAt) {

    final var now = Instant.now();
    if (properties.hasWaitedForVisibilityLongEnough(writtenAt, now)) {
      // the block keeps the attempt gruelbox counted, like every other block. If gruelbox
      // blocked the entry itself, the row says so already
      if (!entry.isBlocked() && !blockNow(
          entry,
          "it waited longer than '%s' for its BPMS - the next answer \"not yet\" tries again"
              .formatted(PhaseTwoOutboxProperties.WAIT_FOR_VISIBILITY_AT_MOST_PROPERTY))) {
        return;
      }
      reportWaitedTooLong(entry, cause, Duration.between(writtenAt, now));
      return;
    }

    final var gruelboxCounted = entry.getAttempts();
    final var gruelboxBlocked = entry.isBlocked();
    final var gruelboxWrote = entry.getNextAttemptTime();
    // truncated the way gruelbox truncates its own distances. So the moment in the row
    // and the moment in the entry are the same, whatever the database stores
    final var askedFor = now
        .plus(retryAfter)
        .truncatedTo(ChronoUnit.MILLIS);
    try {
      entry.setAttempts(Math.max(0, gruelboxCounted - 1));
      entry.setBlocked(false);
      entry.setNextAttemptTime(askedFor);
      transactionManager.inTransactionThrows(transaction -> persistor.update(transaction, entry));
    } catch (final Exception e) {
      entry.setAttempts(gruelboxCounted);
      entry.setBlocked(gruelboxBlocked);
      entry.setNextAttemptTime(gruelboxWrote);
      if (gruelboxBlocked) {
        reportAttemptsUsedUp(entry, cause, false);
        return;
      }
      log.debug(
          "Could not write the due time the dispatch of the outbox entry '{}' asked for - it is "
              + "dispatched again after '{}' instead, and this attempt counts",
          entry.getId(),
          PhaseTwoOutboxProperties.ATTEMPT_FREQUENCY_PROPERTY,
          e);
      return;
    }
    final var call = argumentsOf(entry);
    log.info(
        "Phase two ({}) of BPMN process '{}' of workflow module '{}' for aggregate '{}' cannot run "
            + "yet - the outbox entry '{}' is dispatched again in {}, and it waits until {} at most: {}",
        call[0],
        call[2],
        call[1],
        call[3],
        entry.getId(),
        retryAfter,
        writtenAt.plus(properties.waitForVisibilityAtMost()),
        cause.getMessage());

  }

  /**
   * Reads when an entry was written, from the session {@link #extractSession()} stored with
   * it.
   *
   * @param entry The entry a dispatch failed for
   * @return The moment, or <code>null</code> if the entry carries none, or one which cannot be
   *         read. Such an entry keeps counting the answer "not yet" as an attempt
   */
  static Instant writtenAtOf(
      final TransactionOutboxEntry entry) {

    final var session = entry
        .getInvocation()
        .getSession();
    if (session == null) {
      return null;
    }
    final var writtenAt = session.get(WRITTEN_AT);
    if (writtenAt == null) {
      return null;
    }
    try {
      return Instant.parse(writtenAt);
    } catch (final DateTimeParseException e) {
      return null;
    }

  }

  /**
   * Writes the due time a dispatch asked for, on an entry which does not know when it was written.
   * The attempt gruelbox counted stays counted there. Without that moment, only the attempt budget
   * could end a wait which never ends. Gruelbox has one distance for the whole outbox, and it has
   * just written it onto the row. The window of the dispatch replaces it either way. An adapter
   * which names a window knows when its BPMS can answer. A store configured once for every workflow
   * knows less. So a window longer than the distance of the store delays the entry, and it is not
   * dropped. That way, the same window means the same thing on every store VanillaBP ships.
   *
   * @param entry The entry whose attempt was rejected
   * @param cause What the dispatch was rejected with
   */
  private void dueWhenTheDispatchAskedFor(
      final TransactionOutboxEntry entry,
      final Throwable cause) {

    final var retryAfter = PhaseTwoRetryLater.retryAfter(cause);
    if (retryAfter == null) {
      return;
    }
    // truncated the way gruelbox truncates its own distances. So the moment in the row
    // and the moment in the entry are the same, whatever the database stores
    final var askedFor = Instant
        .now()
        .plus(retryAfter)
        .truncatedTo(ChronoUnit.MILLIS);
    final var gruelboxWrote = entry.getNextAttemptTime();
    try {
      entry.setNextAttemptTime(askedFor);
      transactionManager.inTransactionThrows(transaction -> persistor.update(transaction, entry));
    } catch (final Exception e) {
      entry.setNextAttemptTime(gruelboxWrote);
      log.debug(
          "Could not write the due time the dispatch of the outbox entry '{}' asked for - it is "
              + "dispatched again after '{}' instead",
          entry.getId(),
          PhaseTwoOutboxProperties.ATTEMPT_FREQUENCY_PROPERTY,
          e);
      return;
    }
    final var call = argumentsOf(entry);
    log.info(
        "Phase two ({}) of BPMN process '{}' of workflow module '{}' for aggregate '{}' cannot run "
            + "yet - the outbox entry '{}' is dispatched again in {} ({} of {} attempts used): {}",
        call[0],
        call[2],
        call[1],
        call[3],
        entry.getId(),
        retryAfter,
        entry.getAttempts(),
        properties.getBlockAfterAttempts(),
        cause.getMessage());

  }

  /**
   * Writes the blocked flag of an entry.
   *
   * @param entry The entry to block
   * @param why Why the entry is to be blocked, and what happens if it cannot be. It is for the
   *          line which reports a write that failed
   * @return Whether the entry was blocked. A write may not find the entry at the version it was
   *         handed over with. Then somebody else changed the same row first, and that other
   *         writer decides what the entry is
   */
  private boolean blockNow(
      final TransactionOutboxEntry entry,
      final String why) {

    try {
      entry.setBlocked(true);
      transactionManager.inTransactionThrows(transaction -> persistor.update(transaction, entry));
      return true;
    } catch (final Exception e) {
      entry.setBlocked(false);
      log.warn(
          "Could not block the outbox entry '{}' although {}",
          entry.getId(),
          why,
          e);
      return false;
    }

  }

  /**
   * Says which workflow was lost when this listener blocked the entry, in the words which the
   * stores VanillaBP writes itself use.
   *
   * @param entry The blocked entry
   * @param cause What the dispatch failed with
   */
  private void reportPermanentFailure(
      final TransactionOutboxEntry entry,
      final Throwable cause) {

    final var call = argumentsOf(entry);
    count(call, true);
    log.error(
        "Dispatching phase two ({}) of BPMN process '{}' of workflow module '{}' for aggregate '{}' "
            + "on adapter '{}' failed for a reason repeating cannot fix - the outbox entry '{}' is "
            + "blocked. How to find it, open it again or delete it: "
            + BLOCKED_ENTRIES_GUIDE,
        call[0],
        call[2],
        call[1],
        call[3],
        adapterOf(call),
        entry.getId(),
        cause);

  }

  /**
   * Says which workflow was lost when an entry waited too long for its BPMS, in the words which
   * the stores VanillaBP writes itself use.
   *
   * @param entry The blocked entry
   * @param cause The last answer "not yet"
   * @param waited How long the entry waited since it was written
   */
  private void reportWaitedTooLong(
      final TransactionOutboxEntry entry,
      final Throwable cause,
      final Duration waited) {

    final var call = argumentsOf(entry);
    count(call, false);
    log.error(
        "Phase two ({}) of BPMN process '{}' of workflow module '{}' for aggregate '{}' waited {} "
            + "for its BPMS to report the workflow, which is longer than '{}' allows - the outbox "
            + "entry '{}' is now blocked. How to find it, open it again or delete it: "
            + BLOCKED_ENTRIES_GUIDE
            + " - the last answer was: {}",
        call[0],
        call[2],
        call[1],
        call[3],
        waited,
        PhaseTwoOutboxProperties.WAIT_FOR_VISIBILITY_AT_MOST_PROPERTY,
        entry.getId(),
        cause.getMessage());

  }

  /**
   * Says which workflow was lost when gruelbox gave up on the entry itself.
   *
   * @param entry The blocked entry
   * @param cause What the last attempt failed with
   * @param permanent Whether that last failure was one the adapter calls permanent
   */
  private void reportAttemptsUsedUp(
      final TransactionOutboxEntry entry,
      final Throwable cause,
      final boolean permanent) {

    final var call = argumentsOf(entry);
    count(call, permanent);
    log.error(
        "Dispatching phase two ({}) of BPMN process '{}' of workflow module '{}' for aggregate '{}' "
            + "on adapter '{}' failed {} times - the outbox entry '{}' is now blocked. How to find it, "
            + "open it again or delete it: "
            + BLOCKED_ENTRIES_GUIDE,
        call[0],
        call[2],
        call[1],
        call[3],
        adapterOf(call),
        entry.getAttempts(),
        entry.getId(),
        cause);

  }

  /**
   * @param call The arguments of the blocked dispatch
   * @param permanent Whether the adapter said that repeating cannot help
   */
  private void count(
      final Object[] call,
      final boolean permanent) {

    metrics
        .get()
        .outboxEntryBlocked(STORE, String.valueOf(call[0]), permanent);

  }

  /**
   * @param call The arguments of the blocked dispatch
   * @return The adapter the operation was meant for. An operation which was scheduled before any
   *         adapter was elected carries none. In a log line, "none" reads better than the word
   *         null
   */
  private static String adapterOf(
      final Object[] call) {

    return (call[4] == null)
        ? "none"
        : String.valueOf(call[4]);

  }

  /**
   * @param entry The entry a dispatch failed for
   * @return The arguments the dispatch was scheduled with, in the order
   *         {@link GruelboxPhaseTwoDispatch#dispatch} declares them
   */
  private static Object[] argumentsOf(
      final TransactionOutboxEntry entry) {

    return entry
        .getInvocation()
        .getArgs();

  }

  /**
   * @param entry The entry a dispatch failed for
   * @return Whether the entry carries a phase-two dispatch of VanillaBP
   */
  private static boolean isPhaseTwoDispatch(
      final TransactionOutboxEntry entry) {

    final var invocation = entry.getInvocation();
    return (invocation != null) && PHASE_TWO_DISPATCH.getName().equals(invocation.getMethodName()) && Arrays
        .equals(PHASE_TWO_DISPATCH.getParameterTypes(), invocation.getParameterTypes());

  }

  private static Method phaseTwoDispatchMethod() {

    try {
      return GruelboxPhaseTwoDispatch.class.getMethod(
          "dispatch",
          String.class,
          String.class,
          String.class,
          String.class,
          String.class,
          String.class);
    } catch (final NoSuchMethodException e) {
      throw new IllegalStateException(
          "The phase-two dispatch of the gruelbox outbox changed its signature, so a failed entry "
              + "can no longer be recognised as VanillaBP's!", e);
    }

  }

}
