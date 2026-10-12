package io.vanillabp.outbox.gruelbox;

import java.time.Instant;
import java.util.OptionalLong;
import java.util.function.Supplier;

import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;

import com.gruelbox.transactionoutbox.TransactionOutbox;

import io.vanillabp.integration.adapter.migration.config.PhaseTwoOutboxProperties;
import io.vanillabp.integration.adapter.migration.observability.VanillaBpMetrics;
import io.vanillabp.integration.adapter.migration.outbox.DueEntryPoller;
import io.vanillabp.integration.adapter.migration.outbox.JdbcHousekeepingLease;
import io.vanillabp.integration.adapter.migration.outbox.OutboxHousekeeping;
import io.vanillabp.integration.deployment.SpringBootDeploymentService;
import io.vanillabp.integration.spi.PhaseTwoPayloadStore;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;

/**
 * Processes the gruelbox transaction outbox in the background. Right after a commit, gruelbox
 * dispatches the scheduled call itself. But recovery after a crash and retries need a poller which
 * calls {@link TransactionOutbox#flush()}. A flush also deletes dispatched entries whose retention
 * has passed. That is the cleanup, later and in the background, which the contract "DONE instead
 * of delete" asks for. The poller starts on {@link ApplicationReadyEvent}. Its first run also
 * dispatches entries left over from an instance which crashed before.
 * <p>
 * It does not flush on a fixed rhythm. A flush costs three database commands, whether anything
 * waits or not. So between two flushes the poller sleeps until the moment when the table of
 * gruelbox says the next entry is due ({@link GruelboxPhaseTwoOutbox#earliestDueAt()}). It sleeps
 * at most <code>vanillabp.outbox.poll-interval</code>, for the one case which the table cannot
 * show: work which another node wrote down before it died (see {@link DueEntryPoller}).
 * <p>
 * Starting the poller is also what lets an outbox built with the
 * {@link GruelboxRedispatchAwareSubmitter} of VanillaBP dispatch after a commit at all. That
 * submitter keeps its entries until this dispatcher runs. Spring Boot answers requests while the
 * models are still on their way to the BPMS. A workflow started then waits for the first poll. It
 * does not reach a BPMS which cannot know it yet.
 * <p>
 * The poller runs on a private daemon executor with one thread. No
 * {@link org.springframework.scheduling.TaskScheduler} bean is registered or used. So the
 * scheduling setup of the application (e.g. <code>&#64;EnableScheduling</code>) stays as it is.
 * <p>
 * <strong>How this store holds an entry while it dispatches it.</strong> The stores which
 * VanillaBP owns lease an entry, and they renew the lease while the dispatch runs
 * ({@link io.vanillabp.integration.adapter.migration.outbox.DispatchLease}). So a slow dispatch
 * never runs twice, and it costs no attempt. Gruelbox does this differently and needs nothing
 * added. Its flush selects with <code>FOR UPDATE SKIP LOCKED</code>, and then it locks the row it
 * is about to dispatch. So the flush of another node skips an entry which somebody dispatches,
 * however long that takes. The price is the one which the stores of VanillaBP avoid. The lock is
 * a database transaction which is held for the whole dispatch, and so is a connection. That is the
 * design of gruelbox, not a choice of this dispatcher.
 * <p>
 * It also decides what the row says about an attempt. A flush which takes an entry stamps the time
 * of the last attempt and moves the due time on by <code>attempt-frequency</code>, before the
 * dispatch runs. So an entry taken by a flush looks attempted while it is still on its way. The
 * count of attempts goes up only when a dispatch failed. The table belongs to gruelbox, so this
 * is written down and not changed.
 * <p>
 * A dispatch which knows that a repetition will help soon says so
 * ({@link io.vanillabp.integration.spi.PhaseTwoRetryLater}, for a workflow which its BPMS has not
 * made searchable yet). Gruelbox would plan the next attempt from the
 * <code>attemptFrequency</code> of the whole outbox. {@link GruelboxPhaseTwoFailureListener}
 * overwrites that with the window the dispatch named. So such an entry is due here at the same
 * time as on the stores VanillaBP writes itself. It is dispatched at the first poll after that
 * moment. The poller sleeps until the earliest due moment or the configured cap, whichever comes
 * first ({@link DueEntryPoller}). So this store adds at most
 * <code>vanillabp.outbox.poll-interval</code> to the window. No thread waits for it, so the
 * entries of every other workflow are dispatched while that one waits.
 */
@Slf4j
public class GruelboxPhaseTwoOutboxDispatcher implements OutboxHousekeeping.Store {

  private final TransactionOutbox transactionOutbox;

  /**
   * The store this dispatcher polls. It answers when the next flush has something to do. It is
   * never <code>null</code>, because every poll asks that question.
   */
  private final GruelboxPhaseTwoOutbox outbox;

  /**
   * The submitter whose gate is opened when polling starts. It is <code>null</code> for an outbox
   * built with a submitter from somebody else.
   */
  private final GruelboxRedispatchAwareSubmitter submitter;

  private final DueEntryPoller poller;

  /**
   * Where the payloads of the entries a flush finished are removed. It is never
   * <code>null</code>. A dispatcher is built with the place it cleans up, the way the store is
   * built with the place it writes to.
   */
  private final PhaseTwoPayloadStore payloadStore;

  /**
   * Where this node says that it cleans up this store tonight. So no other node measures its work
   * at the same time.
   */
  private final JdbcHousekeepingLease housekeepingLease;

  /**
   * What removes the orphaned payloads, and when. The dispatched entries are the business of
   * gruelbox, see {@link #removeDispatchedEntriesOlderThan(Instant, int)}.
   */
  private final OutboxHousekeeping housekeeping;

  /**
   * Polls the outbox, holds its submitter back until it does, and cleans up the payloads which
   * nobody removed. Building this dispatcher closes the gate of the submitter. So a submitter never
   * waits for a dispatcher which does not exist (see {@link GruelboxRedispatchAwareSubmitter}).
   *
   * @param transactionOutbox The outbox to poll
   * @param properties The bound <code>vanillabp.outbox</code> section
   * @param submitter The submitter the outbox was built with, <code>null</code> if gruelbox
   *          was built with a submitter from somebody else
   * @param outbox The store, asked when the next flush has something to do
   * @param payloadStore Where the payloads of this outbox lie
   * @param housekeepingLease Where this node says that it cleans up this store tonight
   * @param metrics What the numbers of a closed housekeeping window are published to
   * @throws IllegalArgumentException If the store or the payload store is missing
   */
  public GruelboxPhaseTwoOutboxDispatcher(
      final TransactionOutbox transactionOutbox,
      final PhaseTwoOutboxProperties properties,
      final GruelboxRedispatchAwareSubmitter submitter,
      final GruelboxPhaseTwoOutbox outbox,
      final PhaseTwoPayloadStore payloadStore,
      final JdbcHousekeepingLease housekeepingLease,
      final Supplier<VanillaBpMetrics> metrics) {

    requireOutbox(outbox);
    requirePayloadStore(payloadStore);
    this.transactionOutbox = transactionOutbox;
    this.submitter = submitter;
    this.outbox = outbox;
    this.payloadStore = payloadStore;
    this.housekeepingLease = housekeepingLease;
    this.housekeeping = new OutboxHousekeeping(this, properties, metrics);
    this.poller = new DueEntryPoller(
        "vanillabp-outbox", properties.getPollInterval(), this::flush, this::earliestDueAt);
    if (submitter != null) {
      submitter.holdBackUntilDispatchingStarted();
    }

  }

  /**
   * Refuses a dispatcher which cannot read the store it polls.
   *
   * @param outbox The store this dispatcher polls
   */
  private static void requireOutbox(
      final GruelboxPhaseTwoOutbox outbox) {

    if (outbox != null) {
      return;
    }
    throw new IllegalArgumentException(
        """
            This gruelbox phase-two outbox dispatcher was built without the store it polls! The \
            store says when the next flush has something to do, so the poller sleeps until then \
            instead of asking at the configured cap, and it says how many dispatched entries \
            gruelbox has not deleted yet, which is the number the housekeeping publishes when its \
            window closes. Pass a GruelboxPhaseTwoOutbox to the constructor of this class, or let \
            VanillaBP's GruelboxPhaseTwoOutboxAutoConfiguration build the dispatcher.""");

  }

  /**
   * Refuses a dispatcher which has no payloads to clean up.
   * <p>
   * The store next to this class is built with the payload store as well, for the same reason. A
   * check which waits for the right call reports a setup problem while the application works. The
   * dispatcher is built at a point where the answer is already known.
   *
   * @param payloadStore Where the payloads of this outbox lie
   */
  private static void requirePayloadStore(
      final PhaseTwoPayloadStore payloadStore) {

    if (payloadStore != null) {
      return;
    }
    throw new IllegalArgumentException(
        """
            This gruelbox phase-two outbox dispatcher was built without a payload store! Every \
            flush removes the bytes of the entries it finished, and with them what a crash \
            between the two writes of a schedule left behind. A dispatcher without that store \
            house-keeps nothing, so the payload table grows for as long as the application runs \
            and no error says so. Pass a PhaseTwoPayloadStore to the constructor of this class, \
            or let VanillaBP's GruelboxPhaseTwoOutboxAutoConfiguration build the dispatcher.""");

  }

  /**
   * When the next flush has something to do.
   *
   * @return The moment of the earliest entry gruelbox still owes something to
   */
  private Instant earliestDueAt() {

    return outbox.earliestDueAt();

  }

  /**
   * Starts the poller. The first run starts at once. It dispatches the entries which an instance
   * committed but did not process before it crashed, and those the submitter kept while the
   * application started. The order of the listeners makes sure that workflow processing started
   * BEFORE any recovered entry is dispatched (see
   * {@link SpringBootDeploymentService#OUTBOX_DISPATCHER_LISTENER_ORDER}).
   */
  @Order(SpringBootDeploymentService.OUTBOX_DISPATCHER_LISTENER_ORDER)
  @EventListener(ApplicationReadyEvent.class)
  public void startPolling() {

    // opened before the poller starts, because a flush hands what it picked up to
    // this same submitter. A gate which is still closed would keep those entries, and
    // each of them would be due again only after 'attempt-frequency', not at once
    if (submitter != null) {
      submitter.dispatchingStarted();
    }
    poller.start();
    housekeeping.start();

  }

  /**
   * Stops the poller when the application context closes. An entry which was not dispatched stays
   * in the table of gruelbox, and the first flush of the next start carries it.
   */
  @PreDestroy
  public void stopPolling() {

    poller.stop();
    housekeeping.stop();

  }

  /**
   * Flushes the outbox until there is no more work. Exceptions are caught, so the poller stays
   * alive.
   */
  private void flush() {

    try {
      //noinspection StatementWithEmptyBody
      while (transactionOutbox.flush()) {
        // empty: the condition does the work, until no due entry is left
      }
    } catch (Exception e) {
      log.error("Flushing the VanillaBP phase-two outbox failed - will retry", e);
    }
  }

  @Override
  public String storeName() {

    return getClass().getSimpleName();

  }

  @Override
  public boolean claimHousekeepingUntil(
      final String owner,
      final Instant until) {

    return housekeepingLease.claimUntil(outbox.getTableName(), owner, until);

  }

  @Override
  public void releaseHousekeeping(
      final String owner) {

    housekeepingLease.release(outbox.getTableName(), owner);

  }

  /**
   * {@inheritDoc}
   * <p>
   * Nothing, because gruelbox deletes its own dispatched entries. A flush of that library removes
   * what its retention lets go, and the retention is <code>vanillabp.outbox.retention</code>. The
   * table belongs to gruelbox, so this is written down and not changed. On this store, the window
   * only decides about the payloads. The entries leave with the flushes, as they always did.
   */
  @Override
  public int removeDispatchedEntriesOlderThan(
      final Instant threshold,
      final int maxRows) {

    return 0;

  }

  @Override
  public int removeOrphanedPayloadsOlderThan(
      final Instant threshold,
      final int maxRows) {

    return payloadStore.removeOrphansOlderThan(threshold, maxRows);

  }

  /**
   * {@inheritDoc}
   * <p>
   * What gruelbox has not deleted yet. This node removes none of them itself. So the number says
   * how far the flushes of the library got, not how far the window got. That is what an operator
   * of this store needs to know.
   */
  @Override
  public OptionalLong countDispatchedEntriesOlderThan(
      final Instant threshold) {

    return outbox.countDispatchedEntriesPastTheirRetention();

  }

}
