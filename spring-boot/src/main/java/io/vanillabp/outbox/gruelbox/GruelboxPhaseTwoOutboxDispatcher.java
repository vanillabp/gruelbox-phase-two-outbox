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
 * Background processing of the gruelbox transaction outbox: right after a commit
 * gruelbox dispatches the scheduled call itself, but for crash recovery and retries a
 * poller calling {@link TransactionOutbox#flush()} is required. Flushing
 * also deletes successfully dispatched entries whose retention threshold passed (the
 * asynchronous cleanup of the "DONE instead of delete" contract). The poller is
 * started on {@link ApplicationReadyEvent} (the first run also dispatches entries
 * left over from a previous crashed instance).
 * <p>
 * It does not flush on a rhythm. A flush is three database commands whether or not
 * anything is waiting, so between two of them the poller sleeps until the moment
 * gruelbox' own table says the next entry wants something
 * ({@link GruelboxPhaseTwoOutbox#earliestDueAt()}), bounded by
 * <code>vanillabp.outbox.poll-interval</code> for the one case nothing can be read from
 * the table: work another node wrote down before it died (see {@link DueEntryPoller}).
 * <p>
 * Starting the poller is also what lets an outbox built with VanillaBP's
 * {@link GruelboxRedispatchAwareSubmitter} dispatch after a commit at all. That
 * submitter keeps its entries until this dispatcher runs. A workflow started while
 * Spring Boot answers requests and the models are still on their way to the BPMS
 * therefore waits for the first poll, instead of reaching a BPMS which cannot know
 * it.
 * <p>
 * The poller runs on a private single-thread daemon executor - no
 * {@link org.springframework.scheduling.TaskScheduler} bean is registered or used, so
 * an application's own scheduling setup (e.g. <code>&#64;EnableScheduling</code>)
 * stays unaffected.
 * <p>
 * <strong>How this store holds an entry while it dispatches it.</strong> The stores VanillaBP
 * owns lease an entry and renew the lease while the dispatch runs
 * ({@link io.vanillabp.integration.adapter.migration.outbox.DispatchLease}), so a slow
 * dispatch is never carried out twice and costs no attempt. Gruelbox does it differently and
 * needs nothing added: its flush selects with <code>FOR UPDATE SKIP LOCKED</code> and then
 * locks the row it is about to invoke for, so the flush of another node skips an entry
 * somebody is dispatching however long that takes. The price is the one VanillaBP's own
 * stores avoid - the lock is a database transaction held for the length of the dispatch, and
 * therefore a connection - and it is gruelbox' design, not something this dispatcher chooses.
 * <p>
 * What it does mean is that gruelbox counts an attempt when it takes an entry rather than
 * when the attempt ended. Since nobody else can take that entry meanwhile, the two numbers
 * are the same here, and an entry only looks attempted while it is still travelling. The
 * table belongs to gruelbox, so this is written down rather than changed.
 * <p>
 * A dispatch which knows that repeating helps in a moment says so
 * ({@link io.vanillabp.integration.spi.PhaseTwoRetryLater} - a workflow its BPMS has not
 * made searchable yet), and gruelbox would schedule the next attempt from the
 * <code>attemptFrequency</code> of the whole outbox. The window the dispatch named is
 * written over it by {@link GruelboxPhaseTwoFailureListener}, so such an entry is due here
 * when it is due on the stores VanillaBP wrote itself. It is dispatched at the poll after
 * that moment, and the poller above sleeps until the earliest due moment or the configured
 * cap, whichever comes first ({@link DueEntryPoller}), so the delay this store adds to the
 * window is at most <code>vanillabp.outbox.poll-interval</code>. Nothing waits on any
 * thread for it, so the entries of every other workflow are dispatched while that one is
 * due again.
 */
@Slf4j
public class GruelboxPhaseTwoOutboxDispatcher implements OutboxHousekeeping.Store {

  private final TransactionOutbox transactionOutbox;

  /**
   * The store this dispatcher polls. It answers when the next flush has something to do.
   * Never <code>null</code>: that question is asked on every poll.
   */
  private final GruelboxPhaseTwoOutbox outbox;

  /**
   * The submitter whose gate is opened when polling starts, <code>null</code> for an
   * outbox built with a submitter of somebody else's.
   */
  private final GruelboxRedispatchAwareSubmitter submitter;

  private final DueEntryPoller poller;

  /**
   * Where the payloads of the entries a flush finished are removed. Never
   * <code>null</code>: a dispatcher is built with the place it house-keeps, the way the
   * store is built with the place it writes to.
   */
  private final PhaseTwoPayloadStore payloadStore;

  /**
   * Where this node says that it is house-keeping this store tonight, so no other node
   * measures its work at the same time.
   */
  private final JdbcHousekeepingLease housekeepingLease;

  /**
   * What removes the orphaned payloads, and when. The dispatched entries are gruelbox'
   * own business - see {@link #removeDispatchedEntriesOlderThan(Instant, int)}.
   */
  private final OutboxHousekeeping housekeeping;

  /**
   * Polls the outbox, holds its submitter back until it does and house-keeps the
   * payloads nobody removed. Building this dispatcher is what closes the submitter's
   * gate, so a submitter never waits for a dispatcher which does not exist (see
   * {@link GruelboxRedispatchAwareSubmitter}).
   *
   * @param transactionOutbox The outbox to poll
   * @param properties The bound <code>vanillabp.outbox</code> section
   * @param submitter The submitter the outbox was built with, <code>null</code> where
   *          gruelbox was built with a submitter of somebody else's
   * @param outbox The store, asked when the next flush has something to do
   * @param payloadStore Where the payloads of this outbox lie
   * @param housekeepingLease Where this node says that it is house-keeping this store
   *          tonight
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
   * Refuses a dispatcher which has no payloads to house-keep.
   * <p>
   * The store beside this class is built with the payload store as well, and for the
   * reason it is asked here: a check which waits for the right call reports a setup
   * problem while the application is working, and the dispatcher is built where the
   * answer is already known.
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
   * Starts the poller. The first run is executed immediately, dispatching
   * committed-but-unprocessed entries of a previously crashed instance and those the
   * submitter kept while the application was starting. The listener
   * order guarantees that workflow processing started BEFORE any recovered entry is
   * dispatched (see
   * {@link SpringBootDeploymentService#OUTBOX_DISPATCHER_LISTENER_ORDER}).
   */
  @Order(SpringBootDeploymentService.OUTBOX_DISPATCHER_LISTENER_ORDER)
  @EventListener(ApplicationReadyEvent.class)
  public void startPolling() {

    // opened before the poller starts, because a flush hands what it picked up to
    // this same submitter: a gate still closed would keep those entries, and each of
    // them would be due again only after 'attempt-frequency' instead of at once
    if (submitter != null) {
      submitter.dispatchingStarted();
    }
    poller.start();
    housekeeping.start();

  }

  /**
   * Stops the poller when the application context closes. An entry which was not dispatched
   * stays in gruelbox' table, and the first flush of the next start carries it.
   */
  @PreDestroy
  public void stopPolling() {

    poller.stop();
    housekeeping.stop();

  }

  /**
   * Flushes the outbox until no more work is done. Exceptions are caught to keep the
   * poller alive.
   */
  private void flush() {

    try {
      //noinspection StatementWithEmptyBody
      while (transactionOutbox.flush()) {
        // repeat until all due outbox entries were processed
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
   * Nothing, because gruelbox deletes its own dispatched entries. A flush of that library
   * removes what its retention threshold lets go, and the threshold is
   * <code>vanillabp.outbox.retention</code>. The table belongs to gruelbox, so this is
   * written down rather than changed: on this store the window governs the payloads
   * alone, and the entries leave on the rhythm of the flush as they always did.
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
   * What gruelbox has not deleted yet. This node removes none of them itself, so the
   * number says how far the flushes of the library have got rather than how far the
   * window did - which is the truth an operator of this store needs.
   */
  @Override
  public OptionalLong countDispatchedEntriesOlderThan(
      final Instant threshold) {

    return outbox.countDispatchedEntriesPastTheirRetention();

  }

}
