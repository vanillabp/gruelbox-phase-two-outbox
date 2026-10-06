package io.vanillabp.outbox.gruelbox;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.OptionalLong;

import javax.sql.DataSource;

import org.springframework.jdbc.datasource.DataSourceUtils;

import com.gruelbox.transactionoutbox.AlreadyScheduledException;
import com.gruelbox.transactionoutbox.InvocationSerializer;
import com.gruelbox.transactionoutbox.TransactionOutbox;

import io.vanillabp.integration.spi.PhaseTwoCall;
import io.vanillabp.integration.spi.PhaseTwoOutbox;
import io.vanillabp.integration.spi.PhaseTwoPayloadStore;
import lombok.extern.slf4j.Slf4j;

/**
 * The {@link PhaseTwoOutbox} implementation of a Spring Boot application which persists
 * its workflow aggregates via JPA and has this artifact on its classpath. Adding the
 * dependency is what selects this store, and <code>vanillabp.outbox.gruelbox.enabled</code>
 * only switches it off again. It delegates to a
 * <a href="https://github.com/gruelbox/transaction-outbox">gruelbox
 * transaction-outbox</a> configured with Spring's transaction manager, so the outbox
 * entry is enlisted in the currently running local (JDBC) transaction.
 * <p>
 * The idempotency contract of {@link PhaseTwoOutbox} maps onto gruelbox's
 * <code>uniqueRequestId</code> mechanism: the {@link PhaseTwoCall#idempotencyKey()} is
 * used as unique request ID, enforced by a unique constraint of gruelbox's outbox
 * table. A duplicate schedule raises {@link AlreadyScheduledException} which is turned
 * into the contract's no-op (<code>false</code>). Successfully dispatched entries with
 * a unique request ID are retained by gruelbox until the configured retention
 * threshold passes (the contract's "DONE instead of delete").
 * <p>
 * <strong>Deduplication has to span the entries still waiting for their dispatch
 * only</strong>, and gruelbox' unique constraint spans its retained entries as well.
 * Its table has no column this store could move a dispatched key into, so the release
 * happens when it is needed: before scheduling, the store reads the row of that unique
 * request ID and looks at gruelbox' <code>processed</code> flag. A processed row is
 * DELETED - it has done its work, and its trail ends there, which is the price of
 * gruelbox owning the table - and the new operation is then scheduled. A row which is
 * not processed yet means an identical operation is still planned, and the schedule is
 * discarded. Both happen in the caller's transaction, on the connection
 * {@code DataSourceUtils} binds to it, so a rollback takes the release with it.
 * <p>
 * <strong>A younger call may take the waiting entry's place</strong> instead of being
 * discarded against it, where it says so
 * ({@link PhaseTwoCall#replacingWhatIsStillWaiting()}). Replacing is not in gruelbox'
 * API, so it is the row which goes: the waiting entry is DELETED and the younger call
 * is scheduled under the same unique request ID, in the caller's transaction, and the
 * payload of the entry which went is removed with it. What decides is gruelbox'
 * <code>version</code> column together with the register of
 * {@link GruelboxRedispatchAwareSubmitter}, and a dispatch is never waited for: see
 * {@link #deleteEntryNoDispatchHasTaken(PhaseTwoCall, WaitingEntry)} for why it takes
 * both. Where one of them says that a dispatch has the entry, the younger call is
 * scheduled with NO unique request ID, because the key belongs to the entry on its
 * way, and two calls then reach the handler where one was asked for.
 * <p>
 * The at-least-once guarantee is not weakened by that: a redispatch reads the very row
 * which is not processed yet, so gruelbox' own attempt bookkeeping carries it - never
 * the unique request ID. Which is also why the key VanillaBP derives is bounded to
 * {@link PhaseTwoCall#MAX_IDEMPOTENCY_KEY_LENGTH} characters: gruelbox refuses a longer
 * unique request ID before any database sees it.
 * <p>
 * <strong>All of that is a read of gruelbox' table</strong>, so the store is built with the
 * data source the table lives on and with its name, and it refuses to be built without
 * them. A store which cannot read the table leaves every answer to gruelbox, and gruelbox'
 * unique constraint spans the retained entries as well: the next operation of a workflow
 * whose key is still retained would be discarded, a younger call would never take the place
 * of a waiting one, and no payload would ever be removed. Work would be lost rather than
 * delayed, and none of it would show up as an error, which is why the refusal comes at the
 * constructor.
 * <p>
 * {@link PhaseTwoOutbox#adapterIdsOfPendingCalls(String, String)} is the one question of
 * the contract this store does not answer AT A START. gruelbox keeps a call as a
 * serialized invocation rather than in columns, so there is no adapter id to ask about
 * without reading and deserializing the whole table, and a start which reads every waiting
 * entry is a cost VanillaBP does not take, while a table of gruelbox' is not one VanillaBP
 * adds a column to. What the start would have said is said at the first dispatch instead:
 * the entry is deserialized there anyway, so the id it waits for is known, and an id which
 * is gone from the configuration is reported in the words the start uses - once per adapter
 * id, whatever the backlog. See decision 2 in the repository's DECISIONS.md.
 * <p>
 * <strong>Two operations of one workflow are not kept in order here</strong>, which is
 * what the dispatch lanes of the stores VanillaBP writes itself do. There are no lanes
 * around gruelbox: it submits an entry the moment the scheduling transaction commits
 * while a flush carries whatever else is due, so the two race, and a failed attempt moves
 * the column a flush orders by. The promise an application is given is the weaker one
 * (give an operation which has to go first a transaction of its own), so this store keeps
 * it; see decision 2 in the repository's DECISIONS.md.
 * <p>
 * {@link PhaseTwoOutbox#ageOfOldestPendingCall()} is the other question this store
 * leaves unanswered, and this one it cannot answer at all. gruelbox keeps no moment of
 * writing: it puts that moment into <code>nextAttemptTime</code> and overwrites it the
 * first time a flush picks the entry up, so the oldest waiting entry - which is usually
 * one which was picked up and failed - no longer says when it was planned. Answering
 * from the entries nothing has touched yet would report a young age while old ones
 * stand next to them, which reads as an outbox that is up to date. So no age is
 * published for this store, and <code>vanillabp.outbox.pending</code> stays the number
 * to watch here. The wait of a dispatch is reported for the entries where the moment is
 * still there, which is every entry submitted right after its transaction committed
 * (see {@link GruelboxRedispatchAwareSubmitter#whenTheEntryWasWritten()}).
 */
@Slf4j
public class GruelboxPhaseTwoOutbox implements PhaseTwoOutbox {

  /**
   * Reads back what gruelbox wrote into its <code>invocation</code> column. gruelbox
   * builds the same one unless an application replaces the serializer of its persistor,
   * and where it did, the reference of a replaced entry stays unknown and its payload is
   * removed one retention period later (see {@link #payloadReferenceOf(String)}).
   */
  private static final InvocationSerializer INVOCATION_SERIALIZER = InvocationSerializer
      .createDefaultJsonSerializer();

  private final TransactionOutbox transactionOutbox;

  /**
   * Where gruelbox' table lives. Never <code>null</code>: every question this store
   * answers about its entries is a read of that table.
   */
  private final DataSource dataSource;

  /**
   * The table gruelbox stores its entries in.
   */
  private final String tableName;

  /**
   * Where the bytes of a call which carries a payload are written, in the transaction
   * which writes the entry. Never <code>null</code>: a store is built with the place it
   * writes to, the way it is built with gruelbox' table.
   */
  private final PhaseTwoPayloadStore payloadStore;

  /**
   * The store, which is what {@link GruelboxPhaseTwoOutboxAutoConfiguration} builds where
   * an application asked for gruelbox.
   *
   * @param transactionOutbox The gruelbox transaction outbox
   * @param dataSource Where gruelbox' table lives
   * @param tableName The table gruelbox stores its entries in
   * @param payloadStore Where the payload of a call which carries one is written
   * @throws IllegalArgumentException If the data source, the name of gruelbox' table or
   *           the payload store is missing
   */
  public GruelboxPhaseTwoOutbox(
      final TransactionOutbox transactionOutbox,
      final DataSource dataSource,
      final String tableName,
      final PhaseTwoPayloadStore payloadStore) {

    requireGruelboxTable(dataSource, tableName);
    requirePayloadStore(payloadStore);
    this.transactionOutbox = transactionOutbox;
    this.dataSource = dataSource;
    this.tableName = tableName;
    this.payloadStore = payloadStore;

  }

  /**
   * Refuses a store which has nowhere to put a payload.
   * <p>
   * An application whose extensions pass no payload never notices the difference, and
   * that used to be the reason this was asked at the first call which carried one. It is
   * asked here now, because a check which waits for the right call reports a setup
   * problem while the application is working, and the store is built where the answer is
   * already known.
   *
   * @param payloadStore Where the payload of a call which carries one is written
   */
  private static void requirePayloadStore(
      final PhaseTwoPayloadStore payloadStore) {

    if (payloadStore != null) {
      return;
    }
    throw new IllegalArgumentException(
        """
            This gruelbox phase-two outbox was built without a payload store! A phase-two \
            call may carry the state the application saw at its sync point, and gruelbox' \
            table has no column for those bytes, so they lie in a store of their own. A \
            store without it would let such a call reach the BPMS without what it carries, \
            which the handler on the other side cannot ask for again. Pass a \
            PhaseTwoPayloadStore to the constructor of this class, or let VanillaBP's \
            GruelboxPhaseTwoOutboxAutoConfiguration build the store.""");

  }

  /**
   * Refuses a store which could not read gruelbox' table, with the message saying what
   * such a store would cost and how to build a complete one.
   *
   * @param dataSource Where gruelbox' table lives
   * @param tableName The table gruelbox stores its entries in
   */
  private static void requireGruelboxTable(
      final DataSource dataSource,
      final String tableName) {

    if ((dataSource != null) && (tableName != null)) {
      return;
    }
    throw new IllegalArgumentException(
        """
            This gruelbox phase-two outbox was built without %s! The store reads gruelbox' \
            table before it schedules: that is how the key of an entry which was dispatched \
            is freed, how a younger call takes the place of one which is still waiting, and \
            how the housekeeping learns which payloads an entry still names. A store which \
            cannot read it discards the next operation of a workflow whose key is still \
            retained and removes no payload at all, so it is refused here instead of losing \
            work later. Pass the data source gruelbox' table lives on and the name of that \
            table to the constructor, or let VanillaBP's \
            GruelboxPhaseTwoOutboxAutoConfiguration build the store."""
            .formatted(missingPart(dataSource, tableName)));

  }

  /**
   * Which half of gruelbox' table the caller left out, so the message names what is
   * actually missing instead of both.
   *
   * @param dataSource Where gruelbox' table lives
   * @param tableName The table gruelbox stores its entries in
   * @return The words the message puts in
   */
  private static String missingPart(
      final DataSource dataSource,
      final String tableName) {

    if (dataSource == null) {
      return tableName == null
          ? "a data source and the name of gruelbox' table"
          : "a data source";
    }
    return "the name of gruelbox' table";

  }

  /**
   * Counts the entries gruelbox has not processed yet. gruelbox has no API for it, so
   * the count reads its table directly - along the index it creates itself
   * (<code>IX_TXNO_OUTBOX_1</code> over <code>processed, blocked,
   * nextAttemptTime</code>), which is why one column is enough and no dialect-specific
   * literal is needed: the driver knows how to write a boolean into whatever type the
   * column has on this database.
   */
  @Override
  public OptionalLong pendingCalls() {

    final var countPending = "SELECT COUNT(*) FROM %s WHERE processed = ?".formatted(tableName);
    try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(countPending)) {
      statement.setBoolean(1, false);
      try (var resultSet = statement.executeQuery()) {
        return resultSet.next()
            ? OptionalLong.of(resultSet.getLong(1))
            : OptionalLong.empty();
      }
    } catch (final SQLException e) {
      // a metric must never be the reason an application fails - the gauge reports
      // nothing for this collection and the next one tries again
      log.debug("Could not count the pending entries of gruelbox' outbox table '{}'", tableName, e);
      return OptionalLong.empty();
    }

  }

  /**
   * When gruelbox' next flush has something to do, which is what its dispatcher waits for.
   * <p>
   * Both halves of a flush read the same column, because gruelbox keeps both moments in
   * <code>nextAttemptTime</code>: an entry waiting for its dispatch carries its next attempt
   * there, and an entry which was dispatched carries the moment its retention runs out. What
   * differs is the <code>processed</code> flag, and they are therefore asked as two questions
   * instead of one: the index gruelbox creates for its own flush spans
   * <code>(processed, blocked, nextAttemptTime)</code>, so a question naming both flags is
   * answered from that index, while one naming only <code>blocked</code> would read the whole
   * table - and that cost grows with everything the table ever held.
   * <p>
   * A BLOCKED entry is left out of both, and that is the point of the predicate: it waits for a
   * person rather than for a clock, so a store which holds nothing else has nothing to be woken
   * for.
   *
   * @return The moment of the earliest entry, or <code>null</code> where nothing is owed,
   *         which leaves the poller on the configured cap
   */
  public Instant earliestDueAt() {

    final var selectEarliest = "SELECT MIN(nextAttemptTime) FROM %s WHERE processed = ? AND blocked = ?"
        .formatted(tableName);
    try (var connection = dataSource.getConnection()) {
      final var nextAttempt = earliest(connection, selectEarliest, false);
      final var retentionRunsOut = earliest(connection, selectEarliest, true);
      if (nextAttempt == null) {
        return retentionRunsOut;
      }
      if (retentionRunsOut == null) {
        return nextAttempt;
      }
      return nextAttempt.isBefore(retentionRunsOut) ? nextAttempt : retentionRunsOut;
    } catch (final SQLException e) {
      // the flush which follows reports the same problem with its own message, and a
      // poller which stops asking is worse than one which asks at the cap
      log.debug("Could not read the next attempt time of gruelbox' outbox table '{}'", tableName, e);
      return null;
    }

  }

  /**
   * The table gruelbox stores its entries in, asked from outside where the housekeeping
   * needs a name to claim this store by.
   *
   * @return The table of this store
   */
  public String getTableName() {

    return tableName;

  }

  /**
   * How many dispatched entries gruelbox has not deleted yet - what its flushes still
   * owe. VanillaBP removes none of them itself on this store, so this is the number the
   * housekeeping publishes when its window closes.
   * <p>
   * gruelbox marks a dispatched entry <code>processed</code> and pushes its
   * <code>nextAttemptTime</code> out by the retention threshold, which is
   * <code>vanillabp.outbox.retention</code>, so an entry whose moment has come is one a
   * flush would delete. It reads gruelbox' own index over
   * <code>processed, blocked, nextAttemptTime</code>.
   *
   * @return How many there are, empty where the table could not be asked
   */
  public OptionalLong countDispatchedEntriesPastTheirRetention() {

    final var countExpired = "SELECT COUNT(*) FROM %s WHERE processed = ? AND blocked = ? AND nextAttemptTime <= ?"
        .formatted(tableName);
    try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(countExpired)) {
      statement.setBoolean(1, true);
      statement.setBoolean(2, false);
      statement.setTimestamp(3, Timestamp.from(Instant.now()));
      try (var resultSet = statement.executeQuery()) {
        return resultSet.next()
            ? OptionalLong.of(resultSet.getLong(1))
            : OptionalLong.empty();
      }
    } catch (final SQLException e) {
      // a number which could not be read stays a gap in the meter rather than a zero
      log.debug("Could not count the dispatched entries of gruelbox' outbox table '{}'", tableName, e);
      return OptionalLong.empty();
    }

  }

  /**
   * The earliest moment one of the two kinds of entry wants something.
   *
   * @param connection The connection to ask on
   * @param query The aggregate over the table, taking the two flags
   * @param processed Whether to look at the entries which were dispatched already
   * @return The moment or <code>null</code> where there is no such entry
   */
  private Instant earliest(
      final Connection connection,
      final String query,
      final boolean processed) throws SQLException {

    try (var statement = connection.prepareStatement(query)) {
      statement.setBoolean(1, processed);
      statement.setBoolean(2, false);
      try (var resultSet = statement.executeQuery()) {
        if (!resultSet.next()) {
          return null;
        }
        final var earliest = resultSet.getTimestamp(1);
        return earliest == null ? null : earliest.toInstant();
      }
    }

  }

  @Override
  public boolean schedule(
      final PhaseTwoCall call) {

    final var idempotencyKey = call
        .idempotencyKey()
        .orElse(null);
    // the key of the entry this call ends up carrying: its own, unless an entry a
    // dispatch has taken still holds it - then this call takes no part in the
    // deduplication of that key, the way a call without one does not
    var uniqueRequestId = idempotencyKey;
    String replacedPayloadReference = null;
    if (idempotencyKey != null) {
      final var waiting = entryStillWaiting(call, idempotencyKey);
      if (waiting != null) {
        if (!call.replacesWhatIsStillWaiting()) {
          logDiscardedSchedule(call);
          return false;
        }
        if (deleteEntryNoDispatchHasTaken(call, waiting)) {
          replacedPayloadReference = waiting.payloadReference();
          logReplacedEntry(call);
        } else {
          logSecondEntryBesideAClaimedOne(call);
          uniqueRequestId = null;
        }
      }
    }
    try {
      transactionOutbox
          .with()
          .uniqueRequestId(uniqueRequestId)
          .schedule(GruelboxPhaseTwoDispatch.class)
          .dispatch(
              call.operation(),
              call.workflowModuleId(),
              call.bpmnProcessId(),
              call.workflowAggregateId(),
              call.adapterId(),
              PhaseTwoCall.serializeArgs(call.args()));
      // the entry is in, so the bytes it names may follow - on the connection bound to
      // this transaction, and only now, because a schedule discarded as a duplicate
      // must leave nothing behind
      if (call.hasPayload()) {
        payloadStore.write(call);
      }
      // and the bytes of the entry which was replaced have no reader left: the entry
      // is gone, and no dispatch had ever taken it
      if (replacedPayloadReference != null) {
        payloadStore.remove(replacedPayloadReference);
      }
      return true;
    } catch (AlreadyScheduledException e) {
      // two nodes scheduling the same operation at the same moment, so neither of them
      // saw the entry of the other when it read the table
      logDiscardedSchedule(call);
      return false;
    }

  }

  /**
   * A call which replaces goes the same way as any other, and this is the one method
   * which says so - the mark travels in the call, so {@link #schedule(PhaseTwoCall)}
   * reads it where the entry is written.
   */
  @Override
  public boolean scheduleReplacingWhatIsStillWaiting(
      final PhaseTwoCall call) {

    return schedule(call.replacingWhatIsStillWaiting());

  }

  /**
   * The entry of this unique request ID which is still waiting for its dispatch, or
   * <code>null</code> where the key is free for a new one.
   * <p>
   * An entry gruelbox already processed is DELETED here - it has done its work, and its
   * trail ends there, which is the price of gruelbox owning the table - so the key is
   * free and this answers <code>null</code>.
   * <p>
   * A BLOCKED entry gives its key away and stays. It waits for a person, and the stores
   * VanillaBP writes itself free the key of such an entry when they block it, so the
   * application can plan the operation again. gruelbox' unique constraint spans a blocked
   * row too, and its table has no second column the key could move into, so the key is
   * set to <code>null</code> here, where it is needed. gruelbox allows any number of rows
   * without a unique request ID. The write counts gruelbox' <code>version</code> up, so a
   * listener which is about to open the row again loses gruelbox' own optimistic lock and
   * leaves it blocked. See decision 5 in the repository's DECISIONS.md.
   *
   * @return The waiting entry or <code>null</code>
   */
  private WaitingEntry entryStillWaiting(
      final PhaseTwoCall call,
      final String idempotencyKey) {

    final var selectEntry = "SELECT id, processed, version, invocation, blocked FROM %s WHERE uniqueRequestId = ?"
        .formatted(tableName);
    final var connection = DataSourceUtils.getConnection(dataSource);
    try {
      final String entryId;
      final boolean blocked;
      final WaitingEntry waiting;
      try (var statement = connection.prepareStatement(selectEntry)) {
        statement.setString(1, idempotencyKey);
        try (var resultSet = statement.executeQuery()) {
          if (!resultSet.next()) {
            return null;
          }
          entryId = resultSet.getString(1);
          final var processed = resultSet.getBoolean(2);
          blocked = !processed && resultSet.getBoolean(5);
          waiting = (processed || blocked)
              ? null
              : new WaitingEntry(entryId, resultSet.getInt(3), payloadReferenceOf(resultSet.getString(4)));
        }
      }
      if (waiting != null) {
        return waiting;
      }
      if (blocked) {
        freeTheKeyOfABlockedEntry(connection, call, entryId);
        return null;
      }
      final var deleteEntry = "DELETE FROM %s WHERE id = ? AND processed = ?".formatted(tableName);
      try (var statement = connection.prepareStatement(deleteEntry)) {
        statement.setString(1, entryId);
        statement.setBoolean(2, true);
        // 0 rows: the dispatcher's retention cleanup got there first, which frees the
        // key just as well
        statement.executeUpdate();
      }
      log.debug(
          "Phase two ({}) of BPMN process '{}' of workflow module '{}' for aggregate '{}' was "
              + "dispatched before - released the entry so this operation can be planned again",
          call.operation(),
          call.bpmnProcessId(),
          call.workflowModuleId(),
          call.workflowAggregateId());
      return null;
    } catch (final SQLException e) {
      throw new IllegalStateException(
          """
              Could not look up the phase-two outbox entry of BPMN process '%s' of workflow module \
              '%s' in gruelbox' table '%s'!"""
              .formatted(call.bpmnProcessId(), call.workflowModuleId(), tableName), e);
    } finally {
      DataSourceUtils.releaseConnection(connection, dataSource);
    }

  }

  /**
   * Takes the key away from a blocked entry, so the operation it failed for can be
   * planned again. The row stays as it is otherwise: blocked, with its attempts, its
   * invocation and the payload it names, for whoever repairs it. Opened again later, it
   * is dispatched without a key, so the operation may reach the BPMS twice - the same
   * residual the stores VanillaBP writes itself document for a blocked entry.
   * <p>
   * 0 rows means that somebody opened or removed the entry between the read and this
   * write. The insert which follows then meets the key again or does not, and gruelbox'
   * unique constraint decides, as for two nodes scheduling at the same moment.
   *
   * @param connection The connection bound to the caller's transaction
   * @param call The call which plans the operation again
   * @param entryId The id of the blocked entry
   * @throws SQLException If the write fails
   */
  private void freeTheKeyOfABlockedEntry(
      final Connection connection,
      final PhaseTwoCall call,
      final String entryId) throws SQLException {

    final var freeTheKey = """
        UPDATE %s SET uniqueRequestId = NULL, version = version + 1 \
        WHERE id = ? AND blocked = ? AND processed = ?"""
        .formatted(tableName);
    try (var statement = connection.prepareStatement(freeTheKey)) {
      statement.setString(1, entryId);
      statement.setBoolean(2, true);
      statement.setBoolean(3, false);
      statement.executeUpdate();
    }
    log.debug(
        "Phase two ({}) of BPMN process '{}' of workflow module '{}' for aggregate '{}' is planned "
            + "again - the blocked outbox entry '{}' gave its key away and stays blocked",
        call.operation(),
        call.bpmnProcessId(),
        call.workflowModuleId(),
        call.workflowAggregateId(),
        entryId);

  }

  /**
   * Deletes the entry a younger call replaces, so its unique request ID is free for
   * that call - which is what replacing means on a store whose API does not know it.
   * <p>
   * Two things have to say that no dispatch has taken the entry, because gruelbox
   * reaches a dispatch two ways. A FLUSH pushes the entry back first, which counts
   * gruelbox' <code>version</code> up, and the delete carries <code>version = 0</code>
   * - gruelbox' own optimistic lock, so this is the very race the flush runs, decided
   * by the same column. A commit, on the other hand, submits the entry straight away
   * and writes nothing, so the row still reads as untouched while the dispatch holds
   * it; that one is asked of
   * {@link GruelboxRedispatchAwareSubmitter#isBeingDispatched(String)}.
   * <p>
   * Asking matters because gruelbox locks the row of an entry it dispatches and keeps
   * the lock until the handler returned: a delete which met it would make the
   * application's transaction wait for a remote call. The register answers for this
   * application, and the residual is an instance which dispatches an entry another
   * instance replaces in that window - a workflow written by two instances at once,
   * which VanillaBP names as the application's own business anyway.
   *
   * @return Whether the entry was deleted
   */
  private boolean deleteEntryNoDispatchHasTaken(
      final PhaseTwoCall call,
      final WaitingEntry waiting) {

    if ((waiting.version() > 0) || GruelboxRedispatchAwareSubmitter.isBeingDispatched(waiting.id())) {
      return false;
    }
    final var deleteEntry = "DELETE FROM %s WHERE id = ? AND version = 0 AND processed = ?".formatted(tableName);
    final var connection = DataSourceUtils.getConnection(dataSource);
    try (var statement = connection.prepareStatement(deleteEntry)) {
      statement.setString(1, waiting.id());
      statement.setBoolean(2, false);
      return statement.executeUpdate() == 1;
    } catch (final SQLException e) {
      throw new IllegalStateException(
          """
              Could not replace the phase-two outbox entry of BPMN process '%s' of workflow module \
              '%s' in gruelbox' table '%s'!"""
              .formatted(call.bpmnProcessId(), call.workflowModuleId(), tableName), e);
    } finally {
      DataSourceUtils.releaseConnection(connection, dataSource);
    }

  }

  /**
   * The payload reference an entry of this store names, read out of the invocation
   * gruelbox serialized. There is no column for it: gruelbox keeps a call as one
   * serialized invocation, and the arguments of that invocation are the six strings
   * {@link GruelboxPhaseTwoDispatch#dispatch} takes, the last of them being the
   * serialized {@link PhaseTwoCall#args()}.
   * <p>
   * Read with gruelbox' own serializer, which is what wrote it. An entry this store did
   * not write, or one written by a persistor built with a serializer of the
   * application's own, is not understood here - the reference then stays unknown and the
   * replaced payload waits for the housekeeping, which removes it once no entry names it
   * any more. Nothing else changes.
   *
   * @param invocation The serialized invocation of the entry
   * @return The reference or <code>null</code> where the entry names none
   */
  private String payloadReferenceOf(
      final String invocation) {

    if (invocation == null) {
      return null;
    }
    try (var reader = new StringReader(invocation)) {
      final var args = INVOCATION_SERIALIZER.deserializeInvocation(reader).getArgs();
      if ((args == null) || (args.length < 6) || !(args[5] instanceof final String serializedArgs)) {
        return null;
      }
      return PhaseTwoCall.deserializeArgs(serializedArgs).get(PhaseTwoCall.ARG_PAYLOAD_REFERENCE);
    } catch (final Exception e) {
      log
          .debug(
              "Could not read the payload reference of a phase-two outbox entry of gruelbox' table "
                  + "'{}' - a payload of that entry is left to the age sweep of the payload store",
              tableName,
              e);
      return null;
    }

  }

  /**
   * What this store needs to know about the entry a younger call meets: which row it
   * is, whether a dispatch has taken it (gruelbox counts the version up when it does),
   * and which payload it names.
   *
   * @param id The entry's own id
   * @param version gruelbox' optimistic-lock counter - zero means untouched
   * @param payloadReference The reference of its payload, or <code>null</code>
   */
  private record WaitingEntry(String id, int version, String payloadReference) {
  }

  /**
   * A younger call took the place of the entry which was waiting. At DEBUG for the
   * reason a discard is: it is what the caller asked for, and under a backlog it
   * happens as often as reports are planned.
   */
  private static void logReplacedEntry(
      final PhaseTwoCall call) {

    log.debug(
        "Phase two ({}) of BPMN process '{}' of workflow module '{}' for aggregate '{}' replaced the "
            + "entry which was waiting for its dispatch",
        call.operation(),
        call.bpmnProcessId(),
        call.workflowModuleId(),
        call.workflowAggregateId());

  }

  /**
   * A younger call could not take the place of the entry it meant to replace, because a
   * dispatch had claimed it. It becomes an entry of its own, so the handler is called
   * twice - worth a line, because an application counting its reports finds the second
   * one here.
   */
  private static void logSecondEntryBesideAClaimedOne(
      final PhaseTwoCall call) {

    log.debug(
        "Phase two ({}) of BPMN process '{}' of workflow module '{}' for aggregate '{}' asked to "
            + "replace an entry a dispatch had already taken - that one runs to its end and this "
            + "call becomes an entry of its own",
        call.operation(),
        call.bpmnProcessId(),
        call.workflowModuleId(),
        call.workflowAggregateId());

  }

  /**
   * The technical half of a discarded schedule. Which of the two causes it was - a
   * redelivered dispatch or an operation lost against one still waiting - the store
   * cannot tell, so the core reports it to the caller and this line stays at DEBUG.
   */
  private static void logDiscardedSchedule(
      final PhaseTwoCall call) {

    log.debug(
        "Phase two ({}) of BPMN process '{}' of workflow module '{}' for aggregate '{}' is still "
            + "waiting for its dispatch - the schedule of an identical operation was discarded",
        call.operation(),
        call.bpmnProcessId(),
        call.workflowModuleId(),
        call.workflowAggregateId());

  }

}
