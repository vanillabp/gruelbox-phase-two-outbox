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
 * The {@link PhaseTwoOutbox} of a Spring Boot application which persists its workflow aggregates
 * with JPA and has this artifact on its classpath. Adding the dependency selects this store, and
 * <code>vanillabp.outbox.gruelbox.enabled</code> only switches it off again. It hands the work to
 * a <a href="https://github.com/gruelbox/transaction-outbox">gruelbox transaction-outbox</a> which
 * uses the transaction manager of Spring. So the outbox entry joins the local (JDBC) transaction
 * which runs right now.
 * <p>
 * The idempotency contract of {@link PhaseTwoOutbox} maps onto the <code>uniqueRequestId</code>
 * of gruelbox. The {@link PhaseTwoCall#idempotencyKey()} is the unique request ID, and a unique
 * constraint of the gruelbox table enforces it. A duplicate schedule throws
 * {@link AlreadyScheduledException}, and that becomes the no-op of the contract
 * (<code>false</code>). Gruelbox keeps dispatched entries with a unique request ID until the
 * configured retention has passed. That is "DONE instead of delete" of the contract.
 * <p>
 * <strong>Deduplication must only cover the entries which still wait for their
 * dispatch</strong>, but the unique constraint of gruelbox covers its retained entries as well.
 * Its table has no column this store could move a dispatched key into. So the key is released when
 * it is needed. Before it schedules, the store reads the row of that unique request ID and looks
 * at the <code>processed</code> flag of gruelbox. A processed row is DELETED. It has done its
 * work, and its trail ends there. That is the price of gruelbox owning the table. Then the new
 * operation is scheduled. A row which is not processed yet means that an identical operation is
 * still planned, and the schedule is discarded. Both happen in the transaction of the caller, on
 * the connection which {@code DataSourceUtils} binds to it. So a rollback takes the release with
 * it.
 * <p>
 * <strong>A younger call may take the place of the waiting entry</strong> instead of being
 * discarded, if it says so ({@link PhaseTwoCall#replacingWhatIsStillWaiting()}). The API of
 * gruelbox cannot replace an entry, so the row goes. The waiting entry is DELETED, and the younger
 * call is scheduled under the same unique request ID, in the transaction of the caller. The
 * payload of the deleted entry is removed with it. Two things decide: the <code>version</code>
 * column of gruelbox, and the register of {@link GruelboxRedispatchAwareSubmitter}. A dispatch is
 * never waited for. {@link #deleteEntryNoDispatchHasTaken(PhaseTwoCall, WaitingEntry)} says why
 * it takes both. If one of them says that a dispatch has the entry, the younger call is scheduled
 * with NO unique request ID, because the key belongs to the entry on its way. Then two calls reach
 * the handler where one was asked for.
 * <p>
 * That does not weaken the at-least-once guarantee. A redispatch reads the same row, which is not
 * processed yet. So the attempt bookkeeping of gruelbox carries it, never the unique request ID.
 * That is also why the key VanillaBP derives has at most
 * {@link PhaseTwoCall#MAX_IDEMPOTENCY_KEY_LENGTH} characters. Gruelbox refuses a longer unique
 * request ID before any database sees it.
 * <p>
 * <strong>All of that reads the gruelbox table.</strong> So the store is built with the data
 * source of the table and with its name, and it refuses to be built without them. A store which
 * cannot read the table leaves every answer to gruelbox, and the unique constraint of gruelbox
 * covers the retained entries as well. The next operation of a workflow whose key is still
 * retained would be discarded. A younger call would never take the place of a waiting one. And no
 * payload would ever be removed. Work would be lost, not just delayed, and none of it would show
 * up as an error. That is why the constructor refuses it.
 * <p>
 * {@link PhaseTwoOutbox#adapterIdsOfPendingCalls(String, String)} is the one question of the
 * contract which this store does not answer AT A START. Gruelbox keeps a call as a serialized
 * invocation, not in columns. So there is no adapter id to ask about, unless the whole table is
 * read and deserialized. VanillaBP does not take that cost at a start, and it does not add a
 * column to a table of gruelbox. So the first dispatch says what the start would have said. The
 * entry is deserialized there anyway, so the id it waits for is known. An id which is gone from
 * the configuration is reported in the words the start uses, once per adapter id, however large
 * the backlog is. See decision 2 in the repository's DECISIONS.md.
 * <p>
 * <strong>Two operations of one workflow are not kept in order here.</strong> The stores which
 * VanillaBP writes itself do that with their dispatch lanes. There are no lanes around gruelbox.
 * It submits an entry the moment the scheduling transaction commits, while a flush carries
 * whatever else is due. So the two race, and a failed attempt moves the column a flush orders by.
 * An application is given the weaker promise: give an operation which has to go first a
 * transaction of its own. This store keeps that promise; see decision 2 in the repository's
 * DECISIONS.md.
 * <p>
 * {@link PhaseTwoOutbox#ageOfOldestPendingCall()} is the other question this store leaves
 * unanswered, and this one it cannot answer at all. Gruelbox keeps no moment of writing. It puts
 * that moment into <code>nextAttemptTime</code>, and it overwrites it the first time a flush picks
 * the entry up. The oldest waiting entry is usually one which was picked up and failed. So it no
 * longer says when it was planned. An answer from the entries nothing has touched yet would report
 * a young age, while old ones stand next to them. That reads like an outbox which is up to date.
 * So no age is published for this store, and <code>vanillabp.outbox.pending</code> stays the
 * number to watch here. The wait of a dispatch is reported for the entries which still have the
 * moment. That is every entry which was submitted right after its transaction committed (see
 * {@link GruelboxRedispatchAwareSubmitter#whenTheEntryWasWritten()}).
 */
@Slf4j
public class GruelboxPhaseTwoOutbox implements PhaseTwoOutbox {

  /**
   * Reads back what gruelbox wrote into its <code>invocation</code> column. Gruelbox builds the
   * same serializer, unless an application replaces the serializer of its persistor. If it did,
   * the reference of a replaced entry stays unknown, and its payload is removed one retention
   * period later (see {@link #payloadReferenceOf(String)}).
   */
  private static final InvocationSerializer INVOCATION_SERIALIZER = InvocationSerializer
      .createDefaultJsonSerializer();

  private final TransactionOutbox transactionOutbox;

  /**
   * Where the table of gruelbox lives. It is never <code>null</code>, because every question this
   * store answers about its entries reads that table.
   */
  private final DataSource dataSource;

  /**
   * The table gruelbox stores its entries in.
   */
  private final String tableName;

  /**
   * Where the bytes of a call with a payload are written, in the transaction which writes the
   * entry. It is never <code>null</code>. A store is built with the place it writes to, the way it
   * is built with the table of gruelbox.
   */
  private final PhaseTwoPayloadStore payloadStore;

  /**
   * The store. {@link GruelboxPhaseTwoOutboxAutoConfiguration} builds it if an application asked
   * for gruelbox.
   *
   * @param transactionOutbox The gruelbox transaction outbox
   * @param dataSource Where the table of gruelbox lives
   * @param tableName The table gruelbox stores its entries in
   * @param payloadStore Where the payload of a call which carries one is written
   * @throws IllegalArgumentException If the data source, the name of the gruelbox table or the
   *           payload store is missing
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
   * An application whose extensions pass no payload never notices the difference. That used to
   * be the reason why this was checked at the first call which carried a payload. Now it is
   * checked here. A check which waits for the right call reports a setup problem while the
   * application works. The store is built at a point where the answer is already known.
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
   * Refuses a store which could not read the table of gruelbox. The message says what such a store
   * would cost, and how to build a complete one.
   *
   * @param dataSource Where the table of gruelbox lives
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
   * Which half of the gruelbox table the caller left out. So the message names what is really
   * missing, not both.
   *
   * @param dataSource Where the table of gruelbox lives
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
   * Counts the entries gruelbox has not processed yet. Gruelbox has no API for it, so the count
   * reads its table directly, along the index gruelbox creates itself
   * (<code>IX_TXNO_OUTBOX_1</code> over <code>processed, blocked, nextAttemptTime</code>). So one
   * column is enough. No literal for a SQL dialect is needed, because the driver knows how to write
   * a boolean into whatever type the column has on this database.
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
      // a metric must never make an application fail. The gauge reports nothing this
      // time, and the next collection tries again
      log.debug("Could not count the pending entries of gruelbox' outbox table '{}'", tableName, e);
      return OptionalLong.empty();
    }

  }

  /**
   * When the next flush of gruelbox has something to do. Its dispatcher waits for that moment.
   * <p>
   * Both halves of a flush read the same column, because gruelbox keeps both moments in
   * <code>nextAttemptTime</code>. An entry which waits for its dispatch carries its next attempt
   * there. An entry which was dispatched carries the moment its retention runs out. Only the
   * <code>processed</code> flag differs. So they are asked as two questions, not one. The index
   * gruelbox creates for its own flush covers <code>(processed, blocked, nextAttemptTime)</code>.
   * So a question which names both flags is answered from that index. A question which names only
   * <code>blocked</code> would read the whole table, and that cost grows with everything the table
   * ever held.
   * <p>
   * A BLOCKED entry is left out of both, and that is the point of the condition. It waits for a
   * person, not for a clock. So a store which holds nothing else has no reason to wake up.
   *
   * @return The moment of the earliest entry, or <code>null</code> if nothing is due. Then the
   *         poller waits for the configured cap
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
      // the next flush reports the same problem with its own message. A poller which
      // stops asking is worse than one which asks at the cap
      log.debug("Could not read the next attempt time of gruelbox' outbox table '{}'", tableName, e);
      return null;
    }

  }

  /**
   * The table gruelbox stores its entries in. The housekeeping asks for it, because it needs a
   * name to claim this store by.
   *
   * @return The table of this store
   */
  public String getTableName() {

    return tableName;

  }

  /**
   * How many dispatched entries gruelbox has not deleted yet, which its flushes still owe.
   * VanillaBP removes none of them itself on this store. So this is the number the housekeeping
   * publishes when its window closes.
   * <p>
   * Gruelbox marks a dispatched entry <code>processed</code>, and it moves its
   * <code>nextAttemptTime</code> out by the retention, which is
   * <code>vanillabp.outbox.retention</code>. So a flush would delete an entry whose moment has
   * come. It reads the index of gruelbox over <code>processed, blocked, nextAttemptTime</code>.
   *
   * @return How many there are, empty if the table could not be asked
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
      // a number which could not be read stays a gap in the meter, not a zero
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
   * @return The moment, or <code>null</code> if there is no such entry
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
    // the key of the entry this call ends up with: its own, unless an entry which a
    // dispatch has taken still holds it. Then this call takes no part in the
    // deduplication of that key, like a call without a key
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
      // the entry is in, so the bytes it names may follow, on the connection bound to
      // this transaction. Only now, because a schedule discarded as a duplicate must
      // leave nothing behind
      if (call.hasPayload()) {
        payloadStore.write(call);
      }
      // the bytes of the replaced entry have no reader left: the entry is gone, and no
      // dispatch had ever taken it
      if (replacedPayloadReference != null) {
        payloadStore.remove(replacedPayloadReference);
      }
      return true;
    } catch (AlreadyScheduledException e) {
      // two nodes scheduled the same operation at the same moment, so neither of them
      // saw the entry of the other when it read the table
      logDiscardedSchedule(call);
      return false;
    }

  }

  /**
   * A call which replaces goes the same way as any other. This is the one method which marks
   * it. The mark travels in the call, so {@link #schedule(PhaseTwoCall)} reads it where the entry
   * is written.
   */
  @Override
  public boolean scheduleReplacingWhatIsStillWaiting(
      final PhaseTwoCall call) {

    return schedule(call.replacingWhatIsStillWaiting());

  }

  /**
   * The entry of this unique request ID which still waits for its dispatch, or <code>null</code>
   * if the key is free for a new one.
   * <p>
   * An entry gruelbox already processed is DELETED here. It has done its work, and its trail ends
   * there. That is the price of gruelbox owning the table. So the key is free, and this answers
   * <code>null</code>.
   * <p>
   * A BLOCKED entry gives its key away and stays. It waits for a person. The stores which
   * VanillaBP writes itself free the key of such an entry when they block it, so the application
   * can plan the operation again. The unique constraint of gruelbox covers a blocked row too, and
   * its table has no second column the key could move into. So the key is set to
   * <code>null</code> here, where it is needed. Gruelbox allows any number of rows without a
   * unique request ID. The write counts up the <code>version</code> of gruelbox. So a listener
   * which is about to open the row again loses the optimistic lock of gruelbox, and the row stays
   * blocked. See decision 5 in the repository's DECISIONS.md.
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
        // 0 rows: the retention cleanup of the dispatcher was faster, and that frees the
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
   * Takes the key away from a blocked entry, so the operation it failed for can be planned again.
   * Otherwise the row stays as it is: blocked, with its attempts, its invocation and the payload it
   * names, for whoever repairs it. If it is opened again later, it is dispatched without a key. So
   * the operation may reach the BPMS twice. The stores which VanillaBP writes itself document the
   * same gap for a blocked entry.
   * <p>
   * 0 rows means that somebody opened or removed the entry between the read and this write. The
   * insert which follows then meets the key again or not, and the unique constraint of gruelbox
   * decides, as for two nodes which schedule at the same moment.
   *
   * @param connection The connection bound to the transaction of the caller
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
   * Deletes the entry which a younger call replaces, so its unique request ID is free for that
   * call. That is what replacing means on a store whose API does not know it.
   * <p>
   * Two things have to say that no dispatch has taken the entry, because gruelbox reaches a
   * dispatch in two ways. A FLUSH first pushes the entry back, which counts up the
   * <code>version</code> of gruelbox. The delete carries <code>version = 0</code>, the optimistic
   * lock of gruelbox. So this is the same race the flush runs, decided by the same column. A
   * commit, on the other hand, submits the entry at once and writes nothing. So the row still looks
   * untouched while the dispatch holds it. For that case,
   * {@link GruelboxRedispatchAwareSubmitter#isBeingDispatched(String)} is asked.
   * <p>
   * Asking matters, because gruelbox locks the row of an entry it dispatches, and it keeps the
   * lock until the handler returned. A delete which met that lock would make the transaction of
   * the application wait for a remote call. The register answers for this application only. The
   * gap left is an instance which dispatches an entry while another instance replaces it. That is a
   * workflow written by two instances at once, and VanillaBP calls that the business of the
   * application anyway.
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
   * The payload reference which an entry of this store names, read from the invocation gruelbox
   * serialized. There is no column for it. Gruelbox keeps a call as one serialized invocation. The
   * arguments of that invocation are the six strings {@link GruelboxPhaseTwoDispatch#dispatch}
   * takes. The last of them is the serialized {@link PhaseTwoCall#args()}.
   * <p>
   * It is read with the serializer of gruelbox, which wrote it. This method does not understand an
   * entry which this store did not write, or one written by a persistor with a serializer of the
   * application. The reference then stays unknown, and the replaced payload waits for the
   * housekeeping. The housekeeping removes it once no entry names it any more. Nothing else
   * changes.
   *
   * @param invocation The serialized invocation of the entry
   * @return The reference, or <code>null</code> if the entry names none
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
   * What this store needs to know about the entry which a younger call meets: which row it is,
   * whether a dispatch has taken it (gruelbox then counts up the version), and which payload it
   * names.
   *
   * @param id The id of the entry
   * @param version The counter of the optimistic lock of gruelbox. Zero means untouched
   * @param payloadReference The reference of its payload, or <code>null</code>
   */
  private record WaitingEntry(String id, int version, String payloadReference) {
  }

  /**
   * A younger call took the place of the entry which was waiting. It is logged at DEBUG, for the
   * same reason as a discard: the caller asked for it, and under a backlog it happens as often as
   * reports are planned.
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
   * A younger call could not take the place of the entry it meant to replace, because a dispatch
   * had claimed it. It becomes an entry of its own, so the handler is called twice. That is worth a
   * line, because an application which counts its reports finds the second one here.
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
   * The technical half of a discarded schedule. There are two causes: a dispatch which was
   * delivered again, or an operation which lost against one which still waits. The store cannot
   * tell which one it was. So the core reports it to the caller, and this line stays at DEBUG.
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
