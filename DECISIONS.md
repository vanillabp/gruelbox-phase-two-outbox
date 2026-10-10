# Decision log

Decisions this repository's code points at. A number is handed out once and never reused or
renumbered, so a citation stays resolvable; a decision which gets overturned keeps its entry,
marked as superseded and naming the entry which replaced it.

A citation in code reads `see decision 2 in the repository's DECISIONS.md`, and it names an entry
of THIS repository only. A decision which the platform shares has its own entry in
`adapter-platform-integration`, written from that side; a pointer into another repository is the
fragile kind this log exists to avoid.

Links below point into this repository's [`README.md`](./README.md), which carries the detail an
entry deliberately leaves out.

### 1. The store lives outside the platform

`PhaseTwoOutbox` is an interface with several implementations, and VanillaBP writes three of them
itself: one table on a relational database, one collection on MongoDB, and the dispatcher which
serves both. This one is the fourth, and it is the only one which needs a third party library.

A library in the platform is paid for by everybody. It is on the classpath of every application
which uses VanillaBP on Spring Boot, it is scanned there, it is in every dependency report, and it
has to be answered for in every CVE report, whether an application ever builds this store or not.
That was worth it while gruelbox was what VanillaBP used itself. Since VanillaBP writes its own
outbox, the price buys one group of users something and costs all the others.

So the store moved here. The repository that everybody builds carries no gruelbox, and an
application which wants this store adds one artifact. What it does not have to change is its
configuration: the section stays `vanillabp.outbox.gruelbox.*` and the table stays `TXNO_OUTBOX`,
so an application which ran this store as part of VanillaBP swaps a dependency and nothing else.

The same argument put `hazelcast-shared-election-cache` outside, which is the repository this one
is modelled on: a technique not everybody wants, behind an interface everybody has.

The version line is this repository's own. An upgrade of gruelbox must not wait for a release of
the platform, and a release of the platform must not be held up by this store. The README says
which platform versions a version of this artifact works with.

### 2. What this store cannot do, and why it is offered anyway

gruelbox owns its table, its dispatch and its retry policy. This artifact is a thin layer over
them, and that thinness is the point: an application which ran gruelbox before keeps the rows it
already has, the tooling it built around that table and the failure modes it knows.

The price is a list of things the stores VanillaBP writes itself do and this one does not.

- **One retry distance.** gruelbox schedules every failed attempt at the one distance it was built
  with, so `vanillabp.outbox.attempt-frequency` is that distance and `max-attempt-frequency`, with
  the doubling it caps, has no effect. A window an adapter names still lands on the row, written by
  the failure listener, because a window is what the adapter knows about its BPMS.
- **No lanes, so no order.** The stores VanillaBP writes itself dispatch the operations of one
  aggregate in the order they were scheduled. Here an entry reaches a dispatch two ways - gruelbox
  submits it the moment the scheduling transaction commits, and a flush carries whatever else is
  due - so two operations of one workflow race. The promise an application is given is the weaker
  one, that an operation which has to go first gets a transaction of its own, and this store keeps
  that one.
- **No age of the oldest waiting entry.** gruelbox has no column for the moment an entry was
  written. It puts that moment into `nextAttemptTime` and overwrites it at the first flush, so an
  entry which waited is exactly the one whose age cannot be read. The wait of a dispatched entry is
  published, because the submitter still sees it.
- **No adapter id at the start.** A call is one serialized invocation, so the ids a backlog is
  waiting for cannot be read without deserializing the whole table. What the start of the other
  stores says is said at the first dispatch instead, where the entry is read anyway, and it is said
  once per adapter id however long the backlog is.
- **A blocked entry holds its key.** *Superseded by decision 5.* gruelbox keeps the unique request id of a blocklisted row
  until the row is removed, so the operation which failed cannot be scheduled again in the
  meantime.

Every one of these is in the README as well, because an application choosing a store reads the
README and not this log.

### 3. gruelbox' table stays gruelbox', and VanillaBP's tables stay VanillaBP's

`TXNO_OUTBOX` has gruelbox' columns and is created by gruelbox' own schema migration.
`vanillabp.outbox.jdbc.table`, which names the table VanillaBP writes itself, has no say over it,
and nothing here adds a column to it. Two things follow.

The schema is handed over differently. `io.vanillabp:vanillabp-schema` carries the statements for
the tables VanillaBP writes and cannot carry this one, so an application which switches
`vanillabp.outbox.create-schema` off applies gruelbox' statements itself and gets them from
`com.gruelbox.transactionoutbox.DefaultPersistor.writeSchema(Writer)`. The startup checks that the
table is there and says so, because a missing table would otherwise surface at the first workflow
started on a remote BPMS.

The payload of a call lies beside the entry. A phase-two call may carry bytes, and a row of
gruelbox' table has no room for them, so they go into a table of VanillaBP's own and the entry
names them. Finding the payloads which no entry names any more therefore costs a scan of the
entries: the reference sits inside the serialized invocation, and there is no column to join on.
On the stores VanillaBP writes itself that question runs over an index.

### 4. Waiting for a BPMS uses time, not attempts

The adapter's window means the same on this store as on the stores VanillaBP writes itself, and so
does the end of a wait. An entry whose dispatch an adapter answers with `PhaseTwoRetryLater` uses no
attempt of `vanillabp.outbox.block-after-attempts`. It is due again after the window the adapter
named. It is blocked once `vanillabp.outbox.wait-for-visibility-at-most` passed since it was
written. `PhaseTwoOutboxProperties#hasWaitedForVisibilityLongEnough` of the platform is the rule,
and the platform asks the same method in its own stores. The platform states the reason in its own
log: counting those answers had blocked entries after about eight minutes of a stopped Camunda 8
exporter, while a database which was away for hours blocked nothing.

gruelbox does not make this easy, and three things follow from that.

- **gruelbox counts first.** It counts the failed attempt, writes it and only then calls the
  listener. `GruelboxPhaseTwoFailureListener` therefore takes the attempt back in the same write
  which sets the window. A block for waiting too long keeps the one attempt gruelbox counted, the
  same as every other block.
- **gruelbox keeps no moment of writing.** The listener puts that moment into the session of the
  stored invocation when gruelbox builds a new entry (`extractSession`). The session is gruelbox'
  own place for what an add-on keeps next to a call, so no column is added to `TXNO_OUTBOX` and
  decision 3 holds. A younger call which replaces a waiting entry is a new entry, so its wait
  starts again. An entry without the moment, written by an earlier version of this artifact or by
  an outbox built without the listener, keeps the old rule: the answer counts as an attempt, and
  the attempt budget ends the wait. That entry ends by the other budget, and it is not lost.
- **gruelbox decides about its own block before the listener runs.** Where the attempt it just
  counted was the last one of the budget, it has blocked the row already. If the wait is not over,
  the listener opens the entry again, because the answer is looked at before the budget, as on the
  other stores. gruelbox still writes its ERROR line "Blocking failing entry" and calls the
  `blocked` method of the listeners chained after VanillaBP's. Both are wrong in that case, and
  nothing here can stop them. It only happens where earlier real failures used up all attempts but
  one, or where the budget is a single attempt.

The moment in the session could also give this store an age of its oldest waiting entry, which
decision 2 says it cannot report. It does not do so yet. Reading it needs every waiting invocation
deserialized, which is the cost decision 2 declines for the adapter ids at a start.

### 5. A blocked entry gives its key away when the same operation is planned again

*Supersedes the last bullet of decision 2 ("A blocked entry holds its key").*

The stores VanillaBP writes itself free the key of an entry at the moment they block it. The row
stays for whoever repairs it, and the application can plan the same operation again. This store did
not. gruelbox' unique constraint on `uniqueRequestId` spans a blocked row as well, so the operation
which failed could not be planned again until somebody removed the row. The answer "already planned"
looks exactly like a correct deduplication, so nobody noticed.

It was measured on 2026-10-06 with `APermanentFailureOnGruelboxIsBlockedAndFreesItsKeyTest`, H2,
gruelbox 7.1.750, `attempt-frequency` and `poll-interval` at half a second. A `PhaseTwoPermanentFailure`
was attempted once and blocked: one attempt in the row and one call of the handler, still one after two
seconds. The next `schedule` of the same key returned `false` and nothing was dispatched.

The store now frees the key when it is asked for. `schedule` reads the row of the key anyway. Where
that row is blocked and not processed, it sets `uniqueRequestId` to `NULL` and counts `version` up, in
the caller's transaction, and then writes the new entry. The blocked row keeps everything else.

Freeing the key at the moment of the block was weighed and not taken. The failure listener would need
the data source and the table name next to gruelbox' persistor, and it would only catch the blocks it
writes itself. An entry gruelbox blocked without VanillaBP's listener, or one blocked by an earlier
version of this artifact, would keep its key. Freeing it on demand covers all of them and costs no
extra statement, because the row is read before every schedule already.

`version` is counted up because gruelbox' listener may open a row again right after gruelbox blocked
it (decision 4). That write carries `version` in its condition, so it loses against this one and the
row stays blocked, rather than being open and without a key next to the new entry.

The price is the one the other stores pay as well. A blocked row which is opened again later is
dispatched without a key. If the operation was planned again in the meantime, it reaches the BPMS
twice. The README says to check `uniqueRequestId` before opening an entry for that reason.

### 6. A younger call replaces a waiting entry by deleting its row and scheduling again

*This paragraph moved here from decision 68 of `adapter-platform-integration`, which sets the rule
for every store: the youngest call replaces the entry still waiting, and only where the call says
so. It describes code of this repository, so it is kept here, word for word. Decision 68 there now
points here.*

Gruelbox has no API for replacing, so there the row goes: the waiting entry is deleted and the
younger call is scheduled under the same `uniqueRequestId`, in the caller's transaction. Two
things have to agree that no dispatch holds it. `version = 0` is gruelbox' own optimistic lock
and covers every entry a flush picked up, on any instance. A commit, though, submits its entry
straight away and writes nothing, so the row still reads as untouched while gruelbox holds it
with `SELECT ... FOR UPDATE`, and a delete meeting that lock would make the application's
transaction wait for a remote call - which is what an outbox exists to prevent. That case is
asked of a register the submitter keeps, which is where gruelbox already hands every entry
over before it invokes anything. The register answers for its own instance. What it leaves is
one instance dispatching an entry while another replaces it, which means two instances writing
one workflow at once, and VanillaBP names that the application's own business anyway.
