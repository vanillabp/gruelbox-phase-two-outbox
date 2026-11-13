# Decision log

The code of this repository points at the decisions below. A number is handed out once. It is never
reused or renumbered, so a citation can always be followed. A decision which is overturned keeps its
entry. The entry is marked as superseded and names the entry which replaced it.

A citation in code reads `see decision 2 in the repository's DECISIONS.md`. It names an entry of THIS
repository only. A decision the platform shares has its own entry in `adapter-platform-integration`,
written from that side. A pointer into another repository breaks easily, and this log exists to avoid
that kind.

The links below point into this repository's [`README.md`](./README.md). The README has the details
an entry leaves out on purpose.

### 1. The store lives outside the platform

`PhaseTwoOutbox` is an interface with several implementations. VanillaBP writes three of them itself:
one table on a relational database, one collection on MongoDB, and the dispatcher which serves both.
This one is the fourth. It is the only one which needs a third party library.

Everybody pays for a library in the platform. It is on the classpath of every application which uses
VanillaBP on Spring Boot. It is scanned there, it shows up in every dependency report, and every CVE
report (a published security vulnerability) about it needs an answer. That is true whether an
application ever builds this store or not. The price was worth it while VanillaBP used gruelbox
itself. Now that VanillaBP writes its own outbox, the price buys something for one group of users and
costs all the others.

So the store moved here. The repository everybody builds carries no gruelbox. An application which
wants this store adds one artifact. It does not have to change its configuration: the section stays
`vanillabp.outbox.gruelbox.*` and the table stays `TXNO_OUTBOX`. An application which ran this store
as part of VanillaBP swaps a dependency and nothing else.

The same argument put `hazelcast-shared-election-cache` outside, and this repository is modelled on
that one: a technique not everybody wants, behind an interface everybody has.

This repository has its own version line. An upgrade of gruelbox must not wait for a release of the
platform, and a release of the platform must not wait for this store. The README says which platform
versions work with which version of this artifact.

### 2. What this store cannot do, and why it is offered anyway

gruelbox owns its table, its dispatch and its retry rules. This artifact is a thin layer over them,
and that is on purpose. An application which ran gruelbox before keeps the rows it already has, the
tools it built around that table, and the failures it knows.

The price is a list of things the stores VanillaBP writes itself do and this one does not.

- **One retry distance.** gruelbox plans every failed attempt at the one distance it was built with.
  So `vanillabp.outbox.attempt-frequency` is that distance, and `max-attempt-frequency`, with the
  doubling it limits, has no effect. A window an adapter names is still written to the row, by the
  failure listener. The window is what the adapter knows about its BPMS.
- **No lanes, so no order.** The stores VanillaBP writes itself dispatch the operations of one
  aggregate in the order they were scheduled. Here an entry reaches a dispatch in two ways. gruelbox
  submits it as soon as the scheduling transaction commits, and a flush carries everything else which
  is due. So two operations of one workflow race each other. An application is given the weaker
  promise: an operation which has to go first gets a transaction of its own. This store keeps that
  promise.
- **No age of the oldest waiting entry.** gruelbox has no column for the moment an entry was
  written. It puts that moment into `nextAttemptTime` and overwrites it at the first flush. So an
  entry which waited is exactly the one whose age cannot be read. The wait of a dispatched entry is
  published, because the submitter still sees it.
- **No adapter id at the start.** A call is one serialized invocation. The ids a backlog waits for
  cannot be read without deserializing the whole table. So what the other stores say at the start is
  said here at the first dispatch, which reads the entry anyway. It is said once per adapter id,
  however long the backlog is.
- **A blocked entry holds its key.** *Superseded by decision 5.* gruelbox keeps the unique request
  id of a blocked row until the row is removed. So the operation which failed cannot be scheduled
  again until then.

Each of these is in the README as well. An application which chooses a store reads the README, not
this log.

### 3. gruelbox' table stays gruelbox', and VanillaBP's tables stay VanillaBP's

`TXNO_OUTBOX` has gruelbox' columns, and gruelbox' own schema migration creates it.
`vanillabp.outbox.jdbc.table` names the table VanillaBP writes itself and has no say over this one.
Nothing here adds a column to it. Two things follow.

First, the schema is handed over in a different way. `io.vanillabp:vanillabp-schema` carries the
statements for VanillaBP's tables and cannot carry this one. An application which switches
`vanillabp.outbox.create-schema` off applies gruelbox' statements itself. It gets them from
`com.gruelbox.transactionoutbox.DefaultPersistor.writeSchema(Writer)`. The startup checks that the
table exists and says so if it does not. Otherwise a missing table would only show up at the first
workflow started on a remote BPMS.

Second, the payload of a call lies beside the entry. A phase-two call may carry bytes, and a row of
gruelbox' table has no room for them. So they go into a table of VanillaBP's own, and the entry names
them. To find the payloads which no entry names any more, the store has to scan the entries. The
reference sits inside the serialized invocation, and there is no column to join on. On the stores
VanillaBP writes itself, that question is answered from an index.

### 4. Waiting for a BPMS uses time, not attempts

The adapter's window means the same on this store as on the stores VanillaBP writes itself. So does
the end of a wait. An adapter may answer a dispatch with `PhaseTwoRetryLater`. That entry uses no
attempt of `vanillabp.outbox.block-after-attempts`. It is due again after the window the adapter
named. It is blocked once `vanillabp.outbox.wait-for-visibility-at-most` has passed since it was
written. The rule is `PhaseTwoOutboxProperties#hasWaitedForVisibilityLongEnough` of the platform, and
the platform's own stores ask the same method. The platform gives the reason in its own log. Counting
those answers had blocked entries after about eight minutes of a stopped Camunda 8 exporter. A
database which was away for hours blocked nothing.

gruelbox does not make this easy. Three things follow.

- **gruelbox counts first.** It counts the failed attempt, writes it, and only then calls the
  listener. So `GruelboxPhaseTwoFailureListener` takes the attempt back in the same write which sets
  the window. A block for waiting too long keeps the one attempt gruelbox counted, like every other
  block.
- **gruelbox keeps no moment of writing.** When gruelbox builds a new entry, the listener puts that
  moment into the session of the stored invocation (`extractSession`). The session is gruelbox' own
  place for what an add-on keeps next to a call. So no column is added to `TXNO_OUTBOX`, and
  decision 3 holds. A younger call which replaces a waiting entry is a new entry, so its wait starts
  again. Some entries have no such moment: those written by an earlier version of this artifact, or by
  an outbox built without the listener. They keep the old rule. The answer counts as an attempt, and
  the attempt budget ends the wait. Such an entry ends by the other budget, and it is not lost.
- **gruelbox decides about its own block before the listener runs.** If the attempt it just counted
  was the last one of the budget, gruelbox has already blocked the row. If the wait is not over, the
  listener opens the entry again. As on the other stores, the answer is looked at before the budget.
  gruelbox still writes its ERROR line "Blocking failing entry". It also calls the `blocked` method of
  the listeners chained after VanillaBP's. Both are wrong in that case, and nothing here can stop
  them. It only happens if earlier real failures used up all attempts but one, or if the budget is a
  single attempt.

The moment in the session could also give this store the age of its oldest waiting entry. Decision 2
says this store cannot report that age. It does not do so yet. To read it, every waiting invocation
would have to be deserialized. That is the cost decision 2 turns down for the adapter ids at a start.

**Addendum (story 2061): a deadline the adapter names ends the wait.** Decision 4 stays as it is.
`PhaseTwoRetryLater` may carry a deadline, for example a message time-to-live. Where it does, the
deadline replaces `vanillabp.outbox.wait-for-visibility-at-most` for that entry. The rule is now
`PhaseTwoOutboxProperties#hasWaitedLongEnough`, which takes the deadline. The platform's own stores
ask the same method, so a time-to-live means the same on every store. Without this, the default of
about four hours cut a longer time-to-live short, and the ERROR named the wrong setting. The ERROR
now names the deadline. An entry which knows no moment of writing keeps the old rule, even with a
deadline. The attempt budget ends its wait.

### 5. A blocked entry gives its key away when the same operation is planned again

*Supersedes the last bullet of decision 2 ("A blocked entry holds its key").*

The stores VanillaBP writes itself free the key of an entry at the moment they block it. The row stays
for whoever repairs it, and the application can plan the same operation again. This store did not do
that. gruelbox' unique constraint on `uniqueRequestId` covers a blocked row as well. So the operation
which failed could not be planned again until somebody removed the row. The answer "already planned"
looks exactly like a correct deduplication, so nobody noticed.

It was measured on 2026-10-06 with `APermanentFailureOnGruelboxIsBlockedAndFreesItsKeyTest`, H2,
gruelbox 7.1.750, and `attempt-frequency` and `poll-interval` at half a second. A
`PhaseTwoPermanentFailure` was attempted once and blocked. The row showed one attempt, and the handler
was called once. Two seconds later it was still once. The next `schedule` of the same key returned
`false`, and nothing was dispatched.

Now the store frees the key when the key is asked for. `schedule` reads the row of the key anyway. If
that row is blocked and not processed, the store sets `uniqueRequestId` to `NULL` and counts `version`
up, in the caller's transaction. Then it writes the new entry. The blocked row keeps everything else.

Freeing the key at the moment of the block was considered and not chosen. The failure listener would
need the data source and the table name next to gruelbox' persistor. And it would only catch the
blocks it writes itself. An entry gruelbox blocked without VanillaBP's listener would keep its key,
and so would an entry blocked by an earlier version of this artifact. Freeing the key on demand covers
all of them. It costs no extra statement, because the row is read before every schedule already.

`version` is counted up for this reason. gruelbox' listener may open a row again right after gruelbox
blocked it (decision 4). That write has `version` in its condition. So it loses against this one, and
the row stays blocked. Without it, the row could be open and without a key, next to the new entry.

The price is the same one the other stores pay. A blocked row which is opened again later is
dispatched without a key. If the operation was planned again in the meantime, it reaches the BPMS
twice. That is why the README says to check `uniqueRequestId` before you open an entry.

### 6. A younger call replaces a waiting entry by deleting its row and scheduling again

*This paragraph moved here from decision 68 of `adapter-platform-integration`. Decision 68 sets the
rule for every store: the youngest call replaces the entry still waiting, but only where the call says
so. The paragraph describes code of this repository, so it is kept here. Decision 68 there now points
here.*

Gruelbox has no API for replacing, so the row goes. The waiting entry is deleted, and the younger call
is scheduled under the same `uniqueRequestId`, in the caller's transaction. Two checks have to agree
that no dispatch holds the entry.

The first is `version = 0`. That is gruelbox' own optimistic lock, and it covers every entry a flush
picked up, on any instance. A commit, though, submits its entry straight away and writes nothing. So
the row still looks untouched while gruelbox holds it with `SELECT ... FOR UPDATE`. A delete which
meets that lock would make the application's transaction wait for a remote call. Preventing exactly
that is what an outbox is for.

So that case is the second check, and it asks a register the submitter keeps. gruelbox already hands
every entry over to the submitter before it invokes anything. The register answers for its own
instance only. That leaves one case: one instance dispatches an entry while another replaces it. Then
two instances write one workflow at the same time, and VanillaBP calls that the application's own
business anyway.

### 7. A snapshot reaches Maven Central again, without a sources jar, and a refused request is repeated

Proposed by stories 2162 and 2160, 2026-11-13. Story 2158 brought the profile `publish-snapshots`,
which sends a snapshot to GitHub Packages and to the snapshot repository of Maven Central in one
build. It wrote no decision here. `adapter-platform-integration` made the same change, see decision
139 in adapter-platform-integration for the URL and decision 140 in adapter-platform-integration for
the sources jar and the repeats.

**The snapshot reaches Maven Central again.** The profile `publish-snapshots` sets the snapshot
repository of the POM to GitHub Packages, because that is where `maven-deploy-plugin` uploads.
`central-publishing-maven-plugin` 0.12.0 reads the same setting: where its configuration names no
`centralSnapshotsUrl`, it uploads a snapshot to the snapshot repository of the POM. So since story
2158 every snapshot of this repository went to GitHub Packages twice and never to Maven Central, and
the build stayed green. The configuration of the plugin in the profile now names
`https://central.sonatype.com/repository/maven-snapshots/` as `centralSnapshotsUrl`. The modules
which are not published keep `skip.central.release`, so the list of published modules is the same as
before.

**A snapshot carries no sources jar.** Maven Central answers the GitHub runners with 429 Too Many
Requests, and every file a publish uploads is one more request. The root POM no longer binds
`maven-source-plugin`, so a snapshot carries the jar, the POM and their checksums, and nothing else.
GitHub Packages gets no sources jar either, because both repositories get their files from one
build. A release still carries a sources jar and a javadoc jar: the release workflow runs with the
profile `release` of `io.vanillabp:release-parent`, which binds both. A developer who opens a class
of a snapshot in the IDE sees decompiled code again, as before the root POM bound the plugin.
Modules which build no jar and publish nothing (`test-coverage-report/coverage-gate`) bind the
plugin themselves. Without a file of its own in a module, `central-publishing-maven-plugin` stops
the whole publish, also for a module it leaves out, and the sources jar is the one file such a
module has.

**A refused request is repeated.** `.mvn/maven.config` tells Maven to repeat a request which a
server answers with 429 or 503. It repeats a request at most twice and waits as long as the
`Retry-After` header asks, up to 35 minutes. Central blocks a runner for about half an hour, and
every request during the block makes it longer, so waiting once is better than asking again. Maven
also uploads one file after the other instead of five at once. The file applies to every build of
this repository, on a laptop and in a pull request as well.

**Where it is referred to:** the comments at `centralSnapshotsUrl` and `skipPublishing` in the
profile `publish-snapshots` of `pom.xml`, the comment in `pom.xml` where `maven-source-plugin` used
to be bound, the comment at `maven-source-plugin` in the POM of each module which builds no jar, and
the comments in `.mvn/maven.config`. None of them cites a number.
