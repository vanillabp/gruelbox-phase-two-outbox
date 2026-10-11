![Header](./readme/vanillabp-headline.png)

# VanillaBP phase-two outbox on gruelbox

[![Apache License V.2](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](./LICENSE)

This is an implementation of [VanillaBP](https://www.vanillabp.io)'s `PhaseTwoOutbox`. It stores the
phase-two calls of a Spring Boot application in the
[gruelbox transaction outbox](https://github.com/gruelbox/transaction-outbox). Without it, VanillaBP
stores them in a table it writes itself.

If you want to **use** this store, read this file. It is the only documentation of this repository,
because there is not enough to say for a wiki. What a phase-two outbox is for is part of the platform.
The platform explains it on the wiki page
[Spring Boot integration](https://github.com/vanillabp/adapter-platform-integration/wiki/Spring-Boot-integration#what-the-outbox-guarantees).

## Documentation and supported platform

[![Coverage](https://img.shields.io/badge/dynamic/regex?url=https%3A%2F%2Fvanillabp.github.io%2Fgruelbox-phase-two-outbox%2Fspring-boot-report%2Findex.html&search=Total.*%3F.([0-9]%2B)[^0-9]*%3F%25&replace=%241%25&flags=m&label=Coverage&color=green&cacheSeconds=60)](https://vanillabp.github.io/gruelbox-phase-two-outbox/spring-boot-report)

Spring Boot with JPA, and nothing else. gruelbox joins an entry to the transaction through the Spring
transaction manager. So there is no Quarkus version of this store, and there will not be one. A Quarkus
application which sets `vanillabp.outbox.gruelbox.*` is told so while it is built.

## Who wants this

Almost nobody, to be honest. VanillaBP writes its own phase-two outbox on every platform it supports,
and that is the store to use.

This one exists for applications which ran gruelbox before. VanillaBP used to store its phase-two
calls there. Such an application has rows in `TXNO_OUTBOX`, tools which read that table, and operators
who know it. Add this artifact and you keep all of that. The configuration section is the same, the
table is the same, and nothing has to be drained before the upgrade.

An application which starts today takes the store VanillaBP writes itself. That store answers more
questions than this one, and the list [below](#what-this-store-does-not-do) says which.

## Coordinates

All modules are `1.0.0-SNAPSHOT`, groupId `io.vanillabp`. This repository has its own version line.
An upgrade of gruelbox must not drag the platform's release along, and a release of the platform must
not wait for this store.

There is one module and one artifact. `spring-boot/` builds `io.vanillabp:gruelbox-phase-two-outbox`,
which is the store together with its auto-configuration. The directory name says which platform the
code is for. There will be no second one, because gruelbox joins an entry to the transaction through
the Spring transaction manager.

```xml
<dependency>
  <groupId>io.vanillabp</groupId>
  <artifactId>gruelbox-phase-two-outbox</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```

The dependency is all the wiring you need. This artifact registers the outbox bean VanillaBP looks
for. So as soon as it is on the classpath, the store VanillaBP writes itself steps back. An
application which brings a `PhaseTwoOutbox` of its own still wins over both.

### Which versions work together

| This artifact |           VanillaBP            | gruelbox  |
|---------------|--------------------------------|-----------|
| `1.0.0`       | `2.0` and later on Spring Boot | `7.1.750` |

This artifact is compiled and tested against the gruelbox version in the table. An application which
manages a different version gets that one. Nothing here uses an API which changed between the 7.1
patches.

### Coming from VanillaBP 1

A version 1 application on Spring Boot with JPA already ran gruelbox, without knowing it. Adding this
artifact keeps it there. The platform's `UPGRADE.md` says the same from its side. Changing nothing is
also a valid answer. VanillaBP then writes its entries into its own table. At startup it reports what
is left in `TXNO_OUTBOX`, so nothing gets lost without a word.

## Switching it off again

```yaml
vanillabp:
  outbox:
    gruelbox:
      enabled: false
```

This key is for an application which cannot remove the dependency. Maybe a second artifact of its own
pulls it in, or a release is not worth it just for this. With `false`, the store VanillaBP writes
itself takes over. The entries then go into `VANILLABP_PHASE_TWO_OUTBOX`, and the next startup reports
what is left in `TXNO_OUTBOX`. So drain the table before you switch.

The whole rule fits in two lines. The artifact asks for this store. `false`, or no artifact at all,
hands the entries to the store VanillaBP writes itself.

`vanillabp.outbox.jdbc.enabled=false` is a third answer, and it means what it always meant: no
relational outbox at all, this one included. It is for an application which keeps its phase-two
entries in a place VanillaBP does not manage.

Only this artifact reads the key. If an application sets it but does not have this artifact, nothing
reads that line. VanillaBP's own store serves, the start goes on, and nobody is told. A property of an
artifact which is not there has nothing to say. VanillaBP does report what is left undispatched in
`TXNO_OUTBOX`, and it does so at every start until the table is empty.

## Waiting for a BPMS which is behind

A BPMS (business process management system) may not report a workflow yet. One example is a Camunda 8
cluster whose exporter is behind. The adapter then answers "not yet" and names a window. This store
handles that answer like the stores VanillaBP writes itself. The entry is due again after the window.
The answer uses no attempt of `vanillabp.outbox.block-after-attempts`, because a read model which is
behind is not a failure of the entry.

Time is what ends a wait which would otherwise go on forever. Once
`vanillabp.outbox.wait-for-visibility-at-most` has passed since the entry was written, the next "not
yet" blocks the entry. An ERROR reports it and names that key. If the key is not set, its value is the
time which the attempts of `vanillabp.outbox.block-after-attempts` take, with the growing backoff of
the stores VanillaBP writes itself. With the defaults that is 3 hours and 52.5 minutes.

gruelbox counts every failed attempt itself. VanillaBP's listener on the outbox takes the attempt back
in the same write which sets the window. gruelbox also has no column for the moment an entry was
written. So the listener stores that moment in the session which gruelbox keeps with every call. Two
kinds of entry have no such moment: one written by an earlier version of this artifact, and one written
by an outbox built without VanillaBP's listener. For them, the answer "not yet" still counts as an
attempt, and the attempt budget ends the wait. No entry is lost either way.

## A blocked entry and the way back

An entry is blocked in three cases:

- The adapter or an extension said that trying again cannot help (`PhaseTwoPermanentFailure`), and the
  entry had its one attempt.
- `vanillabp.outbox.block-after-attempts` attempts failed.
- The entry waited longer than `vanillabp.outbox.wait-for-visibility-at-most` for its BPMS.

Nothing dispatches a blocked entry again, and nothing deletes it. It waits for a person. The ERROR which
reports it names the workflow and the entry id, and it links to this section.

For the stores VanillaBP writes itself, the same steps are on the wiki page
[Blocked outbox entries](https://github.com/vanillabp/adapter-platform-integration/wiki/Blocked-outbox-entries).
Here the table is gruelbox' `TXNO_OUTBOX`, and the steps differ a little.

### Finding it

```sql
SELECT id, uniqueRequestId, attempts, lastAttemptTime, invocation
  FROM TXNO_OUTBOX
 WHERE blocked = TRUE AND processed = FALSE;
```

`id` is the id the ERROR names. `invocation` is the call as JSON. Its arguments are, in this order: the
operation, the workflow module, the BPMN process, the aggregate id, the adapter id and the arguments of
the operation. A database without a boolean type, such as SQL Server, takes `1` and `0` instead of
`TRUE` and `FALSE`.

### Opening it again

Fix the cause first. Then:

```sql
UPDATE TXNO_OUTBOX
   SET blocked = FALSE, attempts = 0, nextAttemptTime = CURRENT_TIMESTAMP
 WHERE id = '...' AND blocked = TRUE AND processed = FALSE;
```

gruelbox' own `TransactionOutbox.unblock(id)` also writes the first two columns, inside a transaction
of the application. It leaves the due time as it was, and that time has passed already. Either way, the
next flush takes the entry, and the next flush is at most `vanillabp.outbox.poll-interval` away.
`APermanentFailureOnGruelboxIsBlockedAndFreesItsKeyTest` opens an entry with `unblock` and checks that
it is dispatched.

An entry which was blocked because it waited too long for its BPMS needs one more thought. The moment
it was written is stored in its `invocation`, under the session key `vanillabp.writtenAt`. Opening the
entry does not change that moment. So if the BPMS still answers "not yet", the entry is blocked again
at that first answer. Open it only once the BPMS reports the workflow.

### Deleting it

```sql
DELETE FROM TXNO_OUTBOX WHERE id = '...' AND blocked = TRUE AND processed = FALSE;
```

After that, the operation is gone. Delete an entry only if nobody wants the operation any more, or if a
younger entry carries it (see below). A payload the entry named stays in
`VANILLABP_PHASE_TWO_OUTBOX_PAYLOAD` for a while. Once `vanillabp.outbox.retention` has passed, the
housekeeping removes every payload which no entry names.

### What happens to the key

A blocked entry keeps its `uniqueRequestId` until the application plans the same operation again. Then
this store takes the key away from the blocked row, so the row's `uniqueRequestId` is empty. It writes
a new entry with that key. The blocked row stays where it is. The stores VanillaBP writes itself end up
in the same place, but they free the key at the moment they block an entry. This store frees it when
the key is asked for, because gruelbox has no second column to move the key into.
`APermanentFailureOnGruelboxIsBlockedAndFreesItsKeyTest` checks this: one attempt, the entry blocked,
and the next plan of the same operation dispatched as an entry of its own.

So look at `uniqueRequestId` before you open a blocked entry:

- It still holds the key. Nobody planned the operation again, and opening the entry is the only way
  the operation happens. While the entry waits, a new plan of the same operation is discarded as usual.
- It is empty. The operation was planned again, and a younger row carries the key. That row may be
  dispatched already. Opening the blocked entry sends the operation a second time, so deleting it is
  usually the better answer.

## What this store does not do

The stores VanillaBP writes itself own their table, their dispatch and their retry rules. This one is
a thin layer over gruelbox', and that is on purpose: the rows stay the rows an application already has.
Four things follow from it.

**One retry distance.** gruelbox plans every failed attempt at the one distance it was built with.
`vanillabp.outbox.attempt-frequency` is that distance. `vanillabp.outbox.max-attempt-frequency`, and
the doubling it limits, have no effect here. A window an adapter names is still written to the row.
The adapter knows when its BPMS can answer. A store which is configured once for all workflows does not
know that.

**No order between two operations of one workflow.** VanillaBP's own stores dispatch the operations of
one aggregate in the order they were scheduled. Here an entry reaches a dispatch in two ways. gruelbox
submits it as soon as the scheduling transaction commits, and a flush carries everything else which is
due. So two operations of one workflow race each other. An application can rely on the weaker promise
which every store keeps: an operation which has to go first gets a transaction of its own.

**No age of the oldest waiting entry.** gruelbox has no column for the moment an entry was written. It
puts that moment into `nextAttemptTime` and overwrites it at the first flush. So the entry which waited
longest is exactly the one whose age cannot be read. The wait of a dispatched entry is published,
because the submitter still sees it.

**The adapter ids of a backlog are named at the first dispatch, not at the start.** A call is one
serialized invocation here. To find out which adapters a backlog waits for, a start would have to
deserialize the whole table. So the first dispatch names them instead, because it reads the entry
anyway. It names each adapter id once, however long the backlog is.

## The tables

`TXNO_OUTBOX` is gruelbox' own table with gruelbox' own columns. Nothing here renames it or adds a
column to it. `vanillabp.outbox.jdbc.table` names the table VanillaBP writes itself and has no say over
this one.

Two more tables belong to VanillaBP. They are created the same way as for every Spring Boot
application. One holds the payload of a call which carries bytes. The other holds the claim a node takes
while it does the housekeeping. A row of gruelbox' table has no room for a payload, which is why the
payload lies beside the entry.

### Handing the schema over

`vanillabp.outbox.create-schema` is `true` by default. The tables are then created while the
application starts: gruelbox' migration creates `TXNO_OUTBOX`, and VanillaBP creates its own two.

Some applications have a schema which is reviewed and released on its own. Such an application sets the
switch to `false` and applies the statements itself. The statements for VanillaBP's tables ship as
`io.vanillabp:vanillabp-schema`. The statements for `TXNO_OUTBOX` are gruelbox', and
`com.gruelbox.transactionoutbox.DefaultPersistor.writeSchema(Writer)` writes them for the database you
configure. With the switch off, the startup checks that `TXNO_OUTBOX` exists, and it names the table if
it does not. Otherwise a missing table would only show up at the first workflow started on a remote
BPMS.

## Bringing your own gruelbox

The beans of this store find each other by name, so an application may replace any single one of them.
The names are constants of `GruelboxPhaseTwoOutboxAutoConfiguration`. The one worth knowing is
`vanillaBpTransactionOutbox`. A bean with that name is the `TransactionOutbox` this store uses. That is
how an application gets another table name, another dialect or a listener of its own.

Two parts must stay. The submitter has to be the one this configuration builds, because it holds an
entry back until VanillaBP has deployed its models. The listener has to include
`GruelboxPhaseTwoFailureListener`, chained with your own by `andThen`. Without it, a permanent failure
is attempted until the budget is used up, and waiting for a BPMS counts attempts again.

## Contributing

[`CONTRIBUTING.md`](./CONTRIBUTING.md) says how to contribute, [`AGENTS.md`](./AGENTS.md) says what the
rules are, and [`DECISIONS.md`](./DECISIONS.md) says why the code looks the way it does.

![Phactum](./readme/phactum.png)
