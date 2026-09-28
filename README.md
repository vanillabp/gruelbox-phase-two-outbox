![Header](./readme/vanillabp-headline.png)

# VanillaBP phase-two outbox on gruelbox

[![Apache License V.2](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](./LICENSE)

An implementation of [VanillaBP](https://www.vanillabp.io)'s `PhaseTwoOutbox` which stores the
phase-two calls of a Spring Boot application in the
[gruelbox transaction outbox](https://github.com/gruelbox/transaction-outbox) instead of the table
VanillaBP writes itself.

Developers who want to **use** it read this file: it is the only documentation of this repository,
because there is not enough of it to justify a wiki. What a phase-two outbox is for belongs to the
platform and is documented there, on the wiki page
[Spring Boot integration](https://github.com/vanillabp/adapter-platform-integration/wiki/Spring-Boot-integration#what-the-outbox-guarantees).

## Documentation and supported platform

[![Coverage](https://img.shields.io/badge/dynamic/regex?url=https%3A%2F%2Fvanillabp.github.io%2Fgruelbox-phase-two-outbox%2Fspring-boot-report%2Findex.html&search=Total.*%3F.([0-9]%2B)[^0-9]*%3F%25&replace=%241%25&flags=m&label=Coverage&color=green&cacheSeconds=60)](https://vanillabp.github.io/gruelbox-phase-two-outbox/spring-boot-report)

Spring Boot with JPA, and nothing else. gruelbox enlists an entry through the Spring transaction
manager, so there is no Quarkus version of this store and there is not going to be one. A Quarkus
application which writes `vanillabp.outbox.gruelbox.*` is told so while it is built.

## Who wants this

Almost nobody, and that is the honest answer. VanillaBP writes its own phase-two outbox on every
platform it supports, and that store is the one to use.

This one exists for applications which ran gruelbox before, because VanillaBP used to store its
phase-two calls there. Such an application has rows in `TXNO_OUTBOX`, tooling which reads that
table and operators who know it. Adding this artifact keeps all of that: the configuration section
is the same, the table is the same, and nothing has to be drained before the upgrade.

An application starting today takes the store VanillaBP writes itself. It answers more questions
than this one does, and the list below says which.

## Coordinates

All modules are `1.0.0-SNAPSHOT`, groupId `io.vanillabp`. The version line is this repository's own:
an upgrade of gruelbox must not drag the platform's release with it, and a release of the platform
must not wait for this store.

One module and one artifact: `spring-boot/` builds `io.vanillabp:gruelbox-phase-two-outbox`, which is
the store together with its auto-configuration. The directory says which platform the code inside is
for, and there will be no second one, because gruelbox enlists an entry through the Spring transaction
manager.

```xml
<dependency>
  <groupId>io.vanillabp</groupId>
  <artifactId>gruelbox-phase-two-outbox</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```

The dependency is the entire wiring. The store VanillaBP writes itself steps back as soon as this
one is on the classpath, because it registers the outbox bean VanillaBP looks for, and an
application which brings a `PhaseTwoOutbox` of its own still wins over both.

### Which versions work together

| This artifact |           VanillaBP            | gruelbox  |
|---------------|--------------------------------|-----------|
| `1.0.0`       | `2.0` and later on Spring Boot | `7.1.750` |

The gruelbox version is the one this artifact is compiled and tested against. An application which
manages a different one gets that one, and nothing here reads an API which moved between the 7.1
patches.

### Coming from VanillaBP 1

A version-1 application on Spring Boot with JPA already ran gruelbox, without knowing. Adding this
artifact is what keeps it there, and `UPGRADE.md` of the platform says it from that side. Changing
nothing is the other valid answer: VanillaBP then writes its entries into its own table and reports
at startup what is left in `TXNO_OUTBOX`, so nothing is lost silently.

## Switching it off again

```yaml
vanillabp:
  outbox:
    gruelbox:
      enabled: false
```

The key exists for an application which cannot take the dependency out - a second artifact of its
own pulls it in, or a release is not worth cutting for it. With `false` the store VanillaBP writes
itself takes over, so the entries go into `VANILLABP_PHASE_TWO_OUTBOX`, and what is left in
`TXNO_OUTBOX` is reported at the next startup: drain the table before you switch.

So the whole rule is two lines. The artifact is what asks for this store. `false`, or no artifact at
all, hands the entries to the store VanillaBP writes itself.

`vanillabp.outbox.jdbc.enabled=false` is the third answer and means what it always meant: no
relational outbox at all, this one included. It is for an application which keeps its phase-two
entries somewhere VanillaBP does not manage.

The key belongs to this artifact and is read by it alone. An application which writes it without
having this artifact writes a line nothing reads: VanillaBP's own store serves, the start does not
stop and nobody is told, because a property of an artifact which is not there has nothing to say. What
VanillaBP does say in that case is what is left undispatched in `TXNO_OUTBOX`, which it says at every
start until the table is empty.

## What this store does not do

The stores VanillaBP writes itself own their table, their dispatch and their retry policy. This one
is a thin layer over gruelbox', and the thinness is the point: the rows stay the rows an
application already has. Five things follow from it.

**One retry distance.** gruelbox schedules every failed attempt at the one distance it was built
with. `vanillabp.outbox.attempt-frequency` is that distance, and `vanillabp.outbox.max-attempt-frequency`
with the doubling it caps has no effect here. A window an adapter names still lands on the row: an
adapter knows when its BPMS can answer, which is more than a store configured once for every
workflow knows.

**No order between two operations of one workflow.** VanillaBP's own stores dispatch the operations
of one aggregate in the order they were scheduled. Here an entry reaches a dispatch two ways -
gruelbox submits it the moment the scheduling transaction commits, and a flush carries whatever else
is due - so two operations of one workflow race. What an application can rely on is the weaker
promise every store keeps: an operation which has to go first gets a transaction of its own.

**No age of the oldest waiting entry.** gruelbox has no column for the moment an entry was written.
It puts that moment into `nextAttemptTime` and overwrites it at the first flush, so the entry which
waited longest is exactly the one whose age cannot be read. The wait of a dispatched entry is
published, because the submitter still sees it.

**The adapter ids of a backlog are named at the first dispatch, not at the start.** A call is one
serialized invocation here, so a start would have to deserialize the whole table to find out which
adapters a backlog is waiting for. It is said at the first dispatch instead, where the entry is read
anyway, and once per adapter id however long the backlog is.

**A blocked entry holds its key.** gruelbox keeps the unique request id of a blocklisted row until
the row is removed, so the operation which failed cannot be scheduled again in the meantime.

## The tables

`TXNO_OUTBOX` is gruelbox' own table with gruelbox' own columns, and nothing here renames it or adds
a column to it. `vanillabp.outbox.jdbc.table`, which names the table VanillaBP writes itself, has no
say over it.

Two more tables belong to VanillaBP and are created the way they are for every Spring Boot
application: the payload of a call which carries bytes, and the claim one node takes while it
house-keeps. A row of gruelbox' table has no room for a payload, which is why it lies beside the
entry.

### Handing the schema over

`vanillabp.outbox.create-schema` is `true` by default, and the tables are then created while the
application starts: gruelbox' migration writes `TXNO_OUTBOX`, VanillaBP writes its own two.

An application whose schema is a reviewed artifact sets the switch to `false` and applies the
statements itself. The ones for VanillaBP's tables ship as `io.vanillabp:vanillabp-schema`. The
statements for `TXNO_OUTBOX` are gruelbox', and
`com.gruelbox.transactionoutbox.DefaultPersistor.writeSchema(Writer)` writes them for the database
you configure. With the switch off the startup checks that `TXNO_OUTBOX` is there and names it if it
is not, because a missing table would otherwise surface at the first workflow started on a remote
BPMS.

## Bringing your own gruelbox

The beans of this store reference each other by name, so an application may replace any single one
of them. The names are constants of `GruelboxPhaseTwoOutboxAutoConfiguration`, and the one worth
knowing is `vanillaBpTransactionOutbox`: a bean of that name is the `TransactionOutbox` this store
uses, which is how an application gets another table name, another dialect or a listener of its own.
Its submitter has to stay the one this configuration builds, because that submitter is what holds an
entry back until VanillaBP has deployed its models.

## Contributing

[`CONTRIBUTING.md`](./CONTRIBUTING.md) says how, [`AGENTS.md`](./AGENTS.md) what the rules are, and
[`DECISIONS.md`](./DECISIONS.md) why the code looks the way it does.

![Phactum](./readme/phactum.png)
