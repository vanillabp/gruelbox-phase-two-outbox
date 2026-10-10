# Working on gruelbox-phase-two-outbox

The implementation of VanillaBP's `PhaseTwoOutbox` which keeps the phase-two calls of a Spring Boot
application in the gruelbox transaction outbox.

Read [`README.md`](./README.md) first. What a phase-two outbox is for is documented on the platform's
side, on the wiki page
[Spring Boot integration](https://github.com/vanillabp/adapter-platform-integration/wiki/Spring-Boot-integration#what-the-outbox-guarantees)
and in the javadoc of `io.vanillabp.integration.spi.PhaseTwoOutbox`; that javadoc is the contract
this repository implements, and it is worth reading before every change here.

## The decision log is binding

[`DECISIONS.md`](./DECISIONS.md) holds the decisions several places in this repository rely on. It is
the ONLY thing the code is allowed to cite, in the plain greppable form
`see decision 2 in the repository's DECISIONS.md`, and only entries of THIS repository.

Read it before you change behaviour. An entry is not background reading, it is the reason the code
around it looks the way it does, so a change which contradicts one is wrong until the entry says
otherwise.

**A decision is changed or replaced only after asking.** Where your change would make an entry
untrue, stop and put the question to the maintainer before you write the change. If the answer is
yes, the same commit updates the log: the old entry STAYS, marked as superseded and naming the entry
which replaced it, and the new decision takes the next free number. Numbers are never reused and
never renumbered, because a citation in an older release still points at them.

Adding an entry has the same rule. A decision earns a number when several places rely on it and
copying the explanation to each of them would rot; anything smaller is a comment where it belongs,
and anything larger is documentation.

## Before you open a pull request

A number your branch hands out can be taken by the time you open the pull request. Another branch was
open at the same time and got there first. So check your numbers against `origin/main` and against
every open pull request, before the pull request exists:

```bash
bin/check-decision-numbers.sh
```

The script reports and changes nothing.

After you renumber, run `bin/check-decision-citations.sh`. It says whether every citation of a
decision still points at an entry, also where a citation is wrapped over two lines. The *Checks*
workflow runs it on every pull request.

`bin/check-orphaned-javadoc.sh` finds a javadoc block standing directly in front of a second one,
which javadoc drops without a word, so the text appears nowhere. Run it when you wrote or moved a
comment. Hang a block it reports back on the element it describes rather than delete it.

## Three rules which are easy to break here

**The thinness is the feature.** This store exists so that an application which ran gruelbox keeps
its rows, its tooling and its failure modes. A change which makes this store behave like the ones
VanillaBP writes itself by holding gruelbox' entries back, by adding a column to its table or by
picking entries in an order of our own takes that away, and an application which wanted that
behaviour has the other store already.

**What this store cannot answer, it says it cannot answer.** `ageOfOldestPendingCall` reports nothing
and `adapterIdsOfPendingCalls` reports nothing at a start, and both say why in their javadoc. A
number this store cannot back must not be reported as if it could.

**Nothing may be carried to a BPMS before the models are there.** `GruelboxRedispatchAwareSubmitter`
holds every entry until the dispatcher starts polling, which happens once per application start and
after VanillaBP deployed. An application which builds its own `TransactionOutbox` bean has to keep
that submitter, and `GruelboxHoldsEntriesBackUntilDispatchingStartedTest` is where the promise lives.

## The extension double exists twice

`SampleExtension` under `io/vanillabp/outbox/gruelbox/it` is a copy. The platform has one of the same
name, in a test module of its own which is not published, so a repository outside the platform cannot
depend on it. Two tests here need such a double: one operation which says it replaces what is still
waiting, a dispatch which can be rejected with a window, and a dispatch which can be held while a test
plans against a claimed entry.

Keeping it as a copy is the decision (Stephan, 2026-09-28): inside VanillaBP what belongs to VanillaBP
is tested, inside this repository only that the contract is kept, and a copy lets the two move at their
own speed. The price is that a change to one of the two does not reach the other, so whoever changes
this one reads the platform's before deciding whether the difference is on purpose. The copy is already
smaller than the platform's: the half which invokes an annotated method through `ExtensionHandlers`
needs the platform's sample extension and stayed there, and here a dispatch writes into the workflow
aggregate through its repository instead.

## How we write

Most people who read this code read English as a second language. A sentence they have to read twice
costs more than the sentence saved. Short sentences, everyday words, one subordinate clause at most,
and the actor named instead of a passive voice.

A javadoc, README or `DECISIONS.md` sentence which promises behaviour is part of the behaviour.
Either a test fails when it stops being true, or the sentence says that it is an assumption and what
would disprove it.

## What code may point at

Nothing which a later change can invalidate without anything noticing: no story or prompt number, no
issue or pull-request number, no chat transcript, no person. Those record a conversation at a point
in time. A decision entry lives next to the code and is overhauled in the same commit, which is what
makes it citable. A decision of ANOTHER repository is the fragile kind too, even when it is
VanillaBP's own: it is renumbered and superseded on a schedule nothing here sees.

Where a name can carry the reason, the name is the better fix. Where it cannot, a comment says why in
its own words, complete where it stands. Only what several places have to carry becomes an entry in
the log.

Commit messages and pull-request descriptions may cite whatever they like. They are records of a
point in time themselves.

## Building

```bash
mvn install
```

The build needs the platform, which is `2.0.0-SNAPSHOT` until the 2.0 release. Two ways to get it:
build `adapter-platform-integration` locally, or let Maven take the published snapshot from the
snapshot repository of Maven Central, which needs no token.

Tests: the unit tests boot a Spring context with the auto-configurations under test, the tests under
`io/vanillabp/outbox/gruelbox/it` boot a whole application against H2 and the BPMS double the
platform publishes. Every test class carries
`@ExtendWith(SuppressOutputExtension.class)`, which `TestClassConventionsTest` of the coverage gate
checks, and the gate breaks the build below 85 % instruction coverage.
