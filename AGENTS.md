# Working on gruelbox-phase-two-outbox

This repository implements VanillaBP's `PhaseTwoOutbox` for Spring Boot. It keeps the phase-two
calls of an application in the gruelbox transaction outbox.

Read [`README.md`](./README.md) first. The platform documents what a phase-two outbox is for. It
does so on the wiki page
[Spring Boot integration](https://github.com/vanillabp/adapter-platform-integration/wiki/Spring-Boot-integration#what-the-outbox-guarantees)
and in the javadoc of `io.vanillabp.integration.spi.PhaseTwoOutbox`. That javadoc is the contract
which this repository implements. Read it before every change here.

## The decision log is binding

[`DECISIONS.md`](./DECISIONS.md) holds the decisions which several places in this repository rely
on. It is the ONLY thing the code may cite. The citation has the plain form
`see decision 2 in the repository's DECISIONS.md`, so a grep finds it, and it names only entries of
THIS repository.

Read it before you change behaviour. An entry is not background reading. It is the reason why the
code around it looks the way it does. So a change which contradicts an entry is wrong until the
entry says otherwise.

**A decision is changed or replaced only after asking.** If your change would make an entry untrue,
stop and ask the maintainer before you write the change. If the answer is yes, the same commit
updates the log. The old entry STAYS. It is marked as superseded and names the entry which replaced
it. The new decision takes the next free number. Numbers are never reused and never renumbered,
because a citation in an older release still points at them.

The same rule holds for adding an entry. A decision gets a number when several places rely on it
and a copy of the explanation in each place would go stale. Anything smaller is a comment in the
place where it belongs. Anything larger is documentation.

## How we write

Most people who read this repository read English as a second language, and so does the
maintainer. Long sentences, rare words and stacked nouns slow them down. Write so that
nobody has to read a sentence twice.

Short main sentences, one thought each. One subordinate clause is enough. Active voice.
The common word instead of the rare one: `use` instead of `leverage`, `about` instead of
`regarding`, `so` instead of `consequently`, `has` instead of `possesses`. A technical term
stays a technical term, but say what it means the first time it turns up, and write an
abbreviation out once. If a sentence trips you up when you read it aloud, rewrite it.

This holds for every English text here: the README, `DECISIONS.md`, this file, the Javadoc and
comments which explain something, and the texts of commits and pull requests. It holds for the
[wiki](https://github.com/vanillabp/gruelbox-phase-two-outbox/wiki) as well, once the repository
has one, because a wiki clone has no `AGENTS.md` of its own.

Nothing a program reads is renamed for the sake of language. Class and method names,
configuration keys, artifact coordinates and the headlines of decision log entries stay as
they are, because code, tests and other repositories point at them.

Before and after, from this repository. The first example comes from the rules further down in
this file:

> A change which makes this store behave like the ones VanillaBP writes itself by holding gruelbox'
> entries back, by adding a column to its table or by picking entries in an order of our own takes
> that away, and an application which wanted that behaviour has the other store already.

becomes

> Some changes would make this store behave like the stores VanillaBP writes itself: holding
> gruelbox' entries back, adding a column to its table, or picking entries in an order of our own.
> Such a change takes that away. An application which wants that behaviour already has the other
> store.

The second one comes from the section about the extension double:

> The price is that a change to one of the two does not reach the other, so whoever changes this
> one reads the platform's before deciding whether the difference is on purpose.

becomes

> The price: a change to one copy does not reach the other. So if you change this copy, read the
> platform's copy first, and decide whether the difference is on purpose.

A promise in a text is part of the behaviour. A javadoc, README or `DECISIONS.md` sentence which
promises behaviour either has a test which fails when the sentence stops being true, or it says
that it is an assumption and what would disprove it.

## Before you open a pull request

A number your branch hands out can be taken by the time you open the pull request. Another branch
was open at the same time and got there first. So check your numbers against `origin/main` and
against every open pull request, before the pull request exists:

```bash
bin/check-decision-numbers.sh
```

The script reports and changes nothing.

After you renumber, run `bin/check-decision-citations.sh`. It says whether every citation of a
decision still points at an entry. It also finds a citation which is wrapped over two lines. The
*Checks* workflow runs it on every pull request.

`bin/check-orphaned-javadoc.sh` finds a javadoc block which stands directly in front of a second
one. Javadoc drops such a block without a word, so its text appears nowhere. Run the script when
you wrote or moved a comment. If it reports a block, move the block back to the element it
describes. Do not delete it.

## Three rules which are easy to break here

**The thinness is the feature.** This store exists so that an application which ran gruelbox
keeps its rows, its tooling and its failure modes. Some changes would make this store behave like
the stores VanillaBP writes itself: holding gruelbox' entries back, adding a column to its table,
or picking entries in an order of our own. Such a change takes that away. An application which
wants that behaviour already has the other store.

**What this store cannot answer, it says it cannot answer.** `ageOfOldestPendingCall` reports
nothing. `adapterIdsOfPendingCalls` reports nothing at a start. Both say why in their javadoc. Do
not report a number this store cannot back as if it could.

**Nothing may be carried to a BPMS before the models are there.**
`GruelboxRedispatchAwareSubmitter` holds back every entry until the dispatcher starts polling.
That happens once per application start, after VanillaBP deployed the models. An application
which builds its own `TransactionOutbox` bean has to keep that submitter.
`GruelboxHoldsEntriesBackUntilDispatchingStartedTest` is the test which holds that promise.

## The extension double exists twice

`SampleExtension` under `io/vanillabp/outbox/gruelbox/it` is a copy. The platform has one with the
same name. It lies in a test module of its own which is not published, so a repository outside the
platform cannot depend on it. Two tests here need such a double. They need one operation which
says that it replaces what is still waiting, a dispatch which can be rejected with a window, and a
dispatch which can be held while a test plans against a claimed entry.

Keeping it as a copy is the decision (Stephan, 2026-09-28). Inside VanillaBP, the tests check what
belongs to VanillaBP. Inside this repository, they only check that the contract is kept. A copy
lets the two move at their own speed. The price: a change to one copy does not reach the other. So
if you change this copy, read the platform's copy first, and decide whether the difference is on
purpose. The copy is already smaller than the platform's. One half of the platform's double calls
an annotated method through `ExtensionHandlers`. That half needs the platform's sample extension,
so it stayed there. Here, a dispatch writes into the workflow aggregate through its repository
instead.

## What code may point at

Code may not point at anything which a later change can make wrong without anybody noticing. So no
story or prompt number, no issue or pull request number, no chat transcript and no person. These
record a conversation at one point in time. A decision entry lives next to the code and is updated
in the same commit as the code. That is why code may cite it. A decision of ANOTHER repository can go wrong
unnoticed as well, even a decision of VanillaBP itself. It is renumbered and superseded on a
schedule which nothing here sees.

Where a name can carry the reason, the better fix is the name. Where it cannot, a comment says why
in its own words, complete where it stands. Only what several places have to carry becomes an
entry in the log.

Commit messages and pull request descriptions may cite whatever they like. They record one point
in time themselves.

## Building

```bash
mvn install
```

The build needs the platform, which is `2.0.0-SNAPSHOT` until the 2.0 release. There are two ways
to get it. Build `adapter-platform-integration` locally, or let Maven take the published snapshot
from the snapshot repository of Maven Central. That needs no token.

The tests come in two kinds. The unit tests boot a Spring context with the auto-configurations
under test. The tests under `io/vanillabp/outbox/gruelbox/it` boot a whole application against H2
and against the BPMS double which the platform publishes. Every test class carries
`@ExtendWith(SuppressOutputExtension.class)`. `TestClassConventionsTest` of the coverage gate
checks that. The gate breaks the build below 85 % instruction coverage.
