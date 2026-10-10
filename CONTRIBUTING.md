# Contributing

This repository holds one implementation of VanillaBP's `PhaseTwoOutbox`: the phase-two calls of a
Spring Boot application go into the gruelbox transaction outbox instead of the table VanillaBP writes
itself. It is not a BPMS adapter and it carries no business rules. Everything it does for the people
using it is in [`README.md`](./README.md), which is the only documentation here, because there is not
enough of it to justify a wiki. What a phase-two outbox is for belongs to the platform and is
documented
[there](https://github.com/vanillabp/adapter-platform-integration/wiki/Spring-Boot-integration#what-the-outbox-guarantees).

Where the rules are: [`AGENTS.md`](./AGENTS.md) says how work is done here, in the form an agent
reads, and [`DECISIONS.md`](./DECISIONS.md) holds the decisions several places rely on, which is the
only thing the code is allowed to cite.

## From a bug to a pull request

Found a bug? You can hand it to a coding agent.

1. Clone [`development-workspace`](https://github.com/vanillabp/development-workspace) with
   `git clone --recurse-submodules`. It holds the VanillaBP repositories side by side. Its
   `.claude/skills` folder holds the skills Claude Code reads when it starts at the root of the
   workspace.
2. Start your coding agent at the root of the workspace and describe the bug: what you did, what
   you expected and what happened instead.
3. The agent builds a scenario which reproduces the bug, looks for the cause and fixes it. Then it
   pushes a branch to your fork and opens a pull request from there.
4. The VanillaBP team reviews the pull request and merges it.

You need a GitHub account, the GitHub CLI logged in with `gh auth login`, a fork of the repository
you change (the agent can create it with `gh repo fork`), and Java 21, Maven and Docker for the
build. The submodules are cloned over SSH. If GitHub has no SSH key of yours, run
`git config --global url."https://github.com/".insteadOf "git@github.com:"` before you clone. If
the bug is in a repository the workspace does not hold, the agent clones it next to the others.
Before it opens the pull request, the agent follows the `CONTRIBUTING.md` and the `AGENTS.md` of
the repository it changes, where it has one. The two Camunda adapters live in the Camunda Community
Hub, which asks you to sign its contributor license agreement on your first pull request there.

## Building and testing

A JDK 21 or newer, and Maven, without a wrapper. The workflows build with the JDK named in
`.github/workflows`, currently 25, so build with that one if you want to see what the pipeline sees.
The class files stay at Java 21 either way, because that is what the property `version.java` in the
root `pom.xml` compiles against:

```bash
mvn spotless:apply
mvn install
```

`install` and not `install verify`: `install` already runs every phase `verify` has, so naming both
walks two lifecycles per module and reports every compiler warning twice. Spotless formats Markdown
too.

The build needs the platform, which is a snapshot until the 2.0 release. Either build
`adapter-platform-integration` yourself, or let Maven take its published snapshot from the snapshot
repository of Maven Central. The parent `io.vanillabp:release-parent` names that repository, and
reading it needs no login and no token.

Two tools read the javadoc, and each one sees a part the other misses. The compiler checks every
class for a broken reference or broken HTML, the package private ones included. The javadoc plugin
checks what the published documentation shows, so it starts at protected and stops there. One thing
below protected is shown as well: the fields a serializable class carries into its serialized form,
which is why a private field of an exception is asked for a comment too.

A comment which is missing breaks the build. Everything this repository publishes has one now, and
the plugin fails on a warning so that it stays that way. Write the sentence rather than switching
the check off, and write the one a reader needs: what this repository publishes is read by somebody
wiring it into an application, and `@return the value` is the same gap in a longer form. A module
which publishes nothing sets `maven.javadoc.skip`, so a test module is never asked for comments.

One thing the javadoc plugin cannot see is what Lombok generates, because it reads the source and
Lombok writes bytecode. So a published comment names a property in words rather than linking a
getter which is not in the file, and a published class which takes its constructor from Lombok
writes that constructor out, because the documentation otherwise shows a parameterless one which
does not exist.

Two javadoc blocks in a row are the gap neither tool sees. Javadoc keeps the last block before an
element and drops the earlier ones without a word, so a comment somebody wrote and kept up to date
appears nowhere. `bin/check-orphaned-javadoc.sh` finds that shape. A block it reports describes
something, usually the element next door, so hang it back there rather than delete it.

Nothing here needs Docker or a network. H2 in memory is the database, one per test class, because
two classes sharing one meet each other's entries and the test which reads the table then reads
somebody else's row. The tests under `io/vanillabp/outbox/gruelbox/it` boot a real application, and
VanillaBP refuses to start without a BPMS adapter, so `io.vanillabp:bpms-double-spring-boot` is on the
test classpath. It runs no engine and records everything, which is what a store's test needs: what it
asserts is the row in the table, never what a BPMS did with it.

An outbox is only ever wrong in the moment a transaction fails, so the tests which crash an
application in the middle are the important ones. Keep them, and read what they claim from the STORE
rather than from a listener: a listener runs inside the dispatch, and the entry is finished one update
later.

## What a POM hands an application

A tool which only translates our source belongs in scope `provided`, and the scope stands at the
declaration in the module which uses the tool. Lombok is such a tool, an annotation processor is
another. An application asked for a workflow engine, and every jar it did not ask for is one more
thing to ship and to answer a CVE report about.

Writing `<optional>true</optional>` in a `dependencyManagement` does not do it. Maven copies a
managed version, scope and exclusions into a dependency and leaves the optional flag behind, so the
POM we publish says nothing at all about that dependency. Lombok reached the runtime classpath of
every application that way, here and in the platform.

## A POM comment says what this POM does

A comment in a POM says what this POM sets and why it differs from what it would get for free. It
does not repeat what the parent sets. `io.vanillabp:release-parent` is released on its own schedule,
so a copy of its settings in a child here goes stale without anything noticing. The javadoc
comments of three POMs in this organisation did exactly that: they said the parent switched the
javadoc check off, after a release of it had switched the check on.

Where a module really does rely on something its parent does or does not do, say so as an
assumption and name the version it was checked against. Then the next reader can check it again in
one command, `mvn help:effective-pom`, instead of believing a sentence.

## How we write

Most people who read this repository read English as a second language, and so does the maintainer.
Long sentences, rare words and stacked nouns slow them down. Write so that nobody has to read a
sentence twice.

Short main sentences, one thought each. One subordinate clause is enough. Active voice. The common
word instead of the rare one: `use` instead of `leverage`, `about` instead of `regarding`, `so`
instead of `consequently`. A technical term stays a technical term, but say what it means the first
time it turns up, and write an abbreviation out once. If a sentence trips you up when you read it
aloud, rewrite it.

This holds for every English text here, the javadoc, the commit message and the pull request
included. Nothing a program reads is renamed for the sake of language: type and method names,
configuration keys and artifact coordinates stay as they are, because code in other repositories
points at them.

## What is asked before the code is written

Where a change would make an entry of [`DECISIONS.md`](./DECISIONS.md) untrue, ask before you write
it and wait for the answer. An entry is never edited away: it stays, marked as superseded and naming
its successor, and the new decision takes the next free number.

The second question is the interface. `PhaseTwoOutbox` is published by the platform integration and
an application may bring its own implementation, so what a store promises is decided there. What this
store does differently, and what it cannot answer at all, is decided here and written in
[`DECISIONS.md`](./DECISIONS.md).

## Opening a pull request

Work on a branch of your own and keep one subject per pull request. The description says what moved
and why it had to. It may cite an issue or a conversation, because it is a record of a moment itself,
which the code is not.

Check the numbers your branch hands out before you open it. Another branch may have taken the
decision number you used while you were writing, and once a pull request is merged a
`see decision 2` in a Java file can no longer be corrected on GitHub:

```bash
bin/check-decision-numbers.sh
```

After you corrected a number, `bin/check-decision-citations.sh` says whether every citation of a
decision still points at an entry of `DECISIONS.md`. The *Checks* workflow runs it on every pull
request.

The *Build and publish snapshots* workflow builds and tests every pull request and publishes nothing
from a branch. A red check is a finding about your change. Read the log and fix what it says rather
than pushing again to see whether it goes away. Releases are a second workflow, started by hand with
the version to release, and it refuses to run while a POM still names a SNAPSHOT.

A pull request from a fork builds and tests like any other, because the snapshots it needs can be
read without a login. It cannot publish anything, because GitHub gives it no secrets of this
repository.

`main` carries a ruleset, and it requires two green checks before a merge: `publish`, which is the
build of *Build and publish snapshots*, and `orphaned-javadoc-check`, which is the *Checks* workflow
running the script named above. While one of them is red, GitHub does not offer the merge.

## License

VanillaBP is published under the [Apache License, Version 2.0](./LICENSE), and by contributing you
agree that your contribution is licensed the same way. [`NOTICE`](./NOTICE) names who holds the
copyright.
