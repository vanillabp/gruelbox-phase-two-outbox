package io.vanillabp.coverage;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.vanillabp.integration.test.utils.CoverageGate;
import io.vanillabp.integration.test.utils.PrintsWhenPassing;

/**
 * The coverage gate. It breaks the build when the combined coverage drops below the threshold.
 * So a drop is noticed when it happens, not a year later. The threshold is 85 in every VanillaBP
 * repository, while the rule is 90. So a bad week in one repository is not answered by editing
 * the number.
 * <p>
 * The <code>check</code> goal of JaCoCo cannot do this. It judges the classes of one module
 * against one file of execution data. The number of this repository comes from a combined
 * report over several modules. So the gate reads exactly the report which is published, and
 * report and gate can never disagree.
 * <p>
 * One platform is measured here. Every other VanillaBP repository measures two and keeps them
 * strictly apart, because a Spring test which says nothing about Quarkus code must not lift the
 * Quarkus number. This store runs on Spring Boot only, because gruelbox enlists an entry through
 * the Spring transaction manager. So there is no second number, and the layout of the reports
 * stays the one every repository uses.
 * <p>
 * The completeness test comes first for a reason. A threshold checked against an incomplete
 * report fails builds for coverage which exists but is not counted. Nobody can fix that by
 * writing a test.
 * <p>
 * This is the one test class in VanillaBP which prints while it passes. So it is the one without
 * {@code SuppressOutputExtension}. Everywhere else a passing test says nothing, because output
 * nobody reads hides the output somebody has to read. Here the passing run is the measurement. It
 * is the only place where the build says where this repository stands against the rule. A
 * repository between the threshold and the rule has a gap. Without this output, only somebody who
 * opens the report would see it.
 */
@PrintsWhenPassing(
  "the measurement is the result - what the suite reached belongs in the log of every "
      + "build and not only of a red one, so whoever just wrote tests reads there whether the "
      + "gap to the rule got smaller")
public class CoverageGateTest {

  private static final Path ROOT = CoverageGate.repositoryRoot("coverage.repository.root");

  /**
   * What a report is expected to show. The threshold is something else: the build stops
   * there. The rule is printed on every run but never asserted. If the build broke at the
   * rule, a repository could do nothing but edit the rule.
   */
  private static final double RULE = Double.parseDouble(System.getProperty("coverage.rule"));

  /**
   * The command line this build was started with. The Surefire configuration of the module
   * hands it over. It answers the one question the gate cannot read from the disk: does
   * this run write the report at all.
   */
  private static final String MAVEN_COMMAND_LINE = System.getProperty("coverage.maven.command", "");

  /**
   * Where the Spring Boot report is among the paths {@link CoverageGate} resolves. It knows
   * the layout every VanillaBP repository uses, with Spring Boot first. So this repository
   * keeps that layout, while it measures the one platform it has.
   */
  private static final int SPRING_BOOT = 0;

  private static final List<Path> AGGREGATE_POMS = List
      .of(ROOT.resolve("test-coverage-report/spring-boot/pom.xml"));

  /**
   * Modules whose execution data belongs to no coverage report. Each entry is a decision.
   * A module which is missing here and missing from the report is the defect this test
   * exists for.
   */
  private static final Set<String> DELIBERATELY_NOT_AGGREGATED = Set.of();

  @Test
  @DisplayName("Every module producing coverage data is read by an aggregated report")
  public void everyModuleProducingCoverageDataIsAggregated() {

    final var missing = CoverageGate
        .modulesMissingFromAggregates(ROOT, AGGREGATE_POMS, DELIBERATELY_NOT_AGGREGATED);

    assertTrue(missing.isEmpty(), () -> CoverageGate.describeMissingModules(missing, AGGREGATE_POMS));

  }

  @Test
  @DisplayName("The Spring Boot report is above the coverage threshold")
  public void theSpringBootReportIsAboveTheThreshold() {

    assertAboveThreshold("Spring Boot", SPRING_BOOT, "coverage.threshold.spring-boot");

  }

  /**
   * A run which stops before <code>verify</code> writes no report. The gate says so and
   * skips. It does not fail over a file this run could not have written. But it stays loud
   * about it. The line is printed and the test is reported as skipped, so nobody reads a
   * green run as a checked one.
   * <p>
   * The threshold holds the same 85 in every VanillaBP repository. That number is the floor
   * against regression, not the goal. The goal is the {@code coverage.rule} property. A
   * repository between the two has a gap somebody still owes a test for.
   *
   * @param platform The name of the platform measured, used in what is printed
   * @param report Which of the report paths {@link CoverageGate} resolves is read
   * @param thresholdProperty The system property holding the threshold of that platform
   */
  private void assertAboveThreshold(
      final String platform,
      final int report,
      final String thresholdProperty) {

    if (CoverageGate.stopsBeforeTheReportsAreWritten(MAVEN_COMMAND_LINE)) {
      final var reason = CoverageGate.describeRunWithoutReports(MAVEN_COMMAND_LINE);
      System.out.println("coverage gate | %s | %s".formatted(platform, reason));
      Assumptions.abort(reason);
    }

    final var threshold = Double.parseDouble(System.getProperty(thresholdProperty));

    final var coverage = CoverageGate
        .read(
            CoverageGate
                .reportsOfBothPlatforms(ROOT)
                .get(report),
            platform,
            CoverageGate.Metric.INSTRUCTIONS);

    report(coverage, threshold);

    assertTrue(
        coverage.percentage() >= threshold,
        () -> """
            %s - below the %s %% at which every VanillaBP build stops. The rule is %s, so \
            anything under that is already a gap, and this number is where the gap grew too big \
            to carry. Sort the per-package numbers of the report's jacoco.csv by MISSED \
            instructions, not by percentage, and put the test where the uncovered code belongs. \
            Code nobody can reach is dead and gets deleted rather than covered."""
            .formatted(coverage, plain(threshold), plain(RULE)));

  }

  /**
   * The line a passing run leaves behind. It names both numbers, because the one which
   * breaks the build is not the one to aim at. It says how far the suite is from the
   * rule while that distance is still small enough to close.
   *
   * @param coverage What the report showed
   * @param threshold Where this build would have stopped
   */
  private void report(
      final CoverageGate.Coverage coverage,
      final double threshold) {

    final var verdict = coverage.percentage() >= RULE
        ? "at the rule of %s %%".formatted(plain(RULE))
        : "%.2f points below the rule of %s %%, build breaks below %s %%"
            .formatted(RULE - coverage.percentage(), plain(RULE), plain(threshold));

    System.out.println("coverage gate | %s | %s".formatted(coverage, verdict));

  }

  /**
   * @param percentage The number to write
   * @return A whole percentage without the '.0' a double would print
   */
  private static String plain(
      final double percentage) {

    return percentage == Math.rint(percentage)
        ? String.valueOf((long) percentage)
        : String.valueOf(percentage);

  }

}
