package io.vanillabp.outbox.gruelbox.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.ResolvableType;
import org.springframework.transaction.support.TransactionTemplate;

import io.vanillabp.integration.test.utils.CapturedOutput;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;
import io.vanillabp.spi.process.ProcessService;

/**
 * An adapter id which a waiting outbox entry names although the configuration does not.
 * <p>
 * The stores VanillaBP writes itself keep the id in a column and name it while the
 * application boots. Gruelbox keeps a call as a serialized invocation, so the boot cannot
 * ask without reading the whole table. It answers at the first dispatch instead, where the
 * entry is read anyway, and it says what the boot of the other stores says - see decision 2
 * in the repository's DECISIONS.md.
 * <p>
 * Two contexts on one database: the first runs with an adapter 'old-bpms' at first priority
 * and leaves an entry of it undispatched, the second does not configure that adapter at all.
 * The entry the second one recovers is the stale one a renamed or a removed adapter leaves
 * behind.
 */
@ExtendWith(SuppressOutputExtension.class)
@SuppressOutputExtension.SuppressBackgroundOutput
public class StaleAdapterIdNamedAtDispatchTest {

  /**
   * A database of this test's own, kept alive between the two contexts. Sharing it would
   * hand the second context entries another test wrote, and the entry this test is about
   * would no longer be the only stale one.
   */
  private static final String DATASOURCE_URL = "jdbc:h2:mem:outbox-stale-adapter;DB_CLOSE_DELAY=-1";

  private static final String STALE_ADAPTER = "old-bpms";

  /**
   * How often the entry has to be attempted before the count of reports is asked for.
   * Four, because the first attempt is the one which reports and three more of them are
   * enough to show that the report does not come back with every attempt.
   */
  private static final int ATTEMPTS_WORTH_ONE_REPORT = 4;

  /**
   * What the report names, which is the adapter id and the reading an application has to
   * choose between.
   */
  private static final String THE_REPORT_NAMING_IT = "The adapter id '%s' is NOT configured any more"
      .formatted(STALE_ADAPTER);

  /**
   * The application as the first context runs it: an adapter nobody has heard of yet is
   * configured and prioritized, so the workflow starts there and its entry names it.
   *
   * @param pollInterval How long the store waits between two polls
   * @return The running application context
   */
  private ConfigurableApplicationContext runWithTheAdapterWhichGoesAway(
      final String pollInterval) {

    return run(
        pollInterval,
        "--vanillabp.adapters.%s.type=dummy".formatted(STALE_ADAPTER),
        "--vanillabp.prioritized-adapters=%s,test".formatted(STALE_ADAPTER),
        "--vanillabp.workflow-modules.test-module.adapters.%s.resources-location=classpath*:test-module/processes/dummy"
            .formatted(STALE_ADAPTER));

  }

  /**
   * The application as it is configured from here on: the adapter of the entry is gone.
   *
   * @param pollInterval How long the store waits between two polls
   * @return The running application context
   */
  private ConfigurableApplicationContext runWithoutThatAdapter(
      final String pollInterval) {

    return run(pollInterval);

  }

  private ConfigurableApplicationContext run(
      final String pollInterval,
      final String... adapterConfiguration) {

    final var arguments = new java.util.ArrayList<String>();
    arguments.add("--spring.datasource.url="
        + DATASOURCE_URL);
    arguments.add("--vanillabp.outbox.poll-interval="
        + pollInterval);
    arguments.add("--vanillabp.outbox.attempt-frequency=PT0.5S");
    arguments.addAll(java.util.List.of(adapterConfiguration));
    return new SpringApplicationBuilder(TestApplication.class)
        .web(WebApplicationType.NONE)
        .run(arguments.toArray(String[]::new));

  }

  /**
   * How often the given text stands in what the application wrote.
   */
  private static int occurrencesOf(
      final String text,
      final CapturedOutput output) {

    final var written = output.getAll();
    var occurrences = 0;
    var found = written.indexOf(text);
    while (found >= 0) {
      occurrences++;
      found = written.indexOf(text, found + text.length());
    }
    return occurrences;

  }

  @Test
  @DisplayName("The id of a stale entry is named at its first dispatch, and named once")
  public void theStaleAdapterIdIsNamedAtTheFirstDispatch(
      final CapturedOutput output) throws Exception {

    // the first context leaves an entry of 'old-bpms' behind: its dispatch fails and the
    // poll interval is too long for a second attempt, which is the shape of a crash
    try (var context = runWithTheAdapterWhichGoesAway("PT1H")) {
      final var listener = context.getBean(RecordingPhaseTwoListener.class);
      listener.failNextDispatches(Integer.MAX_VALUE);
      @SuppressWarnings("unchecked")
      final var processService = (ProcessService<Aggregate>) context
          .getBeanProvider(ResolvableType.forClassWithGenerics(ProcessService.class, Aggregate.class))
          .getObject();
      final var transactionTemplate = context.getBean(TransactionTemplate.class);
      final var attachedAggregate = transactionTemplate.execute(status -> {
        final var aggregate = new Aggregate();
        aggregate.setContent("stale-adapter-id");
        return processService.startWorkflow(aggregate);
      });
      assertNotNull(attachedAggregate);
      listener.awaitInvocations(1, 10000);
      // the failed attempt has to be written before the context goes away, otherwise the
      // second context meets an entry which was never dispatched
      FailedAttempts.awaitWrittenDown(context, 1);
      assertEquals(
          0,
          occurrencesOf(THE_REPORT_NAMING_IT, output),
          "while the adapter is configured there is nothing to report");
    }

    try (var context = runWithoutThatAdapter("PT0.5S")) {
      final var giveUpAt = System.currentTimeMillis() + 15000;
      while ((System.currentTimeMillis() < giveUpAt) && (occurrencesOf(THE_REPORT_NAMING_IT, output) == 0)) {
        Thread.sleep(100);
      }

      assertTrue(
          occurrencesOf(THE_REPORT_NAMING_IT, output) > 0,
          "the store cannot name the id while booting, so its dispatch has to: "
              + output.getAll());
      assertTrue(
          output.getAll().contains("retired-adapters"),
          "the report names the property which says the leftovers are known");
      assertTrue(
          output.getAll().contains("waiting phase-two outbox entries"),
          "and it names what is left over, as a boot of the other stores does");

      // the entry is attempted again every 'attempt-frequency', so a report per attempt
      // would be a line every half second for as long as the application runs. The
      // attempts are read from the store instead of waited for: a wait would pass on a
      // machine where the dispatcher never came back, which is the one case this must
      // not call green
      FailedAttempts.awaitAttemptsOfAWaitingEntry(context, ATTEMPTS_WORTH_ONE_REPORT);
      assertEquals(
          1,
          occurrencesOf(THE_REPORT_NAMING_IT, output),
          "an adapter id is worth one report, however often its entries are attempted");
    }

  }

}
