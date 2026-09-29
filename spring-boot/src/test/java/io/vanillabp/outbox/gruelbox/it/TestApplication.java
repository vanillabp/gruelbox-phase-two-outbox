package io.vanillabp.outbox.gruelbox.it;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import io.vanillabp.integration.spi.PhaseOperationRegistry;

/**
 * The application the tests of this store boot: the BPMS double is forced to require a
 * two-phase commit for starting a workflow (property
 * <code>dummy-adapter.at-least-once-delivery</code>), a {@link RecordingPhaseTwoListener}
 * observes and can fail phase two, and a {@link SampleExtension} stands in for an
 * extension which schedules an operation of its own.
 * <p>
 * It runs no BPMS. What these tests read is the row in gruelbox' table and what reached
 * the consumer of a call, never what an engine did with it.
 */
@SpringBootApplication
public class TestApplication {

  /**
   * Built by Spring Boot while it starts this application.
   */
  public TestApplication() {

  }

  /**
   * @return What observes phase two of the double, and fails it where a test asks for that
   */
  @Bean
  public RecordingPhaseTwoListener recordingPhaseTwoListener() {

    return new RecordingPhaseTwoListener();

  }

  /**
   * Stands in for an extension contributing an operation of its own to the outbox.
   *
   * @param registry VanillaBP's operation registry
   * @param aggregates Where the write of a dispatch goes
   * @return The sample extension
   */
  @Bean
  public SampleExtension sampleExtension(
      final PhaseOperationRegistry registry,
      final AggregateRepository aggregates) {

    return new SampleExtension(registry, aggregates);

  }

}
