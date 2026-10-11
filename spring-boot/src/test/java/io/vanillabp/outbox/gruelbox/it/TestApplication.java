package io.vanillabp.outbox.gruelbox.it;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import io.vanillabp.integration.spi.PhaseOperationRegistry;

/**
 * The application which the tests of this store start. The BPMS double has to use a two-phase
 * commit for starting a workflow (property <code>dummy-adapter.at-least-once-delivery</code>). A
 * {@link RecordingPhaseTwoListener} watches phase two and can make it fail. A
 * {@link SampleExtension} stands for an extension which schedules an operation of its own.
 * <p>
 * It runs no BPMS. These tests read the row in the gruelbox table and what reached the consumer of
 * a call, never what an engine did with it.
 */
@SpringBootApplication
public class TestApplication {

  /**
   * Spring Boot creates it while it starts this application.
   */
  public TestApplication() {

  }

  /**
   * @return What watches phase two of the double, and makes it fail if a test asks for that
   */
  @Bean
  public RecordingPhaseTwoListener recordingPhaseTwoListener() {

    return new RecordingPhaseTwoListener();

  }

  /**
   * Stands for an extension which adds an operation of its own to the outbox.
   *
   * @param registry The operation registry of VanillaBP
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
