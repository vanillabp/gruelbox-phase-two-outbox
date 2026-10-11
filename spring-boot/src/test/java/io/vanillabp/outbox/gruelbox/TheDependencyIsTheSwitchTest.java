package io.vanillabp.outbox.gruelbox;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.hibernate.autoconfigure.HibernateJpaAutoConfiguration;
import org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration;
import org.springframework.boot.jdbc.autoconfigure.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.test.context.assertj.AssertableApplicationContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import io.vanillabp.integration.outbox.jdbc.JdbcPhaseTwoOutbox;
import io.vanillabp.integration.outbox.jdbc.JdbcPhaseTwoOutboxAutoConfiguration;
import io.vanillabp.integration.spi.PhaseTwoOutbox;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * Holds which of the two stores serves a relational workflow aggregate, and what decides it.
 * <p>
 * The rule has two halves, and this class holds both. The dependency asks for this store: an
 * application which has this artifact and writes nothing gets it. The key takes that back:
 * <code>vanillabp.outbox.gruelbox.enabled=false</code> hands the entries to the store VanillaBP
 * writes itself. That half is worth a test of its own. Switching this store off and getting NO
 * store at all would be just as silent as getting the wrong one.
 * <p>
 * Both stores use one bean name. So the assertions read the TYPE behind that name, and they count
 * the stores in the context. Two beans would mean that the two configurations no longer exclude
 * each other. One bean of the wrong type would mean that the wrong store serves.
 */
@ExtendWith(SuppressOutputExtension.class)
public class TheDependencyIsTheSwitchTest {

  /**
   * An application with a relational database, both outbox configurations on the classpath, and
   * nothing else. Each test has its own database, because each one creates the tables of the store
   * it gets.
   *
   * @param database The name of the in-memory database
   * @param properties What the application writes about the store
   * @return The runner
   */
  private static ApplicationContextRunner applicationOn(
      final String database,
      final String... properties) {

    final var configuration = new String[properties.length + 2];
    configuration[0] = "spring.datasource.url=jdbc:h2:mem:%s;DB_CLOSE_DELAY=-1".formatted(database);
    configuration[1] = "spring.jpa.hibernate.ddl-auto=none";
    System.arraycopy(properties, 0, configuration, 2, properties.length);

    return new ApplicationContextRunner()
        .withPropertyValues(configuration)
        .withConfiguration(
            AutoConfigurations.of(
                DataSourceAutoConfiguration.class, DataSourceTransactionManagerAutoConfiguration.class,
                HibernateJpaAutoConfiguration.class, JdbcPhaseTwoOutboxAutoConfiguration.class,
                GruelboxPhaseTwoOutboxAutoConfiguration.class));

  }

  /**
   * The one store of the context, looked up by the name both configurations use.
   *
   * @param context The started application
   * @return The store serving its relational aggregates
   */
  private static PhaseTwoOutbox theOnlyStoreOf(
      final AssertableApplicationContext context) {

    assertNull(context.getStartupFailure(), "the two configurations have to exclude each other");
    assertEquals(
        1,
        context.getBeanNamesForType(PhaseTwoOutbox.class).length,
        "one relational aggregate is served by one store, and two beans would be a coin toss");
    return context.getBean(JdbcPhaseTwoOutboxAutoConfiguration.DEFAULT_OUTBOX_BEAN_NAME, PhaseTwoOutbox.class);

  }

  @Test
  @DisplayName("An application which writes nothing gets this store, because it has the artifact")
  public void theArtifactAloneSwitchesItOn() {

    applicationOn("the-switch-by-dependency")
        .run(context -> assertInstanceOf(
            GruelboxPhaseTwoOutbox.class,
            theOnlyStoreOf(context),
            "the dependency is what asks for this store"));

  }

  @Test
  @DisplayName("An application which switches this store off gets the one VanillaBP writes itself")
  public void theKeyHandsTheEntriesBack() {

    applicationOn("the-switch-off", "%s=false".formatted(GruelboxOutboxProperties.ENABLED))
        .run(context -> assertInstanceOf(
            JdbcPhaseTwoOutbox.class,
            theOnlyStoreOf(context),
            "switching this store off must leave the platform's own one in charge, not nothing"));

  }

}
