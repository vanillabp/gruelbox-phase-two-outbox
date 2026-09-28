package io.vanillabp.outbox.gruelbox;

import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import io.vanillabp.integration.outbox.gruelbox.GruelboxMissingAutoConfiguration;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * VanillaBP ends the boot of an application which configures this store without having it, and this
 * is the other side of that check: an application which HAS this artifact must not be stopped,
 * whether it writes the key or leaves it alone.
 * <p>
 * The check belongs to the platform, so the platform tests what it says. What only this side can
 * prove is that the condition it hangs on looks at something this artifact brings along: it reads a
 * class of the gruelbox library, and this artifact is the reason that class is on the classpath. A
 * rename or a move of it in the platform turns this test red, which is the moment to notice.
 */
@ExtendWith(SuppressOutputExtension.class)
public class TheStoreIsNotReportedAsMissingTest {

  private static ApplicationContextRunner applicationWithThisStore() {

    return new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(GruelboxMissingAutoConfiguration.class));

  }

  @Test
  @DisplayName("An application which asks for this store by name boots")
  public void anApplicationAskingForItBoots() {

    applicationWithThisStore()
        .withPropertyValues("%s=true".formatted(GruelboxOutboxProperties.ENABLED))
        .run(context -> assertNull(
            context.getStartupFailure(),
            "the store is here, so nothing is missing"));

  }

  @Test
  @DisplayName("An application which writes nothing boots as well")
  public void anApplicationWhichWritesNothingBoots() {

    applicationWithThisStore()
        .run(context -> assertNull(
            context.getStartupFailure(),
            "the dependency is the decision, so the key does not have to be written"));

  }

}
