package io.vanillabp.outbox.gruelbox;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * Holds that <code>vanillabp.outbox.gruelbox.enabled</code> is bound by this artifact. It is the
 * key of this artifact. So this artifact binds it, and it answers a value which is no boolean.
 * <p>
 * The condition of {@link GruelboxPhaseTwoOutboxAutoConfiguration} reads the environment and needs
 * no bean for it. So such a key could exist without anybody binding it. An application which
 * writes <code>enabled: sometimes</code> would then switch nothing on, and nobody would tell it.
 * Binding the key hands that value to the converter of Spring instead, and the startup stops with a
 * message which names the key.
 * <p>
 * The default is the other half. The dependency asks for this store. So an application which has
 * this artifact and writes nothing about it runs this store, and the bound value says the same.
 */
@ExtendWith(SuppressOutputExtension.class)
public class TheKeyOfThisStoreIsBoundHereTest {

  /**
   * Binds the section and nothing else. The auto-configuration binds it too. But it would also
   * build the store, which needs a database and says nothing about the key.
   */
  @Configuration
  @EnableConfigurationProperties(GruelboxOutboxProperties.class)
  static class BindingTheSection {

  }

  private static ApplicationContextRunner application() {

    return new ApplicationContextRunner().withUserConfiguration(BindingTheSection.class);

  }

  @Test
  @DisplayName("An application which writes nothing about the store runs it")
  public void anApplicationWhichWritesNothingRunsIt() {

    application()
        .run(context -> assertTrue(
            context.getBean(GruelboxOutboxProperties.class).isEnabled(),
            "the dependency is what asks for this store, so the default has to say yes"));

  }

  @Test
  @DisplayName("The key switches the store off and is bound where it is read")
  public void theKeySwitchesTheStoreOff() {

    application()
        .withPropertyValues("%s=false".formatted(GruelboxOutboxProperties.ENABLED))
        .run(context -> assertFalse(
            context.getBean(GruelboxOutboxProperties.class).isEnabled(),
            "'false' hands the entries to the store VanillaBP writes itself"));

  }

  @Test
  @DisplayName("A value which is no boolean ends the startup naming the key")
  public void aValueWhichIsNoBooleanEndsTheStartup() {

    application()
        .withPropertyValues("%s=sometimes".formatted(GruelboxOutboxProperties.ENABLED))
        .run(context -> {

          final var failure = context.getStartupFailure();
          assertNotNull(failure, "a value nothing can read must not pass as 'off'");
          assertTrue(
              fullText(failure).contains(GruelboxOutboxProperties.ENABLED),
              "the message has to name the key the developer wrote");

        });

  }

  /**
   * @param failure What the startup ended with
   * @return The messages of the failure and of everything it was caused by
   */
  private static String fullText(
      final Throwable failure) {

    final var text = new StringBuilder();
    for (var cause = failure; cause != null; cause = cause.getCause()) {
      text.append(cause.getMessage()).append('\n');
    }
    return text.toString();

  }

}
