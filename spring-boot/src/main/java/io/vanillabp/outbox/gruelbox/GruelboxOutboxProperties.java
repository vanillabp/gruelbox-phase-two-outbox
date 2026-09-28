package io.vanillabp.outbox.gruelbox;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * The section <code>vanillabp.outbox.gruelbox.*</code>: whether this store takes the place
 * of the one VanillaBP writes itself.
 * <p>
 * The section keeps the name it had while this store was part of VanillaBP, so an
 * application which switches to this artifact keeps the configuration it already wrote.
 * What changed is the default. The dependency is the decision now, so an application which
 * writes nothing here runs this store, and the key is what switches it off again for an
 * application which cannot take the dependency out - a second artifact of its own pulls it
 * in, or a release is not worth cutting for it.
 * <p>
 * A development environment proposes the key because this class stands in this module: the
 * annotation processor of Spring Boot writes the metadata of what it compiles here, and the
 * text it shows is the javadoc of the field below.
 */
@ConfigurationProperties(GruelboxOutboxProperties.PREFIX)
public class GruelboxOutboxProperties {

  /**
   * The section this class binds. It hangs below the outbox section of VanillaBP, which
   * knows the keys of the stores VanillaBP writes itself.
   */
  public static final String PREFIX = "vanillabp.outbox.gruelbox";

  /**
   * The key the condition of {@link GruelboxPhaseTwoOutboxAutoConfiguration} reads,
   * written the way an application writes it.
   */
  public static final String ENABLED = PREFIX
      + ".enabled";

  /**
   * Whether this store is built in place of the JDBC one VanillaBP writes itself. Its
   * entries go into gruelbox' own table 'TXNO_OUTBOX'. The default is 'true': an
   * application which has this artifact on its classpath asked for this store, and
   * 'false' hands the phase-two entries back to VanillaBP's own table
   * 'VANILLABP_PHASE_TWO_OUTBOX'. Entries which are still waiting in 'TXNO_OUTBOX' are
   * reported at startup, so dispatch them before you switch.
   */
  private boolean enabled = true;

  /**
   * Spring Boot builds the empty instance and then fills it from the environment. An
   * application which writes nothing about gruelbox runs this store, because it has the
   * artifact.
   */
  public GruelboxOutboxProperties() {

  }

  /**
   * Whether this store is built in place of the JDBC one VanillaBP writes itself, see
   * {@link #enabled}.
   *
   * @return <code>true</code> where this store serves the relational aggregates
   */
  public boolean isEnabled() {

    return enabled;

  }

  /**
   * Whether this store is built in place of the JDBC one VanillaBP writes itself, see
   * {@link #enabled}.
   *
   * @param enabled <code>false</code> to hand the entries back to VanillaBP's own table
   */
  public void setEnabled(
      final boolean enabled) {

    this.enabled = enabled;

  }

}
