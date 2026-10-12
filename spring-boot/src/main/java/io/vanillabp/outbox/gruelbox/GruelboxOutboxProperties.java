package io.vanillabp.outbox.gruelbox;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * The section <code>vanillabp.outbox.gruelbox.*</code>. It says whether this store takes the place
 * of the one VanillaBP writes itself.
 * <p>
 * The section keeps the name it had while this store was part of VanillaBP. So an application
 * which switches to this artifact keeps the configuration it already wrote. Only the default
 * changed. The dependency is the decision now. An application which writes nothing here runs this
 * store. The key switches it off again for an application which cannot remove the dependency,
 * for example because another artifact of its own pulls it in, or because a new release is not
 * worth it.
 * <p>
 * An IDE suggests the key because this class is in this module. The annotation processor of
 * Spring Boot writes the metadata of what it compiles here, and the text it shows is the javadoc
 * of the field below.
 */
@ConfigurationProperties(GruelboxOutboxProperties.PREFIX)
public class GruelboxOutboxProperties {

  /**
   * The section this class binds. It sits below the outbox section of VanillaBP, which knows the
   * keys of the stores VanillaBP writes itself.
   */
  public static final String PREFIX = "vanillabp.outbox.gruelbox";

  /**
   * The key which the condition of {@link GruelboxPhaseTwoOutboxAutoConfiguration} reads, written
   * the way an application writes it.
   */
  public static final String ENABLED = PREFIX
      + ".enabled";

  /**
   * Whether this store is built in place of the JDBC one VanillaBP writes itself. Its entries go
   * into the table 'TXNO_OUTBOX' of gruelbox. The default is 'true', because an application which
   * has this artifact on its classpath asked for this store. 'false' hands the phase-two entries
   * back to the table 'VANILLABP_PHASE_TWO_OUTBOX' of VanillaBP. Entries which still wait in
   * 'TXNO_OUTBOX' are reported at startup, so dispatch them before you switch.
   */
  private boolean enabled = true;

  /**
   * Spring Boot creates the empty instance and then fills it from the environment. An application
   * which writes nothing about gruelbox runs this store, because it has the artifact.
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
   * @param enabled <code>false</code> to hand the entries back to the table of VanillaBP
   */
  public void setEnabled(
      final boolean enabled) {

    this.enabled = enabled;

  }

}
