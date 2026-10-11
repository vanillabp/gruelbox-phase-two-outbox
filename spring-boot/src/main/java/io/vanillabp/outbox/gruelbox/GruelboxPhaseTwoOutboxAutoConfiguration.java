package io.vanillabp.outbox.gruelbox;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnSingleCandidate;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.transaction.PlatformTransactionManager;

import com.gruelbox.transactionoutbox.DefaultPersistor;
import com.gruelbox.transactionoutbox.Dialect;
import com.gruelbox.transactionoutbox.Persistor;
import com.gruelbox.transactionoutbox.Submitter;
import com.gruelbox.transactionoutbox.TransactionOutbox;
import com.gruelbox.transactionoutbox.TransactionOutboxListener;
import com.gruelbox.transactionoutbox.spring.SpringInstantiator;
import com.gruelbox.transactionoutbox.spring.SpringTransactionManager;

import io.vanillabp.integration.adapter.migration.config.PhaseTwoOutboxProperties;
import io.vanillabp.integration.adapter.migration.delivery.JdbcConnectionAccess;
import io.vanillabp.integration.adapter.migration.jdbc.JdbcSchema;
import io.vanillabp.integration.adapter.migration.observability.VanillaBpMetrics;
import io.vanillabp.integration.adapter.migration.outbox.JdbcHousekeepingLease;
import io.vanillabp.integration.adapter.migration.outbox.JdbcPhaseTwoOutboxStore;
import io.vanillabp.integration.adapter.migration.outbox.JdbcPhaseTwoPayloadStore;
import io.vanillabp.integration.adapter.migration.processservice.PhaseTwoRouter;
import io.vanillabp.integration.config.VanillaBpConfigurationProperties;
import io.vanillabp.integration.outbox.jdbc.JdbcPhaseTwoOutbox;
import io.vanillabp.integration.outbox.jdbc.JdbcPhaseTwoOutboxAutoConfiguration;
import io.vanillabp.integration.processservice.SpringBootMigrationAdapterAutoConfiguration;
import io.vanillabp.integration.spi.PhaseTwoCall;
import io.vanillabp.integration.spi.PhaseTwoOutbox;
import io.vanillabp.integration.utils.config.JpaSpringDataUtilConfiguration;
import jakarta.persistence.EntityManagerFactory;

/**
 * Auto-configuration of the {@link PhaseTwoOutbox} for aggregates persisted with JPA. It is backed
 * by the <a href="https://github.com/gruelbox/transaction-outbox">gruelbox transaction-outbox</a>.
 * Adding the dependency is all the wiring there is. As soon as this artifact is on the classpath,
 * it takes the place of the store VanillaBP writes itself
 * ({@link JdbcPhaseTwoOutboxAutoConfiguration}). <code>vanillabp.outbox.gruelbox.enabled</code>
 * switches it off again without removing the dependency. It needs Spring Data JPA and exactly one
 * {@link EntityManagerFactory}. It lives NEXT TO the MongoDB default. Each workflow aggregate is
 * served by the outbox which matches its persistence (chosen per aggregate, see
 * {@link io.vanillabp.integration.spi.PhaseTwoOutboxAware}). So outbox entries always join the
 * transaction of the aggregate itself, also in applications with more than one kind of
 * persistence. Switch it off with <code>vanillabp.outbox.jdbc.enabled</code> if no relational
 * outbox is wanted at all.
 * <p>
 * The outbox table is <code>TXNO_OUTBOX</code>. It is the table of gruelbox, with the columns of
 * gruelbox, so VanillaBP does not rename it. <code>vanillabp.outbox.jdbc.table</code> names the
 * table VanillaBP writes itself and has no say here (see decision 3 in the repository's
 * DECISIONS.md). An application which needs another name builds the {@link TransactionOutbox} bean
 * itself, under the name {@value #DEFAULT_TRANSACTION_OUTBOX_BEAN_NAME}. The schema migration of
 * gruelbox creates the table, unless <code>vanillabp.outbox.create-schema</code> is set to
 * <code>false</code> (see the <code>README.md</code> of this repository for managing the schema by
 * hand). If that migration is off, the table is checked AT STARTUP (see
 * {@link #validateOutboxTableExists(DataSource)}). This one table belongs to gruelbox, so
 * <code>io.vanillabp:vanillabp-schema</code> does not cover it.
 * <p>
 * <strong>How the contract maps, and where it differs:</strong> the {@link PhaseTwoOutbox}
 * contract maps onto what gruelbox can do by itself. Idempotency keys become
 * <code>uniqueRequestId</code>s (a unique constraint of <code>TXNO_OUTBOX</code>). "DONE instead of
 * delete" becomes the retention of gruelbox for processed entries with a unique request ID.
 * <code>vanillabp.outbox.retention</code> maps to the retention of gruelbox, and the flush in the
 * background deletes expired entries. Blocking after
 * <code>vanillabp.outbox.block-after-attempts</code> failed attempts is the blocklist of gruelbox.
 * Gruelbox knows nothing about how VanillaBP sorts failures. So a listener of VanillaBP
 * ({@link GruelboxPhaseTwoFailureListener}) blocks a failure which the adapter calls permanent. It
 * also gives a blocked entry an ERROR which names the workflow, not only the id of the entry. The
 * same listener writes the short due time of a workflow which is not searchable yet, because
 * gruelbox plans every failed attempt from the one distance it knows. It also takes back the
 * attempt gruelbox counted for such an answer. And it blocks the entry once
 * <code>vanillabp.outbox.wait-for-visibility-at-most</code> has passed. Two things this store
 * cannot do, while the stores of VanillaBP can: its retry policy knows ONE fixed distance, so
 * <code>max-attempt-frequency</code> and the doubling it caps have no effect here. And a
 * blocklisted entry holds its <code>uniqueRequestId</code> until the row is removed, so the
 * operation it failed at cannot be scheduled again in the meantime. The
 * {@link PhaseTwoCall#args()} map travels in its serialized form, because the invocation
 * serializer of gruelbox accepts only scalar parameter types (see
 * {@link GruelboxPhaseTwoDispatch}). Like the stores of VanillaBP, this store waits for
 * VanillaBP. Its {@link GruelboxRedispatchAwareSubmitter} keeps an entry until the dispatcher below
 * starts. So nothing is carried to a BPMS which has not seen the models yet.
 */
@AutoConfiguration(
    before = JdbcPhaseTwoOutboxAutoConfiguration.class,
    after = JpaSpringDataUtilConfiguration.class,
    afterName = {
        "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration", "org.springframework.boot.jdbc.autoconfigure.DataSourceTransactionManagerAutoConfiguration", "org.springframework.boot.hibernate.autoconfigure.HibernateJpaAutoConfiguration"
    })
@ConditionalOnClass(JpaRepository.class)
@ConditionalOnSingleCandidate(EntityManagerFactory.class)
@ConditionalOnBean({
    DataSource.class, PlatformTransactionManager.class
})
// both keys have to hold: the first one switches off every relational outbox, the
// second one only this store
@ConditionalOnBooleanProperty(
    name = {
        "vanillabp.outbox.jdbc.enabled", GruelboxOutboxProperties.ENABLED
    },
    matchIfMissing = true)
@EnableConfigurationProperties({
    GruelboxOutboxProperties.class, VanillaBpConfigurationProperties.class
})
public class GruelboxPhaseTwoOutboxAutoConfiguration {

  /**
   * The name of the outbox bean of this store. VanillaBP gives this name to the outbox of every
   * relational workflow aggregate.
   * <p>
   * Taking that name is all the wiring there is. The resolver of VanillaBP gives an aggregate
   * persisted with JPA to the bean of this name. The store VanillaBP writes itself steps back if
   * the name is already taken, because its auto-configuration is {@link ConditionalOnMissingBean}
   * and this one is applied before it. So an application which adds this artifact runs this store
   * and keeps everything else it configured.
   */
  public static final String DEFAULT_OUTBOX_BEAN_NAME = JdbcPhaseTwoOutboxAutoConfiguration.DEFAULT_OUTBOX_BEAN_NAME;

  /**
   * The name of the default gruelbox {@link TransactionOutbox} bean. The beans of the default refer
   * to each other BY NAME. So an application may define more gruelbox instances, e.g. an outbox
   * on its own table for a process under high load, without hiding or confusing the default.
   */
  public static final String DEFAULT_TRANSACTION_OUTBOX_BEAN_NAME = "vanillaBpTransactionOutbox";

  /**
   * The name of the submitter the default outbox hands its entries to. It is a bean of its own,
   * because the dispatcher needs the same submitter the outbox was built with. That submitter holds
   * entries back until dispatching starts.
   */
  public static final String DEFAULT_SUBMITTER_BEAN_NAME = "vanillaBpGruelboxSubmitter";

  /**
   * The name of the store which holds the payloads of the calls which carry one. It is a bean of
   * its own, so the outbox, the dispatch and the housekeeping all use the same one. And an
   * application may replace it.
   */
  public static final String DEFAULT_PAYLOAD_STORE_BEAN_NAME = "vanillaBpGruelboxPhaseTwoPayloadStore";

  /**
   * The name of the bean which holds the housekeeping claim of this outbox. It has a name, because
   * the dispatcher asks for it by name, the way it asks for the payload store.
   */
  public static final String DEFAULT_HOUSEKEEPING_LEASE_BEAN_NAME = "vanillaBpGruelboxHousekeepingLease";

  /**
   * The table gruelbox stores outbox entries in. It is the default name of the library, and the
   * only table its schema migration ever creates. That is why it is a constant here, and not a
   * VanillaBP property.
   */
  public static final String DEFAULT_OUTBOX_TABLE_NAME = "TXNO_OUTBOX";

  /**
   * Spring Boot creates it while it applies its auto-configurations, and only if the conditions
   * above hold. Nothing in VanillaBP creates it.
   */
  public GruelboxPhaseTwoOutboxAutoConfiguration() {
  }

  /**
   * The submitter of the default outbox. It carries "this entry was attempted before" to the
   * dispatch bean, and it keeps entries until VanillaBP starts dispatching (see
   * {@link GruelboxRedispatchAwareSubmitter}).
   *
   * @return The submitter
   */
  @Bean(DEFAULT_SUBMITTER_BEAN_NAME)
  @ConditionalOnMissingBean(name = DEFAULT_SUBMITTER_BEAN_NAME)
  public GruelboxRedispatchAwareSubmitter vanillaBpGruelboxSubmitter() {

    return new GruelboxRedispatchAwareSubmitter(Submitter.withDefaultExecutor());

  }

  /**
   * The gruelbox {@link TransactionOutbox}. It enlists entries in JDBC transactions which Spring
   * manages, and it gets the scheduled {@link GruelboxPhaseTwoDispatch} from the application
   * context.
   *
   * @param applicationContext Finds the scheduled bean at dispatch time
   * @param transactionManagers All Spring transaction managers. Entries are enlisted with the
   *          JDBC/JPA one (see {@link #selectJdbcTransactionManager(Map)})
   * @param dataSource The data source which holds the outbox table
   * @param vanillaBpProperties The bound <code>vanillabp.*</code> tree with the
   *          <code>vanillabp.outbox</code> section. It is registered here as well, so the outbox
   *          works in contexts without the full VanillaBP auto-configuration
   * @param metrics Provides what a blocked entry is counted into. Micrometer is optional, so the
   *          bean may be missing
   * @param applicationListeners The outbox listeners the application brings. They are still
   *          called, next to the one of VanillaBP
   * @param submitter The submitter of this outbox
   * @return The transaction outbox
   */
  @Bean(DEFAULT_TRANSACTION_OUTBOX_BEAN_NAME)
  @ConditionalOnMissingBean(name = DEFAULT_TRANSACTION_OUTBOX_BEAN_NAME)
  public TransactionOutbox vanillaBpTransactionOutbox(
      final ApplicationContext applicationContext,
      final Map<String, PlatformTransactionManager> transactionManagers,
      final DataSource dataSource,
      final VanillaBpConfigurationProperties vanillaBpProperties,
      final ObjectProvider<VanillaBpMetrics> metrics,
      final ObjectProvider<TransactionOutboxListener> applicationListeners,
      @Qualifier(DEFAULT_SUBMITTER_BEAN_NAME) final GruelboxRedispatchAwareSubmitter submitter) {

    final var properties = vanillaBpProperties.getOutbox();
    final var migrate = properties.isCreateSchema();
    if (!migrate) {
      validateOutboxTableExists(dataSource);
    }
    // the persistor and the transaction manager are local variables, because the listener
    // needs both to write the blocked flag of an entry gruelbox would keep retrying
    final var persistor = DefaultPersistor
        .builder()
        .dialect(detectDialect(dataSource))
        .migrate(migrate)
        .build();
    final var transactionManager = new SpringTransactionManager(
        selectJdbcTransactionManager(transactionManagers), dataSource);
    return TransactionOutbox
        .builder()
        .transactionManager(transactionManager)
        .instantiator(new SpringInstantiator(applicationContext))
        .persistor(persistor)
        .listener(
            outboxListener(persistor, transactionManager, metrics, applicationListeners, properties))
        // carries "this entry was attempted before" to the dispatch bean, and keeps
        // entries until VanillaBP dispatches (see the javadoc of the submitter)
        .submitter(submitter)
        .attemptFrequency(properties.getAttemptFrequency())
        .blockAfterAttempts(properties.getBlockAfterAttempts())
        .retentionThreshold(properties.getRetention())
        .initializeImmediately(true)
        .build();

  }

  /**
   * The listener the outbox reports its failures to: the one of VanillaBP, followed by the
   * listeners the application defined. Gruelbox takes exactly one listener. So the listeners of the
   * application are chained behind the one of VanillaBP, and they do not replace it. An application
   * which listens to its outbox keeps hearing everything it heard before.
   *
   * @param persistor The persistor of this outbox
   * @param transactionManager The transaction manager of this outbox
   * @param metrics Provider of what a blocked entry is counted into
   * @param applicationListeners The listeners the application brings
   * @param properties The settings of <code>vanillabp.outbox</code>: the attempt budget, and how
   *          long an entry may wait for a BPMS which does not report its workflow yet
   * @return The listener to hand to the outbox
   */
  private static TransactionOutboxListener outboxListener(
      final Persistor persistor,
      final SpringTransactionManager transactionManager,
      final ObjectProvider<VanillaBpMetrics> metrics,
      final ObjectProvider<TransactionOutboxListener> applicationListeners,
      final PhaseTwoOutboxProperties properties) {

    TransactionOutboxListener listener = new GruelboxPhaseTwoFailureListener(
        persistor, transactionManager, () -> SpringBootMigrationAdapterAutoConfiguration
            .vanillaBpMetricsOf(metrics), properties);
    for (final var applicationListener : applicationListeners) {
      listener = listener.andThen(applicationListener);
    }
    return listener;

  }

  /**
   * Where the payload of a phase-two call is stored while its gruelbox entry waits. Gruelbox keeps
   * a call as one serialized invocation, and its table belongs to gruelbox. So the bytes cannot
   * travel in the entry. They are kept in a table of VanillaBP, and the entry names them. A row of
   * the gruelbox table has no room for a payload.
   * <p>
   * The connection is the one Spring binds to the running transaction. So a payload becomes
   * visible exactly when the entry does. The table is created at startup, unless
   * <code>vanillabp.outbox.create-schema</code> is switched off. Then the table is checked
   * instead.
   * <p>
   * {@link JdbcPhaseTwoOutboxStore#payloadTableName} finds the name, like the name of the payload
   * table of the JDBC outbox of VanillaBP. So it follows
   * <code>vanillabp.outbox.jdbc.payload-table</code>. If that is not set, it is the name behind
   * <code>vanillabp.outbox.jdbc.table</code> plus a suffix. Neither key says anything about the
   * <code>TXNO_OUTBOX</code> of gruelbox. That table belongs to the library, and this one belongs
   * to VanillaBP.
   *
   * @param dataSource The data source the payload table lives in
   * @param vanillaBpProperties The bound <code>vanillabp.*</code> tree. It names the table if the
   *          application configured one of its own
   * @return The payload store of the default gruelbox outbox
   */
  @Bean(DEFAULT_PAYLOAD_STORE_BEAN_NAME)
  @ConditionalOnMissingBean(name = DEFAULT_PAYLOAD_STORE_BEAN_NAME)
  public JdbcPhaseTwoPayloadStore vanillaBpGruelboxPhaseTwoPayloadStore(
      final DataSource dataSource,
      final VanillaBpConfigurationProperties vanillaBpProperties) {

    final var payloadTable = JdbcPhaseTwoOutboxStore.payloadTableName(vanillaBpProperties.getOutbox());
    final var store = new JdbcPhaseTwoPayloadStore(
        new JdbcConnectionAccess() {
          @Override
          public Connection acquire() {

            // bound to the transaction Spring manages, if one runs (the write needs
            // one). Otherwise a plain connection of the pool, which the reads of the
            // dispatch and the housekeeping use
            return DataSourceUtils.getConnection(dataSource);

          }

          @Override
          public void release(
              final Connection connection) {

            DataSourceUtils.releaseConnection(connection, dataSource);

          }
        }, payloadTable,
        // gruelbox owns its table and keeps a call as one serialized invocation. So
        // there is no column to join on. The housekeeping looks for the reference
        // inside that text, which costs a scan of the entries (see decision 3 in the
        // repository's DECISIONS.md)
        JdbcPhaseTwoPayloadStore.EntriesNamingTheirPayload
            .insideAText(DEFAULT_OUTBOX_TABLE_NAME, "invocation"));
    if (vanillaBpProperties.getOutbox().isCreateSchema()) {
      store.createSchemaIfNotExists();
    } else {
      store.validateSchemaExists();
    }
    return store;

  }

  /**
   * The phase-two outbox of every workflow aggregate this application persists in its relational
   * database. Spring builds it INSTEAD OF the store VanillaBP writes itself, because this artifact
   * is on the classpath and there is exactly one {@link EntityManagerFactory}. An aggregate in
   * MongoDB is served by the MongoDB outbox next to it. Decision 2 in the repository's DECISIONS.md
   * says why this store is offered at all.
   *
   * @param transactionOutbox The gruelbox transaction outbox
   * @param dataSource The data source which holds the table of gruelbox. It counts the entries
   *          which wait for their dispatch
   * @param payloadStore Where the payload of a call which carries one is written
   * @return The {@link PhaseTwoOutbox} used by the process services
   */
  @Bean(DEFAULT_OUTBOX_BEAN_NAME)
  public GruelboxPhaseTwoOutbox vanillaBpGruelboxPhaseTwoOutbox(
      @Qualifier(DEFAULT_TRANSACTION_OUTBOX_BEAN_NAME) final TransactionOutbox transactionOutbox,
      final DataSource dataSource,
      @Qualifier(DEFAULT_PAYLOAD_STORE_BEAN_NAME) final JdbcPhaseTwoPayloadStore payloadStore) {

    return new GruelboxPhaseTwoOutbox(
        transactionOutbox, dataSource, DEFAULT_OUTBOX_TABLE_NAME, payloadStore);

  }

  /**
   * The bean the outbox calls at dispatch time. The <code>SpringInstantiator</code> of gruelbox
   * finds it as the only bean of type {@link GruelboxPhaseTwoDispatch}. It builds the
   * {@link PhaseTwoCall} again and hands it to the {@link PhaseTwoRouter} of the core.
   *
   * @param phaseTwoRouter Provides the router the call is handed to
   * @param payloadStore Where the payload of an entry which names one is read from
   * @param metrics Provides where the wait of a dispatched entry is reported. Micrometer is
   *          optional, so the bean may be missing
   * @return The dispatch bean
   */
  @Bean
  public GruelboxPhaseTwoDispatch vanillaBpGruelboxPhaseTwoDispatch(
      final ObjectProvider<PhaseTwoRouter> phaseTwoRouter,
      @Qualifier(DEFAULT_PAYLOAD_STORE_BEAN_NAME) final ObjectProvider<JdbcPhaseTwoPayloadStore> payloadStore,
      final ObjectProvider<VanillaBpMetrics> metrics) {

    return (
        operation,
        workflowModuleId,
        bpmnProcessId,
        workflowAggregateId,
        adapterId,
        serializedArgs) -> new GruelboxPhaseTwoDispatchBean(
            phaseTwoRouter.getObject(), payloadStore
                .getObject(), SpringBootMigrationAdapterAutoConfiguration
                    .vanillaBpMetricsOf(metrics))
            .dispatch(
                operation, workflowModuleId, bpmnProcessId, workflowAggregateId, adapterId, serializedArgs);

  }

  /**
   * Where this node says that it cleans up the payloads of this outbox tonight.
   * <p>
   * Gruelbox deletes its own dispatched entries. So here the window only decides about the payload
   * table. But that table belongs to VanillaBP. If two nodes cleaned it up at the same time, each
   * would measure the work of the other.
   *
   * @param dataSource The data source the claim lives in
   * @param vanillaBpProperties The bound <code>vanillabp.*</code> tree. It names the table if the
   *          application configured one of its own
   * @return The claim of this outbox
   */
  @Bean(DEFAULT_HOUSEKEEPING_LEASE_BEAN_NAME)
  @ConditionalOnMissingBean(name = DEFAULT_HOUSEKEEPING_LEASE_BEAN_NAME)
  public JdbcHousekeepingLease vanillaBpGruelboxHousekeepingLease(
      final DataSource dataSource,
      final VanillaBpConfigurationProperties vanillaBpProperties) {

    final var lease = new JdbcHousekeepingLease(
        JdbcPhaseTwoOutbox.connectionsOf(dataSource), vanillaBpProperties
            .getOutbox()
            .getJdbc()
            .housekeepingTableName());
    if (vanillaBpProperties.getOutbox().isCreateSchema()) {
      lease.createSchemaIfNotExists();
    } else {
      lease.validateSchemaExists();
    }
    return lease;

  }

  /**
   * What flushes gruelbox. It dispatches the entries a crashed instance left behind. It gives a
   * failed entry its next attempt once the due time has passed. And it deletes what the retention
   * let go. It also opens the gate of the submitter, so nothing reaches a BPMS before the models
   * did.
   *
   * @param transactionOutbox The gruelbox transaction outbox
   * @param vanillaBpProperties The bound <code>vanillabp.*</code> tree with the
   *          <code>vanillabp.outbox</code> section. It is registered here as well, so the outbox
   *          works in contexts without the full VanillaBP auto-configuration
   * @param submitter The submitter the outbox was built with. It is held back until this
   *          dispatcher starts polling
   * @param outbox The store. It is asked when the next flush has something to do, so the poller
   *          can sleep until then
   * @param payloadStore Where the payloads of the entries a flush finished are removed. That also
   *          removes what a crash between the two writes of a schedule left behind
   * @param housekeepingLease Where this node says that it cleans up tonight
   * @param metrics What the numbers of a closed housekeeping window are published to
   * @return The dispatcher which polls the outbox for recovery, retries and the cleanup of the
   *         retention. It runs on a private executor with one thread, without a
   *         {@link org.springframework.scheduling.TaskScheduler}
   */
  @Bean
  public GruelboxPhaseTwoOutboxDispatcher vanillaBpGruelboxPhaseTwoOutboxDispatcher(
      @Qualifier(DEFAULT_TRANSACTION_OUTBOX_BEAN_NAME) final TransactionOutbox transactionOutbox,
      final VanillaBpConfigurationProperties vanillaBpProperties,
      @Qualifier(DEFAULT_SUBMITTER_BEAN_NAME) final GruelboxRedispatchAwareSubmitter submitter,
      @Qualifier(DEFAULT_OUTBOX_BEAN_NAME) final GruelboxPhaseTwoOutbox outbox,
      @Qualifier(DEFAULT_PAYLOAD_STORE_BEAN_NAME) final JdbcPhaseTwoPayloadStore payloadStore,
      @Qualifier(DEFAULT_HOUSEKEEPING_LEASE_BEAN_NAME) final JdbcHousekeepingLease housekeepingLease,
      final ObjectProvider<VanillaBpMetrics> metrics) {

    return new GruelboxPhaseTwoOutboxDispatcher(
        transactionOutbox, vanillaBpProperties
            .getOutbox(), submitter, outbox, payloadStore, housekeepingLease, () -> metrics
                .getIfAvailable(() -> VanillaBpMetrics.NONE));

  }

  /**
   * Selects the transaction manager the outbox entries are enlisted with. It is the JDBC one,
   * whose transaction also persists the JPA workflow aggregates. An application with more than one
   * kind of persistence has a second transaction manager (e.g. for MongoDB). The JDBC/JPA one is
   * found by the usual Spring Boot bean name <code>transactionManager</code>.
   *
   * @param transactionManagers All transaction managers, keyed by bean name
   * @return The JDBC/JPA transaction manager
   * @throws IllegalStateException If several transaction managers exist and none is
   *           named <code>transactionManager</code>
   */
  static PlatformTransactionManager selectJdbcTransactionManager(
      final Map<String, PlatformTransactionManager> transactionManagers) {

    if (transactionManagers.size() == 1) {
      return transactionManagers
          .values()
          .iterator()
          .next();
    }
    final var conventional = transactionManagers.get("transactionManager");
    if (conventional != null) {
      return conventional;
    }
    throw new IllegalStateException(
        """
            Several transaction managers exist (%s), but none is named 'transactionManager'! The \
            JDBC-based phase-two outbox enlists its entries with the transaction manager persisting \
            the JPA workflow aggregates - name that one 'transactionManager' (Spring Boot's \
            convention) or define your own gruelbox TransactionOutbox bean named '%s'."""
            .formatted(transactionManagers.keySet(), DEFAULT_TRANSACTION_OUTBOX_BEAN_NAME));

  }

  /**
   * Checks that the outbox table exists, if <code>vanillabp.outbox.create-schema</code> switched
   * off the schema migration of gruelbox. An application which applies its schema itself does that.
   * <p>
   * This table belongs to gruelbox, unlike the tables of VanillaBP. So the message points to
   * gruelbox for the statements, not to <code>io.vanillabp:vanillabp-schema</code>. Without the
   * check, a missing table shows up at the first workflow started on a remote BPMS, hours after a
   * deployment which started cleanly.
   *
   * @param dataSource The data source holding the outbox table
   * @throws IllegalStateException If the table is missing
   */
  private static void validateOutboxTableExists(
      final DataSource dataSource) {

    try (var connection = dataSource.getConnection()) {
      if (JdbcSchema.tableExists(connection, DEFAULT_OUTBOX_TABLE_NAME)) {
        return;
      }
    } catch (final SQLException e) {
      throw new IllegalStateException(
          "Could not check whether the phase-two outbox table '%s' exists!"
              .formatted(DEFAULT_OUTBOX_TABLE_NAME), e);
    }
    throw new IllegalStateException(
        """
            The phase-two outbox table '%s' does not exist! Starting a workflow on a remote BPMS \
            writes an entry into it inside the caller's transaction, so without the table nothing \
            can be started. This table is gruelbox's own, not VanillaBP's: it is NOT part of \
            'io.vanillabp:vanillabp-schema' and gruelbox's schema migration is switched off here. \
            Either
            - apply gruelbox's schema with your migration tool: \
            'com.gruelbox.transactionoutbox.DefaultPersistor.writeSchema(Writer)' writes the \
            statements for the database you configure, or
            - let gruelbox create the table by setting '%s' to \
            'true' (the default)."""
            .formatted(DEFAULT_OUTBOX_TABLE_NAME, PhaseTwoOutboxProperties.CREATE_SCHEMA_PROPERTY));

  }

  /**
   * Finds the gruelbox SQL dialect from the metadata of the data source.
   * <p>
   * It is package-private, so a test can check the mapping and the message for an unsupported
   * database without a database of each product.
   *
   * @param dataSource The data source used for the outbox table
   * @return The dialect
   * @throws IllegalStateException If the database is not supported by gruelbox
   */
  static Dialect detectDialect(
      final DataSource dataSource) {

    final String productName;
    try (var connection = dataSource.getConnection()) {
      productName = connection.getMetaData().getDatabaseProductName();
    } catch (SQLException e) {
      throw new IllegalStateException(
          "Could not determine the database product name used to configure the VanillaBP phase-two outbox!", e);
    }
    final var product = productName.toLowerCase();
    if (product.contains("h2")) {
      return Dialect.H2;
    }
    if (product.contains("postgres")) {
      return Dialect.POSTGRESQL_9;
    }
    if (product.contains("mysql") || product.contains("mariadb")) {
      return Dialect.MY_SQL_8;
    }
    if (product.contains("oracle")) {
      return Dialect.ORACLE;
    }
    if (product.contains("microsoft")) {
      return Dialect.MS_SQL_SERVER;
    }
    throw new IllegalStateException(
        """
            Database '%s' is not supported by the gruelbox-based VanillaBP phase-two outbox! \
            Define your own com.gruelbox.transactionoutbox.TransactionOutbox bean or provide a custom \
            implementation of io.vanillabp.integration.spi.PhaseTwoOutbox."""
            .formatted(productName));

  }

}
