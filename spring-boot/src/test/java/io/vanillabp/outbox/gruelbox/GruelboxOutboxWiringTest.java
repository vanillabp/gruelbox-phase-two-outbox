package io.vanillabp.outbox.gruelbox;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import javax.sql.DataSource;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.transaction.PlatformTransactionManager;

import com.gruelbox.transactionoutbox.Dialect;
import com.gruelbox.transactionoutbox.TransactionOutbox;

import io.vanillabp.integration.adapter.migration.jdbc.JdbcSchema;
import io.vanillabp.integration.config.VanillaBpConfigurationProperties;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * The two decisions the gruelbox-based default outbox makes while it is wired, both of
 * them invisible until they are wrong: which SQL dialect gruelbox writes with, and which
 * transaction manager its entries are enlisted in. A wrong dialect produces SQL the
 * database rejects at the first workflow started on a remote BPMS, and a wrong
 * transaction manager commits the entry separately from the aggregate - the very split
 * the outbox exists to prevent.
 */
@ExtendWith(SuppressOutputExtension.class)
public class GruelboxOutboxWiringTest {

  private static DataSource reporting(
      final String databaseProductName) throws Exception {

    final var metaData = Mockito.mock(DatabaseMetaData.class);
    Mockito.when(metaData.getDatabaseProductName()).thenReturn(databaseProductName);
    final var connection = Mockito.mock(Connection.class);
    Mockito.when(connection.getMetaData()).thenReturn(metaData);
    final var dataSource = Mockito.mock(DataSource.class);
    Mockito.when(dataSource.getConnection()).thenReturn(connection);
    return dataSource;

  }

  @Test
  @DisplayName("The dialect is read from the product name the driver reports")
  public void theDialectIsReadFromTheProductName() throws Exception {

    // gruelbox writes its statements in the dialect chosen here - a wrong one is
    // syntactically valid Java and invalid SQL, and shows at the first workflow
    // started on a remote BPMS
    assertEquals(Dialect.H2, GruelboxPhaseTwoOutboxAutoConfiguration.detectDialect(reporting("H2")));
    assertEquals(
        Dialect.POSTGRESQL_9,
        GruelboxPhaseTwoOutboxAutoConfiguration.detectDialect(reporting("PostgreSQL")));
    assertEquals(Dialect.MY_SQL_8, GruelboxPhaseTwoOutboxAutoConfiguration.detectDialect(reporting("MySQL")));
    // MariaDB reports its own product name and speaks the MySQL dialect
    assertEquals(Dialect.MY_SQL_8, GruelboxPhaseTwoOutboxAutoConfiguration.detectDialect(reporting("MariaDB")));
    assertEquals(Dialect.ORACLE, GruelboxPhaseTwoOutboxAutoConfiguration.detectDialect(reporting("Oracle")));
    assertEquals(
        Dialect.MS_SQL_SERVER,
        GruelboxPhaseTwoOutboxAutoConfiguration.detectDialect(reporting("Microsoft SQL Server")));

  }

  @Test
  @DisplayName("A database gruelbox has no dialect for fails naming it and both ways out")
  public void anUnsupportedDatabaseFailsGuiding() throws Exception {

    final var dataSource = reporting("Informix Dynamic Server");

    final var exception = assertThrows(
        IllegalStateException.class,
        () -> GruelboxPhaseTwoOutboxAutoConfiguration.detectDialect(dataSource));

    assertTrue(exception.getMessage().contains("Informix Dynamic Server"), exception.getMessage());
    assertTrue(exception.getMessage().contains("TransactionOutbox"), exception.getMessage());
    assertTrue(exception.getMessage().contains("PhaseTwoOutbox"), exception.getMessage());

  }

  @Test
  @DisplayName("With one transaction manager that one is used, whatever it is called")
  public void theSingleTransactionManagerIsUsed() {

    final var only = Mockito.mock(PlatformTransactionManager.class);

    assertSame(
        only,
        GruelboxPhaseTwoOutboxAutoConfiguration.selectJdbcTransactionManager(Map.of("whateverItIsCalled", only)));

  }

  @Test
  @DisplayName("With several transaction managers the one named by Spring Boot's convention wins")
  public void theConventionallyNamedTransactionManagerWins() {

    final var jpa = Mockito.mock(PlatformTransactionManager.class);
    final var mongo = Mockito.mock(PlatformTransactionManager.class);
    final var managers = new LinkedHashMap<String, PlatformTransactionManager>();
    // the Mongo one first: picking "the first" would be wrong, and the outbox table
    // lives in the JDBC data source
    managers.put("mongoTransactionManager", mongo);
    managers.put("transactionManager", jpa);

    assertSame(jpa, GruelboxPhaseTwoOutboxAutoConfiguration.selectJdbcTransactionManager(managers));

  }

  @Test
  @DisplayName("Several transaction managers without the conventional name fail naming all of them")
  public void severalUnconventionalTransactionManagersFailGuiding() {

    final var managers = new LinkedHashMap<String, PlatformTransactionManager>();
    managers.put("mongoTransactionManager", Mockito.mock(PlatformTransactionManager.class));
    managers.put("jpaTransactionManager", Mockito.mock(PlatformTransactionManager.class));

    final var exception = assertThrows(
        IllegalStateException.class,
        () -> GruelboxPhaseTwoOutboxAutoConfiguration.selectJdbcTransactionManager(managers));

    assertTrue(exception.getMessage().contains("mongoTransactionManager"), exception.getMessage());
    assertTrue(exception.getMessage().contains("jpaTransactionManager"), exception.getMessage());
    assertTrue(exception.getMessage().contains("transactionManager"), exception.getMessage());
    assertTrue(
        exception
            .getMessage()
            .contains(GruelboxPhaseTwoOutboxAutoConfiguration.DEFAULT_TRANSACTION_OUTBOX_BEAN_NAME),
        exception.getMessage());

  }

  private static SingleConnectionDataSource h2() {

    // one connection kept open: the in-memory database lives as long as it does, which
    // is what makes "the table gruelbox created is there" assertable
    final var dataSource = new SingleConnectionDataSource(
        "jdbc:h2:mem:%s;DB_CLOSE_DELAY=-1".formatted(UUID.randomUUID()), "sa", "", true);
    dataSource.setDriverClassName("org.h2.Driver");
    return dataSource;

  }

  private static TransactionOutbox buildOutbox(
      final DataSource dataSource,
      final VanillaBpConfigurationProperties properties) {

    try (var context = new AnnotationConfigApplicationContext()) {
      context.refresh();
      return new GruelboxPhaseTwoOutboxAutoConfiguration()
          .vanillaBpTransactionOutbox(
              context,
              Map.of("transactionManager", new DataSourceTransactionManager(dataSource)),
              dataSource,
              properties,
              context.getBeanProvider(
                  io.vanillabp.integration.adapter.migration.observability.VanillaBpMetrics.class),
              context.getBeanProvider(com.gruelbox.transactionoutbox.TransactionOutboxListener.class),
              new GruelboxRedispatchAwareSubmitter(com.gruelbox.transactionoutbox.Submitter.withDefaultExecutor()));
    }

  }

  @Test
  @DisplayName("With the default configuration gruelbox creates its table while the outbox is built")
  public void theDefaultConfigurationCreatesTheOutboxTable() throws Exception {

    final var dataSource = h2();

    assertNotNull(buildOutbox(dataSource, new VanillaBpConfigurationProperties()));

    try (var connection = dataSource.getConnection()) {
      assertTrue(
          JdbcSchema.tableExists(connection, GruelboxPhaseTwoOutboxAutoConfiguration.DEFAULT_OUTBOX_TABLE_NAME),
          "gruelbox' migration did not run");
    }

  }

  @Test
  @DisplayName("An application managing the schema itself is stopped at startup when the table is missing")
  public void aMissingTableStopsTheStartupInsteadOfTheFirstWorkflow() {

    // Without the check the missing table surfaces at the first workflow
    // started on a remote BPMS, hours after a deployment which booted cleanly
    final var properties = new VanillaBpConfigurationProperties();
    properties.getOutbox().setCreateSchema(false);
    final var dataSource = h2();

    final var exception = assertThrows(IllegalStateException.class, () -> buildOutbox(dataSource, properties));

    final var message = exception.getMessage();
    assertTrue(
        message.contains(GruelboxPhaseTwoOutboxAutoConfiguration.DEFAULT_OUTBOX_TABLE_NAME),
        message);
    // the table belongs to gruelbox, not to io.vanillabp:vanillabp-schema - the
    // message has to point where the statements actually come from
    assertTrue(message.contains("gruelbox"), message);

  }

  @Test
  @DisplayName("The table of VanillaBP's own outbox does not rename gruelbox' table")
  public void theNameOfVanillaBpsOwnTableLeavesGruelboxAlone() throws Exception {

    // the two tables hold different columns, so one name for both would create the
    // wrong schema for whichever store the application ends up running
    final var properties = new VanillaBpConfigurationProperties();
    properties.getOutbox().getJdbc().setTable("MY_OWN_OUTBOX");
    final var dataSource = h2();

    assertNotNull(buildOutbox(dataSource, properties));

    try (var connection = dataSource.getConnection()) {
      assertTrue(
          JdbcSchema.tableExists(connection, GruelboxPhaseTwoOutboxAutoConfiguration.DEFAULT_OUTBOX_TABLE_NAME),
          "gruelbox' migration did not run");
      assertFalse(
          JdbcSchema.tableExists(connection, "MY_OWN_OUTBOX"),
          "gruelbox wrote a table named after VanillaBP's own outbox");
    }

  }

}
