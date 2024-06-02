package com.example;

import java.util.Arrays;
import java.util.regex.Pattern;
import schemacrawler.schema.Catalog;
import schemacrawler.schema.Column;
import schemacrawler.schema.Schema;
import schemacrawler.schema.Table;
import schemacrawler.schema.View;
import schemacrawler.schemacrawler.LimitOptions;
import schemacrawler.schemacrawler.LimitOptionsBuilder;
import schemacrawler.schemacrawler.LoadOptions;
import schemacrawler.schemacrawler.LoadOptionsBuilder;
import schemacrawler.schemacrawler.SchemaCrawlerOptions;
import schemacrawler.schemacrawler.SchemaCrawlerOptionsBuilder;
import schemacrawler.schemacrawler.SchemaInfoLevel;
import schemacrawler.schemacrawler.SchemaInfoLevelBuilder;
import schemacrawler.tools.utility.SchemaCrawlerUtility;
import us.fatehi.utility.datasource.DatabaseConnectionSource;
import us.fatehi.utility.datasource.DatabaseConnectionSources;
import us.fatehi.utility.datasource.MultiUseUserCredentials;

public class Issue1525 {

  private static SchemaInfoLevel getSchemaInfoLevel() {
    System.out.println("IS_FIELD_LIST_REQUESTED: " + true);
    return SchemaInfoLevelBuilder.builder()
        .fromOptions(SchemaInfoLevelBuilder.minimum())
        .setRetrieveTables(true)
        .setRetrieveViewInformation(true)
        .setRetrieveTableColumns(true)
        .setRetrieveColumnDataTypes(true)
        .setRetrieveDatabaseInfo(true)
        .setRetrieveAdditionalColumnAttributes(true)
        .setRetrieveAdditionalColumnMetadata(true)
        .setRetrieveAdditionalTableAttributes(true)
        .setRetrieveAdditionalDatabaseInfo(false)
        .setRetrieveDatabaseUsers(false)
        .setRetrievePrimaryKeys(true)
        .setRetrieveForeignKeys(false)
        // get indexes along with FK
        .setRetrieveIndexes(false)
        .setRetrieveIndexInformation(false)
        .setRetrieveRoutineInformation(false)
        .setRetrieveRoutines(false)
        .setRetrieveRoutineParameters(false)
        .setRetrieveSequenceInformation(false)
        .setRetrieveServerInfo(false)
        .setRetrieveSynonymInformation(false)
        .setRetrieveTableColumnPrivileges(false)
        .setRetrieveTableConstraintDefinitions(false)
        .setRetrieveTableConstraintInformation(false)
        .setRetrieveTablePrivileges(false)
        .setRetrieveTriggerInformation(false)
        .setRetrieveUserDefinedColumnDataTypes(false)
        .toOptions();
  }

  private static LimitOptionsBuilder schemaCrawlerLimitOptionsBuilder(
      final String tableNamePatternForPage) {
    final LimitOptionsBuilder limitOptionsBuilder = LimitOptionsBuilder.builder();
    limitOptionsBuilder.tableTypes(Arrays.asList("TABLE"));
    if (tableNamePatternForPage != null) {
      limitOptionsBuilder.includeTables(
          Pattern.compile(tableNamePatternForPage, Pattern.CASE_INSENSITIVE));
    }
    return limitOptionsBuilder;
  }

  private static LoadOptionsBuilder schemaCrawlerLoadOptionsBuilder() {
    return LoadOptionsBuilder.builder().withSchemaInfoLevel(getSchemaInfoLevel());
  }

  private static SchemaCrawlerOptions schemaCrawlerOptionsBuilder(
      final String tableNamePatternForPage) {
    final LimitOptions limitOptions =
        schemaCrawlerLimitOptionsBuilder(tableNamePatternForPage).toOptions();
    final LoadOptions loadOptions = schemaCrawlerLoadOptionsBuilder().toOptions();
    return SchemaCrawlerOptionsBuilder.newSchemaCrawlerOptions()
        .withLimitOptions(limitOptions)
        .withLoadOptions(loadOptions);
  }

  private static DatabaseConnectionSource getDatabase() {
    // "jdbc:sqlserver://localhost:1433;DatabaseName=User;encrypt=false" form
    final String url = "jdbc:sqlserver://localhost:1433;DatabaseName=AdventureWorks;encrypt=false";
    return DatabaseConnectionSources.newDatabaseConnectionSource(
        url, new MultiUseUserCredentials("SA", "Schem#Crawl3r")); // sql server userId and password
  }

  public static void main(String[] args) {

    final DatabaseConnectionSource scDatabaseConnectionSource = getDatabase();

    final Catalog catalog =
        SchemaCrawlerUtility.getCatalog(
            scDatabaseConnectionSource, schemaCrawlerOptionsBuilder("AdventureWorks.HumanResources.EmployeePayHistory"));
    for (final Schema schema : catalog.getSchemas()) {
      System.out.println(schema);
      for (final Table table : catalog.getTables(schema)) {
        System.out.print("o--> " + table);
        if (table instanceof View) {
          System.out.println(" (VIEW)");
        } else {
          System.out.println();
        }

        for (final Column column : table.getColumns()) {
          System.out.printf("     o--> %s (%s)%n", column, column.getType());
        }
      }
    }

  }
}
