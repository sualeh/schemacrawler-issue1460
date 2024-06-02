package com.example;

import java.util.Arrays;
import java.util.logging.Level;
import org.json.JSONArray;
import org.json.JSONObject;
import schemacrawler.schema.Catalog;
import schemacrawler.schema.Column;
import schemacrawler.schema.Index;
import schemacrawler.schema.IndexColumn;
import schemacrawler.schema.Schema;
import schemacrawler.schema.Table;
import schemacrawler.schemacrawler.LimitOptionsBuilder;
import schemacrawler.schemacrawler.LoadOptionsBuilder;
import schemacrawler.schemacrawler.SchemaCrawlerOptions;
import schemacrawler.schemacrawler.SchemaCrawlerOptionsBuilder;
import schemacrawler.schemacrawler.SchemaInfoLevel;
import schemacrawler.schemacrawler.SchemaInfoLevelBuilder;
import schemacrawler.schemacrawler.exceptions.SchemaCrawlerException;
import schemacrawler.tools.utility.SchemaCrawlerUtility;
import us.fatehi.utility.LoggingConfig;
import us.fatehi.utility.datasource.DatabaseConnectionSource;
import us.fatehi.utility.datasource.DatabaseConnectionSources;
import us.fatehi.utility.datasource.MultiUseUserCredentials;

public final class Issue1460 {

  public static void main(String[] args) throws SchemaCrawlerException {

    JSONObject result = new JSONObject();
    try {
      new LoggingConfig(Level.SEVERE);

      final LimitOptionsBuilder limitOptionsBuilder =
          LimitOptionsBuilder.builder()
              // I don't know why but sql server version need .dbo to get only table what I created
              // ANSWER: The schema for SchemaCrawler is the Microsoft SQL Server database + schema
              // separatd by a dot. It may be surrounded by double quotes.
              // .includeSchemas(schema -> Arrays.asList("User.dbo").contains(schema))
              .includeSchemas(schema -> Arrays.asList("AdventureWorks.dbo").contains(schema))
              .tableTypes("table");
      final SchemaInfoLevel schemaInfoLevel = SchemaInfoLevelBuilder.standard();
      final LoadOptionsBuilder loadOptionsBuilder =
          LoadOptionsBuilder.builder().withSchemaInfoLevel(schemaInfoLevel);
      final SchemaCrawlerOptions schemaCrawlerOptions =
          SchemaCrawlerOptionsBuilder.newSchemaCrawlerOptions()
              .withLimitOptions(limitOptionsBuilder.toOptions())
              .withLoadOptions(loadOptionsBuilder.toOptions());

      final Catalog catalog = SchemaCrawlerUtility.getCatalog(getDatabase(), schemaCrawlerOptions);
      for (Schema schema : catalog.getSchemas()) {
        for (final Table table : catalog.getTables(schema)) {
          JSONObject tableData = new JSONObject();
          JSONArray columns = new JSONArray();
          JSONArray pk = new JSONArray();
          JSONArray fk = new JSONArray();
          JSONObject ix = new JSONObject();
          for (final Column column : table.getColumns()) {
            JSONObject _col = new JSONObject();
            if (column.isPartOfPrimaryKey()) {
              pk.put(column.getName());
            }
            if (column.isPartOfForeignKey()) {
              fk.put(column.getName());
            }
            _col.put("column", column.getName());
            _col.put("type", column.getColumnDataType().getName().split(" ")[0]);
            _col.put("length", column.getSize());
            _col.put("decimal", column.getDecimalDigits());
            _col.put("constraint", column.isNullable() ? "null" : "not null");
            columns.put(_col);
          }
          for (Index index : table.getIndexes()) {
            JSONObject _index = new JSONObject();
            JSONArray _indexColumns = new JSONArray();
            for (IndexColumn indexColumn : index.getColumns()) {
              _indexColumns.put(indexColumn.getName());
            }
            _index.put("columns", _indexColumns);
            ix.put(index.getName(), _index);
          }
          tableData.put("table", table.getName());
          tableData.put("columns", columns);
          tableData.put("pk", pk);
          tableData.put("fk", fk);
          tableData.put("index", ix);
          result.put(table.getName(), tableData);
        }
      }
      System.out.println(result.toString(2));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static DatabaseConnectionSource getDatabase() {
    // "jdbc:sqlserver://localhost:1433;DatabaseName=User;encrypt=false" form
    final String url = "jdbc:sqlserver://localhost:1433;DatabaseName=AdventureWorks;encrypt=false";
    return DatabaseConnectionSources.newDatabaseConnectionSource(
        url, new MultiUseUserCredentials("SA", "Schem#Crawl3r")); // sql server userId and password
  }
}
