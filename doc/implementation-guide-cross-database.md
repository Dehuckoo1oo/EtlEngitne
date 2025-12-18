# Implementation Guide: Cross-Database Support + TypeConversionIntegrationTest Fixes

**Document Version:** 1.0
**Created:** 2025-12-18
**Status:** Ready for Implementation
**Priority:** P0 (блокирующие тесты)
**Estimated Time:** 25-30 минут

---

## Table of Contents
1. [Overview](#overview)
2. [Problem Context](#problem-context)
3. [Implementation Part 1: SqlVariantQueryGenerator](#implementation-part-1-sqlvariantquerygenerator)
4. [Implementation Part 2: AvroToRecordTransformer](#implementation-part-2-avrotorecordtransformer)
5. [Testing and Verification](#testing-and-verification)
6. [Rollback Plan](#rollback-plan)

---

## Overview

### Goal
Fix 3 critical bugs (P0) that are blocking TypeConversionIntegrationTest:
1. **P0-1:** Incorrect parsing of 3-part table names (database.schema.table)
2. **P0-2:** Missing cross-database support in INFORMATION_SCHEMA queries
3. **P0-3:** Missing precision/scale in ColumnMetadata from Avro schema

### Affected Files
- `src/main/java/ru/pospelov/etl/engine/steps/extractor/jdbc/SqlVariantQueryGenerator.java`
- `src/main/java/ru/pospelov/etl/engine/steps/transformer/AvroToRecordTransformer.java`

### Current Test Status
```
TypeConversionIntegrationTest:
✅ nullValues_allTypes_shouldPreserveNulls - PASS
✅ dateConversion_cornerCases_shouldPreserveExactValues - PASS
✅ uniqueidentifierConversion_shouldPreserveExactGuid - PASS
❌ decimalConversion_precisionAndScale_shouldPreserveExactly - FAIL
❌ binaryConversion_shouldPreserveExactBytes - FAIL
❌ sqlVariantConversion_differentBaseTypes_shouldPreserveTypesAndValues - FAIL
```

---

## Problem Context

### Problem 1: Incorrect Parsing of 3-Part Table Names

**Symptom:**
Tests use table name `SUPPORT.dbo.type_conversion_src` but current code incorrectly parses it.

**Current Code Behavior (SqlVariantQueryGenerator.java:56-59):**
```java
if (tableName.contains(".")) {
    String[] parts = tableName.split("\\.", 2);  // Split only on FIRST dot
    schema = parts[0];  // "SUPPORT" ← WRONG (this is database, not schema)
    table = parts[1];   // "dbo.type_conversion_src" ← WRONG
}
```

**Result:**
- schema = "SUPPORT" (incorrect - this is database name)
- table = "dbo.type_conversion_src" (incorrect - this contains both schema and table)

### Problem 2: Missing Cross-Database Support

**Current Code (SqlVariantQueryGenerator.java:127-132):**
```java
String query = """
        SELECT COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
        """;
```

**Issue:**
- `FROM INFORMATION_SCHEMA.COLUMNS` queries the **current** database
- When JdbcTemplate is connected to database X, but table is in database Y (SUPPORT), query returns 0 rows
- For cross-database, need: `FROM SUPPORT.INFORMATION_SCHEMA.COLUMNS`

**Cascading Effect:**
1. `queryInformationSchema()` returns empty list
2. Fallback to `SELECT * FROM SUPPORT.dbo.type_conversion_src`
3. No SQL_VARIANT_PROPERTY columns added
4. JdbcStreamingResultSetExtractor can't detect sql_variant columns
5. Column read as raw Java type (Integer, String)
6. TypeConverter fails: "Cannot convert Integer to Avro STRING"

### Problem 3: Missing precision/scale in ColumnMetadata

**Current Code (AvroToRecordTransformer.java:68-75):**
```java
metadata.put(field.name(), new ColumnMetadata(
    field.name(),
    jdbcType,
    sqlTypeName,
    0, // precision - hardcoded 0 ← PROBLEM
    0, // scale - hardcoded 0 ← PROBLEM
    field.schema().isNullable()
));
```

**Issue:**
- For DECIMAL fields, precision=0 and scale=0 is invalid
- SQL Server bulk copy fails: "Length or precision specification 0 is invalid"
- Avro decimal logical type contains precision/scale that must be extracted

---

## Implementation Part 1: SqlVariantQueryGenerator

### File Location
`src/main/java/ru/pospelov/etl/engine/steps/extractor/jdbc/SqlVariantQueryGenerator.java`

### Current State Summary
- Lines 1-40: Package, imports, JavaDoc, class declaration
- Lines 41-117: `generateSelectQuery(String)` method
- Lines 119-149: `queryInformationSchema(String, String)` method
- Lines 151-158: `ColumnInfo` record

### Changes Overview
1. Add `TableIdentifier` record after line 40
2. Add `parseTableName(String)` method after line 50
3. Update `generateSelectQuery(String)` method (lines 51-117)
4. Update `queryInformationSchema` signature and implementation (lines 119-149)
5. Update JavaDoc

---

### Change 1.1: Add TableIdentifier Record

**Location:** After line 40 (after JdbcTemplate field declaration)

**Add this code:**
```java
    /**
     * Parsed components of a table identifier.
     * Supports formats: table, schema.table, database.schema.table
     *
     * @param database Database name (null = current database)
     * @param schema Schema name (never null, defaults to "dbo")
     * @param table Table name (never null)
     * @param fullName Original full name for use in FROM clause
     */
    private record TableIdentifier(
            String database,
            String schema,
            String table,
            String fullName
    ) {}
```

---

### Change 1.2: Add parseTableName Method

**Location:** After the TableIdentifier record, before `generateSelectQuery` method

**Add this code:**
```java
    /**
     * Parses table name into database, schema, and table components.
     * Supports multiple formats:
     * <ul>
     * <li>"table" → database=null, schema="dbo", table="table"</li>
     * <li>"schema.table" → database=null, schema="schema", table="table"</li>
     * <li>"database.schema.table" → database="database", schema="schema", table="table"</li>
     * </ul>
     *
     * @param tableName Full table name (may include database and/or schema)
     * @return Parsed table identifier
     * @throws IllegalArgumentException if table name is invalid
     */
    private TableIdentifier parseTableName(String tableName) {
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("Table name cannot be null or blank");
        }

        tableName = tableName.trim();

        if (!tableName.contains(".")) {
            // Format: table
            return new TableIdentifier(null, "dbo", tableName, tableName);
        }

        String[] parts = tableName.split("\\.");

        if (parts.length == 2) {
            // Format: schema.table
            return new TableIdentifier(null, parts[0], parts[1], tableName);
        } else if (parts.length >= 3) {
            // Format: database.schema.table (or more parts - take last 3)
            String database = parts[parts.length - 3];
            String schema = parts[parts.length - 2];
            String table = parts[parts.length - 1];
            return new TableIdentifier(database, schema, table, tableName);
        }

        throw new IllegalArgumentException("Invalid table name format: " + tableName);
    }
```

---

### Change 1.3: Update generateSelectQuery Method

**Location:** Replace lines 51-117

**BEFORE (Current Code):**
```java
    public String generateSelectQuery(String tableName) {
        // Parse schema and table name
        String schema = "dbo"; // default schema for SQL Server
        String table = tableName;

        if (tableName.contains(".")) {
            String[] parts = tableName.split("\\.", 2);
            schema = parts[0];
            table = parts[1];
        }

        log.debug("Generating SELECT query for table: schema={}, table={}", schema, table);

        // Query INFORMATION_SCHEMA to get all columns and identify sql_variant columns
        List<ColumnInfo> columns = queryInformationSchema(schema, table);

        if (columns.isEmpty()) {
            log.warn("No columns found for table {}.{}, returning simple SELECT *", schema, table);
            return "SELECT * FROM " + tableName;
        }

        // Check if there are any sql_variant columns
        boolean hasVariantColumns = columns.stream()
                .anyMatch(col -> "sql_variant".equalsIgnoreCase(col.dataType));

        if (!hasVariantColumns) {
            log.debug("No sql_variant columns found in {}.{}, returning simple SELECT *", schema, table);
            return "SELECT * FROM " + tableName;
        }

        // Build SELECT with SQL_VARIANT_PROPERTY for variant columns
        StringBuilder sql = new StringBuilder("SELECT\n");
        boolean first = true;

        for (ColumnInfo col : columns) {
            if (!first) {
                sql.append(",\n");
            }
            first = false;

            // Add column itself
            sql.append("  ").append(col.columnName);

            // If it's sql_variant, add SQL_VARIANT_PROPERTY columns
            if ("sql_variant".equalsIgnoreCase(col.dataType)) {
                sql.append(",\n");
                sql.append("  SQL_VARIANT_PROPERTY(").append(col.columnName)
                        .append(", 'BaseType') as __variant_").append(col.columnName).append("_basetype,\n");
                sql.append("  SQL_VARIANT_PROPERTY(").append(col.columnName)
                        .append(", 'Precision') as __variant_").append(col.columnName).append("_precision,\n");
                sql.append("  SQL_VARIANT_PROPERTY(").append(col.columnName)
                        .append(", 'Scale') as __variant_").append(col.columnName).append("_scale,\n");
                sql.append("  SQL_VARIANT_PROPERTY(").append(col.columnName)
                        .append(", 'MaxLength') as __variant_").append(col.columnName).append("_maxlength");
            }
        }

        sql.append("\nFROM ").append(tableName);

        String generatedQuery = sql.toString();
        log.info("Generated SQL query for table {} with {} sql_variant columns:\n{}",
                tableName,
                columns.stream().filter(c -> "sql_variant".equalsIgnoreCase(c.dataType)).count(),
                generatedQuery);

        return generatedQuery;
    }
```

**AFTER (New Code):**
```java
    public String generateSelectQuery(String tableName) {
        // Parse table name into database, schema, and table components
        TableIdentifier tableId = parseTableName(tableName);

        log.debug("Generating SELECT query for table: database={}, schema={}, table={} (original={})",
                tableId.database, tableId.schema, tableId.table, tableId.fullName);

        // Query INFORMATION_SCHEMA to get all columns and identify sql_variant columns
        List<ColumnInfo> columns = queryInformationSchema(tableId);

        if (columns.isEmpty()) {
            log.warn("No columns found for table {}, returning simple SELECT *", tableId.fullName);
            return "SELECT * FROM " + tableId.fullName;
        }

        // Check if there are any sql_variant columns
        boolean hasVariantColumns = columns.stream()
                .anyMatch(col -> "sql_variant".equalsIgnoreCase(col.dataType));

        if (!hasVariantColumns) {
            log.debug("No sql_variant columns found in {}, returning simple SELECT *", tableId.fullName);
            return "SELECT * FROM " + tableId.fullName;
        }

        // Build SELECT with SQL_VARIANT_PROPERTY for variant columns
        StringBuilder sql = new StringBuilder("SELECT\n");
        boolean first = true;

        for (ColumnInfo col : columns) {
            if (!first) {
                sql.append(",\n");
            }
            first = false;

            // Add column itself
            sql.append("  ").append(col.columnName);

            // If it's sql_variant, add SQL_VARIANT_PROPERTY columns
            if ("sql_variant".equalsIgnoreCase(col.dataType)) {
                sql.append(",\n");
                sql.append("  SQL_VARIANT_PROPERTY(").append(col.columnName)
                        .append(", 'BaseType') as __variant_").append(col.columnName).append("_basetype,\n");
                sql.append("  SQL_VARIANT_PROPERTY(").append(col.columnName)
                        .append(", 'Precision') as __variant_").append(col.columnName).append("_precision,\n");
                sql.append("  SQL_VARIANT_PROPERTY(").append(col.columnName)
                        .append(", 'Scale') as __variant_").append(col.columnName).append("_scale,\n");
                sql.append("  SQL_VARIANT_PROPERTY(").append(col.columnName)
                        .append(", 'MaxLength') as __variant_").append(col.columnName).append("_maxlength");
            }
        }

        sql.append("\nFROM ").append(tableId.fullName);

        String generatedQuery = sql.toString();
        log.info("Generated SQL query for table {} with {} sql_variant columns:\n{}",
                tableId.fullName,
                columns.stream().filter(c -> "sql_variant".equalsIgnoreCase(c.dataType)).count(),
                generatedQuery);

        return generatedQuery;
    }
```

---

### Change 1.4: Update queryInformationSchema Method

**Location:** Replace lines 119-149

**BEFORE (Current Code):**
```java
    /**
     * Запрашивает INFORMATION_SCHEMA.COLUMNS для получения информации о колонках таблицы.
     *
     * @param schema имя схемы (например, dbo)
     * @param table имя таблицы
     * @return список колонок с их типами, отсортированный по ORDINAL_POSITION
     */
    private List<ColumnInfo> queryInformationSchema(String schema, String table) {
        String query = """
                SELECT COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
                """;

        try {
            return jdbcTemplate.query(
                    query,
                    (rs, rowNum) -> new ColumnInfo(
                            rs.getString("COLUMN_NAME"),
                            rs.getString("DATA_TYPE"),
                            rs.getInt("ORDINAL_POSITION")
                    ),
                    schema,
                    table
            );
        } catch (Exception e) {
            log.error("Failed to query INFORMATION_SCHEMA for table {}.{}: {}", schema, table, e.getMessage(), e);
            throw new RuntimeException("Failed to generate SELECT query for table " + schema + "." + table, e);
        }
    }
```

**AFTER (New Code):**
```java
    /**
     * Queries INFORMATION_SCHEMA.COLUMNS to get column information.
     * Supports cross-database queries by using database-qualified INFORMATION_SCHEMA reference.
     *
     * @param tableId Parsed table identifier with database, schema, and table
     * @return List of columns with their types, sorted by ORDINAL_POSITION
     */
    private List<ColumnInfo> queryInformationSchema(TableIdentifier tableId) {
        // Build INFORMATION_SCHEMA table reference
        // For cross-database: database.INFORMATION_SCHEMA.COLUMNS
        // For same-database: INFORMATION_SCHEMA.COLUMNS
        String infoSchemaTable;
        if (tableId.database != null) {
            infoSchemaTable = tableId.database + ".INFORMATION_SCHEMA.COLUMNS";
        } else {
            infoSchemaTable = "INFORMATION_SCHEMA.COLUMNS";
        }

        String query = String.format("""
                SELECT COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION
                FROM %s
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
                """, infoSchemaTable);

        try {
            return jdbcTemplate.query(
                    query,
                    (rs, rowNum) -> new ColumnInfo(
                            rs.getString("COLUMN_NAME"),
                            rs.getString("DATA_TYPE"),
                            rs.getInt("ORDINAL_POSITION")
                    ),
                    tableId.schema,
                    tableId.table
            );
        } catch (Exception e) {
            log.error("Failed to query INFORMATION_SCHEMA for table {}: {}",
                      tableId.fullName, e.getMessage(), e);
            throw new RuntimeException("Failed to generate SELECT query for table " + tableId.fullName, e);
        }
    }
```

---

### Change 1.5: Update Class JavaDoc

**Location:** Replace lines 11-34

**BEFORE (Current JavaDoc):**
```java
/**
 * Генератор SQL запросов с автоматическим добавлением SQL_VARIANT_PROPERTY
 * для table-based конфигураций JDBC extractor.
 *
 * <p>Этот класс:
 * <ul>
 * <li>Запрашивает INFORMATION_SCHEMA.COLUMNS для получения списка колонок таблицы</li>
 * <li>Определяет sql_variant колонки по типу данных</li>
 * <li>Генерирует SELECT с дополнительными SQL_VARIANT_PROPERTY колонками</li>
 * </ul>
 *
 * <p>Пример сгенерированного SQL:
 * <pre>
 * SELECT
 *   order_id,
 *   customer_name,
 *   metadata_col,
 *   SQL_VARIANT_PROPERTY(metadata_col, 'BaseType') as __variant_metadata_col_basetype,
 *   SQL_VARIANT_PROPERTY(metadata_col, 'Precision') as __variant_metadata_col_precision,
 *   SQL_VARIANT_PROPERTY(metadata_col, 'Scale') as __variant_metadata_col_scale,
 *   SQL_VARIANT_PROPERTY(metadata_col, 'MaxLength') as __variant_metadata_col_maxlength
 * FROM dbo.orders
 * </pre>
 */
```

**AFTER (New JavaDoc):**
```java
/**
 * SQL query generator with automatic SQL_VARIANT_PROPERTY column addition
 * for table-based JDBC extractor configurations.
 *
 * <p>Features:
 * <ul>
 * <li>Queries INFORMATION_SCHEMA.COLUMNS to get table column list</li>
 * <li>Identifies sql_variant columns by data type</li>
 * <li>Generates SELECT with additional SQL_VARIANT_PROPERTY columns</li>
 * <li>Supports cross-database queries (database.schema.table format)</li>
 * </ul>
 *
 * <p>Supported table name formats:
 * <ul>
 * <li>"table" → uses default schema (dbo), current database</li>
 * <li>"schema.table" → uses current database</li>
 * <li>"database.schema.table" → cross-database query</li>
 * </ul>
 *
 * <p>Example generated SQL:
 * <pre>
 * SELECT
 *   order_id,
 *   customer_name,
 *   metadata_col,
 *   SQL_VARIANT_PROPERTY(metadata_col, 'BaseType') as __variant_metadata_col_basetype,
 *   SQL_VARIANT_PROPERTY(metadata_col, 'Precision') as __variant_metadata_col_precision,
 *   SQL_VARIANT_PROPERTY(metadata_col, 'Scale') as __variant_metadata_col_scale,
 *   SQL_VARIANT_PROPERTY(metadata_col, 'MaxLength') as __variant_metadata_col_maxlength
 * FROM SUPPORT.dbo.orders
 * </pre>
 */
```

---

## Implementation Part 2: AvroToRecordTransformer

### File Location
`src/main/java/ru/pospelov/etl/engine/steps/transformer/AvroToRecordTransformer.java`

### Current State Summary
- Lines 1-59: Package, imports, JavaDoc, class declaration, transform method
- Lines 60-79: `buildMetadataFromAvroSchema(Schema)` method ← **NEEDS CHANGES**
- Lines 81-178: Helper methods (unwrapNullable, avroTypeToSqlTypeName, etc.)

---

### Change 2.1: Update buildMetadataFromAvroSchema Method

**Location:** Replace lines 60-79

**BEFORE (Current Code):**
```java
    /**
     * Build ColumnMetadata from Avro schema for type conversion.
     * Maps Avro logical types to SQL types.
     */
    private java.util.Map<String, ru.pospelov.etl.engine.model.ColumnMetadata> buildMetadataFromAvroSchema(org.apache.avro.Schema avroSchema) {
        java.util.Map<String, ru.pospelov.etl.engine.model.ColumnMetadata> metadata = new java.util.LinkedHashMap<>();

        for (org.apache.avro.Schema.Field field : avroSchema.getFields()) {
            org.apache.avro.Schema fieldSchema = unwrapNullable(field.schema());
            String sqlTypeName = avroTypeToSqlTypeName(fieldSchema);
            int jdbcType = sqlTypeNameToJdbcType(sqlTypeName);

            metadata.put(field.name(), new ru.pospelov.etl.engine.model.ColumnMetadata(
                field.name(),
                jdbcType,
                sqlTypeName,
                0, // precision - not critical for conversion
                0, // scale - not critical for conversion
                field.schema().isNullable()
            ));
        }

        return metadata;
    }
```

**AFTER (New Code):**
```java
    /**
     * Build ColumnMetadata from Avro schema for type conversion.
     * Maps Avro logical types to SQL types and extracts precision/scale for decimal types.
     */
    private java.util.Map<String, ru.pospelov.etl.engine.model.ColumnMetadata> buildMetadataFromAvroSchema(org.apache.avro.Schema avroSchema) {
        java.util.Map<String, ru.pospelov.etl.engine.model.ColumnMetadata> metadata = new java.util.LinkedHashMap<>();

        for (org.apache.avro.Schema.Field field : avroSchema.getFields()) {
            org.apache.avro.Schema fieldSchema = unwrapNullable(field.schema());
            String sqlTypeName = avroTypeToSqlTypeName(fieldSchema);
            int jdbcType = sqlTypeNameToJdbcType(sqlTypeName);

            // Extract precision and scale for decimal types
            int precision = 0;
            int scale = 0;
            org.apache.avro.LogicalType logicalType = fieldSchema.getLogicalType();
            if (logicalType instanceof org.apache.avro.LogicalTypes.Decimal decimalType) {
                precision = decimalType.getPrecision();
                scale = decimalType.getScale();
            }

            metadata.put(field.name(), new ru.pospelov.etl.engine.model.ColumnMetadata(
                field.name(),
                jdbcType,
                sqlTypeName,
                precision,  // Now extracted from Avro decimal logical type
                scale,      // Now extracted from Avro decimal logical type
                field.schema().isNullable()
            ));
        }

        return metadata;
    }
```

**Key Changes:**
1. Added extraction of `LogicalType` from field schema
2. Check if it's `Decimal` logical type using pattern matching
3. Extract `precision` and `scale` from `Decimal` type
4. Pass extracted values to `ColumnMetadata` constructor
5. Updated JavaDoc to mention precision/scale extraction

---

## Testing and Verification

### Step 1: Compile the Code

```bash
cd D:\dev\EtlEngine
mvn clean compile
```

**Expected:** Build SUCCESS with no compilation errors.

### Step 2: Run TypeConversionIntegrationTest

```bash
mvn clean test -Dtest=TypeConversionIntegrationTest
```

**Expected Results:**
```
✅ nullValues_allTypes_shouldPreserveNulls - PASS
✅ dateConversion_cornerCases_shouldPreserveExactValues - PASS
✅ uniqueidentifierConversion_shouldPreserveExactGuid - PASS
✅ decimalConversion_precisionAndScale_shouldPreserveExactly - PASS (was FAIL)
✅ binaryConversion_shouldPreserveExactBytes - PASS (was FAIL)
✅ sqlVariantConversion_differentBaseTypes_shouldPreserveTypesAndValues - PASS (was FAIL)

Tests run: 6, Failures: 0, Errors: 0, Skipped: 0
```

### Step 3: Verify Logs

Check test logs for:

**✅ Should see:**
```
Generating SELECT query for table: database=SUPPORT, schema=dbo,
    table=type_conversion_src (original=SUPPORT.dbo.type_conversion_src)

Generated SQL query for table SUPPORT.dbo.type_conversion_src with 1 sql_variant columns
```

**❌ Should NOT see:**
```
No columns found for table...
Length or precision specification 0 is invalid
Cannot convert Integer to Avro STRING
```

### Step 4: Test Backward Compatibility

Run other integration tests to ensure no regressions:

```bash
mvn clean test -Dtest=KafkaToSqlIntegrationTest
mvn clean test -Dtest=EtlErrorHandlingTest
mvn clean test -Dtest=EtlPipelineCancellationTest
```

**Expected:** All tests PASS, no failures introduced.

### Step 5: Manual Verification - Table Name Formats

Create a simple test or use debugger to verify parseTableName works correctly:

**Input:** `"orders"`
**Expected:** database=null, schema="dbo", table="orders", fullName="orders"

**Input:** `"myschema.orders"`
**Expected:** database=null, schema="myschema", table="orders", fullName="myschema.orders"

**Input:** `"SUPPORT.dbo.orders"`
**Expected:** database="SUPPORT", schema="dbo", table="orders", fullName="SUPPORT.dbo.orders"

**Input:** `"server.SUPPORT.dbo.orders"` (4 parts)
**Expected:** database="SUPPORT", schema="dbo", table="orders", fullName="server.SUPPORT.dbo.orders"

---

## Rollback Plan

If tests fail or unexpected behavior occurs, rollback is straightforward:

### Git Rollback
```bash
git checkout HEAD -- src/main/java/ru/pospelov/etl/engine/steps/extractor/jdbc/SqlVariantQueryGenerator.java
git checkout HEAD -- src/main/java/ru/pospelov/etl/engine/steps/transformer/AvroToRecordTransformer.java
mvn clean compile
```

### Backup Files (Manual)
Before starting implementation, create backups:
```bash
copy src\main\java\ru\pospelov\etl\engine\steps\extractor\jdbc\SqlVariantQueryGenerator.java SqlVariantQueryGenerator.java.bak
copy src\main\java\ru\pospelov\etl\engine\steps\transformer\AvroToRecordTransformer.java AvroToRecordTransformer.java.bak
```

---

## Implementation Checklist

Use this checklist to track implementation progress:

### Part 1: SqlVariantQueryGenerator
- [ ] Add `TableIdentifier` record (after line 40)
- [ ] Add `parseTableName(String)` method (after TableIdentifier)
- [ ] Update `generateSelectQuery(String)` method (replace lines 51-117)
- [ ] Update `queryInformationSchema` signature and body (replace lines 119-149)
- [ ] Update class JavaDoc (replace lines 11-34)
- [ ] Compile and check for errors: `mvn compile`

### Part 2: AvroToRecordTransformer
- [ ] Update `buildMetadataFromAvroSchema(Schema)` method (replace lines 60-79)
- [ ] Compile and check for errors: `mvn compile`

### Part 3: Testing
- [ ] Run TypeConversionIntegrationTest: `mvn test -Dtest=TypeConversionIntegrationTest`
- [ ] Verify all 6 tests pass
- [ ] Check logs for correct table parsing
- [ ] Run KafkaToSqlIntegrationTest for backward compatibility
- [ ] Run EtlErrorHandlingTest for backward compatibility
- [ ] Run EtlPipelineCancellationTest for backward compatibility

### Part 4: Documentation
- [ ] Update commit message with details
- [ ] Consider adding unit tests for parseTableName method
- [ ] Update any relevant documentation

---

## Additional Notes

### SQL Injection Considerations
The implementation uses `String.format` to inject database name into SQL query:
```java
String infoSchemaTable = tableId.database + ".INFORMATION_SCHEMA.COLUMNS";
```

**Risk Level:** LOW
- Database name comes from user-provided configuration (table name in config)
- Used only in FROM clause for INFORMATION_SCHEMA (read-only view)
- SQL Server validates object identifiers
- Not used in WHERE clause or other dangerous contexts

**Mitigation (Optional - Future Enhancement):**
Consider using quoted identifiers for additional safety:
```java
String infoSchemaTable = "[" + tableId.database + "].INFORMATION_SCHEMA.COLUMNS";
```

### Cross-Database Permissions
For production deployments, ensure the SQL Server user has:
```sql
-- Grant SELECT on INFORMATION_SCHEMA in target database
USE SUPPORT;
GRANT SELECT ON INFORMATION_SCHEMA.COLUMNS TO [etl_user];

-- Or grant db_datareader role
USE SUPPORT;
ALTER ROLE db_datareader ADD MEMBER [etl_user];
```

### Performance Considerations
- INFORMATION_SCHEMA queries are fast (metadata only)
- Consider caching results if same table is queried frequently
- Current implementation queries once per table per job run (acceptable)

---

## Summary of Changes

### Files Modified: 2

**1. SqlVariantQueryGenerator.java**
- Added: `TableIdentifier` record (15 lines)
- Added: `parseTableName(String)` method (40 lines)
- Updated: `generateSelectQuery(String)` method (change logs, use TableIdentifier)
- Updated: `queryInformationSchema` method (support cross-database, dynamic INFORMATION_SCHEMA reference)
- Updated: Class JavaDoc (added cross-database support documentation)
- **Total:** ~100 lines changed/added

**2. AvroToRecordTransformer.java**
- Updated: `buildMetadataFromAvroSchema(Schema)` method (extract precision/scale from Avro decimal)
- **Total:** ~10 lines changed/added

### Expected Outcome
- All 6 TypeConversionIntegrationTest tests pass
- Cross-database table queries work correctly
- DECIMAL types preserve precision/scale through Kafka → SQL pipeline
- No regressions in existing tests

---

**End of Implementation Guide**

If you encounter any issues during implementation, refer to:
- Original plan: `doc/cross-database-support-plan.md`
- Git history for rollback
- Test logs for detailed error messages
