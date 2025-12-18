package ru.pospelov.etl.engine.steps.extractor.jdbc;

import lombok.RequiredArgsConstructor;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.config.extractor.JdbcExtractorConfig;
import ru.pospelov.etl.engine.conversion.TypeConverter;
import ru.pospelov.etl.engine.exception.EtlErrorSeverity;
import ru.pospelov.etl.engine.exception.EtlException;
import ru.pospelov.etl.engine.exception.ExtractionException;
import ru.pospelov.etl.engine.model.EtlBatch;
import ru.pospelov.etl.engine.validation.ValidationException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * JDBC Extractor that extracts data from SQL databases.
 *
 * <p>This extractor:
 * <ul>
 * <li>Reads data from SQL databases using JDBC</li>
 * <li>Supports partitioned extraction for parallel processing</li>
 * <li>Produces EtlBatch with ColumnMetadata from ResultSetMetaData</li>
 * <li>Supports automatic sql_variant query generation for table-based config</li>
 * <li>Does NOT perform type conversion - that's handled by loaders</li>
 * </ul>
 */
@Component
@RequiredArgsConstructor
public class JdbcExtractor {

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(JdbcExtractor.class);

    private final JdbcTemplate jdbcTemplate;
    private final SqlVariantQueryGenerator sqlVariantQueryGenerator;
    private final TypeConverter typeConverter;

    /**
     * Extract data from JDBC source using type-safe configuration.
     *
     * <p>Supports two modes:
     * <ul>
     * <li><b>Table-based:</b> Auto-generates SELECT with SQL_VARIANT_PROPERTY for sql_variant columns</li>
     * <li><b>Custom query:</b> Uses provided SQL query as-is</li>
     * </ul>
     *
     * @param config JDBC extractor configuration
     * @param jobId ETL job identifier
     * @param batchConsumer consumer that receives extracted batches with metadata
     */
    public void extract(
            JdbcExtractorConfig config,
            String jobId,
            Consumer<EtlBatch> batchConsumer
    ) {
        // Determine the query to execute
        String query;
        if (config.isTableBased()) {
            // Table-based: auto-generate SELECT with sql_variant support
            String tableName = config.table().orElseThrow();
            log.info("Job '{}' using table-based extraction: table={}", jobId, tableName);
            query = sqlVariantQueryGenerator.generateSelectQuery(tableName);
            log.debug("Job '{}' generated query:\n{}", jobId, query);
        } else {
            // Custom query: use as-is, but validate sql_variant columns
            query = config.sqlQuery().orElseThrow();
            log.info("Job '{}' using custom query extraction", jobId);

            // Validate that sql_variant columns have required __variant_* columns
            validateCustomQueryForSqlVariant(query, jobId);
        }

        // Type-safe field access
        int threads = config.threads();
        int streamBatchSize = config.streamBatchSize();
        int partitions = config.partitions();

        // Optional - explicit nullable handling
        Optional<String> partitionColumn = config.partitionColumn();
        Optional<String> keyColumn = config.keyColumn();

        log.info("Job '{}' extraction started: threads={}, batchSize={}, partitions={}",
                jobId, threads, streamBatchSize, partitions);

        final ExecutorService executor = Executors.newFixedThreadPool(threads);
        final List<Future<?>> tasks = new ArrayList<>();

        try {
            // Partitioning only if partition column is specified
            if (partitionColumn.isPresent() && partitions > 1) {
                log.info("Job '{}' using partition-based extraction: column={}, partitions={}",
                        jobId, partitionColumn.get(), partitions);

                for (int p = 0; p < partitions; p++) {
                    String partQuery = query +
                            (query.toLowerCase().contains("where") ? " AND " : " WHERE ") +
                            partitionColumn.get() + " = " + p;

                    tasks.add(executor.submit(new JdbcPartitionQueryTask(
                            jdbcTemplate,
                            partQuery,
                            p,
                            streamBatchSize,
                            keyColumn.orElse(""),
                            batchConsumer,
                            typeConverter,
                            jobId
                    )));
                }
            } else {
                // Offset-based extraction
                Integer totalRows = jdbcTemplate.queryForObject(
                        "SELECT COUNT(*) FROM (" + query + ") t",
                        Integer.class
                );
                int total = (totalRows == null) ? 0 : totalRows;

                log.info("Job '{}' using offset-based extraction: totalRows={}, tasks={}",
                        jobId, total, (total + streamBatchSize - 1) / streamBatchSize);

                // Add ORDER BY if not present (required for OFFSET/FETCH NEXT in SQL Server)
                String orderByQuery = query;
                if (keyColumn.isPresent() && !query.toLowerCase().contains("order by")) {
                    orderByQuery = query + " ORDER BY " + keyColumn.get();
                }

                for (int offset = 0; offset < total; offset += streamBatchSize) {
                    tasks.add(executor.submit(new JdbcOffsetQueryTask(
                            jdbcTemplate,
                            orderByQuery,
                            offset,
                            streamBatchSize,
                            keyColumn.orElse(""),
                            batchConsumer,
                            typeConverter,
                            jobId
                    )));
                }
            }

            waitForTasks(tasks, jobId);
            executor.shutdown();
            if (!executor.awaitTermination(1, TimeUnit.HOURS)) {
                throw new ExtractionException("JdbcExtractor did not finish within timeout", jobId);
            }
            log.info("Job '{}' extraction completed: {} tasks finished", jobId, tasks.size());
        } catch (EtlException e) {
            log.error("Job '{}' extraction failed: {}", jobId, e.getMessage());
            throw e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Job '{}' extraction interrupted", jobId);
            throw new ExtractionException("JdbcExtractor interrupted", jobId, null, EtlErrorSeverity.CRITICAL, e);
        } catch (Exception e) {
            log.error("Job '{}' extraction error: {}", jobId, e.getMessage(), e);
            throw new ExtractionException("JdbcExtractor failed", jobId, null, EtlErrorSeverity.CRITICAL, e);
        } finally {
            executor.shutdownNow();
        }
    }

    private static void waitForTasks(List<Future<?>> tasks, String jobId) {
        for (Future<?> task : tasks) {
            try {
                task.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ExtractionException("JdbcExtractor interrupted", jobId, null, EtlErrorSeverity.CRITICAL, e);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof EtlException etlException) {
                    throw etlException;
                }
                throw new ExtractionException("JdbcExtractor task failed", jobId, null, EtlErrorSeverity.CRITICAL, cause);
            }
        }
    }

    /**
     * Validates custom query for sql_variant columns.
     *
     * <p>Checks that for each sql_variant column in the query result,
     * there are corresponding __variant_* metadata columns:
     * <ul>
     * <li>__variant_{column}_basetype</li>
     * <li>__variant_{column}_precision</li>
     * <li>__variant_{column}_scale</li>
     * <li>__variant_{column}_maxlength</li>
     * </ul>
     *
     * <p>If sql_variant columns are found without required metadata columns,
     * throws ValidationException with detailed instructions.
     *
     * @param query custom SQL query to validate
     * @param jobId job identifier for logging
     * @throws ValidationException if sql_variant columns are missing required metadata
     */
    private void validateCustomQueryForSqlVariant(String query, String jobId) {
        // Add WHERE 1=0 to query to get metadata without executing full query
        String validationQuery = addWhereClause(query, "1=0");

        DataSource dataSource = jdbcTemplate.getDataSource();
        if (dataSource == null) {
            log.warn("Job '{}' cannot validate custom query: DataSource is null", jobId);
            return;
        }

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(validationQuery);
             ResultSet rs = ps.executeQuery()) {

            ResultSetMetaData md = rs.getMetaData();
            List<String> sqlVariantColumns = new ArrayList<>();
            Set<String> allColumns = new HashSet<>();

            // Collect all column names and identify sql_variant columns
            for (int i = 1; i <= md.getColumnCount(); i++) {
                String colName = md.getColumnLabel(i);
                allColumns.add(colName);

                String typeName = md.getColumnTypeName(i);
                // Skip __variant_* helper columns themselves
                if ("sql_variant".equalsIgnoreCase(typeName) && !colName.startsWith("__variant_")) {
                    sqlVariantColumns.add(colName);
                }
            }

            if (sqlVariantColumns.isEmpty()) {
                // No sql_variant columns - validation passes
                return;
            }

            log.debug("Job '{}' found {} sql_variant column(s) in custom query: {}",
                    jobId, sqlVariantColumns.size(), sqlVariantColumns);

            // Check that each sql_variant column has all required __variant_* columns
            List<String> missingVariantColumns = new ArrayList<>();
            for (String variantCol : sqlVariantColumns) {
                boolean hasBasetype = allColumns.contains("__variant_" + variantCol + "_basetype");
                boolean hasPrecision = allColumns.contains("__variant_" + variantCol + "_precision");
                boolean hasScale = allColumns.contains("__variant_" + variantCol + "_scale");
                boolean hasMaxLength = allColumns.contains("__variant_" + variantCol + "_maxlength");

                if (!hasBasetype || !hasPrecision || !hasScale || !hasMaxLength) {
                    missingVariantColumns.add(variantCol);
                }
            }

            if (!missingVariantColumns.isEmpty()) {
                // Build detailed error message with instructions
                StringBuilder errorMsg = new StringBuilder();
                errorMsg.append("Configuration error: sql_variant column(s) detected in query result, ");
                errorMsg.append("but required SQL_VARIANT_PROPERTY columns are missing.\n\n");

                for (String col : missingVariantColumns) {
                    errorMsg.append("For column '").append(col).append("', please add:\n");
                    errorMsg.append("  - SQL_VARIANT_PROPERTY(").append(col).append(", 'BaseType') as __variant_").append(col).append("_basetype\n");
                    errorMsg.append("  - SQL_VARIANT_PROPERTY(").append(col).append(", 'Precision') as __variant_").append(col).append("_precision\n");
                    errorMsg.append("  - SQL_VARIANT_PROPERTY(").append(col).append(", 'Scale') as __variant_").append(col).append("_scale\n");
                    errorMsg.append("  - SQL_VARIANT_PROPERTY(").append(col).append(", 'MaxLength') as __variant_").append(col).append("_maxlength\n\n");
                }

                errorMsg.append("Or use 'table' configuration instead of 'query' for automatic generation.");

                log.error("Job '{}' custom query validation failed: {}", jobId, errorMsg);
                throw new ValidationException(errorMsg.toString(), jobId);
            }

            log.debug("Job '{}' custom query validation passed: all sql_variant columns have required metadata", jobId);

        } catch (SQLException e) {
            log.error("Job '{}' failed to validate custom query for sql_variant columns: {}", jobId, e.getMessage());
            throw new ExtractionException(
                    "Failed to validate custom query for sql_variant columns",
                    jobId,
                    null,
                    EtlErrorSeverity.CRITICAL,
                    e
            );
        }
    }

    /**
     * Adds a WHERE clause to a SQL query using JSqlParser.
     *
     * <p>If the query already has a WHERE clause, the condition is added with AND.
     * Otherwise, a new WHERE clause is created.
     *
     * @param query original SQL query
     * @param condition condition to add (e.g., "1=0")
     * @return modified query with WHERE clause
     * @throws ValidationException if query parsing fails
     */
    private String addWhereClause(String query, String condition) {
        try {
            Statement statement = CCJSqlParserUtil.parse(query);

            if (!(statement instanceof Select)) {
                throw new ValidationException("Only SELECT statements are supported for custom query validation", "unknown");
            }

            Select select = (Select) statement;
            PlainSelect plainSelect = (PlainSelect) select.getSelectBody();

            // Parse the condition expression
            Expression whereCondition = CCJSqlParserUtil.parseCondExpression(condition);

            if (plainSelect.getWhere() != null) {
                // Already has WHERE - add with AND
                AndExpression newWhere = new AndExpression(plainSelect.getWhere(), whereCondition);
                plainSelect.setWhere(newWhere);
            } else {
                // No WHERE - add new one
                plainSelect.setWhere(whereCondition);
            }

            return select.toString();
        } catch (JSQLParserException e) {
            log.error("Failed to parse custom SQL query: {}", e.getMessage());
            throw new ValidationException("Failed to parse custom SQL query: " + e.getMessage(), "unknown", e);
        }
    }
}
