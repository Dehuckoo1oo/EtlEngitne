package ru.pospelov.etl.engine.integration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import ru.pospelov.etl.engine.config.extractor.JdbcExtractorConfig;
import ru.pospelov.etl.engine.conversion.TypeConverter;
import ru.pospelov.etl.engine.model.EtlBatch;
import ru.pospelov.etl.engine.steps.extractor.jdbc.JdbcExtractor;
import ru.pospelov.etl.engine.steps.extractor.jdbc.SqlVariantQueryGenerator;
import ru.pospelov.etl.engine.validation.ValidationException;

import java.util.Optional;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.*;

/** мен
 * Integration tests for JdbcExtractor custom query validation.
 *
 * <p>Tests sql_variant column validation:
 * <ul>
 * <li>Custom query without sql_variant - validation passes</li>
 * <li>Custom query with sql_variant AND __variant_* columns - validation passes</li>
 * <li>Custom query with sql_variant WITHOUT __variant_* columns - ValidationException</li>
 * </ul>
 */
@SpringBootTest
class JdbcExtractorValidationIntegrationTest {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private SqlVariantQueryGenerator sqlVariantQueryGenerator;

    @Autowired
    private TypeConverter typeConverter;

    private JdbcExtractor jdbcExtractor;

    @BeforeEach
    void setUp() {
        jdbcExtractor = new JdbcExtractor(jdbcTemplate, sqlVariantQueryGenerator, typeConverter);

        // Create test table with sql_variant column
        jdbcTemplate.execute("DROP TABLE IF EXISTS test_variant_validation");
        jdbcTemplate.execute(
                "CREATE TABLE test_variant_validation (" +
                        "  id INT PRIMARY KEY," +
                        "  variant_col SQL_VARIANT," +
                        "  normal_col NVARCHAR(50)" +
                        ")"
        );

        // Insert test data - separate INSERT for each row because sql_variant needs explicit CAST
        jdbcTemplate.execute(
                "INSERT INTO test_variant_validation (id, variant_col, normal_col) " +
                        "VALUES (1, CAST(123 AS INT), N'test1')"
        );
        jdbcTemplate.execute(
                "INSERT INTO test_variant_validation (id, variant_col, normal_col) " +
                        "VALUES (2, CAST(N'hello' AS NVARCHAR(50)), N'test2')"
        );
    }

    @Test
    void customQuery_withSqlVariantWithoutMetadata_throwsValidationException() {
        // Custom query with sql_variant but WITHOUT __variant_* columns - should fail
        JdbcExtractorConfig config = new JdbcExtractorConfig(
                Optional.of("SELECT id, variant_col FROM test_variant_validation"),
                Optional.empty(),
                Optional.empty(),
                1,
                Optional.empty(),
                1,
                100
        );

        Consumer<EtlBatch> batchConsumer = batch -> {
            fail("Should not reach batch consumer - validation should fail first");
        };

        // Should throw ValidationException with detailed message
        assertThatThrownBy(() -> jdbcExtractor.extract(config, "test-job", batchConsumer))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Configuration error")
                .hasMessageContaining("sql_variant column(s) detected")
                .hasMessageContaining("variant_col")
                .hasMessageContaining("SQL_VARIANT_PROPERTY")
                .hasMessageContaining("__variant_variant_col_basetype")
                .hasMessageContaining("__variant_variant_col_precision")
                .hasMessageContaining("__variant_variant_col_scale")
                .hasMessageContaining("__variant_variant_col_maxlength")
                .hasMessageContaining("Or use 'table' configuration");
    }
}
