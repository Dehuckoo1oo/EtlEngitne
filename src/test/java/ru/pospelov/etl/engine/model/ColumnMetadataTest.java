package ru.pospelov.etl.engine.model;

import org.junit.jupiter.api.Test;

import java.sql.Types;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link ColumnMetadata}.
 */
class ColumnMetadataTest {

    @Test
    void testCreation() {
        ColumnMetadata metadata = new ColumnMetadata(
                "test_column",
                Types.VARCHAR,
                "NVARCHAR",
                100,
                0,
                true
        );

        assertNotNull(metadata);
        assertEquals("test_column", metadata.getColumnName());
        assertEquals(Types.VARCHAR, metadata.getJdbcType());
        assertEquals("NVARCHAR", metadata.getTypeName());
        assertEquals(100, metadata.getPrecision());
        assertEquals(0, metadata.getScale());
        assertTrue(metadata.isNullable());
    }

    @Test
    void testDecimalMetadata() {
        ColumnMetadata metadata = new ColumnMetadata(
                "amount",
                Types.DECIMAL,
                "DECIMAL",
                18,
                2,
                false
        );

        assertEquals("amount", metadata.getColumnName());
        assertEquals(Types.DECIMAL, metadata.getJdbcType());
        assertEquals("DECIMAL", metadata.getTypeName());
        assertEquals(18, metadata.getPrecision());
        assertEquals(2, metadata.getScale());
        assertFalse(metadata.isNullable());
    }

    @Test
    void testSqlVariantMetadata() {
        ColumnMetadata metadata = new ColumnMetadata(
                "variant_col",
                Types.OTHER,
                "sql_variant",
                0,
                0,
                true
        );

        assertEquals("variant_col", metadata.getColumnName());
        assertEquals(Types.OTHER, metadata.getJdbcType());
        assertEquals("sql_variant", metadata.getTypeName());
        assertEquals(0, metadata.getPrecision());
        assertEquals(0, metadata.getScale());
        assertTrue(metadata.isNullable());
    }

    @Test
    void testToString() {
        ColumnMetadata metadata = new ColumnMetadata(
                "id",
                Types.INTEGER,
                "INT",
                0,
                0,
                false
        );

        String str = metadata.toString();
        assertNotNull(str);
        assertTrue(str.contains("id"));
        assertTrue(str.contains("INT"));
    }
}
