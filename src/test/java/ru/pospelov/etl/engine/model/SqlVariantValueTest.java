package ru.pospelov.etl.engine.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link SqlVariantValue}.
 */
class SqlVariantValueTest {

    @Test
    void testIntegerVariant() {
        SqlVariantValue variant = new SqlVariantValue("int", "123", "plain");

        assertNotNull(variant);
        assertEquals("int", variant.getSqlType());
        assertEquals("123", variant.getValue());
        assertEquals("plain", variant.getEncoding());
    }

    @Test
    void testDecimalVariant() {
        SqlVariantValue variant = new SqlVariantValue("decimal(18,2)", "1234.56", "plain");

        assertEquals("decimal(18,2)", variant.getSqlType());
        assertEquals("1234.56", variant.getValue());
        assertEquals("plain", variant.getEncoding());
    }

    @Test
    void testDatetime2Variant() {
        SqlVariantValue variant = new SqlVariantValue(
                "datetime2(7)",
                "2024-01-15T10:30:45.1234567",
                "plain"
        );

        assertEquals("datetime2(7)", variant.getSqlType());
        assertEquals("2024-01-15T10:30:45.1234567", variant.getValue());
        assertEquals("plain", variant.getEncoding());
    }

    @Test
    void testVarcharVariant() {
        SqlVariantValue variant = new SqlVariantValue("varchar(50)", "Hello, World!", "plain");

        assertEquals("varchar(50)", variant.getSqlType());
        assertEquals("Hello, World!", variant.getValue());
        assertEquals("plain", variant.getEncoding());
    }

    @Test
    void testNvarcharVariant() {
        SqlVariantValue variant = new SqlVariantValue("nvarchar(100)", "Привет, мир!", "plain");

        assertEquals("nvarchar(100)", variant.getSqlType());
        assertEquals("Привет, мир!", variant.getValue());
        assertEquals("plain", variant.getEncoding());
    }

    @Test
    void testVarbinaryVariantWithBase64() {
        SqlVariantValue variant = new SqlVariantValue("varbinary(100)", "AQIDBA==", "base64");

        assertEquals("varbinary(100)", variant.getSqlType());
        assertEquals("AQIDBA==", variant.getValue());
        assertEquals("base64", variant.getEncoding());
    }

    @Test
    void testVarbinaryVariantWithHex() {
        SqlVariantValue variant = new SqlVariantValue("varbinary(max)", "0x010203FF", "hex");

        assertEquals("varbinary(max)", variant.getSqlType());
        assertEquals("0x010203FF", variant.getValue());
        assertEquals("hex", variant.getEncoding());
    }

    @Test
    void testEquality() {
        SqlVariantValue variant1 = new SqlVariantValue("int", "123", "plain");
        SqlVariantValue variant2 = new SqlVariantValue("int", "123", "plain");
        SqlVariantValue variant3 = new SqlVariantValue("int", "456", "plain");
        SqlVariantValue variant4 = new SqlVariantValue("bigint", "123", "plain");

        // Same values should be equal
        assertEquals(variant1, variant2);
        assertEquals(variant1.hashCode(), variant2.hashCode());

        // Different values should not be equal
        assertNotEquals(variant1, variant3);
        assertNotEquals(variant1, variant4);
    }

    @Test
    void testToString() {
        SqlVariantValue variant = new SqlVariantValue("decimal(18,2)", "1234.56", "plain");

        String str = variant.toString();
        assertNotNull(str);
        assertTrue(str.contains("decimal(18,2)"));
        assertTrue(str.contains("1234.56"));
        assertTrue(str.contains("plain"));
    }

    @Test
    void testDifferentTypesInDifferentRows() {
        // Simulate different base types in different rows (common sql_variant usage)
        SqlVariantValue row1 = new SqlVariantValue("int", "100", "plain");
        SqlVariantValue row2 = new SqlVariantValue("nvarchar(50)", "text value", "plain");
        SqlVariantValue row3 = new SqlVariantValue("decimal(10,2)", "99.99", "plain");

        assertNotEquals(row1, row2);
        assertNotEquals(row2, row3);
        assertNotEquals(row1, row3);

        // Each has correct type
        assertEquals("int", row1.getSqlType());
        assertEquals("nvarchar(50)", row2.getSqlType());
        assertEquals("decimal(10,2)", row3.getSqlType());
    }

    @Test
    void testNullValue() {
        // sql_variant can contain NULL, but typically handled at higher level
        // This tests that SqlVariantValue can be created with null string representation
        SqlVariantValue variant = new SqlVariantValue("int", null, "plain");

        assertEquals("int", variant.getSqlType());
        assertNull(variant.getValue());
        assertEquals("plain", variant.getEncoding());
    }
}
