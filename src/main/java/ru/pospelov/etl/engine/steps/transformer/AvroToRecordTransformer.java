package ru.pospelov.etl.engine.steps.transformer;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.exception.EtlErrorSeverity;
import ru.pospelov.etl.engine.exception.TransformationException;
import ru.pospelov.etl.engine.model.ColumnMetadata;
import ru.pospelov.etl.engine.model.EtlBatch;
import ru.pospelov.etl.engine.model.EtlRecord;

import java.util.ArrayList;
import java.util.List;

/**
 * Transformer that flattens Avro GenericRecord into EtlRecord fields.
 *
 * <p>This transformer:
 * <ul>
 * <li>Reads GenericRecord from __kafka_value field</li>
 * <li>Flattens all Avro fields into EtlRecord</li>
 * <li>Converts Boolean to Integer (SQL Server compatibility)</li>
 * <li>Creates ColumnMetadata from Avro schema for type conversion in SQL loaders</li>
 * </ul>
 *
 * <p>Note: Type conversion is still handled by SQL loaders using TypeConverter.
 * This transformer only flattens the structure and provides schema metadata.
 */
@Component
public class AvroToRecordTransformer {

    /**
     * Transform batch with Avro GenericRecords to flat EtlRecords.
     *
     * @param batch input batch with GenericRecord in __kafka_value
     * @return batch with flattened records and Avro schema metadata
     */
    public EtlBatch transform(EtlBatch batch) {
        List<EtlRecord> transformedRecords = new ArrayList<>();
        java.util.Map<String, ColumnMetadata> avroMetadata = null;

        for (EtlRecord record : batch.getRecords()) {
            transformedRecords.add(fromAvro(record));

            // Extract metadata from first record's GenericRecord
            if (avroMetadata == null) {
                Object value = record.get("__kafka_value");
                if (value instanceof GenericRecord avro) {
                    avroMetadata = buildMetadataFromAvroSchema(avro.getSchema());
                }
            }
        }

        // Pass Avro schema metadata to SQL loaders for correct type conversion
        return new EtlBatch(transformedRecords, avroMetadata);
    }

    /**
     * Build ColumnMetadata from Avro schema for type conversion.
     * Maps Avro logical types to SQL types and extracts precision/scale for decimal types.
     */
    private java.util.Map<String, ColumnMetadata> buildMetadataFromAvroSchema(Schema avroSchema) {
        java.util.Map<String, ColumnMetadata> metadata = new java.util.LinkedHashMap<>();

        for (Schema.Field field : avroSchema.getFields()) {
            Schema fieldSchema = unwrapNullable(field.schema());
            String sqlTypeName = avroTypeToSqlTypeName(fieldSchema);
            int jdbcType = sqlTypeNameToJdbcType(sqlTypeName);

            // Extract precision and scale based on type
            int precision = 0;
            int scale = 0;
            LogicalType logicalType = fieldSchema.getLogicalType();
            if (logicalType instanceof LogicalTypes.Decimal decimalType) {
                precision = decimalType.getPrecision();
                scale = decimalType.getScale();
            } else if (logicalType instanceof LogicalTypes.TimestampMillis) {
                scale = 3;  // Milliseconds
            } else if (logicalType instanceof LogicalTypes.TimestampMicros) {
                scale = 6;  // Microseconds
            } else if (logicalType instanceof LogicalTypes.TimeMicros) {
                scale = 6;  // Microseconds
            } else {
                // Set sensible defaults for types that require precision in SQL Server bulk copy
                precision = getDefaultPrecision(sqlTypeName);
            }

            metadata.put(field.name(), new ColumnMetadata(
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

    /**
     * Unwrap nullable union to get actual type.
     */
    private Schema unwrapNullable(Schema schema) {
        if (schema.getType() == Schema.Type.UNION) {
            for (Schema s : schema.getTypes()) {
                if (s.getType() != Schema.Type.NULL) {
                    return s;
                }
            }
        }
        return schema;
    }

    /**
     * Map Avro type to SQL type name for TypeConverter.
     */
    private String avroTypeToSqlTypeName(Schema schema) {
        LogicalType logicalType = schema.getLogicalType();

        // Handle logical types
        if (logicalType != null) {
            if (logicalType instanceof LogicalTypes.Date) {
                return "date";
            } else if (logicalType instanceof LogicalTypes.TimestampMillis) {
                return "datetime2";
            } else if (logicalType instanceof LogicalTypes.TimestampMicros) {
                return "datetime2";
            } else if (logicalType instanceof LogicalTypes.TimeMicros) {
                return "time";
            } else if (logicalType instanceof LogicalTypes.Decimal) {
                return "decimal";
            }
        }

        // Handle physical types
        return switch (schema.getType()) {
            case STRING -> "nvarchar";
            case INT -> "int";
            case LONG -> "bigint";
            case FLOAT -> "float";
            case DOUBLE -> "double";
            case BOOLEAN -> "bit";
            case BYTES -> "varbinary";
            default -> "nvarchar"; // fallback
        };
    }

    /**
     * Get default precision for SQL types that require it in bulk copy.
     *
     * <p>SQL Server bulk copy requires valid precision/length for certain types:
     * <ul>
     * <li>varbinary: max length (8000 for varbinary, or use -1 for MAX)</li>
     * <li>nvarchar: max length (4000 for nvarchar, or use -1 for MAX)</li>
     * <li>varchar: max length (8000 for varchar, or use -1 for MAX)</li>
     * </ul>
     *
     * <p>Using -1 indicates MAX length, which SQL Server JDBC driver interprets correctly.
     */
    private int getDefaultPrecision(String sqlTypeName) {
        return switch (sqlTypeName.toLowerCase()) {
            case "varbinary" -> 8000;  // Max for non-MAX varbinary, adequate for most cases
            case "nvarchar" -> 4000;   // Max for non-MAX nvarchar
            case "varchar" -> 8000;    // Max for non-MAX varchar
            default -> 0;              // Other types don't require precision
        };
    }

    /**
     * Map SQL type name to JDBC type constant.
     */
    private int sqlTypeNameToJdbcType(String sqlTypeName) {
        return switch (sqlTypeName.toLowerCase()) {
            case "date" -> java.sql.Types.DATE;
            case "datetime2", "datetime", "smalldatetime" -> java.sql.Types.TIMESTAMP;
            case "time" -> java.sql.Types.TIME;
            case "decimal", "numeric" -> java.sql.Types.DECIMAL;
            case "int" -> java.sql.Types.INTEGER;
            case "bigint" -> java.sql.Types.BIGINT;
            case "float" -> java.sql.Types.FLOAT;
            case "double" -> java.sql.Types.DOUBLE;
            case "bit" -> java.sql.Types.BOOLEAN;
            case "varbinary" -> java.sql.Types.VARBINARY;
            default -> java.sql.Types.VARCHAR;
        };
    }

    private EtlRecord fromAvro(EtlRecord raw) {
        Object value = raw.get("__kafka_value");

        if (!(value instanceof GenericRecord avro)) {
            throw new TransformationException(
                    "Expected GenericRecord in __kafka_value, got: " + value,
                    null,
                    raw,
                    EtlErrorSeverity.NON_CRITICAL
            );
        }

        EtlRecord record = new EtlRecord(
                raw.getTimestamp(),
                raw.getSourcePartition(),
                raw.getOffset()
        );

        // Flatten all Avro fields into EtlRecord
        avro.getSchema().getFields().forEach(field -> {
            Object v = avro.get(field.name());

            if (v instanceof Boolean boolVal) {
                // SQL Server doesn't accept boolean directly - convert to int
                record.put(field.name(), boolVal ? 1 : 0);
            } else if (v instanceof CharSequence) {
                // Avro returns strings as Utf8 (CharSequence), convert to String
                // This is important for sql_variant JSON detection in TypeConverter
                record.put(field.name(), v.toString());
            } else {
                record.put(field.name(), v);
            }
        });

        return record;
    }
}
