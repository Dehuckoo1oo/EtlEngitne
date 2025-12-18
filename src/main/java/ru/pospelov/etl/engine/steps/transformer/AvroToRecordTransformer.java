package ru.pospelov.etl.engine.steps.transformer;

import org.apache.avro.generic.GenericRecord;
import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.exception.EtlErrorSeverity;
import ru.pospelov.etl.engine.exception.TransformationException;
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
        java.util.Map<String, ru.pospelov.etl.engine.model.ColumnMetadata> avroMetadata = null;

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

    /**
     * Unwrap nullable union to get actual type.
     */
    private org.apache.avro.Schema unwrapNullable(org.apache.avro.Schema schema) {
        if (schema.getType() == org.apache.avro.Schema.Type.UNION) {
            for (org.apache.avro.Schema s : schema.getTypes()) {
                if (s.getType() != org.apache.avro.Schema.Type.NULL) {
                    return s;
                }
            }
        }
        return schema;
    }

    /**
     * Map Avro type to SQL type name for TypeConverter.
     */
    private String avroTypeToSqlTypeName(org.apache.avro.Schema schema) {
        org.apache.avro.LogicalType logicalType = schema.getLogicalType();

        // Handle logical types
        if (logicalType != null) {
            if (logicalType instanceof org.apache.avro.LogicalTypes.Date) {
                return "date";
            } else if (logicalType instanceof org.apache.avro.LogicalTypes.TimestampMillis) {
                return "datetime2";
            } else if (logicalType instanceof org.apache.avro.LogicalTypes.TimeMicros) {
                return "time";
            } else if (logicalType instanceof org.apache.avro.LogicalTypes.Decimal) {
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
            } else {
                record.put(field.name(), v);
            }
        });

        return record;
    }
}
