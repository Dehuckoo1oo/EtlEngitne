package ru.pospelov.etl.engine.steps.transformer;

import lombok.RequiredArgsConstructor;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.config.transformer.RecordToAvroTransformerConfig;
import ru.pospelov.etl.engine.exception.EtlErrorSeverity;
import ru.pospelov.etl.engine.exception.TransformationException;
import ru.pospelov.etl.engine.model.EtlBatch;
import ru.pospelov.etl.engine.model.EtlRecord;
import ru.pospelov.etl.engine.schema.SchemaRegistryService;

import java.util.ArrayList;
import java.util.List;

/**
 * Transformer that wraps flat EtlRecord fields into Avro GenericRecord.
 *
 * <p>This transformer:
 * <ul>
 * <li>Fetches Avro schema from Schema Registry</li>
 * <li>Creates GenericRecord with fields from EtlRecord matching the schema</li>
 * <li>Preserves __kafka_key field for Kafka key</li>
 * <li>Stores GenericRecord in __kafka_value field</li>
 * </ul>
 *
 * <p>Note: This transformer does NOT perform type conversion. Type conversion
 * is handled by KafkaByPartitionLoader using TypeConverter when format=AVRO.
 */
@Component
@RequiredArgsConstructor
public class RecordToAvroTransformer {

    private final SchemaRegistryService schemaRegistryService;

    /**
     * Transform batch records to Avro using type-safe configuration.
     *
     * @param batch input batch
     * @param config transformer configuration
     * @return batch with records wrapped in GenericRecord
     */
    public EtlBatch transform(EtlBatch batch, RecordToAvroTransformerConfig config) {
        Schema schema = resolveSchema(config);

        List<EtlRecord> transformedRecords = new ArrayList<>();
        for (EtlRecord record : batch.getRecords()) {
            transformedRecords.add(toAvro(record, schema));
        }

        // Preserve metadata from original batch
        return new EtlBatch(transformedRecords, batch.getColumnMetadata());
    }

    private Schema resolveSchema(RecordToAvroTransformerConfig config) {
        try {
            return schemaRegistryService.getLatestSchema(config.avroSchemaSubject());
        } catch (Exception e) {
            throw new TransformationException(
                    "Failed to fetch schema from Schema Registry for subject: " + config.avroSchemaSubject(),
                    null,
                    null,
                    EtlErrorSeverity.CRITICAL,
                    e
            );
        }
    }

    private EtlRecord toAvro(EtlRecord record, Schema schema) {
        GenericRecord gr = new GenericData.Record(schema);

        // Copy only fields present in Avro schema to avoid AvroRuntimeException
        // Skip __kafka_* fields - they are envelope metadata, not payload
        record.getAll().forEach((name, value) -> {
            if (!name.startsWith("__kafka_") && schema.getField(name) != null) {
                gr.put(name, value);
            }
        });

        EtlRecord res = new EtlRecord(record.getTimestamp(), record.getSourcePartition(), record.getOffset());

        // Preserve Kafka key (renamed from "key" to "__kafka_key")
        Object key = record.get("__kafka_key");
        if (key != null) {
            res.put("__kafka_key", key);
        }

        // Store GenericRecord as Kafka value
        res.put("__kafka_value", gr);

        return res;
    }
}
