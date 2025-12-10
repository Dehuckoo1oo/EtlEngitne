package ru.pospelov.etl.engine.steps.transformer;

import lombok.RequiredArgsConstructor;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.config.transformer.RecordToAvroTransformerConfig;
import ru.pospelov.etl.engine.exception.EtlErrorSeverity;
import ru.pospelov.etl.engine.exception.TransformationException;
import ru.pospelov.etl.engine.model.EtlRecord;
import ru.pospelov.etl.engine.schema.SchemaRegistryService;

import java.util.Collection;
import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
public class RecordToAvroTransformer {

    private final SchemaRegistryService schemaRegistryService;

    /**
     * Transform records to Avro using type-safe configuration.
     */
    public Collection<EtlRecord> transform(Collection<EtlRecord> records, RecordToAvroTransformerConfig config) {
        Schema schema = resolveSchema(config);
        return records.stream()
                .map(r -> toAvro(r, schema))
                .collect(Collectors.toList());
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
        // Copy only fields present in Avro schema to avoid AvroRuntimeException for metadata like "key"
        record.getAll().forEach((name, value) -> {
            if (schema.getField(name) != null) {
                gr.put(name, value);
            }
        });
        EtlRecord res = new EtlRecord(record.getTimestamp(), record.getSourcePartition(), record.getOffset());
        Object key = record.get("key");
        if (key != null) {
            res.put("key", key);
        }
        res.put("value", gr);
        return res;
    }
}
