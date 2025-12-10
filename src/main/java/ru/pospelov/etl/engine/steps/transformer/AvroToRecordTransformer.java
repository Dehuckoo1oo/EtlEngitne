package ru.pospelov.etl.engine.steps.transformer;

import org.apache.avro.generic.GenericRecord;
import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.exception.EtlErrorSeverity;
import ru.pospelov.etl.engine.exception.TransformationException;
import ru.pospelov.etl.engine.model.EtlRecord;

import java.util.Collection;

@Component
public class AvroToRecordTransformer {

    /**
     * Transform Avro GenericRecords to EtlRecords.
     */
    public Collection<EtlRecord> transform(Collection<EtlRecord> records) {
        return records.stream()
                .map(this::fromAvro)
                .toList();
    }

    private EtlRecord fromAvro(EtlRecord raw) {
        Object value = raw.get("value");

        if (!(value instanceof GenericRecord avro)) {
            throw new TransformationException(
                    "Expected GenericRecord, got: " + value,
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

        avro.getSchema().getFields().forEach(field -> {
            Object v = avro.get(field.name());

            if (v instanceof Boolean boolVal) {
                record.put(field.name(), boolVal ? 1 : 0); // SQL Server не принимает boolean
            } else {
                record.put(field.name(), v);
            }
        });

        return record;
    }
}
