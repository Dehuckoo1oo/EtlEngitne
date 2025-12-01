package ru.pospelov.etl.engine.steps.transformer;

import org.apache.avro.generic.GenericRecord;
import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.exception.EtlErrorSeverity;
import ru.pospelov.etl.engine.exception.TransformationException;
import ru.pospelov.etl.engine.model.EtlJob;
import ru.pospelov.etl.engine.model.EtlRecord;

import java.util.Collection;

@Component
public class AvroToRecordTransformer implements Transformer {

    @Override
    public String getType() {
        return "avro";
    }

    @Override
    public Collection<EtlRecord> transform(Collection<EtlRecord> records, EtlJob job) {
        return records.stream()
                .map(record -> fromAvro(record, job))
                .toList();
    }

    private EtlRecord fromAvro(EtlRecord raw, EtlJob job) {
        Object value = raw.get("value");

        if (!(value instanceof GenericRecord avro)) {
            throw new TransformationException(
                    "Expected GenericRecord, got: " + value,
                    job.getJobId(),
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
