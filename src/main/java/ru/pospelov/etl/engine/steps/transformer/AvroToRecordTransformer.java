package ru.pospelov.etl.engine.steps.transformer;

import org.apache.avro.generic.GenericRecord;
import org.springframework.stereotype.Component;
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
                .map(this::fromAvro)
                .toList();
    }

    private EtlRecord fromAvro(EtlRecord raw) {
        Object value = raw.get("value");

        if (!(value instanceof GenericRecord avro)) {
            throw new IllegalStateException("Expected GenericRecord, got: " + value + " for record: " + raw);
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
