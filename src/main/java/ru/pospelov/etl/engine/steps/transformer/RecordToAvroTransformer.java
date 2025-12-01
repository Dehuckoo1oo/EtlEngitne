package ru.pospelov.etl.engine.steps.transformer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.exception.EtlErrorSeverity;
import ru.pospelov.etl.engine.exception.TransformationException;
import ru.pospelov.etl.engine.model.EtlJob;
import ru.pospelov.etl.engine.model.EtlRecord;

import java.util.Collection;
import java.util.stream.Collectors;

@Component
public class RecordToAvroTransformer implements Transformer {

    @Override
    public String getType() {
        return "record-to-avro";
    }

    @Override
    public Collection<EtlRecord> transform(Collection<EtlRecord> records, EtlJob job) {
        Schema schema = resolveSchema(job);
        return records.stream()
                .map(r -> toAvro(r, schema))
                .collect(Collectors.toList());
    }

    private Schema resolveSchema(EtlJob job) {
        Object param = job.getParam("avroSchema");
        if (param instanceof Schema schema) {
            return schema;
        }
        if (param instanceof CharSequence schemaText) {
            try {
                return new Schema.Parser().parse(schemaText.toString());
            } catch (Exception e) {
                throw new TransformationException(
                        "Failed to parse avroSchema parameter",
                        job.getJobId(),
                        null,
                        EtlErrorSeverity.CRITICAL,
                        e
                );
            }
        }
        throw new TransformationException("avroSchema parameter is required for RecordToAvroTransformer", job.getJobId(), null);
    }

    private EtlRecord toAvro(EtlRecord record, Schema schema) {
        GenericRecord gr = new GenericData.Record(schema);
        record.getAll().forEach(gr::put);
        EtlRecord res = new EtlRecord(record.getTimestamp(), record.getSourcePartition(), record.getOffset());
        Object key = record.get("key");
        if (key != null) {
            res.put("key", key);
        }
        res.put("value", gr);
        return res;
    }
}
