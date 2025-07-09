package ru.pospelov.etl.engine.steps.transformer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.springframework.stereotype.Component;
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
        Schema schema = (Schema) job.getParam("avroSchema");
        return records.stream()
                .map(r -> toAvro(r, schema))
                .collect(Collectors.toList());
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
