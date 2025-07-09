package ru.pospelov.etl.engine.steps.extractor;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.model.EtlJob;
import ru.pospelov.etl.engine.model.EtlRecord;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@Component
@RequiredArgsConstructor
public class JdbcExtractor implements Extractor {

    private final JdbcTemplate jdbcTemplate;

    @Override
    public String getType() {
        return "jdbc";
    }

    @Override
    public Collection<EtlRecord> extract(EtlJob job) {
        String query = job.getSourceQuery();
        List<Map<String, Object>> rows = jdbcTemplate.queryForList(query);
        List<EtlRecord> records = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            EtlRecord r = new EtlRecord(Instant.now(), "jdbc", 0);
            row.forEach(r::put);
            records.add(r);
        }
        return records;
    }
}
