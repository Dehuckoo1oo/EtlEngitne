package ru.pospelov.etl.engine.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * Контейнер для batch записей с метаданными колонок.
 *
 * Используется для передачи данных между компонентами ETL pipeline:
 * extractor → transformer → loader.
 *
 * <h2>Назначение columnMetadata</h2>
 * <ul>
 * <li>JDBC extractor заполняет columnMetadata на основе ResultSetMetaData</li>
 * <li>Kafka extractor оставляет columnMetadata = null (метаданные недоступны)</li>
 * <li>Transformer'ы пробрасывают columnMetadata без изменений</li>
 * <li>Loader'ы используют columnMetadata для корректной конвертации типов и диагностики</li>
 * </ul>
 *
 * <h2>Пример: JDBC источник</h2>
 * <pre>
 * Map&lt;String, ColumnMetadata&gt; metadata = new LinkedHashMap&lt;&gt;();
 * metadata.put("id", new ColumnMetadata("id", Types.INTEGER, "INT", 0, 0, false));
 * metadata.put("amount", new ColumnMetadata("amount", Types.DECIMAL, "DECIMAL", 18, 2, true));
 *
 * EtlBatch batch = new EtlBatch(records, metadata);
 * </pre>
 *
 * <h2>Пример: Kafka источник</h2>
 * <pre>
 * EtlBatch batch = new EtlBatch(records, null); // metadata недоступен
 * </pre>
 *
 * @see ColumnMetadata
 */
@AllArgsConstructor
@Getter
@ToString(exclude = "records") // Не выводить records в toString - может быть много данных
public class EtlBatch {
    /**
     * Коллекция записей в batch.
     * Обычно содержит от десятков до тысяч записей в зависимости от конфигурации batchSize.
     */
    private final Collection<EtlRecord> records;

    /**
     * Метаданные колонок источника данных.
     *
     * <p>Ключ: имя колонки (как в SQL или как в EtlRecord.fields).
     * <p>Значение: метаданные колонки (тип, precision, scale, nullable).
     *
     * <p>Может быть null для источников без метаданных (например Kafka).
     *
     * <p>Для JDBC источников содержит информацию из ResultSetMetaData,
     * собранную один раз на весь batch (не дублируется для каждой записи).
     */
    private final Map<String, ColumnMetadata> columnMetadata;

    /**
     * Получить неизменяемую view метаданных колонок.
     *
     * @return неизменяемая Map с метаданными или null если метаданные недоступны
     */
    public Map<String, ColumnMetadata> getColumnMetadata() {
        return columnMetadata != null
                ? Collections.unmodifiableMap(columnMetadata)
                : null;
    }

    /**
     * Проверить наличие метаданных.
     *
     * @return true если метаданные доступны (не null)
     */
    public boolean hasMetadata() {
        return columnMetadata != null;
    }

    /**
     * Получить количество записей в batch.
     *
     * @return количество записей
     */
    public int size() {
        return records.size();
    }

    /**
     * Проверить что batch пустой.
     *
     * @return true если нет записей
     */
    public boolean isEmpty() {
        return records.isEmpty();
    }
}
