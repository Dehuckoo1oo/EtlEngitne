package ru.pospelov.etl.engine.exception;

import ru.pospelov.etl.engine.model.EtlRecord;

/**
 * Исключение при ошибках конвертации типов между Java, Avro и JDBC представлениями.
 *
 * <p>Выбрасывается TypeConverter'ом когда:
 * <ul>
 * <li>Java-тип значения несовместим с Avro физическим типом схемы</li>
 * <li>Avro логический тип не может быть сконвертирован в Java-тип</li>
 * <li>SqlVariantValue содержит невалидные данные или неподдерживаемый базовый тип</li>
 * <li>JSON десериализация SqlVariantValue неуспешна</li>
 * </ul>
 *
 * <h2>Примеры ошибок</h2>
 *
 * <h3>Несовместимость Java → Avro</h3>
 * <pre>
 * // Avro schema ожидает int, но пришел String
 * TypeConversionException: Cannot convert String to Avro int for field 'age':
 * expected int, actual java.lang.String, value="twenty"
 * </pre>
 *
 * <h3>Несовместимость Avro → JDBC</h3>
 * <pre>
 * // Пришел GenericRecord вместо плоского значения
 * TypeConversionException: Cannot convert GenericRecord to JDBC type for field 'data':
 * GenericRecord should be flattened before SQL load
 * </pre>
 *
 * <h3>Невалидный sql_variant</h3>
 * <pre>
 * // Неподдерживаемый базовый тип в SqlVariantValue
 * TypeConversionException: Unsupported sql_variant base type 'geography':
 * spatial types are not supported in current version
 * </pre>
 *
 * <h3>JSON десериализация sql_variant</h3>
 * <pre>
 * // Невалидный JSON или неподдерживаемая версия
 * TypeConversionException: Failed to deserialize SqlVariantValue from JSON:
 * unsupported version 2, only version 1 is supported
 * </pre>
 *
 * @see ru.pospelov.etl.engine.conversion.TypeConverter
 */
public class TypeConversionException extends EtlException {

    /**
     * Создать исключение конвертации типов без контекста записи.
     *
     * <p>Используется для общих ошибок конвертации без привязки к конкретной записи.
     *
     * @param message описание ошибки
     * @param jobId идентификатор ETL job
     * @param stage этап pipeline где произошла ошибка (EXTRACT, TRANSFORM, LOAD)
     */
    public TypeConversionException(String message, String jobId, EtlStage stage) {
        super(message, jobId, stage, null, EtlErrorSeverity.CRITICAL);
    }

    /**
     * Создать исключение конвертации типов с причиной.
     *
     * @param message описание ошибки
     * @param jobId идентификатор ETL job
     * @param stage этап pipeline где произошла ошибка
     * @param cause исходное исключение (например JsonProcessingException, NumberFormatException)
     */
    public TypeConversionException(String message, String jobId, EtlStage stage, Throwable cause) {
        super(message, cause, jobId, stage, null, EtlErrorSeverity.CRITICAL);
    }

    /**
     * Создать исключение конвертации типов с полным контекстом записи.
     *
     * <p>Используется когда известна конкретная запись, где произошла ошибка.
     *
     * @param message описание ошибки
     * @param jobId идентификатор ETL job
     * @param stage этап pipeline где произошла ошибка
     * @param record запись, при обработке которой произошла ошибка
     * @param cause исходное исключение
     */
    public TypeConversionException(String message, String jobId, EtlStage stage, EtlRecord record, Throwable cause) {
        super(message, cause, jobId, stage, record, EtlErrorSeverity.CRITICAL);
    }
}
