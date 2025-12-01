# Метрики и мониторинг ETL Pipeline

Данный документ описывает систему метрик и мониторинга ETL движка, которая позволяет отслеживать выполнение задач, получать статистику и подписываться на события выполнения.

## Содержание

- [Обзор](#обзор)
- [Быстрый старт](#быстрый-старт)
- [Получение метрик](#получение-метрик)
- [Статусы выполнения](#статусы-выполнения)
- [Подписка на события](#подписка-на-события)
- [Структура метрик](#структура-метрик)
- [Примеры использования](#примеры-использования)
- [API Reference](#api-reference)

## Обзор

Система метрик ETL движка предоставляет:

- **Статусы выполнения** - отслеживание текущего состояния задачи (PENDING, RUNNING, EXTRACTING, TRANSFORMING, LOADING, COMPLETED, FAILED, CANCELLED, TIMEOUT)
- **Статистика выполнения** - количество обработанных записей, время выполнения по этапам, количество ошибок
- **Пропускная способность** - скорость обработки (записей/сек)
- **События жизненного цикла** - уведомления о начале/завершении этапов, ошибках, смене статусов
- **История выполнения** - временные метки начала и обновления метрик

## Быстрый старт

### Инжекция сервиса мониторинга

```java
import org.springframework.beans.factory.annotation.Autowired;
import ru.pospelov.etl.engine.metrics.EtlMonitoringService;

@Service
public class MyService {
    
    @Autowired
    private EtlMonitoringService monitoringService;
    
    // Использование сервиса...
}
```

### Получение статуса задачи

```java
Optional<EtlJobStatus> status = monitoringService.getStatus("my-job-id");
if (status.isPresent()) {
    System.out.println("Статус задачи: " + status.get());
}
```

### Получение метрик задачи

```java
Optional<EtlJobMetricsSnapshot> metrics = monitoringService.getMetrics("my-job-id");
if (metrics.isPresent()) {
    EtlJobMetricsSnapshot snapshot = metrics.get();
    System.out.println("Обработано записей: " + snapshot.loadedRecords());
    System.out.println("Время выполнения: " + snapshot.totalDuration().toSeconds() + " сек");
    System.out.println("Пропускная способность: " + snapshot.throughput() + " записей/сек");
}
```

## Получение метрик

### Получение метрик конкретной задачи

```java
Optional<EtlJobMetricsSnapshot> metrics = monitoringService.getMetrics("job-id");

metrics.ifPresentOrElse(
    snapshot -> {
        System.out.println("Job ID: " + snapshot.jobId());
        System.out.println("Статус: " + snapshot.status());
        System.out.println("Извлечено: " + snapshot.extractedRecords());
        System.out.println("Преобразовано: " + snapshot.transformedRecords());
        System.out.println("Загружено: " + snapshot.loadedRecords());
        System.out.println("Ошибок: " + snapshot.errorCount());
        System.out.println("Время извлечения: " + snapshot.extractDurationMillis() + " мс");
        System.out.println("Время преобразования: " + snapshot.transformDurationMillis() + " мс");
        System.out.println("Время загрузки: " + snapshot.loadDurationMillis() + " мс");
        System.out.println("Общее время: " + snapshot.totalDuration().toSeconds() + " сек");
        System.out.println("Пропускная способность: " + String.format("%.2f", snapshot.throughput()) + " записей/сек");
    },
    () -> System.out.println("Метрики для задачи не найдены")
);
```

### Получение метрик всех задач

```java
Map<String, EtlJobMetricsSnapshot> allMetrics = monitoringService.getAllMetrics();

allMetrics.forEach((jobId, snapshot) -> {
    System.out.println("Job: " + jobId + 
                       " | Статус: " + snapshot.status() + 
                       " | Загружено: " + snapshot.loadedRecords());
});
```

## Статусы выполнения

### Описание статусов

| Статус | Описание |
|--------|----------|
| `PENDING` | Задача создана, но выполнение еще не началось |
| `RUNNING` | Пайплайн активно выполняется (общий статус) |
| `EXTRACTING` | Этап извлечения данных в процессе |
| `TRANSFORMING` | Этап преобразования данных в процессе |
| `LOADING` | Этап загрузки данных в процессе |
| `COMPLETED` | Выполнение успешно завершено |
| `FAILED` | Выполнение завершено с ошибкой |
| `CANCELLED` | Выполнение отменено пользователем/системой |
| `TIMEOUT` | Выполнение остановлено из-за таймаута |

### Получение статуса

```java
Optional<EtlJobStatus> status = monitoringService.getStatus("job-id");

status.ifPresent(s -> {
    switch (s) {
        case COMPLETED -> System.out.println("Задача успешно завершена");
        case FAILED -> System.out.println("Задача завершена с ошибкой");
        case RUNNING, EXTRACTING, TRANSFORMING, LOADING -> 
            System.out.println("Задача выполняется: " + s);
        default -> System.out.println("Статус: " + s);
    }
});
```

## Подписка на события

### Создание слушателя метрик

Реализуйте интерфейс `EtlMetrics` для подписки на события выполнения:

```java
import ru.pospelov.etl.engine.metrics.EtlMetrics;
import ru.pospelov.etl.engine.metrics.EtlJobStatus;
import ru.pospelov.etl.engine.model.EtlJob;
import ru.pospelov.etl.engine.model.EtlRecord;
import ru.pospelov.etl.engine.exception.EtlException;

public class MyMetricsListener implements EtlMetrics {
    
    @Override
    public void onJobStatusChanged(EtlJob job, EtlJobStatus status) {
        System.out.println("Job " + job.getJobId() + " изменил статус: " + status);
    }
    
    @Override
    public void onExtractStart(EtlJob job) {
        System.out.println("Начало извлечения для job: " + job.getJobId());
    }
    
    @Override
    public void onExtractComplete(EtlJob job, int extractedRecords, long durationMillis) {
        System.out.println("Извлечение завершено: " + extractedRecords + 
                          " записей за " + durationMillis + " мс");
    }
    
    @Override
    public void onTransformStart(EtlJob job, int inputRecords) {
        System.out.println("Начало преобразования: " + inputRecords + " записей");
    }
    
    @Override
    public void onTransformComplete(EtlJob job, int outputRecords, long durationMillis) {
        System.out.println("Преобразование завершено: " + outputRecords + 
                          " записей за " + durationMillis + " мс");
    }
    
    @Override
    public void onLoadStart(EtlJob job, int inputRecords) {
        System.out.println("Начало загрузки: " + inputRecords + " записей");
    }
    
    @Override
    public void onLoadComplete(EtlJob job, int loadedRecords, long durationMillis) {
        System.out.println("Загрузка завершена: " + loadedRecords + 
                          " записей за " + durationMillis + " мс");
    }
    
    @Override
    public void onRecordProcessed(EtlJob job, EtlRecord record) {
        // Вызывается для каждой обработанной записи
        // Внимание: может быть очень много вызовов!
    }
    
    @Override
    public void onError(EtlJob job, EtlException exception) {
        System.err.println("Ошибка в job " + job.getJobId() + ": " + 
                          exception.getMessage());
    }
}
```

### Регистрация слушателя

```java
MyMetricsListener listener = new MyMetricsListener();
monitoringService.registerListener(listener);

// Выполнение задач...

// Отписка (опционально)
monitoringService.unregisterListener(listener);
```

### Анонимный слушатель

```java
EtlMetrics listener = new EtlMetrics() {
    @Override
    public void onJobStatusChanged(EtlJob job, EtlJobStatus status) {
        if (status == EtlJobStatus.COMPLETED) {
            System.out.println("Задача " + job.getJobId() + " завершена!");
        }
    }
    
    // Реализуйте остальные методы...
    @Override public void onExtractStart(EtlJob job) {}
    @Override public void onExtractComplete(EtlJob job, int extractedRecords, long durationMillis) {}
    @Override public void onTransformStart(EtlJob job, int inputRecords) {}
    @Override public void onTransformComplete(EtlJob job, int outputRecords, long durationMillis) {}
    @Override public void onLoadStart(EtlJob job, int inputRecords) {}
    @Override public void onLoadComplete(EtlJob job, int loadedRecords, long durationMillis) {}
    @Override public void onRecordProcessed(EtlJob job, EtlRecord record) {}
    @Override public void onError(EtlJob job, EtlException exception) {}
};

monitoringService.registerListener(listener);
```

## Структура метрик

### EtlJobMetricsSnapshot

Неизменяемый снимок метрик задачи содержит:

| Поле | Тип | Описание |
|------|-----|----------|
| `jobId` | `String` | Идентификатор задачи |
| `status` | `EtlJobStatus` | Текущий статус выполнения |
| `startedAt` | `Instant` | Время начала выполнения |
| `lastUpdatedAt` | `Instant` | Время последнего обновления метрик |
| `extractedRecords` | `long` | Количество извлеченных записей |
| `processedRecords` | `long` | Количество обработанных записей (с учетом трансформаций) |
| `transformedRecords` | `long` | Количество преобразованных записей |
| `loadedRecords` | `long` | Количество загруженных записей |
| `errorCount` | `long` | Количество ошибок |
| `extractDurationMillis` | `long` | Длительность этапа извлечения (мс) |
| `transformDurationMillis` | `long` | Длительность этапа преобразования (мс) |
| `loadDurationMillis` | `long` | Длительность этапа загрузки (мс) |
| `throughput` | `double` | Пропускная способность (записей/сек) |

### Методы

- `totalDuration()` - возвращает общее время выполнения как `Duration`

## Примеры использования

### Пример 1: Мониторинг выполнения задачи

```java
@Service
public class JobMonitorService {
    
    @Autowired
    private EtlMonitoringService monitoringService;
    
    public void monitorJob(String jobId) {
        // Проверяем статус каждые 5 секунд
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(() -> {
            Optional<EtlJobStatus> status = monitoringService.getStatus(jobId);
            status.ifPresent(s -> {
                System.out.println("Статус задачи " + jobId + ": " + s);
                
                if (s == EtlJobStatus.COMPLETED || s == EtlJobStatus.FAILED) {
                    scheduler.shutdown();
                    printFinalMetrics(jobId);
                }
            });
        }, 0, 5, TimeUnit.SECONDS);
    }
    
    private void printFinalMetrics(String jobId) {
        monitoringService.getMetrics(jobId).ifPresent(snapshot -> {
            System.out.println("=== Финальные метрики ===");
            System.out.println("Загружено записей: " + snapshot.loadedRecords());
            System.out.println("Время выполнения: " + snapshot.totalDuration().toSeconds() + " сек");
            System.out.println("Пропускная способность: " + 
                             String.format("%.2f", snapshot.throughput()) + " записей/сек");
        });
    }
}
```

### Пример 2: Отправка уведомлений при ошибках

```java
@Component
public class ErrorNotificationListener implements EtlMetrics {
    
    @Autowired
    private NotificationService notificationService;
    
    @PostConstruct
    public void register() {
        // Регистрация через EtlMonitoringService
    }
    
    @Override
    public void onError(EtlJob job, EtlException exception) {
        String message = String.format(
            "Ошибка в ETL задаче %s на этапе %s: %s",
            job.getJobId(),
            exception.getStage(),
            exception.getMessage()
        );
        notificationService.sendAlert(message);
    }
    
    // Реализуйте остальные методы интерфейса...
}
```

### Пример 3: Экспорт метрик в Prometheus

```java
@Component
public class PrometheusMetricsExporter implements EtlMetrics {
    
    private final Counter recordsProcessed = Counter.build()
        .name("etl_records_processed_total")
        .help("Total processed records")
        .register();
    
    private final Histogram stageDuration = Histogram.build()
        .name("etl_stage_duration_seconds")
        .help("Stage duration in seconds")
        .register();
    
    @Override
    public void onLoadComplete(EtlJob job, int loadedRecords, long durationMillis) {
        recordsProcessed.inc(loadedRecords);
        stageDuration.observe(durationMillis / 1000.0);
    }
    
    // Реализуйте остальные методы...
}
```

### Пример 4: Дашборд со статистикой

```java
@RestController
@RequestMapping("/api/etl/metrics")
public class MetricsController {
    
    @Autowired
    private EtlMonitoringService monitoringService;
    
    @GetMapping("/{jobId}")
    public ResponseEntity<EtlJobMetricsSnapshot> getMetrics(@PathVariable String jobId) {
        return monitoringService.getMetrics(jobId)
            .map(ResponseEntity::ok)
            .orElse(ResponseEntity.notFound().build());
    }
    
    @GetMapping
    public Map<String, EtlJobMetricsSnapshot> getAllMetrics() {
        return monitoringService.getAllMetrics();
    }
    
    @GetMapping("/{jobId}/status")
    public ResponseEntity<EtlJobStatus> getStatus(@PathVariable String jobId) {
        return monitoringService.getStatus(jobId)
            .map(ResponseEntity::ok)
            .orElse(ResponseEntity.notFound().build());
    }
}
```

## API Reference

### EtlMonitoringService

#### Методы получения метрик

- `Optional<EtlJobMetricsSnapshot> getMetrics(String jobId)` - получить метрики конкретной задачи
- `Map<String, EtlJobMetricsSnapshot> getAllMetrics()` - получить метрики всех задач
- `Optional<EtlJobStatus> getStatus(String jobId)` - получить статус задачи

#### Методы управления слушателями

- `void registerListener(EtlMetrics listener)` - зарегистрировать слушатель событий
- `void unregisterListener(EtlMetrics listener)` - отменить регистрацию слушателя

#### Методы очистки

- `void clear(String jobId)` - удалить метрики конкретной задачи
- `void clearAll()` - удалить все метрики

### EtlMetrics (интерфейс слушателя)

Все методы вызываются синхронно в потоке выполнения пайплайна. **Важно:** избегайте тяжелых операций в слушателях, чтобы не замедлять выполнение задач.

- `void onJobStatusChanged(EtlJob job, EtlJobStatus status)` - смена статуса задачи
- `void onExtractStart(EtlJob job)` - начало этапа извлечения
- `void onExtractComplete(EtlJob job, int extractedRecords, long durationMillis)` - завершение извлечения
- `void onTransformStart(EtlJob job, int inputRecords)` - начало преобразования
- `void onTransformComplete(EtlJob job, int outputRecords, long durationMillis)` - завершение преобразования
- `void onLoadStart(EtlJob job, int inputRecords)` - начало загрузки
- `void onLoadComplete(EtlJob job, int loadedRecords, long durationMillis)` - завершение загрузки
- `void onRecordProcessed(EtlJob job, EtlRecord record)` - обработка одной записи (вызывается очень часто!)
- `void onError(EtlJob job, EtlException exception)` - возникновение ошибки

## Рекомендации

### Производительность

1. **Не выполняйте тяжелые операции в слушателях** - они вызываются синхронно и могут замедлить выполнение задач
2. **Используйте асинхронную обработку** - если нужно отправить уведомление или сохранить метрики, делайте это в отдельном потоке
3. **Ограничьте обработку `onRecordProcessed`** - этот метод вызывается для каждой записи, что может быть миллионы раз

### Обработка ошибок

1. **Обрабатывайте исключения в слушателях** - ошибки в слушателях не должны прерывать выполнение пайплайна
2. **Используйте try-catch** для всех операций в слушателях

### Очистка метрик

1. **Регулярно очищайте старые метрики** - метрики хранятся в памяти, не забывайте их очищать
2. **Используйте `clear()` после завершения задачи** - если метрики больше не нужны

## Связанные документы

- [README.md](README.md) - Общая документация проекта
- [ROADMAP.md](ROADMAP.md) - План развития ETL Engine
- [SQL_TO_KAFKA.md](SQL_TO_KAFKA.md) - Инструкция SQL → Kafka
- [KAFKA_TO_SQL.md](KAFKA_TO_SQL.md) - Инструкция Kafka → SQL

