# ETL Engine - Диаграммы последовательности

Детальные диаграммы выполнения для понимания внутренних процессов ETL Engine.

---

## 1. Kafka → SQL Pipeline (Complete Flow)

### Последовательность вызовов:

```
User/Service
    │
    ├─> new EtlJob(jobId, null, "target_table", params)
    │
    ▼
EtlPipelineFactory.create(job)
    │
    ├─> jobValidator.validateOrThrow(job)
    │       │
    │       ├─> ExtractorValidator.validate(job, "kafka")
    │       │       └─> ✓ topic not empty
    │       │       └─> ✓ startTimestamp < endTimestamp
    │       │
    │       ├─> TransformerValidator.validate(job, "avro")
    │       │       └─> ✓ valid transformer
    │       │
    │       └─> LoaderValidator.validate(job, "fast-sql")
    │               └─> ✓ targetTable specified
    │
    ├─> registry.getExtractor("kafka")
    │       └─> return KafkaExtractor instance
    │
    ├─> registry.getTransformer("avro")
    │       └─> return AvroToRecordTransformer instance
    │
    ├─> registry.getLoader("fast-sql")
    │       └─> return FastSqlServerLoader instance
    │
    └─> return new StreamingEtlPipeline(extractor, transformer, loader, ...)
    │
    ▼
StreamingEtlPipeline.run(job)
    │
    ├─> metrics.onJobStatusChanged(job, RUNNING)
    │
    ├─> metrics.onExtractStart(job)
    │
    ├─> extractor.extract(job, batchConsumer)
    │       │
    │       └─> KafkaExtractor.extract(job, batchConsumer)
    │               │
    │               ├─> Get topic partitions: consumer.partitionsFor(topic)
    │               │       └─> [partition-0, partition-1, ..., partition-71]
    │               │
    │               ├─> Create thread pool: Executors.newFixedThreadPool(4)
    │               │
    │               ├─> For each partition: submit task
    │               │       │
    │               │       ├─> Thread-1: consumePartition(partition-0, batchConsumer)
    │               │       ├─> Thread-2: consumePartition(partition-1, batchConsumer)
    │               │       ├─> Thread-3: consumePartition(partition-2, batchConsumer)
    │               │       └─> Thread-4: consumePartition(partition-3, batchConsumer)
    │               │
    │               └─> await termination
    │
    ▼
[Thread-1] consumePartition(partition-0, batchConsumer)
    │
    ├─> consumer.assign([partition-0])
    ├─> consumer.seek(partition-0, startOffset)
    │
    ├─> Loop: while (true)
    │       │
    │       ├─> records = consumer.poll(500ms)
    │       │
    │       ├─> For each record:
    │       │       │
    │       │       ├─> if (record.timestamp() > endTimestamp)
    │       │       │       └─> break (send remaining batch, exit)
    │       │       │
    │       │       ├─> etlRecord = convertToEtlRecord(record)
    │       │       ├─> currentBatch.add(etlRecord)
    │       │       │
    │       │       └─> if (currentBatch.size() >= streamBatchSize)
    │       │               │
    │       │               └─> batchConsumer.accept(currentBatch)  ──┐
    │       │                       └─> clear batch                   │
    │       │                                                          │
    │       └─> if (records.isEmpty()) break                          │
    │                                                                  │
    └─> batchConsumer.accept(remainingBatch)  ────────────────────────┤
                                                                       │
┌──────────────────────────────────────────────────────────────────────┘
│
▼
batchConsumer.accept(extractedBatch)  [CALLBACK в StreamingEtlPipeline]
    │
    ├─> batchNum = batchCounter.incrementAndGet()
    ├─> totalExtracted += extractedBatch.size()
    │
    ├─> if (shouldUpdateMetrics)
    │       └─> metrics.onExtractComplete(job, totalExtracted, duration)
    │
    ├─> log.info("Batch #{}: extracted {} records", batchNum, extractedBatch.size())
    │
    ▼
    TRANSFORM PHASE
    │
    ├─> if (batchNum == 1)
    │       └─> metrics.onJobStatusChanged(job, TRANSFORMING)
    │
    ├─> metrics.onTransformStart(job, extractedBatch.size())
    │
    ├─> transformedBatch = transformer.transform(extractedBatch, job)
    │       │
    │       └─> AvroToRecordTransformer.transform(extractedBatch, job)
    │               │
    │               └─> For each record in extractedBatch:
    │                       │
    │                       ├─> value = record.get("value")  // GenericRecord
    │                       │
    │                       ├─> if (value instanceof GenericRecord avro)
    │                       │       │
    │                       │       ├─> flattened = new EtlRecord(...)
    │                       │       │
    │                       │       └─> For each field in avro.getSchema():
    │                       │               └─> flattened.put(field.name(), avro.get(field.name()))
    │                       │
    │                       └─> return flattened
    │
    ├─> totalTransformed += transformedBatch.size()
    │
    ├─> if (shouldUpdateMetrics)
    │       └─> metrics.onTransformComplete(job, totalTransformed, duration)
    │
    ▼
    LOAD PHASE
    │
    ├─> if (batchNum == 1)
    │       └─> metrics.onJobStatusChanged(job, LOADING)
    │
    ├─> metrics.onLoadStart(job, transformedBatch.size())
    │
    ├─> loader.load(transformedBatch, job)
    │       │
    │       └─> FastSqlServerLoader.load(transformedBatch, job)
    │               │
    │               ├─> targetTable = job.getTargetTable()
    │               │       └─> "SUPPORT.dbo.orders"
    │               │
    │               └─> bulkInsertBatch(targetTable, transformedBatch, jobId)
    │                       │
    │                       ├─> Connection conn = dataSource.getConnection()
    │                       │
    │                       ├─> SQLServerConnection sqlConn = conn.unwrap(...)
    │                       │
    │                       ├─> SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(sqlConn)
    │                       │
    │                       ├─> bulkCopy.setDestinationTableName(targetTable)
    │                       │
    │                       ├─> Configure options:
    │                       │       ├─> options.setBatchSize(transformedBatch.size())
    │                       │       ├─> options.setTableLock(false)  // CRITICAL for parallelism!
    │                       │       ├─> options.setCheckConstraints(false)
    │                       │       ├─> options.setFireTriggers(false)
    │                       │       └─> options.setKeepNulls(true)
    │                       │
    │                       ├─> Add column mappings:
    │                       │       └─> For each column: bulkCopy.addColumnMapping(col, col)
    │                       │
    │                       ├─> bulkCopy.writeToServer(new EtlBulkRecord(transformedBatch))
    │                       │       │
    │                       │       └─> SQL Server receives data via TDS protocol
    │                       │               └─> Bulk insert into table
    │                       │
    │                       └─> log.info("Inserted {} rows in {}ms", size, duration)
    │
    ├─> totalLoaded += transformedBatch.size()
    │
    ├─> if (shouldUpdateMetrics)
    │       └─> metrics.onLoadComplete(job, totalLoaded, duration)
    │
    └─> [Callback ends, extractor continues with next batch]
    │
    ▼
[Repeat for all batches from all partitions]
    │
    ▼
[All threads complete]
    │
    ├─> executor.awaitTermination()
    │
    └─> extractor.extract() completes
    │
    ▼
StreamingEtlPipeline.run() continues
    │
    ├─> Final metrics update:
    │       ├─> metrics.onExtractComplete(job, totalExtracted, totalExtractDuration)
    │       ├─> metrics.onTransformComplete(job, totalTransformed, totalTransformDuration)
    │       └─> metrics.onLoadComplete(job, totalLoaded, totalLoadDuration)
    │
    ├─> metrics.onJobStatusChanged(job, COMPLETED)
    │
    └─> log.info("Job completed: extracted={}, loaded={}, batches={}, throughput={} rec/sec",
                 totalExtracted, totalLoaded, batchCount, throughput)
```

---

## 2. Параллельная обработка партиций Kafka

### Временная диаграмма (4 потока, 72 партиции Kafka):

```
Timeline →

Thread-1 (partitions 0, 4, 8, ..., 68):
    ├─[Batch1:13k]─→[Transform]─→[Load:500ms]────┐
    ├─[Batch2:14k]─→[Transform]─→[Load:400ms]────┤
    └─[Batch3:13k]─→[Transform]─→[Load:450ms]────┤
                                                  │
Thread-2 (partitions 1, 5, 9, ..., 69):          │
    ├─[Batch1:14k]─→[Transform]─→[Load:520ms]────┤
    ├─[Batch2:13k]─→[Transform]─→[Load:380ms]────┼─→ Параллельно!
    └─[Batch3:14k]─→[Transform]─→[Load:410ms]────┤
                                                  │
Thread-3 (partitions 2, 6, 10, ..., 70):         │
    ├─[Batch1:13k]─→[Transform]─→[Load:480ms]────┤
    ├─[Batch2:14k]─→[Transform]─→[Load:420ms]────┤
    └─[Batch3:13k]─→[Transform]─→[Load:390ms]────┤
                                                  │
Thread-4 (partitions 3, 7, 11, ..., 71):         │
    ├─[Batch1:14k]─→[Transform]─→[Load:510ms]────┤
    ├─[Batch2:13k]─→[Transform]─→[Load:430ms]────┤
    └─[Batch3:14k]─→[Transform]─→[Load:460ms]────┘

Total Time: ~18 seconds for 1M records
Throughput: ~55k records/sec
```

### Ключевые моменты:

1. **Каждый поток независим** - не ждет других
2. **FastSqlServerLoader с setTableLock(false)** - позволяет параллельные вставки
3. **Kafka партиции распределены равномерно** - каждый поток обрабатывает ~18 партиций
4. **Батчи разного размера** - зависит от данных в партиции (~13-14k)

---

## 3. SQL → SQL Pipeline (Партицированное чтение)

### Последовательность для JdbcExtractor:

```
StreamingEtlPipeline.run(job)
    │
    ├─> extractor.extract(job, batchConsumer)
    │       │
    │       └─> JdbcExtractor.extract(job, batchConsumer)
    │               │
    │               ├─> source = job.getSource()  // "SUPPORT.dbo.orders"
    │               │
    │               ├─> Check if partitioning enabled:
    │               │       └─> partitionColumn = "bucket"
    │               │       └─> partitions = 72
    │               │       └─> Yes → extractPartitioned()
    │               │
    │               └─> extractPartitioned(job, batchConsumer)
    │                       │
    │                       ├─> Create thread pool: Executors.newFixedThreadPool(4)
    │                       │
    │                       ├─> For partition in [0..71]:
    │                       │       │
    │                       │       └─> submit task:
    │                       │               extractPartition(job, partition, 72, batchConsumer)
    │                       │
    │                       └─> await termination
    │
    ▼
[Thread-1] extractPartition(job, partition=0, totalPartitions=72, batchConsumer)
    │
    ├─> Build partition query:
    │       SELECT * FROM SUPPORT.dbo.orders
    │       WHERE bucket % 72 = 0
    │
    ├─> jdbcTemplate.query(sql, resultSet -> {
    │       │
    │       ├─> List<EtlRecord> batch = new ArrayList<>(batchSize);
    │       │
    │       ├─> while (resultSet.next()) {
    │       │       │
    │       │       ├─> EtlRecord record = convertRowToRecord(resultSet);
    │       │       │       │
    │       │       │       ├─> For each column:
    │       │       │       │       └─> record.put(columnName, resultSet.getObject(columnName))
    │       │       │       │
    │       │       │       └─> return record
    │       │       │
    │       │       ├─> batch.add(record);
    │       │       │
    │       │       └─> if (batch.size() >= batchSize) {
    │       │               │
    │       │               ├─> batchConsumer.accept(new ArrayList<>(batch))  ──┐
    │       │               │       └─> [Transform & Load happen here]          │
    │       │               │                                                   │
    │       │               └─> batch.clear()                                   │
    │       │       }                                                           │
    │       │                                                                   │
    │       └─> if (!batch.isEmpty()) {                                        │
    │               └─> batchConsumer.accept(batch)  ───────────────────────────┤
    │       }                                                                   │
    │   });                                                                     │
    │                                                                           │
    └─> [Task completes]                                                       │
                                                                                │
┌───────────────────────────────────────────────────────────────────────────────┘
│
▼
batchConsumer.accept(extractedBatch)
    │
    └─> [Same as Kafka flow: Transform → Load]
```

### Распределение партиций:

```
Total rows: 1,000,000
Partitions: 72
Threads: 4

Thread-1 processes partitions: 0, 4, 8, 12, ..., 68  (18 partitions)
    └─> WHERE bucket % 72 IN (0, 4, 8, 12, ..., 68)
    └─> ~13,888 rows per partition
    └─> ~250,000 total rows

Thread-2 processes partitions: 1, 5, 9, 13, ..., 69  (18 partitions)
    └─> ~250,000 total rows

Thread-3 processes partitions: 2, 6, 10, 14, ..., 70  (18 partitions)
    └─> ~250,000 total rows

Thread-4 processes partitions: 3, 7, 11, 15, ..., 71  (18 partitions)
    └─> ~250,000 total rows
```

---

## 4. Метрики в реальном времени

### Поток событий метрик:

```
EtlPipeline execution
    │
    ├─> Event: onJobStatusChanged(job, RUNNING)
    │       │
    │       └─> EtlMetricsCollector.onJobStatusChanged(job, RUNNING)
    │               │
    │               ├─> Update internal state:
    │               │       └─> metricsByJob.get(jobId).updateStatus(RUNNING, now)
    │               │
    │               └─> Publish to listeners:
    │                       └─> For each listener:
    │                               └─> listener.onJobStatusChanged(job, RUNNING)
    │
    ├─> Event: onExtractComplete(job, 139387, 1500ms)
    │       │
    │       └─> EtlMetricsCollector.onExtractComplete(...)
    │               │
    │               ├─> Update metrics:
    │               │       ├─> extractedRecords.set(139387)
    │               │       └─> extractDurationMillis.set(1500)
    │               │
    │               └─> Publish to listeners:
    │                       └─> WebSocket broadcast to UI
    │                       └─> Log to monitoring system
    │
    ├─> Event: onLoadComplete(job, 139387, 800ms)
    │       │
    │       └─> EtlMetricsCollector.onLoadComplete(...)
    │               │
    │               ├─> Update metrics:
    │               │       ├─> loadedRecords.set(139387)
    │               │       └─> loadDurationMillis.addAndGet(800)
    │               │
    │               ├─> Calculate throughput:
    │               │       └─> throughput = loadedRecords / (totalDurationMillis / 1000)
    │               │
    │               └─> Publish to listeners
    │
    └─> Event: onJobStatusChanged(job, COMPLETED)
            │
            └─> Final snapshot:
                    └─> EtlJobMetricsSnapshot(
                            jobId = "kafka-to-sql",
                            status = COMPLETED,
                            extractedRecords = 1000000,
                            loadedRecords = 1000000,
                            extractDurationMillis = 14110,
                            transformDurationMillis = 120,
                            loadDurationMillis = 4180,
                            throughput = 54347.0  // rec/sec
                        )
```

### Расчет метрик:

```java
// Total Duration = сумма длительностей этапов
totalDuration = extractDurationMillis + transformDurationMillis + loadDurationMillis
              = 14110 + 120 + 4180
              = 18410 ms

// Throughput = records per second
throughput = loadedRecords / (totalDuration / 1000)
           = 1000000 / (18410 / 1000)
           = 54347 rec/sec
```

---

## 5. Обработка ошибок

### Error flow с Dead Letter Queue:

```
StreamingEtlPipeline.run(job)
    │
    └─> extractor.extract(job, batchConsumer)
            │
            └─> batchConsumer.accept(batch)
                    │
                    ├─> Try: transformer.transform(batch, job)
                    │       │
                    │       └─> Exception thrown!
                    │               └─> TransformationException
                    │
                    ├─> Catch: TransformationException
                    │       │
                    │       ├─> log.error("Transformation failed: {}", e.getMessage())
                    │       │
                    │       ├─> metrics.onError(job, e)
                    │       │       │
                    │       │       └─> EtlMetricsCollector.onError(job, e)
                    │       │               │
                    │       │               ├─> errorCount.incrementAndGet()
                    │       │               │
                    │       │               └─> Publish to listeners
                    │       │
                    │       ├─> deadLetterQueue.publish(e)
                    │       │       │
                    │       │       └─> Store failed batch info:
                    │       │               ├─> jobId
                    │       │               ├─> batchNumber
                    │       │               ├─> error message
                    │       │               ├─> stack trace
                    │       │               └─> failed records (sample)
                    │       │
                    │       └─> throw e  // Fail fast
                    │
                    └─> Pipeline stops, status = FAILED
```

### Dead Letter Queue structure:

```json
{
  "jobId": "kafka-to-sql-001",
  "timestamp": "2025-12-06T20:30:45Z",
  "batchNumber": 42,
  "errorType": "TransformationException",
  "errorMessage": "Failed to parse Avro record",
  "stackTrace": "...",
  "sampleRecords": [
    {
      "offset": 123456,
      "partition": "order-events-15",
      "timestamp": "2025-12-06T20:25:30Z",
      "value": "..."
    }
  ]
}
```

---

## 6. Cancellation Flow

### Отмена выполнения джоба:

```
User/Service
    │
    ├─> pipeline.cancel()
    │       │
    │       └─> CancellationToken.cancel()
    │               └─> cancelled.set(true)
    │
    ▼
StreamingEtlPipeline (в процессе выполнения)
    │
    ├─> extractor.extract(job, batch -> {
    │       │
    │       ├─> cancellationToken.checkCancellation()  ◄─── Check #1
    │       │       │
    │       │       └─> if (cancelled.get())
    │       │               └─> throw new CancellationException()
    │       │
    │       ├─> transformBatch(job, batch)
    │       │
    │       ├─> cancellationToken.checkCancellation()  ◄─── Check #2
    │       │
    │       ├─> loadBatch(job, batch)
    │       │       │
    │       │       └─> loader.load(batch, job)
    │       │               │
    │       │               └─> cancellationToken.checkCancellation()  ◄─── Check #3
    │       │
    │       └─> cancellationToken.checkCancellation()  ◄─── Check #4
    │   })
    │
    ├─> Catch: CancellationException
    │       │
    │       ├─> metrics.onJobStatusChanged(job, CANCELLED)
    │       │
    │       ├─> log.warn("Job was cancelled: extracted={}, loaded={}",
    │       │            totalExtracted, totalLoaded)
    │       │
    │       └─> throw e  // Propagate cancellation
    │
    └─> Pipeline stops gracefully
```

### Отмена в середине batch processing:

```
Timeline:
    │
    ├─ Batch 1: Extract → Transform → Load  [Completed]
    ├─ Batch 2: Extract → Transform → Load  [Completed]
    ├─ Batch 3: Extract → Transform → [CANCEL] ◄─── User calls cancel()
    │                                  └─> Current batch completes
    │                                  └─> Status = CANCELLED
    └─ Batch 4, 5, ... [Not processed]
```

---

## 7. Component Registration (Spring DI)

### Startup sequence:

```
Spring Application Startup
    │
    ├─> Component Scan:
    │       │
    │       ├─> @Component KafkaExtractor
    │       │       └─> Bean: kafkaExtractor
    │       │
    │       ├─> @Component JdbcExtractor
    │       │       └─> Bean: jdbcExtractor
    │       │
    │       ├─> @Component NoopTransformer
    │       │       └─> Bean: noopTransformer
    │       │
    │       ├─> @Component AvroToRecordTransformer
    │       │       └─> Bean: avroToRecordTransformer
    │       │
    │       ├─> @Component FastSqlServerLoader
    │       │       └─> Bean: fastSqlServerLoader
    │       │
    │       ├─> @Component KafkaLoader
    │       │       └─> Bean: kafkaLoader
    │       │
    │       └─> @Component JdbcLoader
    │               └─> Bean: jdbcLoader
    │
    ▼
EtlComponentRegistry (Construction)
    │
    ├─> @Autowired List<Extractor> extractors
    │       └─> [kafkaExtractor, jdbcExtractor]
    │
    ├─> @Autowired List<Transformer> transformers
    │       └─> [noopTransformer, avroToRecordTransformer, recordToAvroTransformer]
    │
    ├─> @Autowired List<Loader> loaders
    │       └─> [fastSqlServerLoader, kafkaLoader, jdbcLoader]
    │
    └─> Build Maps:
            │
            ├─> extractorMap = {
            │       "kafka" → kafkaExtractor,
            │       "sql" → jdbcExtractor
            │   }
            │
            ├─> transformerMap = {
            │       "noop" → noopTransformer,
            │       "avro" → avroToRecordTransformer,
            │       "record-to-avro" → recordToAvroTransformer
            │   }
            │
            └─> loaderMap = {
                    "fast-sql" → fastSqlServerLoader,
                    "kafka" → kafkaLoader,
                    "jdbc" → jdbcLoader
                }
```

---

## Заключение

Эти диаграммы показывают:

1. **Полный поток выполнения** от создания джоба до завершения
2. **Параллельную обработку** партиций Kafka и SQL
3. **Streaming архитектуру** с batch-by-batch обработкой
4. **Систему метрик** в реальном времени
5. **Обработку ошибок** через Dead Letter Queue
6. **Отмену джобов** с graceful shutdown
7. **Spring DI** и регистрацию компонентов

Все компоненты работают вместе для обеспечения high-performance, fault-tolerant ETL processing.
