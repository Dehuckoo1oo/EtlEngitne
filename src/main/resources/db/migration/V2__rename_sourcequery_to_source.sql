-- Переименование колонки SourceQuery в Source
-- для поддержки как SQL запросов, так и Kafka топиков

EXEC sp_rename 'service.tEtlJob.SourceQuery', 'Source', 'COLUMN';
