-- Рефакторинг схемы job'ов: переход на type-safe конфигурации
-- Удаляем старые колонки Source, Target, Params
-- Добавляем раздельное хранение типов и конфигураций компонентов

-- Удаляем старые колонки
ALTER TABLE service.tEtlJob
DROP COLUMN Source;
GO

ALTER TABLE service.tEtlJob
DROP COLUMN Target;
GO

ALTER TABLE service.tEtlJob
DROP COLUMN Params;
GO

-- Добавляем новые колонки для раздельного хранения
ALTER TABLE service.tEtlJob
ADD ExtractorType NVARCHAR(50) NOT NULL DEFAULT 'sql';
GO

ALTER TABLE service.tEtlJob
ADD ExtractorConfig NVARCHAR(MAX) NOT NULL DEFAULT '{}';
GO

ALTER TABLE service.tEtlJob
ADD TransformerType NVARCHAR(50) NOT NULL DEFAULT 'noop';
GO

ALTER TABLE service.tEtlJob
ADD TransformerConfig NVARCHAR(MAX) NOT NULL DEFAULT '{}';
GO

ALTER TABLE service.tEtlJob
ADD LoaderType NVARCHAR(50) NOT NULL DEFAULT 'sql';
GO

ALTER TABLE service.tEtlJob
ADD LoaderConfig NVARCHAR(MAX) NOT NULL DEFAULT '{}';
GO

-- Удаляем DEFAULT ограничения после добавления колонок
ALTER TABLE service.tEtlJob
ALTER COLUMN ExtractorType NVARCHAR(50) NOT NULL;
GO

ALTER TABLE service.tEtlJob
ALTER COLUMN ExtractorConfig NVARCHAR(MAX) NOT NULL;
GO

ALTER TABLE service.tEtlJob
ALTER COLUMN TransformerType NVARCHAR(50) NOT NULL;
GO

ALTER TABLE service.tEtlJob
ALTER COLUMN TransformerConfig NVARCHAR(MAX) NOT NULL;
GO

ALTER TABLE service.tEtlJob
ALTER COLUMN LoaderType NVARCHAR(50) NOT NULL;
GO

ALTER TABLE service.tEtlJob
ALTER COLUMN LoaderConfig NVARCHAR(MAX) NOT NULL;
GO

-- Создаем индексы для быстрого поиска по типам компонентов
CREATE NONCLUSTERED INDEX IX_tEtlJob_ExtractorType
    ON service.tEtlJob (ExtractorType ASC);
GO

CREATE NONCLUSTERED INDEX IX_tEtlJob_LoaderType
    ON service.tEtlJob (LoaderType ASC);
GO

-- Добавляем комментарии к новым колонкам
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Тип экстрактора: sql, kafka',
    @level0type = N'SCHEMA', @level0name = 'service',
    @level1type = N'TABLE', @level1name = 'tEtlJob',
    @level2type = N'COLUMN', @level2name = 'ExtractorType';
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Конфигурация экстрактора в формате JSON',
    @level0type = N'SCHEMA', @level0name = 'service',
    @level1type = N'TABLE', @level1name = 'tEtlJob',
    @level2type = N'COLUMN', @level2name = 'ExtractorConfig';
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Тип трансформера: noop, avro, record-to-avro',
    @level0type = N'SCHEMA', @level0name = 'service',
    @level1type = N'TABLE', @level1name = 'tEtlJob',
    @level2type = N'COLUMN', @level2name = 'TransformerType';
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Конфигурация трансформера в формате JSON',
    @level0type = N'SCHEMA', @level0name = 'service',
    @level1type = N'TABLE', @level1name = 'tEtlJob',
    @level2type = N'COLUMN', @level2name = 'TransformerConfig';
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Тип загрузчика: sql, fast-sql, kafka',
    @level0type = N'SCHEMA', @level0name = 'service',
    @level1type = N'TABLE', @level1name = 'tEtlJob',
    @level2type = N'COLUMN', @level2name = 'LoaderType';
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Конфигурация загрузчика в формате JSON',
    @level0type = N'SCHEMA', @level0name = 'service',
    @level1type = N'TABLE', @level1name = 'tEtlJob',
    @level2type = N'COLUMN', @level2name = 'LoaderConfig';
GO
