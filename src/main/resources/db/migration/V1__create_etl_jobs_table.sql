-- Создание таблицы для хранения ETL job'ов
-- Схема: SUPPORT.service.tEtlJob

-- Проверяем и создаем схему service, если она не существует
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'service')
BEGIN
    EXEC('CREATE SCHEMA service');
END
GO

-- Создаем таблицу tEtlJob
CREATE TABLE service.tEtlJob
(
    Id          NVARCHAR(255)  NOT NULL,
    Name        NVARCHAR(500)  NULL,
    SourceQuery NVARCHAR(MAX)  NULL,
    Target      NVARCHAR(500)  NULL,
    Params      NVARCHAR(MAX)  NOT NULL,
    CreatedAt   DATETIME2      NOT NULL DEFAULT GETDATE(),
    UpdatedAt   DATETIME2      NOT NULL DEFAULT GETDATE(),
    CreatedBy   NVARCHAR(255)  NULL,
    Status      NVARCHAR(50)   NOT NULL DEFAULT 'ACTIVE',
    Description NVARCHAR(MAX)  NULL,

    CONSTRAINT PK_tEtlJob PRIMARY KEY CLUSTERED (Id ASC)
);
GO

-- Создаем индексы для оптимизации запросов
CREATE NONCLUSTERED INDEX IX_tEtlJob_Status
    ON service.tEtlJob (Status ASC);
GO

CREATE NONCLUSTERED INDEX IX_tEtlJob_CreatedAt
    ON service.tEtlJob (CreatedAt DESC);
GO

-- Добавляем комментарии к таблице и колонкам (Extended Properties)
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Таблица для хранения конфигураций ETL job''ов',
    @level0type = N'SCHEMA', @level0name = 'service',
    @level1type = N'TABLE', @level1name = 'tEtlJob';
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Уникальный идентификатор job''а',
    @level0type = N'SCHEMA', @level0name = 'service',
    @level1type = N'TABLE', @level1name = 'tEtlJob',
    @level2type = N'COLUMN', @level2name = 'Id';
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Название job''а',
    @level0type = N'SCHEMA', @level0name = 'service',
    @level1type = N'TABLE', @level1name = 'tEtlJob',
    @level2type = N'COLUMN', @level2name = 'Name';
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'SQL-запрос источника (для SQL extractor)',
    @level0type = N'SCHEMA', @level0name = 'service',
    @level1type = N'TABLE', @level1name = 'tEtlJob',
    @level2type = N'COLUMN', @level2name = 'SourceQuery';
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Целевая таблица (для SQL loader)',
    @level0type = N'SCHEMA', @level0name = 'service',
    @level1type = N'TABLE', @level1name = 'tEtlJob',
    @level2type = N'COLUMN', @level2name = 'Target';
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Параметры job''а в формате JSON (extractor/transformer/loader конфигурация)',
    @level0type = N'SCHEMA', @level0name = 'service',
    @level1type = N'TABLE', @level1name = 'tEtlJob',
    @level2type = N'COLUMN', @level2name = 'Params';
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Дата и время создания job''а',
    @level0type = N'SCHEMA', @level0name = 'service',
    @level1type = N'TABLE', @level1name = 'tEtlJob',
    @level2type = N'COLUMN', @level2name = 'CreatedAt';
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Дата и время последнего обновления job''а',
    @level0type = N'SCHEMA', @level0name = 'service',
    @level1type = N'TABLE', @level1name = 'tEtlJob',
    @level2type = N'COLUMN', @level2name = 'UpdatedAt';
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Пользователь, создавший job',
    @level0type = N'SCHEMA', @level0name = 'service',
    @level1type = N'TABLE', @level1name = 'tEtlJob',
    @level2type = N'COLUMN', @level2name = 'CreatedBy';
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Статус job''а: ACTIVE - активен, DISABLED - отключен, ARCHIVED - архивирован',
    @level0type = N'SCHEMA', @level0name = 'service',
    @level1type = N'TABLE', @level1name = 'tEtlJob',
    @level2type = N'COLUMN', @level2name = 'Status';
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Описание назначения job''а',
    @level0type = N'SCHEMA', @level0name = 'service',
    @level1type = N'TABLE', @level1name = 'tEtlJob',
    @level2type = N'COLUMN', @level2name = 'Description';
GO
