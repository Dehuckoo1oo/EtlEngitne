-- Unified migration for ETL job storage (merged previous V1-V3)
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'service')
BEGIN
    EXEC('CREATE SCHEMA service');
END
GO

CREATE TABLE service.tEtlJob
(
    Id                NVARCHAR(255) NOT NULL,
    Name              NVARCHAR(500) NULL,
    CreatedAt         DATETIME2     NOT NULL DEFAULT GETDATE(),
    UpdatedAt         DATETIME2     NOT NULL DEFAULT GETDATE(),
    CreatedBy         NVARCHAR(255) NULL,
    Status            NVARCHAR(50)  NOT NULL DEFAULT 'ACTIVE',
    Description       NVARCHAR(MAX) NULL,
    ExtractorType     NVARCHAR(50)  NOT NULL DEFAULT 'sql',
    ExtractorConfig   NVARCHAR(MAX) NOT NULL DEFAULT '{}',
    TransformerType   NVARCHAR(50)  NOT NULL DEFAULT 'noop',
    TransformerConfig NVARCHAR(MAX) NOT NULL DEFAULT '{}',
    LoaderType        NVARCHAR(50)  NOT NULL DEFAULT 'sql',
    LoaderConfig      NVARCHAR(MAX) NOT NULL DEFAULT '{}',

    CONSTRAINT PK_tEtlJob PRIMARY KEY CLUSTERED (Id ASC)
);
GO

CREATE NONCLUSTERED INDEX IX_tEtlJob_Status
    ON service.tEtlJob (Status ASC);
GO

CREATE NONCLUSTERED INDEX IX_tEtlJob_CreatedAt
    ON service.tEtlJob (CreatedAt DESC);
GO

CREATE NONCLUSTERED INDEX IX_tEtlJob_ExtractorType
    ON service.tEtlJob (ExtractorType ASC);
GO

CREATE NONCLUSTERED INDEX IX_tEtlJob_LoaderType
    ON service.tEtlJob (LoaderType ASC);
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Metadata and configuration for ETL jobs',
    @level0type = N'SCHEMA', @level0name = 'service',
    @level1type = N'TABLE', @level1name = 'tEtlJob';
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Unique job identifier',
    @level0type = N'SCHEMA', @level0name = 'service',
    @level1type = N'TABLE', @level1name = 'tEtlJob',
    @level2type = N'COLUMN', @level2name = 'Id';
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Human-readable job name',
    @level0type = N'SCHEMA', @level0name = 'service',
    @level1type = N'TABLE', @level1name = 'tEtlJob',
    @level2type = N'COLUMN', @level2name = 'Name';
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Creation timestamp',
    @level0type = N'SCHEMA', @level0name = 'service',
    @level1type = N'TABLE', @level1name = 'tEtlJob',
    @level2type = N'COLUMN', @level2name = 'CreatedAt';
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Last update timestamp',
    @level0type = N'SCHEMA', @level0name = 'service',
    @level1type = N'TABLE', @level1name = 'tEtlJob',
    @level2type = N'COLUMN', @level2name = 'UpdatedAt';
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Creator or owner of the job',
    @level0type = N'SCHEMA', @level0name = 'service',
    @level1type = N'TABLE', @level1name = 'tEtlJob',
    @level2type = N'COLUMN', @level2name = 'CreatedBy';
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Status: ACTIVE, DISABLED, ARCHIVED',
    @level0type = N'SCHEMA', @level0name = 'service',
    @level1type = N'TABLE', @level1name = 'tEtlJob',
    @level2type = N'COLUMN', @level2name = 'Status';
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Free-form job description',
    @level0type = N'SCHEMA', @level0name = 'service',
    @level1type = N'TABLE', @level1name = 'tEtlJob',
    @level2type = N'COLUMN', @level2name = 'Description';
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Extractor type: sql, kafka, etc.',
    @level0type = N'SCHEMA', @level0name = 'service',
    @level1type = N'TABLE', @level1name = 'tEtlJob',
    @level2type = N'COLUMN', @level2name = 'ExtractorType';
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Extractor configuration JSON',
    @level0type = N'SCHEMA', @level0name = 'service',
    @level1type = N'TABLE', @level1name = 'tEtlJob',
    @level2type = N'COLUMN', @level2name = 'ExtractorConfig';
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Transformer type: noop, avro, record-to-avro',
    @level0type = N'SCHEMA', @level0name = 'service',
    @level1type = N'TABLE', @level1name = 'tEtlJob',
    @level2type = N'COLUMN', @level2name = 'TransformerType';
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Transformer configuration JSON',
    @level0type = N'SCHEMA', @level0name = 'service',
    @level1type = N'TABLE', @level1name = 'tEtlJob',
    @level2type = N'COLUMN', @level2name = 'TransformerConfig';
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Loader type: sql, fast-sql, kafka',
    @level0type = N'SCHEMA', @level0name = 'service',
    @level1type = N'TABLE', @level1name = 'tEtlJob',
    @level2type = N'COLUMN', @level2name = 'LoaderType';
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Loader configuration JSON',
    @level0type = N'SCHEMA', @level0name = 'service',
    @level1type = N'TABLE', @level1name = 'tEtlJob',
    @level2type = N'COLUMN', @level2name = 'LoaderConfig';
GO
