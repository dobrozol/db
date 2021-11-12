CREATE TABLE [config].[cleanup_hs] (
    [SchemaName]    NVARCHAR (300) NULL,
    [TableName]     NVARCHAR (300) NULL,
    [Column_filter] NVARCHAR (300) NULL,
    [interval_type] VARCHAR (30)   NULL,
    [interval]      VARCHAR (10)   NULL
);

