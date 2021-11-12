CREATE TABLE [lse].[SourceConfig] (
    [id]           SMALLINT       IDENTITY (1, 1) NOT NULL,
    [ServerTarget] NVARCHAR (300) NULL,
    [DBNameSource] NVARCHAR (500) NULL,
    [DBNameTarget] NVARCHAR (500) NULL,
    PRIMARY KEY CLUSTERED ([id] ASC)
);

