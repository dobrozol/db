CREATE TABLE [lse].[TargetConfig] (
    [id]               SMALLINT       IDENTITY (1, 1) NOT NULL,
    [ServerSource]     NVARCHAR (300) NOT NULL,
    [DBNameSource]     NVARCHAR (500) NOT NULL,
    [DBNameTarget]     NVARCHAR (500) NOT NULL,
    [FromCopy]         BIT            DEFAULT ((0)) NOT NULL,
    [Suspend]          BIT            DEFAULT ((0)) NOT NULL,
    [InitDate]         DATETIME2 (2)  DEFAULT (NULL) NULL,
    [InitBackupHS_id]  INT            DEFAULT (NULL) NULL,
    [CatalogFilesDB]   NVARCHAR (800) NOT NULL,
    [CatalogLogFiles]  NVARCHAR (800) NULL,
    [StandBy_File]     NVARCHAR (600) NULL,
    [UseFreshDiffBack] BIT            NULL,
    PRIMARY KEY CLUSTERED ([id] ASC)
);

