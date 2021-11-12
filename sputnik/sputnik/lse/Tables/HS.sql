CREATE TABLE [lse].[HS] (
    [config_id]       SMALLINT      NOT NULL,
    [BackupHS_id]     INT           NOT NULL,
    [StartRestore]    DATETIME2 (2) NULL,
    [CompleteRestore] DATETIME2 (2) NULL
);

