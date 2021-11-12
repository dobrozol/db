CREATE TABLE [backups].[BackConf] (
    [DBName]      NVARCHAR (300)  NULL,
    [FG]          NVARCHAR (1000) NULL,
    [LocalDir]    NVARCHAR (200)  NULL,
    [NetDir]      NVARCHAR (200)  NULL,
    [LocalDays]   INT             NULL,
    [NetDays]     INT             NULL,
    [Kind]        NVARCHAR (50)   NULL,
    [LocalPolicy] TINYINT         DEFAULT ((0)) NOT NULL,
    [NetPolicy]   TINYINT         DEFAULT ((0)) NOT NULL
);

