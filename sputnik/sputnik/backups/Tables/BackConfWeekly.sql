CREATE TABLE [backups].[BackConfWeekly] (
    [DBName]            NVARCHAR (300)  NULL,
    [FG]                NVARCHAR (1000) NULL,
    [LocalDir]          NVARCHAR (200)  NULL,
    [NetDir]            NVARCHAR (200)  NULL,
    [LocalDays]         INT             NULL,
    [NetDays]           INT             NULL,
    [Kind]              NVARCHAR (50)   NULL,
    [WeekDay]           TINYINT         NULL,
    [MonthDay]          TINYINT         NULL,
    [Policy_CountFiles] BIT             DEFAULT ((0)) NOT NULL,
    [LocalPolicy]       TINYINT         DEFAULT ((0)) NOT NULL,
    [NetPolicy]         TINYINT         DEFAULT ((0)) NOT NULL
);

