CREATE TABLE [awr].[db_usage_stats] (
    [tt]         DATETIME2 (2)   NOT NULL,
    [startup]    DATETIME2 (2)   NOT NULL,
    [db]         NVARCHAR (1000) NOT NULL,
    [dbid]       INT             NOT NULL,
    [createdate] DATETIME2 (2)   NOT NULL,
    [LastUsed]   DATETIME2 (2)   NULL,
    [cntcalls]   NUMERIC (19)    NULL
);

