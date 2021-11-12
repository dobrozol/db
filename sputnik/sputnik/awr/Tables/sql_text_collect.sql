CREATE TABLE [awr].[sql_text_collect] (
    [tt]        DATETIME       NOT NULL,
    [SQLHandle] VARBINARY (64) NOT NULL,
    [SQLText]   NVARCHAR (MAX) NOT NULL,
    [NumRuns]   BIGINT         DEFAULT ((0)) NOT NULL,
    [NumLocks]  BIGINT         DEFAULT ((0)) NOT NULL
);


GO
CREATE NONCLUSTERED INDEX [NCIX_sqlhandle]
    ON [awr].[sql_text_collect]([SQLHandle] ASC);


GO
GRANT SELECT
    ON OBJECT::[awr].[sql_text_collect] TO [zabbix]
    AS [dbo];

