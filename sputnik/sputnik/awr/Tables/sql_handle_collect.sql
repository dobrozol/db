CREATE TABLE [awr].[sql_handle_collect] (
    [tt]                        DATETIME        NOT NULL,
    [Host]                      NVARCHAR (150)  NULL,
    [Program]                   NVARCHAR (150)  NULL,
    [Login]                     NVARCHAR (200)  NOT NULL,
    [login_time]                DATETIME        NOT NULL,
    [Open_Tran]                 INT             NULL,
    [SPID]                      SMALLINT        NOT NULL,
    [DB]                        NVARCHAR (300)  NULL,
    [start_time]                DATETIME        NULL,
    [status]                    NVARCHAR (30)   NOT NULL,
    [command]                   NVARCHAR (32)   NULL,
    [Blk_By]                    SMALLINT        NULL,
    [perc_complete]             REAL            NULL,
    [wait_type]                 NVARCHAR (60)   NULL,
    [wait_time]                 INT             NULL,
    [wait_resource]             NVARCHAR (256)  NULL,
    [RunTime_sec]               DECIMAL (15, 3) NOT NULL,
    [CPU_sec]                   DECIMAL (15, 3) NOT NULL,
    [MemoryMb]                  DECIMAL (14, 2) NOT NULL,
    [IO_Reads]                  BIGINT          NOT NULL,
    [IO_Writes]                 BIGINT          NOT NULL,
    [Logical_Reads]             BIGINT          NOT NULL,
    [RowCount]                  BIGINT          NOT NULL,
    [SQLHandle]                 VARBINARY (64)  NOT NULL,
    [statement_start]           INT             NULL,
    [statement_end]             INT             NULL,
    [tempdb_current_query]      BIGINT          NULL,
    [tempdb_allocation_query]   BIGINT          NULL,
    [tempdb_current_session]    BIGINT          NULL,
    [tempdb_allocation_session] BIGINT          NULL
);


GO
CREATE NONCLUSTERED INDEX [NCIX_tt_sqlhandle]
    ON [awr].[sql_handle_collect]([tt] DESC, [SQLHandle] ASC);


GO
GRANT SELECT
    ON OBJECT::[awr].[sql_handle_collect] TO [zabbix]
    AS [dbo];

