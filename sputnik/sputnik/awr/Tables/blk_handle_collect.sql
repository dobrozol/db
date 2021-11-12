CREATE TABLE [awr].[blk_handle_collect] (
    [tt]              DATETIME       NOT NULL,
    [spid]            SMALLINT       NOT NULL,
    [command]         NVARCHAR (32)  NULL,
    [status]          NVARCHAR (30)  NOT NULL,
    [start_time]      DATETIME2 (3)  NULL,
    [BlkBy]           SMALLINT       NULL,
    [wait_type]       NVARCHAR (60)  NULL,
    [wait_time]       INT            NULL,
    [wait_resource]   NVARCHAR (256) NULL,
    [trn_iso_lvl]     TINYINT        NULL,
    [SQLHandle]       VARBINARY (64) NOT NULL,
    [DB]              NVARCHAR (300) NULL,
    [Host]            NVARCHAR (150) NULL,
    [Login]           NVARCHAR (200) NULL,
    [Program]         NVARCHAR (150) NULL,
    [Host_pid]        INT            NULL,
    [statement_start] INT            NULL,
    [statement_end]   INT            NULL
);


GO
CREATE NONCLUSTERED INDEX [NCIX_tt_sqlhandle]
    ON [awr].[blk_handle_collect]([tt] DESC, [SQLHandle] ASC);

