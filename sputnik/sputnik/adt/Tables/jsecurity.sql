CREATE TABLE [adt].[jsecurity] (
    [tt]          DATETIME2 (3)  NOT NULL,
    [EventType]   NVARCHAR (100) NOT NULL,
    [ObjectType]  NVARCHAR (100) NULL,
    [ObjectName]  NVARCHAR (300) NULL,
    [CmdText]     XML            NOT NULL,
    [LoginName]   [sysname]      NOT NULL,
    [spid]        SMALLINT       NOT NULL,
    [Host]        NVARCHAR (75)  NOT NULL,
    [HostName]    NVARCHAR (100) NULL,
    [Host_pid]    INT            NULL,
    [ProgramName] NVARCHAR (200) NULL
);


GO
CREATE CLUSTERED INDEX [ci_tt]
    ON [adt].[jsecurity]([tt] ASC);


GO
GRANT INSERT
    ON OBJECT::[adt].[jsecurity] TO [audit_writer]
    AS [dbo];

