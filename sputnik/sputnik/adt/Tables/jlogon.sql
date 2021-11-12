CREATE TABLE [adt].[jlogon] (
    [tt]          DATETIME2 (3)  NOT NULL,
    [LoginName]   [sysname]      NOT NULL,
    [spid]        SMALLINT       NULL,
    [Host]        NVARCHAR (75)  NULL,
    [HostName]    NVARCHAR (100) NULL,
    [Host_pid]    INT            NULL,
    [ProgramName] NVARCHAR (200) NULL
);


GO
CREATE CLUSTERED INDEX [ci_tt]
    ON [adt].[jlogon]([tt] ASC);


GO
GRANT INSERT
    ON OBJECT::[adt].[jlogon] TO [audit_writer]
    AS [dbo];

