CREATE TABLE [adt].[jconfigure] (
    [tt]          DATETIME2 (3)  NOT NULL,
    [PName]       NVARCHAR (50)  NOT NULL,
    [POldValue]   INT            NOT NULL,
    [PNewValue]   INT            NOT NULL,
    [LoginName]   [sysname]      NOT NULL,
    [spid]        SMALLINT       NOT NULL,
    [Host]        NVARCHAR (75)  NOT NULL,
    [HostName]    NVARCHAR (100) NULL,
    [Host_pid]    INT            NULL,
    [ProgramName] NVARCHAR (200) NULL
);


GO
CREATE CLUSTERED INDEX [ci_tt]
    ON [adt].[jconfigure]([tt] ASC);


GO
GRANT INSERT
    ON OBJECT::[adt].[jconfigure] TO [audit_writer]
    AS [dbo];

