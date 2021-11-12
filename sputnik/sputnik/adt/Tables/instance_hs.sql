CREATE TABLE [adt].[instance_hs] (
    [tt]         DATETIME2 (2)   NOT NULL,
    [event_type] NVARCHAR (128)  NOT NULL,
    [db]         NVARCHAR (2000) NULL,
    [spid]       SMALLINT        NULL,
    [login]      NVARCHAR (128)  NULL,
    [host]       NVARCHAR (128)  NULL,
    [program]    NVARCHAR (128)  NULL,
    [sqltext]    NVARCHAR (MAX)  NULL,
    [textdata]   NVARCHAR (2000) NULL
);


GO
CREATE CLUSTERED INDEX [cix01]
    ON [adt].[instance_hs]([tt] ASC, [event_type] ASC);

