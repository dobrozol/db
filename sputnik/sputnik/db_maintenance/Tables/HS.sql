CREATE TABLE [db_maintenance].[HS] (
    [DB_ID]             INT            NOT NULL,
    [Object_ID]         INT            NULL,
    [Index_Stat_ID]     INT            NULL,
    [Index_Stat_Type]   BIT            NULL,
    [Command_Type]      TINYINT        NOT NULL,
    [Command_Text_1000] VARCHAR (1000) NOT NULL,
    [tt_start]          DATETIME2 (2)  NOT NULL,
    [tt_end]            DATETIME2 (2)  NOT NULL,
    [Status]            BIT            NOT NULL,
    [Error_Text_1000]   VARCHAR (1000) NULL
);


GO
CREATE CLUSTERED INDEX [cix_HS_01]
    ON [db_maintenance].[HS]([tt_end] DESC, [Command_Type] ASC);

