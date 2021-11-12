CREATE TABLE [db_maintenance].[RecomputeStatsConf] (
    [DBName]                NVARCHAR (500)  NULL,
    [UniqueName_MW]         NVARCHAR (200)  NOT NULL,
    [UniqueName_SL]         NVARCHAR (200)  NULL,
    [RowLimit]              SMALLINT        DEFAULT ((50)) NOT NULL,
    [timeout_sec]           INT             NULL,
    [delayperiod]           CHAR (12)       DEFAULT ('00:00:00.200') NOT NULL,
    [filter_rows_min]       INT             NULL,
    [filter_rows_max]       INT             NULL,
    [filter_DataUsedMb_min] NUMERIC (9, 1)  NULL,
    [filter_DataUsedMb_max] NUMERIC (9, 1)  NULL,
    [filter_perc_min]       DECIMAL (18, 2) DEFAULT ((8.00)) NOT NULL,
    [filter_perc_max]       DECIMAL (18, 2) NULL,
    [filter_old_hours]      TINYINT         DEFAULT ((24)) NOT NULL,
    [policy_scan]           VARCHAR (100)   NULL,
    [PauseMirroring]        BIT             DEFAULT ((0)) NOT NULL,
    [DeadLck_PR]            SMALLINT        DEFAULT ((0)) NOT NULL,
    [Lck_Timeout]           INT             DEFAULT ((20000)) NOT NULL
);

