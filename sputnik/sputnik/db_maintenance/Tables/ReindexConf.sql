CREATE TABLE [db_maintenance].[ReindexConf] (
    [DBName]           NVARCHAR (500) NULL,
    [UniqueName_MW]    NVARCHAR (200) NOT NULL,
    [UniqueName_SL]    NVARCHAR (200) NULL,
    [RowLimit]         SMALLINT       DEFAULT ((10)) NOT NULL,
    [delayperiod]      CHAR (12)      DEFAULT ('00:00:00.100') NOT NULL,
    [filter_pages_min] INT            DEFAULT ((12)) NOT NULL,
    [filter_pages_max] INT            NULL,
    [filter_fragm_min] TINYINT        DEFAULT ((10)) NOT NULL,
    [filter_fragm_max] TINYINT        NULL,
    [filter_old_hours] TINYINT        DEFAULT ((24)) NOT NULL,
    [fragm_tresh]      TINYINT        DEFAULT ((30)) NOT NULL,
    [set_fillfactor]   TINYINT        DEFAULT ((100)) NOT NULL,
    [set_compression]  CHAR (4)       DEFAULT ('NONE') NOT NULL,
    [set_online]       CHAR (3)       DEFAULT ('OFF') NOT NULL,
    [set_sortintempdb] CHAR (3)       DEFAULT ('OFF') NOT NULL,
    [PauseMirroring]   BIT            DEFAULT ((0)) NOT NULL,
    [DeadLck_PR]       SMALLINT       DEFAULT ((0)) NOT NULL,
    [Lck_Timeout]      INT            DEFAULT ((20000)) NOT NULL,
    [timeout_sec]      INT            NULL,
    [set_maxdop]       SMALLINT       NULL, --set maxdop for reindex operation
    [walp_max_duration]     SMALLINT  NULL, --option WAIT_AT_LOW_PRIORITY parameter MAX_DURATION (in minutes). NULL - this option will not use
    [walp_abort_after_wait] VARCHAR(20) DEFAULT ('NONE') NOT NULL --option WAIT_AT_LOW_PRIORITY parameter ABORT_AFTER_WAIT (NONE, SELF, BLOCKERS)
);

