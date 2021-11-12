CREATE TABLE [db_maintenance].[FreeProcCache] (
    [WeekDay]     TINYINT       NULL,
    [PeriodHours] TINYINT       DEFAULT ((6)) NULL,
    [LastRunDate] DATETIME2 (2) NULL
);

