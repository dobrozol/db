CREATE TABLE [db_maintenance].[mw] (
    [UniqueName] NVARCHAR (200) NOT NULL,
    [WeekDays]   VARCHAR (100)  NULL,
    [TimeOpen]   TIME (7)       NULL,
    [TimeClose]  TIME (7)       NULL,
    [DateOpen]   DATETIME2 (2)  NULL,
    [DateClose]  DATETIME2 (2)  NULL,
    PRIMARY KEY CLUSTERED ([UniqueName] ASC)
);

