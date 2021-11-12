CREATE TABLE [db_maintenance].[StopLists] (
    [UniqueName]   NVARCHAR (200) NOT NULL,
    [StopList_str] NVARCHAR (MAX) NOT NULL,
    PRIMARY KEY CLUSTERED ([UniqueName] ASC)
);

