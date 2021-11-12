CREATE TABLE [awr].[pfc_handle] (
    [id]            SMALLINT       NOT NULL,
    [object_name]   NVARCHAR (300) NOT NULL,
    [counter_name]  NVARCHAR (300) NOT NULL,
    [instance_name] NVARCHAR (300) DEFAULT ('') NOT NULL,
    [counter_type]  INT            NULL,
    PRIMARY KEY CLUSTERED ([id] ASC)
);

