CREATE TABLE [awr].[pfc_handle_dyn] (
    [id]            SMALLINT       IDENTITY (1, 1) NOT NULL,
    [pfc_id]        SMALLINT       NOT NULL,
    [instance_name] NVARCHAR (300) NOT NULL,
    PRIMARY KEY CLUSTERED ([id] ASC),
    FOREIGN KEY ([pfc_id]) REFERENCES [awr].[pfc_handle] ([id])
);

