CREATE TABLE [awr].[pfc_data] (
    [tt]     DATETIME        DEFAULT (getdate()) NOT NULL,
    [pfc_id] SMALLINT        NOT NULL,
    [value]  NUMERIC (19, 2) NOT NULL,
    PRIMARY KEY CLUSTERED ([pfc_id] ASC, [tt] ASC),
    FOREIGN KEY ([pfc_id]) REFERENCES [awr].[pfc_handle] ([id])
);


GO
CREATE NONCLUSTERED INDEX [nci_tt]
    ON [awr].[pfc_data]([tt] ASC) WITH (ALLOW_PAGE_LOCKS = OFF);

