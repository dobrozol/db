CREATE TABLE [awr].[pfc_data_dyn] (
    [tt]         DATETIME        DEFAULT (getdate()) NOT NULL,
    [pfc_dyn_id] SMALLINT        NOT NULL,
    [value]      NUMERIC (19, 2) NOT NULL,
    PRIMARY KEY CLUSTERED ([pfc_dyn_id] ASC, [tt] ASC),
    FOREIGN KEY ([pfc_dyn_id]) REFERENCES [awr].[pfc_handle_dyn] ([id])
);


GO
CREATE NONCLUSTERED INDEX [nci_tt]
    ON [awr].[pfc_data_dyn]([tt] ASC) WITH (ALLOW_PAGE_LOCKS = OFF);

