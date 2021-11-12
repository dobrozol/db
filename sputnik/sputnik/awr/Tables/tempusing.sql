CREATE TABLE [awr].[tempusing] (
    [tt]              DATETIME2 (2) NOT NULL,
    [spid]            SMALLINT      NOT NULL,
    [login_time]      DATETIME      NOT NULL,
    [rq_start_time]   DATETIME      NULL,
    [user_mb]         NUMERIC (9)   NULL,
    [internal_mb]     NUMERIC (9)   NULL,
    [sleep_status]    BIT           NOT NULL,
    [rq_elapsed_sec]  NUMERIC (9)   NULL,
    [rowver_tran_sec] NUMERIC (9)   NULL,
    [open_tran_flag]  BIT           NOT NULL,
    [kill_flag]       BIT           NOT NULL,
    PRIMARY KEY CLUSTERED ([tt] ASC, [spid] ASC)
);


GO
CREATE NONCLUSTERED INDEX [ixkill1]
    ON [awr].[tempusing]([login_time] ASC, [spid] ASC, [kill_flag] ASC, [rq_start_time] ASC) WHERE ([kill_flag]=(1));

