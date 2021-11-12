CREATE TABLE [backups].[BackupHistory] (
    [id]                    INT             IDENTITY (1, 1) NOT NULL,
    [DB_Name]               NVARCHAR (300)  NULL,
    [FG]                    NVARCHAR (1000) NULL,
    [Backup_Type]           VARCHAR (4)     NULL,
    [Backup_File]           NVARCHAR (260)  NULL,
    [backup_start_date]     DATETIME        NULL,
    [backup_finish_date]    DATETIME        NULL,
    [first_LSN]             NUMERIC (25)    NULL,
    [last_LSN]              NUMERIC (25)    NULL,
    [database_backup_LSN]   NUMERIC (25)    NULL,
    [diff_base_LSN]         NUMERIC (25)    NULL,
    [backup_size_Mb]        NUMERIC (19, 3) NULL,
    [backup_compress_ratio] NUMERIC (5, 2)  NULL,
    CONSTRAINT [PK] PRIMARY KEY CLUSTERED ([id] ASC)
);

