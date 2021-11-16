﻿CREATE TABLE [db_maintenance].[ReindexData] (
    [DBName]            NVARCHAR (300) NULL,
    [SchemaName]        NVARCHAR (300) NULL,
    [TableName]         NVARCHAR (300) NULL,
    [IndexName]         NVARCHAR (300) NULL,
    [TableID]           INT            NULL,
    [IndexID]           INT            NULL,
    [IndexType]         TINYINT        NULL,
    [SetFillFactor]     TINYINT        NULL,
    [TableCreateDate]   DATETIME2 (2)  NULL,
    [TableModifyDate]   DATETIME2 (2)  NULL,
    [PrepareDate]       DATETIME2 (2)  NULL,
    [PageCount]         BIGINT         NULL,
    [AVG_Fragm_percent] TINYINT        NULL,
    [~PageUsed_perc]    TINYINT        NULL,
    [~Row_cnt]          BIGINT         NULL,
    [~RowSize_Kb]       NUMERIC (9, 3) NULL,
    [LastUpdateStats]   DATETIME2 (2)  NULL,
    [LastCommand]       NVARCHAR (500) NULL,
    [LastRunDate]       DATETIME2 (2)  NULL,
    [ReindexCount]      INT            DEFAULT ((0)) NULL,
    [NotRunOnline]      BIT            DEFAULT ((0)) NULL,
    [NoReorganize]      BIT            DEFAULT ((0)) NULL
);

