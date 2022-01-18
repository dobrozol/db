/* =============================================
-- Author:		Andrei N. Ivanov (sqland1c)
-- Create date: 20.11.2021 (1.0)
-- Description: Procedure for getting and saving aggregate information from ReindexData into ReindexReport
-- Update:	

-- ============================================= */
CREATE PROCEDURE [db_maintenance].[usp_saveReindexReport]
	@modeIncremental bit = 0
AS
BEGIN
	set nocount on;
	declare @lastReportTime datetime2(2), @insertTime datetime2(2), @startTime datetime2(2), @StrErr varchar(1000), @flag_fail bit;
	begin try
		select @lastReportTime = max(reportTime)
		from [db_maintenance].[ReindexReport]
	
		select @insertTime = sysdatetime()

		select [DBName] as [dbId], concat([DBName],[TableID]) as tabId, 
			concat([DBName],[TableID], [IndexID]) as idxId, [PageCount]/128 as sizeMb,
			[AVG_Fragm_percent], [~PageUsed_perc], [ReindexCount]
		into #src
		from [db_maintenance].[ReindexData]
		where [PageCount] >= 0 and [AVG_Fragm_percent] between 0 and 100 and [~PageUsed_perc] between 0 and 100 and (
			[LastUpdateStats] > @lastReportTime
			or @lastReportTime is null
			or @modeIncremental = 0
		);

		select 'Index fragmentation %' as metricName,
			case 
				when [AVG_Fragm_percent] < 10 then '<10'
				when [AVG_Fragm_percent] between 10 and 19 then '10-20'
				when [AVG_Fragm_percent] between 20 and 29 then '20-30'
				when [AVG_Fragm_percent] between 30 and 39 then '30-40'
				when [AVG_Fragm_percent] between 40 and 49 then '40-50'
				when [AVG_Fragm_percent] between 50 and 59 then '50-60'
				when [AVG_Fragm_percent] between 60 and 69 then '60-70'
				when [AVG_Fragm_percent] between 70 and 79 then '70-80'
				when [AVG_Fragm_percent] between 80 and 89 then '80-90'
				else '>=90'
			end as metricRange,
			*
				into #srcWithMetrics
		from #src
		union
		select 'Page fullness %' as metricName,
			case 
				when [~PageUsed_perc] < 10 then '<10'
				when [~PageUsed_perc] between 10 and 19 then '10-20'
				when [~PageUsed_perc] between 20 and 29 then '20-30'
				when [~PageUsed_perc] between 30 and 39 then '30-40'
				when [~PageUsed_perc] between 40 and 49 then '40-50'
				when [~PageUsed_perc] between 50 and 59 then '50-60'
				when [~PageUsed_perc] between 60 and 69 then '60-70'
				when [~PageUsed_perc] between 70 and 79 then '70-80'
				when [~PageUsed_perc] between 80 and 89 then '80-90'
				else '>=90'
			end as metricRange,
			*
		from #src
		union
		select 'Index size Gb' as metricName,
			case 
				when sizeMb < 1024 then '<1'
				when sizeMb between 1024 and 2047	then '1-2'
				when sizeMb between 2048 and 3071	then '2-3'
				when sizeMb between 3072 and 5119	then '3-5'
				when sizeMb between 5120 and 10239	then '5-10'
				when sizeMb between 10240 and 20479	then '10-20'
				when sizeMb between 20480 and 30719	then '20-30'
				when sizeMb between 30720 and 51199	then '30-50'
				when sizeMb between 51200 and 92159 then '50-90'
				else '>=90'
			end as metricRange,
			*
		from #src

		insert into [db_maintenance].[ReindexReport] (
			[reportTime], [metricName], [metricRange], [countDb], [countTable], [countIndex], 
			[avgFragmIndex%], [medFragmIndex%], [avgPageUsed%], [medPageUsed%],
			[avgSizeMb], [medSizeMb], [sumSizeMb], [avgReindexCnt], [medReindexCnt], [sumReindexCnt]
		)
		select distinct
			@insertTime as reportTime, metricName, metricRange,
			dense_rank() over (partition by metricName, metricRange order by [dbId]) 
				+ dense_rank() over (partition by metricName, metricRange order by [dbId] desc) 
				- 1 as [countDb],
			dense_rank() over (partition by metricName, metricRange order by tabId) 
				+ dense_rank() over (partition by metricName, metricRange order by tabId desc) 
				- 1 as [countTable],
			dense_rank() over (partition by metricName, metricRange order by idxId) 
				+ dense_rank() over (partition by metricName, metricRange order by idxId desc) 
				- 1 as [countIndex],
			avg([AVG_Fragm_percent]) over (partition by metricName, metricRange) as [avgFragmIndex%],
			PERCENTILE_CONT (0.5) WITHIN GROUP ( ORDER BY [AVG_Fragm_percent] ASC ) over (partition by metricName, metricRange) as [medFragmIndex%],
			avg([~PageUsed_perc]) over (partition by metricName, metricRange) as [avgPageUsed%],
			PERCENTILE_CONT (0.5) WITHIN GROUP ( ORDER BY [~PageUsed_perc] ASC ) over (partition by metricName, metricRange) as [medPageUsed%],
			avg(sizeMb) over (partition by metricName, metricRange) as [avgSizeMb],
			PERCENTILE_CONT (0.5) WITHIN GROUP ( ORDER BY sizeMb ASC ) over (partition by metricName, metricRange) as [medSizeMb],
			sum(sizeMb) over (partition by metricName, metricRange) as [sumSizeMb],
			avg([ReindexCount]) over (partition by metricName, metricRange) as [avgReindexCnt],
			PERCENTILE_CONT (0.5) WITHIN GROUP ( ORDER BY [ReindexCount] ASC ) over (partition by metricName, metricRange) as [medReindexCnt],
			sum([ReindexCount]) over (partition by metricName, metricRange) as [sumReindexCnt]
		from #srcWithMetrics

		set @flag_fail=0;

	end try
	begin catch
		set @flag_fail=1;
		set @StrErr=COALESCE(ERROR_MESSAGE(),'An error occurred during the execution of the procedure');
	end catch

	--Logging
	EXEC db_maintenance.usp_WriteHS 
		@DB_ID=0,
		@Index_Stat_Type=0, --0-Index
		@Command_Type=40, --40 - Getting and saving ReindexReport
		@Command_Text_1000='exec [db_maintenance].[usp_saveReindexReport];',
		@tt_start=@insertTime,
		@Status=@flag_fail, --0-Success, 1-Fail(Error)
		@Error_Text_1000=@StrErr;
END
