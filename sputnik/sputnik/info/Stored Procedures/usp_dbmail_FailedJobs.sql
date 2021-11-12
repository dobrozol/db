
CREATE PROCEDURE [info].[usp_dbmail_FailedJobs]
AS
BEGIN
	SET NOCOUNT ON;

	declare @tt float;
	--за последние 12 часов:
	declare @lastxhours smallint = 12;
	set @tt=cast(convert(varchar(8),dateadd(hour,-@lastxhours,getdate()), 112) as float)*1000000+cast(replace(convert(varchar(8), dateadd(hour,-@lastxhours,getdate()), 108),':','') as float);
	--select @tt
	IF object_Id('tempdb.dbo.#Failed_Jobs') is not null
		drop table #Failed_Jobs;
	create table #Failed_Jobs (
		[Status] [varchar](10) NOT NULL,
		[JobId] [uniqueidentifier] NULL,
		[Job Name] [varchar](100) NULL,
		[Step ID] [varchar](5) NULL,
		[Step Name] [varchar](30) NULL,
		--[Start Date Time] [varchar](30) NULL,
		[Start Date Time] [datetime] NULL,
		[Message] [nvarchar](4000) NULL)

	IF object_Id('tempdb.dbo.#Failed_Jobs_agr') is not null
		drop table #Failed_Jobs_agr;
	create table #Failed_Jobs_agr (
		[Job Fails] [int] NOT NULL,
		[Step Status] [varchar](10) NOT NULL,
		[Job Name] [varchar](100) NULL,
		[Step ID] [varchar](5) NULL,
		[Step Name] [varchar](30) NULL,
		--[Start Date Time] [varchar](30) NULL,
		[Step Fails] [int] NULL,
		[Last Start Date] [datetime] NULL,
		[Last Message] [nvarchar](4000) NULL)


	insert into #Failed_Jobs
	select DISTINCT
			'FAILED' as [Status], sjh.job_id as [JobId], cast(sj.name as varchar(100)) as "Job Name",
		   CASE WHEN sjh.step_id=0 THEN NULL ELSE cast(sjs.step_id as varchar(5)) END as "Step ID",
		   CASE WHEN sjh.step_id=0 THEN NULL ELSE cast(sjs.step_name as varchar(30)) END as "Step Name",
		   cast(REPLACE(CONVERT(varchar,convert(datetime,convert(varchar,sjh.run_date)),102),'.','-')+' '+SUBSTRING(RIGHT('000000'+CONVERT(varchar,sjh.run_time),6),1,2)+':'+SUBSTRING(RIGHT('000000'+CONVERT(varchar,sjh.run_time),6),3,2)+':'+SUBSTRING(RIGHT('000000'+CONVERT(varchar,sjh.run_time),6),5,2) as varchar(30)) 'Start Date Time',
		   sjh.message as "Message"
	from msdb.dbo.sysjobs sj
	LEFT join msdb.dbo.sysjobsteps sjs 
	 on sj.job_id = sjs.job_id
	join msdb.dbo.sysjobhistory sjh 
	 on sj.job_id = sjh.job_id and (sjs.step_id = sjh.step_id OR sjh.step_id=0)
	where sjh.run_status <> 1
		and cast(sjh.run_date as float)*1000000+sjh.run_time > @tt
		and sj.name <> '**Monitor DBMail'
	
	
	--union
	--select 'FAILED',cast(sj.name as varchar(100)) as "Job Name",
	--	   'MAIN' as "Step ID",
	--	   'MAIN' as "Step Name",
	--	   cast(REPLACE(CONVERT(varchar,convert(datetime,convert(varchar,sjh.run_date)),102),'.','-')+' '+SUBSTRING(RIGHT('000000'+CONVERT(varchar,sjh.run_time),6),1,2)+':'+SUBSTRING(RIGHT('000000'+CONVERT(varchar,sjh.run_time),6),3,2)+':'+SUBSTRING(RIGHT('000000'+CONVERT(varchar,sjh.run_time),6),5,2) as varchar(30)) 'Start Date Time',
	--	   sjh.message as "Message"
	--from msdb.dbo.sysjobs sj
	--join msdb.dbo.sysjobhistory sjh 
	-- on sj.job_id = sjh.job_id
	--where sjh.run_status <> 1 and sjh.step_id=0
	--  and cast(sjh.run_date as float)*1000000+sjh.run_time > @tt
	--	and sj.name <> '**Monitor DBMail'

	;WITH cte_01 as (
		SELECT DISTINCT 
			[Status],[JobID],[Job Name],
			dense_rank() OVER (partition by [Status],[JobID],[Step ID],[Step Name] order by [JobId], [Start Date Time],[Step ID]) as rn,
			[Step ID],[Step Name],
			[Start Date Time]
		FROM #Failed_Jobs
	)
	,cte_02 as (
		SELECT DISTINCT 
			[Status],[JobID],[Job Name],
			[Step ID],[Step Name],
			COUNT_BIG(*) OVER (partition by [Status],[JobID],[Step ID],[Step Name]) as [Step Fails],
			MAX([Start Date Time]) OVER (partition by [Status],[JobID],[Step ID],[Step Name]) as [Last Start Date],
			rn
		FROM cte_01
		WHERE [Step ID] IS NOT NULL
	)
	,cte_03 as (
		SELECT DISTINCT 
			[Status],[JobID],[Job Name],
			[Step ID],[Step Name],
			COUNT_BIG(*) OVER (partition by [Status],[JobID],[Step ID],[Step Name]) as [Job Fails],
			MAX([Start Date Time]) OVER (partition by [Status],[JobID],[Step ID],[Step Name]) as [Last Start Date],
			rn
		FROM cte_01
		WHERE [Step ID] IS NULL
	)
	
	INSERT INTO #Failed_Jobs_agr
	select DISTINCT 
		COALESCE(j.[Job Fails],0), s.[Status],s.[Job Name],s.[Step ID],s.[Step Name],s.[Step Fails], s.[Last Start Date], dt.[Message] AS [Last Message]
	from cte_02 s
	left join cte_03 j
		ON s.[JobId]=j.[JobId] and (s.rn=j.rn)
	inner join #Failed_Jobs dt
		ON s.[JobId]=dt.[JobId] and s.[Step ID]=dt.[Step ID] and s.[Last Start Date]=dt.[Start Date Time]
	--Дополнительная проверка: в уведомление попадут те Джобы, которые завершились с ошибкой больше 2 раз
	WHERE s.[Step Fails]>2
	;



	declare @cnt int  
	--select @cnt=COUNT(1) from #Failed_Jobs    
	--if (@cnt > 0)
	if exists(
		SELECT TOP 1 *
		FROM #Failed_Jobs_agr 
	)	
	begin

		declare @strsubject varchar(100)
		select @strsubject='Check the following failed jobs on ' + @@SERVERNAME

		declare @tableHTML  nvarchar(max);
		set @tableHTML =
			N'<H1>Failed Jobs Listing - ' + @@SERVERNAME +'</H1>' +
			N'<H3>in last 12 hours</H3>' +
			N'<table border="1">' +
			N'<tr><th>Job Fails</th><th>Step Fails</th><th>Job Name</th>' +
			N'<th>Step ID</th><th>Step Name</th><th>Last Start Date</th>' +
			N'<th>Last Message</th></tr>' +
			CAST ( ( SELECT td = FJ.[Job Fails], '',
							td = FJ.[Step Fails], '',
							td = FJ.[Job Name], '',
							td = FJ.[Step ID], '',
							td = FJ.[Step Name], '',
							td = FJ.[Last Start Date], '',
							td = FJ.[Last Message]
					  FROM #Failed_Jobs_agr FJ
					  ORDER BY FJ.[Step Fails] DESC
					  FOR XML PATH('tr'), TYPE 
			) AS NVARCHAR(MAX) ) +
			N'</table>' ;

		 EXEC msdb.dbo.sp_send_dbmail
		 --@from_address='test@test.com',
		 @recipients='dba-info@ntsmail.ru',
		 @subject = @strsubject,
		 @body = @tableHTML,
		 @body_format = 'HTML' ,
		 @profile_name='sql-info'
	end

	drop table #Failed_Jobs;
	drop table #Failed_Jobs_agr;
END