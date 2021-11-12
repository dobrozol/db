
/* =============================================
-- Author:		Андрей Иванов (sqland1c)
-- Create date: 02.06.2014
-- Description:	
			Эта процедура выдаёт информацию о выполнении заданий (Job) SQL Server Agent 
			В двух вариантах: если не задан параметр @Activity (по умолчанию), то будут выбраны все ошибки заданий за последние @OldDay дней (по умолчанию 1);
			если параметр задан @Activity=1, то будет показана выборка подобно Job Activity Monitor.
-- Update:
				03.06.2014 (1.01)
				В результаты добавлен имя сервера(компьютера). Также изменено название столбца Duration.
				04.06.2014 (1.02)
				Для возвращаемого поле [message] задан Alias Info.
				15.08.2014 (1.1)
				В режим @Activity=1 добавлен алгоритм, который получает информацию о выполнении Job в текущий момент времени!
				При этом, если задание сейчас выполняется, то в поле Duration будет показано, сколько Времени оно выполняется!
				А в поле [DateTimeRun] дата и время запуска задания!
				Теперь это полноценный  Job Activity Monitor!
				19.08.2014 (1.12)
				Внесены исправления в алгоритм формирования отчета Job Activity Monitor.
				22.08.2014 (1.13)
				Добавлено ключевое слово DISTINCT для исключения дубликатов в отчете Job Activity Monitor.
				Такие дубликаты возникают, если задание выполняется в данный момент и у него несколько шагов (Step).
				11.08.2015 (1.20)
				Для активных заданий добавлена информация о текущем шаге задания
				19.01.2017 (1.300)
				Добавлен новый параметр @JobName - фильтр по имени Задания.
				Добавлен новый параметр @GetStatus - возвращает только 1 столбец RUN_STATUS. Требует чтобы параметр @JobName был также указан!
				19.04.2017 (1.316)
				Для параметра @JobName теперь можно задать маску %имя% (т.е. работает LIKE).
				Добавлен новый параметр @Lite. Возвращает 4 столбца: Job, Step, RUN_STATUS, Duration_min. Принудительно устанавливает параметр @Activity=1.
				Добавлен новый параметр @OnlyEnabled. По умолчанию 1. Возвращает только включенные Jobs. Если сделать 0, то увидим все Задания (включенные и отключенные).
-- ============================================= */
CREATE PROCEDURE info.usp_JobMonitor
	@OldDay tinyint = 1,
	@Activity bit = 0,
	@JobName nvarchar(2000)=null,
	@GetStatus bit=0,
	@Lite bit=0,
	@OnlyEnabled bit=1
AS
BEGIN
	SET NOCOUNT ON;
	IF @GetStatus=1
	BEGIN
		IF @JobName is null
		BEGIN
			SELECT 'Чтобы получить статус, укажите параметр @JobName' as RUN_STATUS;
			RETURN;
		END
		SET @Activity=1;		
	END
	IF @Lite=1 AND @Activity=0
		SET @Activity=1;
	
	IF @Activity=0 
	BEGIN
		SELECT 
			SERVERPROPERTY('ServerName') as SQLServerName,
			J.name as Job, 
			H.step_name as Step,
			H.[message] as Info,
			CASE H.run_status
				WHEN 0 THEN 'Failed'
				WHEN 1 THEN 'Successfully'
				WHEN 2 THEN 'Re-run'
				WHEN 3 THEN 'Canceled by user'
			END AS RUN_STATUS,
			MSDB.dbo.agent_datetime(run_date, run_time) AS [DateTimeRun],
			Duration=STUFF(STUFF(RIGHT('000000' + CONVERT(VARCHAR(6),H.run_duration),6 ),3,0,':'),6,0,':')
		FROM
			(	SELECT job_id,name
				FROM msdb.dbo.sysjobs
				WHERE [enabled]=1
					AND name<>'syspolicy_purge_history'
			) J
		INNER JOIN msdb.dbo.sysjobhistory H
			ON J.job_id=H.job_id  
				and H.run_status IN (0,3)  --Ошибка или Отмена пользователем.
				and step_id>0
		WHERE
			DATEDIFF(day,convert(datetime2(2),cast(H.run_date as varchar(8))),SYSDATETIME())<=1 --За один последний день
	END
	ELSE
	BEGIN
	--Job Activity Monitor
		DECLARE @TJ TABLE (SQLServerName varchar(128), Job nvarchar(2000), Step nvarchar(2000), Info nvarchar(max), RUN_STATUS varchar(50), [DateTimeRun] datetime, Duration varchar(100), Duration_min numeric(19,2));
		WITH TT
		AS
		(	SELECT 
				J.name as Job, 
				J.job_id,
				J.[enabled],
				H.Step_name as Step,
				H.[message] as Info,
				H.run_status,
				MSDB.dbo.agent_datetime(run_date, run_time) AS [DateTimeRun],
				H.run_duration
			FROM
				(	SELECT job_id,name,[enabled] 
					FROM msdb.dbo.sysjobs
					WHERE name<>'syspolicy_purge_history' 
						--and [enabled]=1					
						AND (name LIKE @JobName OR @JobName is null)
				) J
			LEFT JOIN msdb.dbo.sysjobhistory H
				ON J.job_id=H.job_id  
					and step_id>0 AND (H.run_status BETWEEN 0 AND 3)
		)
		INSERT INTO @TJ (SQLServerName,Job,Step,Info,RUN_STATUS,[DateTimeRun],Duration,Duration_min)
		SELECT DISTINCT
			CAST(SERVERPROPERTY('ServerName') as varchar(128)) as SQLServerName,
			GMAX.Job, 
			CASE
				WHEN ja.job_id IS NOT NULL THEN ja.step_name
				ELSE DET.Step
			END AS Step,
			CASE 
				WHEN ja.job_id IS NOT NULL THEN NULL
				ELSE DET.Info
			END AS Info,
			CASE 
				WHEN ja.job_id IS NOT NULL THEN 'Running'
				WHEN DET.run_status=0 THEN 'Failed'
				WHEN DET.run_status=1 THEN 'Successfully'
				WHEN DET.run_status=2 THEN 'Re-run'
				WHEN DET.run_status=3 THEN 'Canceled by user'
			END AS RUN_STATUS,
			CASE
				WHEN ja.job_id IS NOT NULL THEN ja.start_execution_date
				ELSE DET.DateTimeRun
			END AS [DateTimeRun],
			CASE 
				WHEN ja.job_id IS NOT NULL THEN 
						RIGHT('00'+CAST ((DATEDIFF(SECOND, ja.start_execution_date, SYSDATETIME())/3600) AS VARCHAR (2)),2)
						+':'+ RIGHT('00'+CAST ((DATEDIFF(SECOND, ja.start_execution_date, SYSDATETIME()) % 3600) / 60 AS VARCHAR(2)),2)
						+':'+ RIGHT('00'+CAST (((DATEDIFF(SECOND, ja.start_execution_date, SYSDATETIME()) % 3600) % 60 ) % 60 AS VARCHAR(2)),2)
				ELSE STUFF(STUFF(RIGHT('000000' + CONVERT(VARCHAR(6),DET.run_duration),6 ),3,0,':'),6,0,':')
			END AS Duration,
			CASE 
				WHEN ja.job_id IS NOT NULL THEN 
						CAST((CAST(DATEDIFF(SECOND, ja.start_execution_date, SYSDATETIME()) AS NUMERIC(19,2))) / 60 AS NUMERIC(19,2))
				ELSE
					CAST(((DET.run_duration/10000) * 60) as numeric(19,2))+ --Hours
					CAST((DET.run_duration/100%100 ) as numeric(19,2))+		--Mins
					CAST((DET.run_duration%100/60.00) as numeric(19,2))		--Secs
			END AS Duration_min
		FROM 
		(
			SELECT 
				job_id, Step, [enabled], Job, MAX(DateTimeRun) AS LastDateTimeRun
			FROM TT
			GROUP BY
				job_id, Step, [enabled], Job
		)GMAX
		LEFT JOIN TT DET 
			ON GMAX.job_id=DET.job_id AND GMAX.Step=DET.Step AND GMAX.LastDateTimeRun=DET.DateTimeRun
		--отсюда получим информацию о работе Job в текущий момент времени:
		LEFT JOIN 
			(
				SELECT
					ja.job_id,
					ja.start_execution_date,      
					-- ISNULL(last_executed_step_id,0)+1 AS current_executed_step_id,
					ja.last_executed_step_date,
					Js.step_name
				FROM msdb.dbo.sysjobactivity ja 
				LEFT JOIN msdb.dbo.sysjobhistory jh 
					ON ja.job_history_id = jh.instance_id
				JOIN msdb.dbo.sysjobsteps js
					ON ja.job_id = js.job_id
					AND ISNULL(ja.last_executed_step_id,0)+1 = js.step_id
				WHERE ja.session_id = (SELECT TOP 1 session_id FROM msdb.dbo.syssessions ORDER BY agent_start_date DESC)
				AND start_execution_date is not null
				AND stop_execution_date is null
			)ja ON GMAX.job_id=ja.job_id
		WHERE ((GMAX.[enabled]=1 AND DET.run_status IS NOT NULL) OR ja.job_id IS NOT NULL) OR (@OnlyEnabled=0)
		
		IF @GetStatus=1
			SELECT TOP 1 RUN_STATUS FROM @TJ WHERE Job LIKE @JobName;
		ELSE IF @Lite=1
			SELECT Job, Step, RUN_STATUS, Duration_min FROM @TJ;
		ELSE
			SELECT SQLServerName,Job,Step,Info,RUN_STATUS,[DateTimeRun],Duration FROM @TJ;
		
	END
END