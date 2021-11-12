
	/* =============================================
	-- Author:		Андрей Иванов (sqland1c)
	-- Create date: 03.08.2015
	-- Description:	Процедура для мониторинга выполняющихся запросов. 
					Выводит информацию о тяжелых запросах за последнюю минуты (интервал можно изменить в параметре @interval_sec)!
					Если @interval_sec=0, то берется самый последний снимок данных.
					Вся информация берётся из схемы awr базы sputnik, а не из dmv!
					Можно задать фильтры по REads, Duration и Text запросов.
					Параметр zabbix если включен, то выводит кол-во проблемных запросов за последнюю минуту.
					Также есть параметр @Kill если включен, то проблемные запросы будут удалены 
					(а на вкладке Messages выводится команда KILL для каждого запроса).
	-- Update:		17.09.2015 (1.02)
					Новый параметр @get_runtime_metr для замера длительности выполнения запросов в разрезах max, all, top5, avg.
					27.01.2016 (1.03)
					Добавлено исключение - трассировка OLAP событий.
					10.03.2016 (1.04)
					Оптимизация - вместо одного запроса со сложным условием, сделано два отдельных.
					Условие IF @interval_sec=0 - тогда используется точный поиск (tt=@last_tt)
					по индексу - гораздо меньше чтений! 
	-- ============================================= */
	CREATE PROCEDURE info.usp_SQLMon
		@Filter_Reads INT = 100000,
		@Filter_DurSec INT = 30,
		@Filter_Text NVARCHAR(500) = NULL,
		@interval_sec SMALLINT = 60,
		@zabbix BIT = 0,
		@kill BIT = 0,
		@get_runtime_metr BIT = 0
	AS
	BEGIN
		SET DATEFORMAT YMD;
		SET DATEFIRST 1;
		SET TRAN ISOLATION LEVEL READ UNCOMMITTED;
		SET NOCOUNT ON;
		declare @last_tt datetime;
		select @last_tt = max(tt) from awr.sql_handle_collect;
		IF datediff(second,@last_tt, getdate())<=60
		BEGIN
			--IF OBJECT_ID('tempdb.dbo.#last_tt') IS NOT NULL
			--	DROP TABLE #last_tt;
			CREATE TABLE #last_tt (tt datetime, spid smallint, start_time datetime);
			IF @interval_sec=0
				insert into #last_tt
				select distinct max(tt) over (partition by spid, start_time) as tt, spid, start_time			 
				from awr.sql_handle_collect
				where
					(tt=@last_tt)
					AND ([status] <> 'sleeping' OR @get_runtime_metr=1) --AND [status] IN ('running', 'runnable')
					AND (Logical_Reads+IO_Reads>@Filter_Reads OR @Filter_Reads=0)
					AND (DATEDIFF(second,start_time,tt)>@Filter_DurSec OR @Filter_DurSec=0)
			ELSE
				insert into #last_tt
				select distinct max(tt) over (partition by spid, start_time) as tt, spid, start_time			 
				from awr.sql_handle_collect
				where
					(tt between DATEADD(SECOND,-@interval_sec,@last_tt) AND @last_tt)
					AND ([status] <> 'sleeping' OR @get_runtime_metr=1) --AND [status] IN ('running', 'runnable')
					AND (Logical_Reads+IO_Reads>@Filter_Reads OR @Filter_Reads=0)
					AND (DATEDIFF(second,start_time,tt)>@Filter_DurSec OR @Filter_DurSec=0)
			IF @get_runtime_metr=1
			BEGIN
				--IF OBJECT_ID('tempdb.dbo.#t01') IS NOT NULL
				--	DROP TABLE #t01;
				select h.tt, ROUND(datediff(second,h.start_time,h.tt)/60.00,2) as RunTime_min
				into #t01
				from #last_tt as last_tt
				inner join awr.sql_handle_collect h
					on last_tt.tt=h.tt AND last_tt.SPID=h.SPID AND last_tt.start_time=h.start_time;
				;with cte_src AS (
					select
						tt, 
						cast(max(RunTime_min) over () as numeric(19,2)) as _max_min,
						cast(sum(RunTime_min) over () as numeric(19,2)) as _all_min,
						cast(avg(RunTime_min) over () as numeric(19,2)) as _avg_min,
						RunTime_min,
						row_number() over (order by RunTime_min desc) as rn
					from #t01	
				),
				cte_results_1 as (
					select distinct
						tt, _max_min as _Max, _avg_min as _Avg, _all_min as _All,
						cast(sum(RunTime_min) over () as numeric(19,2)) as _Top5 
					from cte_src
					where rn<=5
				)
				select tt, 'Run_time_query_min' as counter_name, instance_name, value
				from cte_results_1
				unpivot(value for instance_name in ([_Max],[_Avg],[_All],[_Top5])
				)unpvt
				order by counter_name,instance_name
				;
			END
			ELSE
			BEGIN
				select h.tt, h.SPID, h.Host, h.Program, h.Login, h.login_time, h.status, h.start_time, h.RunTime_sec, h.DB,
					h.wait_resource, h.wait_type, h.wait_time, h.Blk_By,	h.CPU_sec, h.Logical_Reads,h.IO_Reads, h.IO_Writes,
						REPLACE(REPLACE(REPLACE(	
							CASE WHEN [statement_start] > 0 THEN 
								CASE [statement_end] 
									WHEN -1 THEN SUBSTRING([SQLTEXT], ([statement_start]/2) + 1, 2147483647)
									ELSE SUBSTRING([SQLTEXT], ([statement_start]/2) + 1, ([statement_end] - [statement_start])/2)  
								END 
								ELSE CASE [statement_end] 
										WHEN -1 THEN RTRIM(LTRIM([SQLTEXT])) 
										ELSE LEFT([SQLTEXT], ([statement_end]/2) +1) 
								END 
							END
						,char(10),' '),char(9), ' '),char(13),' ')
						AS [Exec_Statement]
				INTO #t1
				FROM #last_tt as last_tt
				inner join awr.sql_handle_collect h
					on last_tt.tt=h.tt AND last_tt.SPID=h.SPID AND last_tt.start_time=h.start_time
				inner join awr.sql_text_collect as t
					on h.SQLHandle=t.SQLHandle
				--WHERE (SQLText LIKE @Filter_Text OR @Filter_Text IS NULL);
				WHERE SQLText NOT LIKE '%SELECT @OlapEvent = %';	--исключаем диагностические запросы (Трассировка OLAP).
				IF @zabbix=0
					SELECT SPID, Host, 
						CASE 
							WHEN Program LIKE 'SQLAgent - TSQL JobStep (Job 0x%' THEN 
								(	SELECT top 1 'SQLAgent Job "'+j.name+'". Step ('+SUBSTRING(T1.Program,CHARINDEX(': Step',T1.Program)+7,100)
									FROM [msdb].[dbo].[sysjobs] j
									WHERE convert(varchar(100),convert(binary(16),  j.job_id),2)=LEFT(REPLACE(T1.Program,'SQLAgent - TSQL JobStep (Job 0x',''),33)
								)
							ELSE Program
						END as Program,
						[Login], login_time, DB, RunTime_sec, [status], wait_type, Blk_By,Exec_Statement
					FROM #t1 as T1
					WHERE (Exec_Statement LIKE @Filter_Text OR @Filter_Text IS NULL);
				ELSE
					SELECT COUNT(*) as cnt FROM #t1 WHERE (Exec_Statement LIKE @Filter_Text OR @Filter_Text IS NULL);		

				IF @kill=1
				BEGIN
					declare @spid smallint, @kill_sql nvarchar(300);
					Declare C Cursor FOR
						SELECT spid FROM #t1 WHERE (Exec_Statement LIKE @Filter_Text OR @Filter_Text IS NULL);
					open C;
					FETCH NEXT FROM C INTO @spid;
					WHILE @@FETCH_STATUS=0
					BEGIN
						IF EXISTS(SELECT session_id FROM sys.dm_exec_sessions where session_id=@spid)
						BEGIN
							set @kill_sql='KILL '+CAST(@spid as varchar(6))+';';
							PRINT(@kill_sql);
							EXEC(@kill_sql);
						END
						FETCH NEXT FROM C INTO @spid;
					END
					close C;
					deallocate C;
				END
			END
	END
	END
GO
GRANT EXECUTE
    ON OBJECT::[info].[usp_SQLMon] TO [zabbix]
    AS [dbo];

