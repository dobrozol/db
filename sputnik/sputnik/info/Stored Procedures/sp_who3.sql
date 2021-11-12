
/* =============================================
-- Author:		Андрей Иванов (sqland1c)
-- Create date: 28.11.2013
-- Description:	Эта процедура возвращает зависшие активные процессы. Параметр @busy_minutes - позволяет задать в минутах
--				сколько висит процесс (в результатах будут только с этим временем и больше!)
-- Update:
--				1.1 ( 28.11.2013) Добавлен параметр @blocks. По умолчанию = 0.
--			    Если @blocks=1 то процедура выполнит запрос по блокирующим друг друга процессам.				
--				1.2 ( 29.11.2013) Добавлен ServerTime в результаты.
--				1.3 ( 23.12.2013) Добавлен параметр @zabbix для мониторинга за долго выполняющимися процессами через систему zabbix!
				1.35 (25.12.2013) Для отчёта Забикса добавлено текстовое поле ForZabbix со значением BusyQuery (чтобы Забикс мог легко определить 
				что есть долгоиграющие запросы!
				1.40 (03.03.2015) Совершенно новый метод для получения информации о блокировщиках (основан на новом скрипте). 
				Будет использован для сбора данных о блокировщиках в новую схему awr!
				2.00 (20.05.2015) Совершенно новый метод для получения информации об активных запросах. Теперь результат подобен процедуре sp_whoisactive.
				Также как и sp_whoisactive выводит информацию о НЕактивных запросах (status=sleeping), у которых Open_Tran>0.
				Будет использован для сбора данных об активных запросах в новую схему awr!
				2.10 (25.05.2015) Для метода получения информации по блокировщикам реализован новый метод сбора!
				Теперь этот сбор полностью аналогичен сбору об активных запросах. Причем Текст запроса сохраняются также в отдельную таблицу!
				Чтобы выполнить сбор информации по блокировщикам нужно задать параметры @blocks и @collect_sql.
				2.11 (18.06.2015) Доработана информация по блокировщикам: добавлено новое поле в результаты - trn_iso_lvl - параметр уровень изоляции транзакции!
				2.12 (24.06.2015) Алгоритм сбора информации "Медленные запросы" для Заббикса перенесен из начала модуля в новый алгоритм сбора по запросам.
				Также добавлено исключение для служебного диагностического запроса в AlwaysOn: sp_server_diagnostics
				2.15 (19.07.2015) Добавлен сбор информации о текущей выполняемой инструкции запроса с помощью полей statement_start_offset и statement_end_offset.
				Теперь собирается и сохраняется информация о всем пакете запросов и о конкретном текущем запросе (как в sp_whoisactive).
				Если sp_who3 выполняется в оперативном режиме (без сбора) то сразу высчитывается текущая инструкция в столбце [Exec_Statement].
				2.17 (19.07.2015) Добавлен сбор информации об использовании tempdb запросами. Это 4 новых столбца:
				tempdb_current_query - используемый объем страниц для текущего запроса, tempdb_allocation_query - весь объем страниц выделененный для текущего запроса (без учета освобожденных страниц),
				tempdb_current_session - используемый объем страниц для текущего сеанса, tempdb_allocation_session - весь объем страниц выделененный для текущего сеанса (без учета освобожденных страниц).
				2.18 (04.08.2015) Добавлено исключение для диагностического запроса AlwaysON от Windows-Кластера SP_SERVER_DIAGNOSTICS
				2.20 (24.08.2015) Добавлен новый параметр @get_count - в результате возвращает 1 столбец - кол-во найденных строк (блокировок или активных сессий).
				2.21 (26.08.2015) Небольшое исправление в исключении диагностического запроса AlwaysOn.
				2.215 (01.12.2017) Исправление в алгоритме выборки для zabbix - теперь разный порог для разных сервисов.
				2.221 (15.02.2018) Изменён алгоритм для получения SQLHandle - теперь получаем "SQLHashID", алгоритм следующий: берём HASH от начала и от конца текста запроса(из-за ограничений входного параметра для функции HASHBYTES) + берём длину текста запроса. И всё это хэшируем. Это оптимизация хранения текстов запросов: когда для 1-го текста запроса получаем несколько разных sqlhandle.
				2.222 (23.03.2018) Добавлен новый параметр @only_sleep_tran - если включен, то будет отобраны только зависшие сессии с открытыми транзакциями. По умолчанию выключен.
				2.251 (17.05.2021) fix for datediff error: The datediff function resulted in an overflow																																		 																							
-- ============================================= */
CREATE PROCEDURE info.sp_who3 
	@busy_minutes int = NULL, @blocks bit = 0 , @zabbix bit = 0,
	@collect_sql bit = 0, @get_count bit = 0, @only_sleep_tran bit=0
AS
BEGIN
	SET NOCOUNT ON;
	declare @time datetime;
						 
																						
	/*
		Старый алгоритм сбора для Заббикса "Медленные запросы"!
	*/
	--set @time=getdate();
	--if @zabbix=1
	--begin
	--	select 'BusyQuery' as [ForZabbix], S.login_name, Q.SPID, Q.command, Q.wait_type, datediff(hour,start_time,@time) as RunningHours
	--	From
	--	(
	--		select session_id as SPID,DB_NAME(database_id) as DB, start_time,status,command,blocking_session_id,
	--			wait_type,wait_time,last_wait_type,wait_resource,
	--			cpu_time,reads,writes,logical_reads,row_count,query_hash
	--		from sys.dm_exec_requests
	--		where status<>'background' 
	--		and command<>'TASK MANAGER'
	--		and datediff(hour,start_time,@time) >= @busy_hours
	--	)Q
	--	inner join sys.dm_exec_sessions S
	--		on Q.SPID=S.session_id
	--	order by start_time
	--end
	--else 
	if @blocks=1
	BEGIN
		/*
			Новый метод! Старый ниже закомментирован!
			Быстрый сбор данных о заблокированных: кто и кем заблокирован!
			Работает на основе нескольких DMV.
			Выдаёт данные только если есть заблокированные
		*/
		if object_id('tempdb..#blk_collect') is not null
			drop table #blk_collect;
		SELECT 
			blk_rs.session_id as spid, blk_rs.command, blk_rs.[status], blk_rs.start_time, blk_rs.blocking_session_id as BlkBy, blk_rs.wait_type, blk_rs.wait_time, blk_rs.wait_resource, 
			blk_rs.[TEXT] as [SQLTEXT], 
			HASHBYTES('SHA2_256',(HASHBYTES('SHA2_256',LEFT(blk_rs.[TEXT],4000))+HASHBYTES('SHA2_256',RIGHT(blk_rs.[TEXT],4000))+cast(LEN(blk_rs.[TEXT]) as varbinary(30)))) AS [SQLHandle],
			blk_rs.DB, dm_ss.host_name as Host, dm_ss.login_name as [Login], dm_ss.program_name as Program, dm_ss.host_process_id as Host_pid,
			CASE 
				WHEN blk_rs.transaction_isolation_level IS NULL then dm_ss.transaction_isolation_level
				WHEN blk_rs.transaction_isolation_level = 0 then dm_ss.transaction_isolation_level
				ELSE blk_rs.transaction_isolation_level 
			END AS trn_iso_lvl,
			statement_start, statement_end
		INTO #blk_collect
		FROM
		(
			--Заблокированные (ожидающие)
			SELECT session_id, command, [status], start_time, blocking_session_id, wait_type, wait_time, wait_resource, DB_NAME(database_id) DB, t.TEXT, sql_handle as SQLHandle, transaction_isolation_level,
				statement_start_offset as statement_start, statement_end_offset as statement_end
			FROM sys.dm_exec_requests 
			CROSS apply sys.dm_exec_sql_text(sql_handle) AS t
			WHERE blocking_session_id > 0
			UNION
			--Блокировщики (активные)
			SELECT session_id, command, [status], start_time, NULL, wait_type, wait_time, wait_resource, DB_NAME(database_id) DB, t.TEXT, sql_handle as SQLHandle, transaction_isolation_level,
				statement_start_offset as statement_start, statement_end_offset as statement_end
			FROM sys.dm_exec_requests 
			CROSS apply sys.dm_exec_sql_text(sql_handle) AS t
			WHERE session_id IN (SELECT blocking_session_id 
								FROM sys.dm_exec_requests 
								WHERE blocking_session_id > 0)
					AND blocking_session_id=0
					AND (@only_sleep_tran=0)
			UNION
			--Блокировщики (зависшие)
			SELECT session_id, NULL, 'sleeping', NULL, NULL, NULL, NULL, NULL, NULL as DB, t.TEXT, most_recent_sql_handle as SQLHandle, NULL as transaction_isolation_level,
				NULL as statement_start, NULL as statement_end
			FROM sys.dm_exec_connections 
			CROSS apply sys.dm_exec_sql_text(most_recent_sql_handle) AS t
			WHERE session_id IN (SELECT blocking_session_id 
								FROM sys.dm_exec_requests 
								WHERE blocking_session_id not in (select session_id from sys.dm_exec_requests))
		) blk_rs
		LEFT JOIN sys.dm_exec_sessions as dm_ss
			ON blk_rs.session_id=dm_ss.session_id;

		IF (@collect_sql=0)
		BEGIN
			IF (@get_count=0)
				SELECT spid, command, [status], start_time, BlkBy, wait_type, wait_time, wait_resource, [SQLTEXT],
					CASE WHEN [statement_start] > 0 THEN 
						CASE [statement_end] 
							WHEN -1 THEN SUBSTRING([SQLTEXT], ([statement_start]/2) + 1, 2147483647)
							ELSE SUBSTRING([SQLTEXT], ([statement_start]/2) + 1, ([statement_end] - [statement_start])/2)  
						END 
					ELSE CASE [statement_end] 
							WHEN -1 THEN RTRIM(LTRIM([SQLTEXT])) 
							ELSE LEFT([SQLTEXT], ([statement_end]/2) +1) 
						END 
					END AS [Exec_Statement],			
					/*[SQLHandle],*/	DB, Host, [Login], Program, Host_pid, trn_iso_lvl
				FROM #blk_collect;
			ELSE
				SELECT Count_Big(*) as cnt
				FROM #blk_collect
				WHERE BlkBy is Not null and BlkBy<>0;

		END	
		ELSE
		BEGIN
			SET @time=GETDATE();
			/* Сбор данных об блокировщиках в две таблицы sputnik.awr.blk_handle_collect и sputnik.awr.sql_text_collect
				blk_handle_collect - содержит всю информацию о блокировщиках и блокируемых, кроме текста запроса.
				sql_text_collect - содержит текст запроса. */
			IF OBJECT_ID('sputnik.awr.blk_handle_collect') IS NOT NULL AND OBJECT_ID('sputnik.awr.sql_text_collect') IS NOT NULL 
			BEGIN
				SET XACT_ABORT ON;
				BEGIN TRAN
					INSERT INTO [awr].[blk_handle_collect]
						(
							[tt], SPID, command, [status], start_time, BlkBy, wait_type, wait_time, wait_resource, [SQLHandle],	DB, Host, [Login], Program, Host_pid, trn_iso_lvl,
							[statement_start],[statement_end]		
						)
					SELECT 
						@time as [tt], SPID, command, [status], start_time, BlkBy, wait_type, wait_time, wait_resource, 
							[SQLHandle], 
						DB, Host, [Login], Program, Host_pid, trn_iso_lvl,
						[statement_start],[statement_end]
					FROM #blk_collect;
					
					MERGE 
						[awr].[sql_text_collect] as target_table
						USING (
								SELECT  DISTINCT @time,[SQLHandle], [SQLText], COUNT(*) OVER (PARTITION BY [SQLHandle])
								FROM	#blk_collect
						) AS source_table ([tt],[SQLHandle], [SQLText], [NumLocks])
						ON (target_table.[SQLHandle]=source_table.[SQLHandle])
						WHEN NOT MATCHED THEN
							INSERT([tt],[SQLHandle], [SQLText], [NumLocks])
							VALUES(source_table.[tt],source_table.[SQLHandle],source_table.[SQLText],source_table.[NumLocks])
						WHEN MATCHED THEN
							UPDATE 
								SET target_table.[tt] = source_table.[tt],
									target_table.[NumLocks] += source_table.[NumLocks]
					;
				COMMIT;
				SET XACT_ABORT OFF;
			END
			ELSE
				PRINT('Сбор данных невозможен: таблица awr.blk_handle_collect или таблица awr.sql_text_collect не определена!');
		END
		
		--Старый метод:
		--select 
		--	BS.host_name as BlockHost, BS.login_name as BlockLogin,Q.blocking_session_id as SPID_Block, BInfo.start_time as BlockStart,
		--	BInfo.status as BlockStatus, BInfo.command as BlockCommand,
		--	S.host_name as WaitHost,S.login_name as WaitLogin,Q.session_id as WaitSPID,DB_NAME(Q.database_id) as DB,
		--	Q.start_time as WaitStart, Q.status as WaitStatus, Q.command as WaitCommand

		--from sys.dm_exec_requests Q
		--inner join sys.dm_exec_sessions S
		--	on Q.session_id=S.session_id
		--left join sys.dm_exec_requests BInfo
		--	on Q.blocking_session_id=BInfo.session_id
		--left join sys.dm_exec_sessions BS
		--	on BInfo.session_id=BS.session_id							
		--where
		--	Q.blocking_session_id<>0
	END
	else
	BEGIN
		/*-------Старый Метод (до версии 2.00)
		select getdate() as ServerTime,S.host_name,S.login_name,S.login_time,Q.*
		From
		(
			select session_id as SPID,DB_NAME(database_id) as DB, start_time,status,command,blocking_session_id,
				wait_type,wait_time,last_wait_type,wait_resource,
				cpu_time,reads,writes,logical_reads,row_count,query_hash
			from sys.dm_exec_requests
			where status<>'sleeping' 
			and status<>'background' 
			and datediff(minute,start_time,@time) >= @busy_minutes
		)Q
		inner join sys.dm_exec_sessions S
			on Q.SPID=S.session_id
		order by start_time
		*/
		
		/* Новый метод (начиная с версии 2.00)! Результаты похожи на sp_whoisactive 
			Будет работать только на SQL Server 2012 и старше (т.к. используется новые возможности T-SQL).	
		*/
		if object_id('tempdb..#qinfo_collect') is not null
			drop table #qinfo_collect;

		  
						  
		select  S.host_name as [Host],S.program_name as Program,
			IIF(S.login_name='',(select name from sys.server_principals where [sid]=S.Security_id),S.login_name) as [Login],
				  
			S.login_time,  S.open_transaction_count as Open_Tran,
		Q.*
		into #qinfo_collect
		From
		(
			select q.session_id as SPID,DB_NAME(q.database_id) as DB, start_time,status,
				command,blocking_session_id as blk_by, percent_complete as perc_complete, 
				wait_type,wait_time,wait_resource,
				cast(cast(total_elapsed_time as decimal(15,3))/1000.000 as decimal(15,3)) as RunTime_sec,
				cast(cast(CPU_Time as decimal(14,2))/1000.000 as decimal(15,3)) as CPU_sec,
				cast(cast(granted_query_memory as decimal(14,2))/128.00 as decimal(14,2)) as MemoryMb,reads as IO_Reads,writes as IO_Writes,Logical_Reads,row_count as [RowCount],
				st.text as SQLText, 
				HASHBYTES('SHA2_256',(HASHBYTES('SHA2_256',LEFT(st.[TEXT],4000))+HASHBYTES('SHA2_256',RIGHT(st.[TEXT],4000))+cast(LEN(st.[TEXT]) as varbinary(30)))) AS [SQLHandle], 
				query_hash as SQLHash,
				q.statement_start_offset as [statement_start] , q.statement_end_offset as [statement_end],
				SUM(COALESCE(tsu.user_objects_alloc_page_count,0)+COALESCE(tsu.internal_objects_alloc_page_count,0)-COALESCE(tsu.user_objects_dealloc_page_count,0)-COALESCE(tsu.internal_objects_dealloc_page_count,0)) over (partition by tsu.session_id,tsu.request_id) as tempdb_current_query,
				SUM(COALESCE(tsu.user_objects_alloc_page_count,0)+COALESCE(tsu.internal_objects_alloc_page_count,0)) over (partition by tsu.session_id,tsu.request_id) as tempdb_allocation_query,
				SUM(COALESCE(ssu.user_objects_alloc_page_count,0)+COALESCE(ssu.internal_objects_alloc_page_count,0)-COALESCE(ssu.user_objects_dealloc_page_count,0)-COALESCE(ssu.internal_objects_dealloc_page_count,0)) over (partition by ssu.session_id) as tempdb_current_session,
				SUM(COALESCE(ssu.user_objects_alloc_page_count,0)+COALESCE(ssu.internal_objects_alloc_page_count,0)) over (partition by ssu.session_id) as tempdb_allocation_session
			from sys.dm_exec_requests q
			CROSS APPLY sys.dm_exec_sql_text(q.sql_handle) AS st
			left join sys.dm_db_task_space_usage tsu
				on q.request_id=tsu.request_id and q.session_id=tsu.session_id
			left join sys.dm_db_session_space_usage ssu
				on q.session_id=ssu.session_id
			where [status] NOT IN('background'/*,'sleeping'*/)
			and q.session_id<>@@SPID 
		)Q
		inner join sys.dm_exec_sessions S on Q.SPID=S.session_id
		WHERE
			--Исключаем диагностический запрос для AlwaysOn от Windows-Кластера
			(S.program_name <> 'Microsoft® Windows® Operating System' AND Q.wait_type<>'SP_SERVER_DIAGNOSTICS_SLEEP' OR Q.wait_type IS NULL OR S.program_name IS NULL)
			AND (@only_sleep_tran=0)
		UNION
		select s.host_name as [Host], s.program_name as [Program],  
			IIF(S.login_name='',(select name from sys.server_principals where [sid]=S.Security_id),S.login_name) as [Login], 
			s.login_time, s.open_transaction_count as Open_Tran,
			s.session_id as SPID, DB_NAME(s.database_id) as DB, s.last_request_start_time as start_time,  s.[status],  
			NULL as command, NULL as blk_by, NULL as perc_complete, 
			'BUSY_SLEEPING_TASK' as wait_type, NULL as wait_time, 'Open_Tran>0' wait_resource, 
			cast(DATEDIFF(SECOND,s.last_request_start_time,s.last_request_end_time) as decimal(15,3)) as RunTime_sec, 
			cast(cast(s.CPU_Time as decimal(14,2))/1000.000 as decimal(15,3)) as CPU_sec,
			cast(cast(s.memory_usage as decimal(14,2))/128.00 as decimal(14,2)) as MemoryMb, s.reads as IO_Reads,s.writes as IO_Writes,s.Logical_Reads,s.row_count as [RowCount],
																		 
			sqltext.text as SQLText,
			HASHBYTES('SHA2_256',(HASHBYTES('SHA2_256',LEFT(sqltext.[TEXT],4000))+HASHBYTES('SHA2_256',RIGHT(sqltext.[TEXT],4000))+cast(LEN(sqltext.[TEXT]) as varbinary(30)))) AS [SQLHandle],
			NULL as SQLHash,
			NULL as [statement_start] , NULL as [statement_end],
			NULL as tempdb_current_query, NULL as tempdb_allocation_query,
			SUM(COALESCE(ssu.user_objects_alloc_page_count,0)+COALESCE(ssu.internal_objects_alloc_page_count,0)-COALESCE(ssu.user_objects_dealloc_page_count,0)-COALESCE(ssu.internal_objects_dealloc_page_count,0)) over (partition by ssu.session_id) as tempdb_current_session,
			SUM(COALESCE(ssu.user_objects_alloc_page_count,0)+COALESCE(ssu.internal_objects_alloc_page_count,0)) over (partition by ssu.session_id) as tempdb_allocation_session
		from sys.dm_exec_sessions s
		inner join sys.dm_exec_connections c on c.session_id=s.session_id
		cross apply sys.dm_exec_sql_text(c.most_recent_sql_handle) as sqltext
		left join sys.dm_db_session_space_usage ssu
			on s.session_id=ssu.session_id
		where (s.open_transaction_count>0 and s.[status]='sleeping');

		SET @time=GETDATE();
		IF @zabbix=1
		BEGIN
			/*
				Новый алгоритм сбора для Заббикса "Медленные запросы"!
			*/
			DECLARE
				@MachineName varchar(300)=CAST(SERVERPROPERTY('MachineName') as varchar(128)),
				@InstanceName varchar(300)=COALESCE(CAST(SERVERPROPERTY('InstanceName') as varchar(128)),''),
				@SQLServer varchar(300),
				@busy_minutes_zabbix smallint;;
			SET @SQLServer=LOWER(@MachineName+CASE WHEN @InstanceName > '' THEN '\' ELSE '' END + @InstanceName);
			SELECT @busy_minutes_zabbix=CASE
				WHEN CHARINDEX('-bi-',@SQLServer)>0 THEN 8*60
				WHEN EXISTS (select top 1 database_id from sys.databases where name IN ('pegasus2008ms','pegasus2008bb')) THEN 1*60
				WHEN (EXISTS (select top 1 database_id from sys.databases where name like 'pegasus2008%') AND CHARINDEX('dev',@SQLServer)=0 AND CHARINDEX('test',@SQLServer)=0 AND CHARINDEX('qc',@SQLServer)=0) THEN 2*60
				ELSE 4*60
			END;
			--SELECT 'BusyQuery' as ForZabbix, [Login] as login_name, SPID, command, wait_type, datediff(hour,start_time,@time) as RunningHours
			SELECT TOP 1 CASE WHEN Count_Big(*)>=1 THEN 'BusyQuery' ELSE 'Its OK' END as ForZabbix, Count_Big(*) as cnt, sputnik.info.uf_FormatTime(SUM(datediff(second,start_time,@time))) as SumDuration, sputnik.info.uf_FormatTime(MAX(datediff(second,start_time,@time))) as MaxDuration
			FROM #qinfo_collect
			WHERE datediff(minute,start_time,@time) >= @busy_minutes_zabbix
				and (wait_type<>'SP_SERVER_DIAGNOSTICS_SLEEP' OR wait_type IS NULL)
		END			
		ELSE IF @collect_sql=0
			IF @get_count=0
				SELECT 
					/*@time as [tt],*/ SPID, SQLText,
					CASE WHEN [statement_start] > 0 THEN 
						CASE [statement_end] 
							WHEN -1 THEN SUBSTRING([SQLTEXT], ([statement_start]/2) + 1, 2147483647)
							ELSE SUBSTRING([SQLTEXT], ([statement_start]/2) + 1, ([statement_end] - [statement_start])/2)  
						END 
					ELSE CASE [statement_end] 
							WHEN -1 THEN RTRIM(LTRIM([SQLTEXT])) 
							ELSE LEFT([SQLTEXT], ([statement_end]/2) +1) 
						END 
					END AS [Exec_Statement],
	--CAST('<?query --
	--'+SQLText+'
	----?>' as xml) as SQLText, 
					[Host], [Program],  [Login], login_time, [Open_Tran],
					DB, [start_time],  [status], command, blk_by, perc_complete, 
					wait_type, IIF(wait_type='BUSY_SLEEPING_TASK',cast(DATEDIFF(SECOND,[start_time],@time) as bigint)*1000,wait_time) as wait_time, wait_resource, 
					RunTime_sec, CPU_sec,MemoryMb, IO_Reads, IO_Writes, Logical_Reads, [RowCount],
					tempdb_current_query, tempdb_allocation_query, tempdb_current_session, tempdb_allocation_session
					/*, SQLHandle, SQLHash*/
				FROM #qinfo_collect
				WHERE ((wait_type='BUSY_SLEEPING_TASK' and DATEDIFF(SECOND,[start_time],@time)>=1) OR wait_type<>'BUSY_SLEEPING_TASK' OR wait_type IS NULL)
					AND (@busy_minutes IS NULL OR @busy_minutes=0 OR DATEDIFF(MINUTE,[start_time],@time)>=@busy_minutes)
											   
				ORDER BY [start_time];
			ELSE
				SELECT Count_Big(*) as cnt
				FROM #qinfo_collect
				WHERE ((wait_type='BUSY_SLEEPING_TASK' and DATEDIFF(SECOND,[start_time],@time)>=1) OR wait_type<>'BUSY_SLEEPING_TASK' OR wait_type IS NULL)
					AND (@busy_minutes IS NULL OR @busy_minutes=0 OR DATEDIFF(MINUTE,[start_time],@time)>=@busy_minutes);	
											   
	   
		ELSE 
		BEGIN
			/* Сбор данных об активных запросах в две таблицы sputnik.awr.sql_handle_collect и sputnik.awr.sql_text_collect
				sql_handle_collect - содержит всю информацию об активных запросах,кроме текста запроса.
				sql_text_collect - содержит текст запроса. */
			IF OBJECT_ID('sputnik.awr.sql_handle_collect') IS NOT NULL AND OBJECT_ID('sputnik.awr.sql_text_collect') IS NOT NULL 
			BEGIN
				SET XACT_ABORT ON;
				BEGIN TRAN
					INSERT INTO [awr].[sql_handle_collect]
						(
							[tt], [Host], [Program],  [Login], login_time, [Open_Tran],
							SPID, DB, [start_time],  [status], command, blk_by, perc_complete, 
							wait_type, wait_time, wait_resource, 
							RunTime_sec, CPU_sec,MemoryMb, IO_Reads, IO_Writes, Logical_Reads, [RowCount],
							[SQLHandle], [statement_start], [statement_end],
							tempdb_current_query, tempdb_allocation_query, tempdb_current_session, tempdb_allocation_session			
						)
					SELECT 
						@time as [tt], [Host], [Program],  [Login], login_time, [Open_Tran],
						SPID, DB, [start_time],  [status], command, blk_by, perc_complete, 
						wait_type, IIF(wait_type='BUSY_SLEEPING_TASK',cast(DATEDIFF(SECOND,[start_time],@time) as bigint)*1000,wait_time) as wait_time, wait_resource, 
						RunTime_sec, CPU_sec,MemoryMb, IO_Reads, IO_Writes, Logical_Reads, [RowCount],
						SQLHandle, [statement_start], [statement_end], 
						tempdb_current_query, tempdb_allocation_query, tempdb_current_session, tempdb_allocation_session
					FROM #qinfo_collect
					WHERE ((wait_type='BUSY_SLEEPING_TASK' and DATEDIFF(SECOND,[start_time],@time)>=1) OR wait_type<>'BUSY_SLEEPING_TASK' OR wait_type IS NULL);
					
					MERGE 
						[awr].[sql_text_collect] as target_table
						USING (
								SELECT  DISTINCT @time,[SQLHandle], [SQLText], COUNT(*) OVER (PARTITION BY [SQLHandle])
								FROM	#qinfo_collect
								WHERE ((wait_type='BUSY_SLEEPING_TASK' and DATEDIFF(SECOND,[start_time],@time)>=1) OR wait_type<>'BUSY_SLEEPING_TASK' OR wait_type IS NULL)
						) AS source_table ([tt],[SQLHandle], [SQLText], [NumRuns])
						ON (target_table.[SQLHandle]=source_table.[SQLHandle])
						WHEN NOT MATCHED THEN
							INSERT([tt],[SQLHandle], [SQLText], [NumRuns])
							VALUES(source_table.[tt],source_table.[SQLHandle],source_table.[SQLText],source_table.[NumRuns])
						WHEN MATCHED THEN
							UPDATE 
								SET target_table.[tt] = source_table.[tt],
									target_table.[NumRuns] += source_table.[NumRuns]
					;
				COMMIT;
				SET XACT_ABORT OFF;
			END
			ELSE
				PRINT('Сбор данных невозможен: таблица awr.sql_handle_collect или таблица awr.sql_text_collect не определена!');
		END
	END
END
GO
GRANT EXECUTE
    ON OBJECT::[info].[sp_who3] TO [zabbix]
    AS [dbo];

