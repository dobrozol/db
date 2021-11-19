	/* =============================================
	-- Author:		Андрей Иванов (sqland1c)
	-- Create date: 01.03.2015 (1.0)
	-- Description: Процедура для Интеллектуального пересчета статистик распределения (UPDATE STATISTICS) 
					по тем статистикам, которые не были пересчитаны при выполнении Реиндексации!
					Есть обязательный параметр - @DBName - имя БД для которой нужно запустить пересчет статистик.
					Параметр @RowLimit - необязательный - определяет кол-во статистик, которые нужно пересчитать за 1 вызов (по умолчанию 100).
					Эта процедура должна вызываться из другой процедуры usp_reindex_start сразу после выполнения Реиндексации!
	-- Update:		
					30.04.2015 (1.1)
					Добавлены новые параметры для более гибкой работы с процедурой. Новые параметры @old_days, @perc_threshold, @rows_threshold
					позволяют при вызове процедуры задать фильтры на отбор статистик. Параметр @obj_filter позволяет отфильтровать статистики
					по имени объекта (таблицы), причем можно использовать % и _ для поиска по шаблону строки.
					Ещё один новый параметр @only_show - позволяет только просмотреть статистики без пересчета.

					01.07.2015 (2.0)
					Новая оптимизированная схема Реиндексации. Теперь пересчет статистик запускается на основании окон обслуживания.

					02.12.2016 (2.102)
					Внесены изменения в алгоритм определения @policy_scan.
					А также добавлен алгоритм Логгирования - запись результатов выполнения в таблицу HS через процедуру usp_WriteHS!
					Также исправлен алгоритм выборки статистик - при расчёте perc_change добавлена защита от деления на 0.
					Также добавлена возможность обновления статистик по нескольким БД (параметр @DBName теперь может принять значения NULL).

					31.03.2017 (2.110)
					Добавлены новые параметры: @ModCntr_max,@ModCntr_min,@timeout_sec.

					12.04.2017 (2.112)
					Исправление в алгоритме выбора статистик (динамический sql в переменной @TSQL). В конец добавлена сортировка:
					ORDER BY mod_count DESC, [perc_change] DESC - чтобы всегда выбирать наименее актуальные статистики.

					02.06.2017	(2.120)
					Добавлен механизм исключений (таблица TabList_US и функция uf_CheckTabList_US).
					Также при выборе Policy_Scan теперь учитывается кол-во ядер CPU, доступных SQL - если менее 12 ядер,
					то Policy_Scan менее трудозатратный.

					18.07.2017	(2.142)
					ПАТЧ 01 - исправление в механизме исключений. Теперь в любом случае сначала будет
					проверка исключения (даже если задан @policy_scan), а потом уже определение @policy_scan. 
					ПАТЧ 02 - исправление при учёте кол-ва ядер CPU. Теперь три варианта: до 5 ядер, от 6 и до 11 ядер, и более 12 ядер.
					ИЗМЕНЕНИЕ 01 - теперь policy_scan учитывается не по числу строк, а по размеру таблицы!
					ИЗМЕНЕНИЕ 02 - добавлены два новых параметра @filter_DataUsedMb_min и @filter_DataUsedMb_max - 
					для фильтрации по размеру таблиц!

					29.12.2017	(2.147)
					Увеличен размер для всех строковых переменных.
					Небольшое изменение алгоритма выбора имени БД - чтобы в обработку попадали БД с кириллицей в названии.
				
					14.11.2018 	(2.160)
					Добавлена совместимость с 2008 (iif заменены на case).

					08.02.2019	(2.165)
					Изменены условия отбора. Теперь будут попадать статистики с незаполненными (NULL) значениями.

					19.11.2021  (2.200)
					added managed locks for multithreading, refactoring, fixing.
	-- ============================================= */
	CREATE PROCEDURE db_maintenance.usp_RecomputeStats
		@DBName nvarchar(2000)=NULL,
		@UniqueName_SL nvarchar(200)=NULL,
		@RowLimit smallint=10,
		@delayperiod char(12)='00:00:00.200',
		@filter_rows_min int=NULL,
		@filter_rows_max int=NULL,
		@filter_DataUsedMb_min numeric(9,1)=NULL,
		@filter_DataUsedMb_max numeric(9,1)=NULL,
		@filter_perc_min decimal(18,2)=15.00,
		@filter_perc_max decimal(18,2)=NULL,
		@filter_old_hours tinyint=24,
		@policy_scan varchar(100)=NULL,
		@PauseMirroring bit=0,
		@DeadLck_PR smallint=0,
		@Lck_Timeout int=20000,
		@obj_filter nvarchar(2000) = null,
		@only_show bit = 0,
		@ModCntr_max bigint=10000000, --Если кол-во измененных строк больше этого, будем обновлять!
		@ModCntr_min int=10, --Защита от маленьких таблиц. Если кол-во измененных строк меньше этого - пропускаем!
		@timeout_sec int = NULL,		--Ограничение времени выполнения в данной процедуре в сек. Если NULL (или 0) - бесконечно.
		@analyze_mode smallint = 0 --Вывод информации в аналитическом виде.
	as
	begin
		set nocount on;
		SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
		DECLARE @tt_start datetime2(2), @StrErr NVARCHAR(MAX);
		declare @tt_start_usp datetime2(2), @time_elapsed_sec int, @i_cnt bigint=0;
		DECLARE @TSQL NVARCHAR(MAX),@obj_filter_str nvarchar(2000), @StopList_str NVARCHAR(MAX);
		set @tt_start_usp=CAST(SYSDATETIME() AS datetime2(2));
		--Формируем список исключений таблиц, индексы для этих таблиц не будут обслужены в текущем запуске.
		select @StopList_str=StopList_str from sputnik.db_maintenance.StopLists where UniqueName=@UniqueName_SL;
		select @StopList_str=COALESCE(@StopList_str,'');

		IF OBJECT_ID('tempdb.dbo.#T_ST') IS NOT NULL
			DROP TABLE #T_ST;
		CREATE TABLE #T_ST (db NVARCHAR(2000), shm NVARCHAR(2000), obj NVARCHAR(2000), stat NVARCHAR(2000), obj_id int, stat_id int, last_upd datetime2(2), mod_count bigint, perc_change decimal(18,2), row_count bigint,DataUsed_Mb decimal(9,1));

		/*	Отбор БД для обслуживания */
		declare @DB_current nvarchar(2000);
		if OBJECT_ID('tempdb..#src_ag_db') IS NOT NULL
			DROP TABLE #src_ag_db;
		CREATE TABLE #src_ag_db (DB nvarchar(2000), [db_id] int, [Role] nvarchar(800), [PartnerReplica] nvarchar(800), [PrimaryReplica] nvarchar(800), sync_state nvarchar(800), health nvarchar(800), DB_State nvarchar(800));
		IF CAST (LEFT (CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(2)),2) AS SMALLINT) >= 11
			--Информация о вторичных репликах AlwaysON AG на текущем сервере:
			INSERT INTO #src_ag_db (DB, [db_id], [Role], [PartnerReplica], [PrimaryReplica], sync_state, health, DB_State)
				SELECT
					DB_NAME(ag_db.database_id) as DB,
					ag_db.database_id as [db_id],
					ISNULL(arstates.role_desc, '') AS [Role],
					ISNULL(AR.replica_server_name, '') as [PartnerReplica],
					ISNULL(agstates.primary_replica, '') AS [PrimaryReplica],
					ag_db.synchronization_state_desc as sync_state,
					ag_db.synchronization_health_desc as health,
					ag_db.database_state_desc as DB_State 
				FROM sys.dm_hadr_database_replica_states ag_db
				LEFT JOIN sys.dm_hadr_availability_group_states as agstates
					on ag_db.group_id=agstates.group_id	
				LEFT JOIN sys.dm_hadr_availability_replica_states AS arstates
					ON ag_db.replica_id = arstates.replica_id
						and ag_db.group_id=arstates.group_id
				LEFT JOIN sys.availability_replicas as AR
					ON ag_db.replica_id=AR.replica_id
						and ag_db.group_id=AR.group_id
				WHERE ag_db.is_local=1 
					AND ISNULL(arstates.role_desc, '') = 'SECONDARY'
		DECLARE DB CURSOR FOR
		select name as DB
		from sys.databases sdb
		left join #src_ag_db adb ON sdb.database_id=adb.[db_id]
		where 
			(name = @DBName or @DBName is null or @DBName='') 
			and state_desc='ONLINE'
			and database_id>4
			and is_read_only=0
			and adb.db is null;
		OPEN DB;
		FETCH NEXT FROM DB INTO @DB_current;
		WHILE @@FETCH_STATUS=0
		BEGIN
			SET @obj_filter_str=COALESCE(@obj_filter,'');
			SET @TSQL=N'USE ['+@DB_current+N'];
			INSERT INTO #T_ST
			SELECT TOP ('+CONVERT(NVARCHAR(10),@RowLimit)+N')
					CAST(DB_NAME() AS NVARCHAR(2000)) as [db], SCHEMA_NAME(objs.schema_id) as shm, objs.name as obj, stat.name as stat, objs.object_id as obj_id, stat.stats_id as stat_id,
					CAST(sp.last_updated as datetime2(2)) as last_upd, sp.modification_counter as mod_count,
					CAST(100 * CAST([sp].[modification_counter] AS DECIMAL(18,2)) / CAST(case when [sp].[rows]=0 then 1 else [sp].[rows] end AS DECIMAL(18,2)) AS DECIMAL(18,2)) AS [perc_change], prts.rows as row_count, prts.DataUsed_Mb
			FROM sys.stats stat
			INNER JOIN sys.objects objs ON stat.object_id=objs.object_id
			INNER JOIN 
				(
					SELECT
						p.object_id,
						SUM(p.row_count) AS rows, 
						CAST(SUM(p.[used_page_count])/128.0 as numeric(9,1)) AS DataUsed_Mb
					FROM sys.dm_db_partition_stats p
					WHERE index_id IN (0,1)
					GROUP BY p.object_id
				) prts
				 ON objs.object_id=prts.object_id
			CROSS APPLY sys.dm_db_stats_properties(stat.object_id, stat.stats_id) AS sp
			WHERE 
				(
					(CAST(100 * CAST([sp].[modification_counter] AS DECIMAL(18,2)) / CAST(case when [sp].[rows]=0 then 1 else [sp].[rows] end AS DECIMAL(18,2)) AS DECIMAL(18,2)) >= '+CONVERT(VARCHAR(18),@filter_perc_min)+'
					AND ('+COALESCE(CONVERT(VARCHAR(20),@filter_perc_max),'NULL')+' IS NULL OR CAST(100 * CAST([sp].[modification_counter] AS DECIMAL(18,2)) / CAST(case when [sp].[rows]=0 then 1 else [sp].[rows] end AS DECIMAL(18,2)) AS DECIMAL(18,2)) <= '+COALESCE(CONVERT(VARCHAR(20),@filter_perc_max),'NULL')+'))
					OR [sp].[modification_counter]>'+COALESCE(CONVERT(VARCHAR(30),@ModCntr_max),'1000000')+' OR [sp].[modification_counter] is null
				)
				AND ([sp].[modification_counter]>'+COALESCE(CONVERT(VARCHAR(30),@ModCntr_min),'10')+' OR [sp].[modification_counter] is null)
				AND (last_updated<DATEADD(HOUR,-'+CONVERT(VARCHAR(5),@filter_old_hours)+',getdate()) or last_updated is NULL)
				AND ('+COALESCE(CONVERT(VARCHAR(20),@filter_rows_min),'NULL')+' IS NULL OR prts.rows>='+COALESCE(CONVERT(VARCHAR(20),@filter_rows_min),'NULL')+')
				AND ('+COALESCE(CONVERT(VARCHAR(20),@filter_rows_max),'NULL')+' IS NULL OR prts.rows<='+COALESCE(CONVERT(VARCHAR(20),@filter_rows_max),'NULL')+')
				AND ('+COALESCE(CONVERT(VARCHAR(20),@filter_DataUsedMb_min),'NULL')+' IS NULL OR prts.DataUsed_Mb>='+COALESCE(CONVERT(VARCHAR(20),@filter_DataUsedMb_min),'NULL')+')
				AND ('+COALESCE(CONVERT(VARCHAR(20),@filter_DataUsedMb_max),'NULL')+' IS NULL OR prts.DataUsed_Mb<='+COALESCE(CONVERT(VARCHAR(20),@filter_DataUsedMb_max),'NULL')+')
				AND ('''+@obj_filter_str+'''='''' OR objs.name LIKE '''+@obj_filter_str+''')
				AND CHARINDEX(objs.name+'';'','''+@StopList_str+''')=0
			ORDER BY mod_count DESC, [perc_change] DESC
			;';

			--PRINT(@TSQL);
			EXEC(@TSQL);
			FETCH NEXT FROM DB INTO @DB_current;
		END
		CLOSE DB;
		DEALLOCATE DB;

		--Создадим индекс по mod_count и perc_change!
		--Для отбора по кол-ву изменённых строк+по % изменений!
		CREATE CLUSTERED INDEX tmp_cix_mod01 ON #T_ST
		(
			mod_count DESC,
			perc_change DESC
		) ;

		IF @only_show=1
		BEGIN
			IF @analyze_mode=0 
				SELECT TOP (@RowLimit) * FROM #T_ST ORDER BY mod_count DESC, perc_change DESC;
			IF @analyze_mode=1
				SELECT DISTINCT 
					[db], [shm], 
					COUNT_BIG(*) over (partition by [db], [shm]) as cnt,
					SUM(mod_count)  over (partition by [db], [shm]) as sum_mod_cnt,
					SUM(row_count)  over (partition by [db], [shm]) as sum_row_count,
					CAST(100 * CAST(SUM(mod_count)  over (partition by [db], [shm]) AS DECIMAL(19,0)) / CAST( case when SUM(row_count)  over (partition by [db], [shm])=0 then 1 else SUM(row_count)  over (partition by [db], [shm]) end AS DECIMAL(19,0)) AS DECIMAL(19,0)) AS [perc_change],
					SUM(DataUsed_Mb)  over (partition by [db], [shm]) as sum_DataUsed_Mb,
					MIN(last_upd) over  (partition by [db], [shm]) as old_upd
				FROM #T_ST
			IF @analyze_mode=2
			BEGIN
				;WITH cte_src01 as(
					SELECT COUNT_BIG(DISTINCT [db]) as db_cnt FROM #T_ST
				)
				SELECT TOP 1 
					(SELECT db_cnt FROM cte_src01) as db_cnt,
					COUNT_BIG(*) over () as cnt,
					SUM(mod_count)  over () as sum_mod_cnt,
					SUM(row_count)  over () as sum_row_count,
					CAST(100 * CAST(SUM(mod_count)  over () AS DECIMAL(19,0)) / CAST(case when SUM(row_count)  over ()=0 then 1 else SUM(row_count)  over () end AS DECIMAL(19,0)) AS DECIMAL(19,0)) AS [perc_change],
					SUM(DataUsed_Mb)  over () as sum_DataUsed_Mb,
					MIN(last_upd) over () as old_upd
				FROM #T_ST
			END
		END
		ELSE
		BEGIN		 
			DECLARE @commant_text_log NVARCHAR(MAX),@HandleCmd NVARCHAR(2000), @Cmd_handle_log NVARCHAR(MAX), @UpdCmd NVARCHAR(MAX), @shm_id int, @obj_id int, @stat_id int, @db_id int, @flag_fail bit;
			DECLARE CST CURSOR FOR
			SELECT TOP(@RowLimit)
				N'SET xact_abort ON;
SET DEADLOCK_PRIORITY '+CAST(@DeadLck_PR as nvarchar(5))+N';
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT '+CAST(@Lck_Timeout as nvarchar(25))+N';
USE ['+DB+N'];
' as Cmd_Handle,
				--Заголовок запроса для логгирования:
				tsql_handle_log=N'--spid='+cast(@@spid as varchar(5))+';--dlck_pr='+CAST(@DeadLck_PR as nvarchar(2))+N';tr_iso_lvl=1;lck_tmt='+CAST(@Lck_Timeout as nvarchar(12))+N';
',
				c.resultCommand as Cmd,
				obj_id, stat_id, DB_ID(DB)
			FROM #T_ST
			outer apply [db_maintenance].[uf_getRecomputePolicyScan](DataUsed_Mb) p
			outer apply [db_maintenance].[uf_addAppLockCommand](
				DB, shm, obj,
				CONCAT('UPDATE STATISTICS ', QUOTENAME(shm), '.', QUOTENAME(obj), ' ', QUOTENAME(stat), ' ',
					COALESCE(
						--First: getting policy scan from exception table
						db_maintenance.uf_CheckTabList_US(QUOTENAME(obj),QUOTENAME(stat)),
						--Second: getting policy scan from config
						@policy_scan,
						--Third: getting policy scan based on the current size of the index
						p.policyScan,
						--And last: constant
						N'WITH SAMPLE 100000 ROWS'
					)
				),
				default
			)c
			ORDER BY mod_count DESC, perc_change DESC;
		
			OPEN CST;
			FETCH NEXT FROM CST INTO @HandleCmd, @Cmd_handle_log, @UpdCmd, @obj_id, @stat_id, @db_id;
			WHILE @@FETCH_STATUS=0
			BEGIN
				--Проверяем TimeOut, если время вышло - пишем в лог HS и выходим!
				IF @timeout_sec is not null AND @timeout_sec>0
				BEGIN
					set @time_elapsed_sec=DATEDIFF(second,@tt_start_usp, CAST(SYSDATETIME() AS datetime2(2)));
					IF @time_elapsed_sec>@timeout_sec
					BEGIN
						set @commant_text_log=N'Достигнут TimeOut в usp_RecomputeStats. @TimeOut_sec='+cast(@TimeOut_sec as nvarchar(30))+N'; @time_elapsed_sec='+cast(@time_elapsed_sec as nvarchar(30));
						--Логгируем в историю Обслуживания БД:
						EXEC sputnik.db_maintenance.usp_WriteHS 
							@DB_ID=@db_id,
							@Command_Type=101, --101-TimeOut for Update Statistics (usp_RecomputeStats)
							@Command_Text_1000=@commant_text_log,
							@tt_start=@tt_start_usp,
							@Status=0; --0-Success
						BREAK; --выход из текущего цикла.
					END
				END

				set @flag_fail=0;
				set @StrErr=NULL;
				BEGIN TRY
					--PRINT(@HandleCmd);
					set @tt_start=CAST(SYSDATETIME() AS datetime2(2));
					EXEC(@HandleCmd+@UpdCmd);

				END TRY
				BEGIN CATCH
					set @flag_fail=1;
					set @StrErr=/*'Ошибка при пересчёте статистик распределения через процедуру [usp_RecomputeStats]! Текст ошибки: '+*/COALESCE(ERROR_MESSAGE(),'null *Error message is not defined');
				END CATCH
				set @commant_text_log=@Cmd_handle_log+@UpdCmd;
				--Логгируем в историю Обслуживания БД:
				EXEC sputnik.db_maintenance.usp_WriteHS 
					@DB_ID=@db_id,
					@Object_ID=@obj_id,
					@Index_Stat_ID=@stat_id,
					@Index_Stat_Type=1, --1-Statistic
					@Command_Type=3, --3-Update Statistics
					@Command_Text_1000=@commant_text_log,
					@tt_start=@tt_start,
					@Status=@flag_fail, --0-Success, 1-Fail(Error)
					@Error_Text_1000=@StrErr;
			
				set @i_cnt+=1;
				--Делаем заданную задержку перед следующим запуском!
				WAITFOR DELAY @delayperiod;

				FETCH NEXT FROM CST INTO @HandleCmd, @Cmd_handle_log, @UpdCmd, @obj_id, @stat_id, @db_id;
			END
			CLOSE CST;
			DEALLOCATE CST;
		END;
		IF @only_show=0
		BEGIN
			set @commant_text_log=N'Задача завершена: usp_RecomputeStats. Обработано объектов: '+CONVERT(NVARCHAR(10),@i_cnt)+N' . Параметры: @DBName='''+COALESCE(@DBName,'NULL')+N''',@RowCount='+CONVERT(nvarchar(20),@RowLimit);
			--Логгируем в историю Обслуживания БД:
			EXEC sputnik.db_maintenance.usp_WriteHS 
				@DB_ID=0,
				@Command_Type=201, --201-TaskCompleted for Update Statistics (usp_RecomputeStats)
				@Command_Text_1000=@commant_text_log,
				@tt_start=@tt_start_usp,
				@Status=0; --0-Success
		END;
	end