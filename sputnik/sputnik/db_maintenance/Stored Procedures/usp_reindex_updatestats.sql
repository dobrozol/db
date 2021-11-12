
/* =============================================
-- Author:		Андрей Иванов (sqland1c)
-- Create date: 15.01.2014 (1.0)
-- Description: Процедура для сбора и обновления статических данных для реиндексации таблиц в базе
				на основании системной функции dm_db_index_physical_stats 
				Параметр @db_name определяет для какой базы собирается статистика.
				Параметр @rowlimit - определяет количество строк для обработки за 1 раз. По умолчанию 500.
				Параметр @countexec - количество подходов (раз) выполнения с заданными настройками. По умолчанию 50 раз.
				Параметр @delayperiod - определяет временную задержку в формате строки 00:00:00, перед запуском обработки следующих строк. По умолчанию 06 сек.
				Эти 3 параметра необходимы, чтобы минимизировать нагрузку на SQL Server.
				Параметр @oldupdhours - определяет количество часов, после которых информация считается устаревшей и требуется обновление! По умолчанию 6 часов.
-- Update:		15.01.2014 (1.1)
				Добавлен алгоритм предварительной проверки найденных строк. Если найденных строк нет, то выполнение прерывается.
				24.01.2014 (1.15)
				Изменены значения параметров по умолчанию.
				24.01.2014 (1.20)
				Для сбора статистики используется метод default вместо DETAILED.
				30.01.2014 (1.4)
				Добавлены алгоритмы проверки указанной базы и главное проверки Таблиц И Индексов в сохранённой статистике!			
				25.02.2014 (1.5)
				Добавлено условие в отбор индексов - только Включенные индексы отбирать.
				26.02.2014 (1.55)
				Изменен параметр @oldupdate на @oldupdhours - теперь возраст статистики будет проверяться в часах, а не в днях. Значение по умолчанию 6 часов.
				27.02.2014 (1.56)
				Значение по умолчанию для параметра @oldupdhours уменьшено с 6 до 3 часов.
				05.03.2014 (1.57)
				Добавлена настройка сеанса Низкий приоритет взаимоблокировки и READ UNCOMMITTED в качестве уровня изоляции транзакций.
				Также размер тектовой переменной @tsql увеличен до 2400.
				06.03.2014 (1.58)
				Изменено значение по умолчанию для параметра @delayperiod с 6 до 2 сек.
				15.10.2014 (1.6)
				Добавлен новый параметр @TableFilter - теперь можно запустить принудительно сбор статистик для конкретной таблицы.
				При этом значение параметра @oldupdhours не учитывается!
				24.06.2015 (2.0)
				Совершенно новый алгоритм сбора дополнительной информации по индексам. Вся проверка актуальности производиться 
				в процедуре usp_reindex_preparedata. Вся информация сохраняется в одну таблицу ReindexData.
				18.08.2016 (2.01) Добавлен новый параметр @rowlimit_max - за 1 запуск обновляем только
				указанное кол-во статистик.
				14.12.2016 (2.020)
				Новый алгоритм сбора информации и логгирования в таблицу HS.
-- ============================================= */
CREATE PROCEDURE db_maintenance.usp_reindex_updatestats
	@db_name nvarchar(300) = NULL,
	@rowlimit int = 50,
	@delayperiod char(12) = '00:00:00:500',
	@oldupdhours tinyint = 3,
	@TableFilter nvarchar(300)=null,
	@rowlimit_max bigint = 1000000
AS
BEGIN
	declare @tt_start_proc datetime2(2);
	set @tt_start_proc=CAST(SYSDATETIME() AS datetime2(2));
	SET NOCOUNT ON;
	SET DEADLOCK_PRIORITY LOW;
	SET LOCK_TIMEOUT 30000;
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
	declare @tsql nvarchar (2400),@tsqlcheck nvarchar (600);
	----Проверка АКТУАЛЬНОСТИ указанной базы данных:
	--declare @DB_current nvarchar(300);
	--select @DB_current = name from sys.databases where name = @db_name and state_desc='ONLINE'
	--if @DB_current is null
	--	return -1;

	if @TableFilter is not null
		set @oldupdhours=0;
	else
		set @TableFilter='';

	/*	Отбор БД для обслуживания */
	declare @DB_current nvarchar(300);
	if OBJECT_ID('tempdb..#src_ag_db') IS NOT NULL
		DROP TABLE #src_ag_db;
	CREATE TABLE #src_ag_db (DB nvarchar(800), [db_id] int, [Role] nvarchar(800), [PartnerReplica] nvarchar(800), [PrimaryReplica] nvarchar(800), sync_state nvarchar(800), health nvarchar(800), DB_State nvarchar(800));
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
		(name = @db_name or @db_name is null or @db_name='') 
		and state_desc='ONLINE'
		and database_id>4
		and is_read_only=0
		and (name NOT LIKE '201%(%)%' and name NOT LIKE 'S201%(%)%')
		and adb.db is null;
	OPEN DB;
	FETCH NEXT FROM DB INTO @DB_current;
	WHILE @@FETCH_STATUS=0
	BEGIN
		
		--Теперь ВСЯ проверка актуальности таблиц и индексов полностью производится в процедуре usp_reindex_preparedata	 			
		--При этом пересоздания таблиц не происходит, а осуществляется СИНХРОНИЗАЦИЯ данных через MERGE!
		exec sputnik.db_maintenance.usp_reindex_preparedata @DB_current;
	
		declare @db_id int, @flag_fail bit, @StrErr varchar(2048), @tt_start datetime2(2), @command_text_log varchar(8000);
		--Новый алгоритм сбора дополнительной информации по индексам: avg_fragmentation_in_percent и page_count.
		declare @IndexID int, @TableID int, @AvgFrag tinyint, @PageCount bigint, @i_limit int=1,@Avg_Page_Used tinyint,@Row_cnt bigint,@RowSize_Kb numeric(9,3);
		set @db_id=DB_ID(@DB_current);

		declare C cursor for
		select TOP (@rowlimit_max) IndexID,TableID
		from   [db_maintenance].[ReindexData]
		where
			DBName=QUOTENAME(@DB_current)
			and (@TableFilter = '' or TableName= QUOTENAME(@TableFilter))
			and ([LastUpdateStats] is null or [LastRunDate]>[LastUpdateStats]
				or DATEDIFF(HOUR,[LastUpdateStats],getdate())>=@oldupdhours)
		order by [LastUpdateStats] ASC;
		open C
		fetch next from C into @IndexID, @TableID
		while @@fetch_status=0
		begin
			set @flag_fail=0;
			set @StrErr=NULL;
			BEGIN TRY
			
				set @tt_start=CAST(SYSDATETIME() AS datetime2(2));
						
				select 			
					@AvgFrag=cast (ROUND(max(avg_fragmentation_in_percent),0) as tinyint),
					@Avg_Page_Used=cast (ROUND(min(avg_page_space_used_in_percent),0) as tinyint),
					@PageCount=sum(page_count),
					@Row_cnt=sum(record_count),
					@RowSize_Kb=cast (max(avg_record_size_in_bytes)/1024.00 as numeric(9,3))
				from sys.dm_db_index_physical_stats (@db_id, @TableID, @IndexID,NULL,'SAMPLED');

				--select @AvgFrag,@Avg_Page_Used,@PageCount,@Row_cnt,@RowSize_Kb
				--select @DB_current,@TableID,@IndexID;
				--select [AVG_Fragm_percent],[PageCount],[~PageUsed_perc],[~Row_cnt],[~RowSize_Kb],[LastUpdateStats]
				--from [db_maintenance].[ReindexData] 
				--where DBName=@DB_current
				--	and TableID=@TableID
				--	and IndexID=@IndexID; 

				update [db_maintenance].[ReindexData] 
				set [AVG_Fragm_percent]=@AvgFrag,
					[PageCount]=@PageCount,
					[~PageUsed_perc]=@Avg_Page_Used,
					[~Row_cnt]=@Row_cnt,
					[~RowSize_Kb]=@RowSize_Kb,
					[LastUpdateStats]=getdate()
				where 
					DBName=QUOTENAME(@DB_current)
					and TableID=@TableID
					and IndexID=@IndexID;
			END TRY
			BEGIN CATCH
				--Логгируем в историю Обслуживания БД (ТОЛЬКО ОШИБКИ!):
				set @flag_fail=1;
				set @StrErr=/*'Ошибка при обновлении данных по индексам через процедуру [usp_reindex_updatestats]! Текст ошибки: '+*/COALESCE(ERROR_MESSAGE(),'null *Error message is not defined');
				set @command_text_log='--dlck_pr=-5;tr_iso_lvl=1;lck_tmt=30000;
		select @AvgFrag=cast(MAX(avg_fragmentation_in_percent) as tinyint), @PageCount=sum(page_count)
		from sys.dm_db_index_physical_stats ('+CAST(@db_id as varchar(100))+','+CAST(@TableID as varchar(100))+','+CAST(@IndexID as varchar(100))+',default,default);
		--update table [db_maintenance].[ReindexData]';
				EXEC sputnik.db_maintenance.usp_WriteHS 
					@DB_ID=@db_id,
					@Object_ID=@TableID,
					@Index_Stat_ID=@IndexID,
					@Index_Stat_Type=0, --0-Index
					@Command_Type=5, --5-Update data for ReIndex (usp_reindex_updatestats)
					@Command_Text_1000=@command_text_log,
					@tt_start=@tt_start,
					@Status=@flag_fail, --0-Success, 1-Fail(Error)
					@Error_Text_1000=@StrErr;
			END CATCH
		
			fetch next from C into @IndexID, @TableID;
		
			if @i_limit>=@rowlimit
			begin
				waitfor delay @delayperiod;
				set @i_limit=1;
			end
			else
				set @i_limit+=1;		
		end
		close C
		deallocate C;

		--Логгируем в историю Обслуживания БД в конце по всему вызову процедуры:
		set @command_text_log='exec [db_maintenance].[usp_reindex_updatestats] @db_name='''+@DB_current+''', @rowlimit='+CAST(@rowlimit as varchar(100))+',@delayperiod='''+@delayperiod+''',@oldupdhours='+CAST(@oldupdhours as varchar(100))+',@TableFilter='+CASE WHEN @TableFilter='' THEN 'NULL' ELSE ''''+@TableFilter+'''' END+',@rowlimit_max='+CAST(@rowlimit_max as varchar(100))+';';
		EXEC sputnik.db_maintenance.usp_WriteHS 
			@DB_ID=@db_id,
			@Index_Stat_Type=0, --0-Index
			@Command_Type=5, --5-Update data for ReIndex (usp_reindex_updatestats)
			@Command_Text_1000=@command_text_log,
			@tt_start=@tt_start_proc,
			@Status=0, --0-Success, 1-Fail(Error)
			@Error_Text_1000=NULL;

		FETCH NEXT FROM DB INTO @DB_current;
	END
	CLOSE DB;
	DEALLOCATE DB;
END