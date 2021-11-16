	/* =============================================
	-- Author:		Андрей Иванов (sqland1c)
	-- Create date: 24.01.2013 (1.0)
	-- Description: Процедура для выполнения интелектуальной реиндексации в уканной базе на основе собранной статистики 
					в таблице db_maintenance.ReindexDataFor_ИмяБазы.

					Параметр @db_name определяет в какой базы будет реиндексация.
				
					Параметр @indexeslimit - определяет количество индексов для обработки за 1 раз. По умолчанию 50.
					Параметр @delayperiod - определяет временную задержку в формате строки 00:00:00, перед запуском обработки следующих индексов. По умолчанию 2 сек.
						Эти 2 параметра необходимы, чтобы минимизировать нагрузку на SQL Server.
				
					Параметр @filter_pages - определяет количество страниц, определяющих примерный размер индекса. Индексы с таким размером и большим, будут попадать в отбор
						для дальнейшего перестроения. По умолчанию 128. Если задать значение null, то в отбор попадут все индексы.
					Параметр @filter_fragm - определяет текущую фрагментацию в %. Индексы с такой фрагментацией и больше, будут попадать в отбор для дальнейшего 
						перестроения. По умолчанию 10. Если задать значение null, то в отбор попадут все индексы.
					Параметр @filter_old_hours - определяет возраст реиндексации в часах. Все индексы, которые перестаивались раньше, должны попасть в отбор для нового перестроения.
						По умолчанию 6. Если задать значение null, то в отбор попадут все индексы.
					Параметр @fragm_tresh - определяет порог фрагментации в %. Индексы с меньшей фрагментацией будут дефрагментироваться (reorginize), а с большим порогом - 
						перестраиваться (rebuild). По умолчанию 30.
					Параметр @set_fillfactor - определяет устанавливаемый параметр FillFactor при перестроении индексов. По умолчанию 97%.
					Параметр @set_compression - определяет устанавливаемый параметр Сжатие данных при перестроение. Поддерживается только в Enterprise! По умолчанию NONE.
					Параметр @set_online - определяет возможность online операции с индексами. Поддерживается только в Enterprise! По умолчанию OFF.						
	-- Update:		
					31.01.2014 (1.1)
					Параметр @filter_pages, значение по умолчанию изменено на 12. 
					Добавлен параметр @set_sortintempdb, если ON, тогда при перестроении индексов будет использован параметр sort_in_tempdb	
					22.02.2014 (1.2)
					Исправлена ошибка - после реиндексации обновлять поле LastUpdateStats не нужно!
					25.02.2014 (1.3)
					Цикл в конце изменён на условие (IF). 
					26.02.2014 (1.4)
					Добавлен учёт количества выполненных операций Перестроения/Дефрагментации индекса в столбце ReindexCount.
					Также изменён входной параметр @filter_oldest -> @filter_old_hours - вместо Дней, задаётся количество часов. По умолчанию 6 часов.
					27.02.2014 (1.5)
					Добавлен алгоритм проверки возможности ONLINE перестроения индекса (только для операции REBUILD).
						Если индекс содержит LOB данные, то ONLINE перестроение невозможно.
					05.03.2014 (1.51)
					Добавлена настройка сеанса Низкий приоритет взаимоблокировки и READ UNCOMMITTED в качестве уровня изоляции транзакций.
					11.03.2014 (1.6)
					Добавлен новый параметр @PauseMirroring (соответствует новому столбцу PauseMirroring в таблице ReindexConf). Определяет приостановку Зеркалирования
					на время Реиндексации (если установлен 1). По умолчанию 0.
					15.10.2014 (1.62)
					Добавлен новый параметр @TableFilter - теперь можно запустить реиндексацию для конкретной таблицы! При этом реиндексация будет запущена
					принудительно (без учета параметра @filter_old_hours).
					18.12.2014 (1.63)
					Добавлена настройка сеанса ожидание блокировки=30 сек (по умолчанию бесконечно). Это может существенно помочь в решении блокировок.
					02.03.2015 (1.64)
					Вызов модуля usp_freeproccache (сброс процедурного кэша) перенесён в usp_reindex_start.
					24.06.2015 (2.00) 
					Полная оптимизация схемы Реиндексации. Процедура запуска значительно переделана под новую схему.
					08.07.2015 (2.01) 
					Небольшое исправление в алгоритме получения данных из ReindexData - изменена сортировка (теперь учитывается кол-во выполненных операций обслуживания - ReindexCount).
					09.07.2015 (2.02) 
					Добавлен параметр @only_show - позволяет просмотреть отобранные для обслуживания индексы (без обслуживания)
					07.11.2015 (2.05)
					Изменен алгоритм получения данных из ReindexData - изменена сортировка. Теперь отбор индексов происходит на основе рейтинга qt.
					Он расчитывается исходя из фрагментации, размера, кол-во обслуживаний в прошлый раз, и как давно последний раз обслуживался индекса. 
					А финальная сортировка перед непосредственным выполнением Реиндекса осуществляется случайным образом!
					01.12.2015 (2.06)
					Исправлен алгоритм расчета рейтинга qt.
					18.10.2016 (2.07)
					Исправлен алгоритм расчета рейтинга qt - теперь [ReindexCount] Кол-во реиндексаций ВЫЧИТАЕМ, а не прибавляем.
					31.03.2017 (3.000)
					Новая версия, большие изменения (переработан алгоритм+запись истории), несколько новых параметров (@PageUsed_tresh,@MaxDop,@timeout_sec,@policy_offline)
					12.04.2017 (3.002)
					Исправлен алгоритм расчёта @MaxDop_set - добавлено "послабление" для серверов с небольшим кол-вом ядер CPU (до 20). 
					Также изменён алгоритм выборки индексов для обслуживания - вставлена конструкция "order by qt desc". Финальная сортировка остаётся по-прежнему "случайной".
					26.04.2017 (3.005)
					Добавлена возможность принудительного перестроения (Rebuild) всех индексов. Для этого нужно в параметре @fragm_tresh задать отрицательное значение (например, -1).
					29.12.2017 (3.006)
					Увеличины размеры строковых переменных.
					14.11.2018 (3.010)
					Добавлена совместимость с 2008 (iif заменены на case).
					16.11.2021 (3.020) 
					added NoReorganize parameter (if page-level locks are disabled in the index).
					reorganize will be replaced with a rebuild index
					16.11.2021 (3.030) 
					added managed locks for multithreading.
	-- ============================================= */
	CREATE PROCEDURE [db_maintenance].[usp_reindex_run]
		@db_name nvarchar(2000)=NULL,
		@UniqueName_SL nvarchar(200)=NULL,
		@rowlimit smallint = 50,
		@delayperiod char(12) = '00:00:00.100',
		@filter_pages_min int = 12,
		@filter_pages_max int = null,
		@filter_fragm_min tinyint = 10,
		@filter_fragm_max tinyint = null,
		@filter_old_hours tinyint = 24,
		@fragm_tresh smallint = 30,
		@set_fillfactor tinyint = 100,
		@set_compression char(4)='NONE',
		@set_online char(3)='OFF',
		@set_sortintempdb char(3)='OFF',
		@PauseMirroring bit=0,
		@TableFilter nvarchar(2000)=null,
		@DeadLck_PR smallint=0,
		@Lck_Timeout int=20000,
		@only_show bit = 0,
		@PageUsed_tresh tinyint = 80,	--% заполнения страницы, если меньше этого значения то обязательно нужен REBUILD, а не reorganize.
		@MaxDop smallint = NULL,		--кол-во ядер CPU на которых будет выполнятся обслуживание индекса (Работает только в Enterprise!).
		@timeout_sec int = NULL,		--Ограничение времени выполнения в данной процедуре в сек. Если NULL (или 0) - бесконечно.
		@policy_offline tinyint = 2,		--Определяет что делать, если Online Rebuild невозможен: 0-Rebuild Offline,1-пропустить,2-Reorganize.
		@walp_max_duration smallint = NULL,	--option WAIT_AT_LOW_PRIORITY parameter MAX_DURATION (in minutes). NULL - this option will not use
		@walp_abort_after_wait varchar(20) = 'NONE' --option WAIT_AT_LOW_PRIORITY parameter ABORT_AFTER_WAIT (NONE, SELF, BLOCKERS)
	AS
	BEGIN
		SET NOCOUNT ON;
		SET DEADLOCK_PRIORITY LOW;
		SET LOCK_TIMEOUT 30000;
		SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

		DECLARE @tt_start datetime2(2), @StrErr NVARCHAR(MAX),@flag_fail bit, @db_id_check int, @obj_id int, @ind_id int, @command_type tinyint, @tsql_handle_log varchar(2000), @commant_text_log Nvarchar(MAX),@AllCores_cnt smallint, @MaxDop_set smallint=@MaxDop;
		declare @tt_start_usp datetime2(2), @time_elapsed_sec int;
		declare @tsql_handle nvarchar (2400), @tsql nvarchar (2400), @tsqlcheck nvarchar (800), @StopList_str NVARCHAR(MAX), @walp_option varchar(300)='', @mtHead varchar(500), @mtBody varchar(500), @mtEnd varchar(500) ;
		declare @MirrorState nvarchar(75);
		set @tt_start_usp=CAST(SYSDATETIME() AS datetime2(2));
		--Определяем текущую редакцию SQL Server. MaxDop будет работать только в Enterprise:
		DECLARE @Ed VARCHAR(3)=LEFT(CAST(SERVERPROPERTY('Edition') AS VARCHAR(128)),3);

		IF @TableFilter is not null
			set @filter_old_hours=0;
		ELSE
			set @TableFilter='';

		--Формируем список исключений таблиц, индексы для этих таблиц не будут обслужены в текущем запуске.
		select @StopList_str=StopList_str from sputnik.db_maintenance.StopLists where UniqueName=@UniqueName_SL;
		select @StopList_str=COALESCE(@StopList_str,'');

		--Setting and checking locks on a maintained index for multithreading
		set @mtHead = '
begin tran
	declare @lockResult int;
	exec @lockResult = sp_getapplock ';
	
		set @mtBody = ', ''Exclusive'', ''Transaction'', 0;
	if @lockResult<0 begin
		rollback;
		throw 60000, ''This index is already locked by another process'', 0;
	end
	else
		'
		set @mtEnd = '
commit
'
	
		--Заголовок запроса для обслуживания индексов!
		set @tsql_handle= N'
	SET DEADLOCK_PRIORITY '+CAST(@DeadLck_PR as varchar(2))+';
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
	SET LOCK_TIMEOUT '+CAST(@Lck_Timeout as varchar(12))+';
		';
		--Заголовок запроса для логгирования:
		set @tsql_handle_log='--dlck_pr='+CAST(@DeadLck_PR as varchar(2))+';tr_iso_lvl=1;lck_tmt='+CAST(@Lck_Timeout as varchar(12))+';
		';
		--Далее получаем индексы для обслуживания и формируем команды для обслуживания, и выполняем их по очереди в отдельном пакете.
		declare @SchemaName nvarchar(2000), @TableName nvarchar(2000), @IndexName nvarchar(2000), @PageCount int, @AVG_Fragm_percent tinyint,@NotRunOnline bit, @NoReorganize bit;
		declare @command nvarchar(MAX), @check_set_online char(3), @PageU_prc tinyint, @i_cnt bigint=0,@i_cnt_skip bigint=0;
		--declare @T_i table (SchemaName nvarchar(300),TableName nvarchar(300),IndexName nvarchar(300), obj_id int, ind_id int, [PageCount] bigint,AVG_Fragm_percent tinyint,PageU_prc tinyint,NotRunOnline bit);
		IF OBJECT_ID('tempdb.dbo.#T_RI') IS NOT NULL
			DROP TABLE #T_RI;
		CREATE TABLE #T_RI (DB nvarchar(2000),SchemaName nvarchar(2000),TableName nvarchar(2000),IndexName nvarchar(2000), obj_id int, ind_id int, [PageCount] bigint,AVG_Fragm_percent tinyint,PageU_prc tinyint,NotRunOnline bit, qt numeric(19,6), NoReorganize bit);


		/*	Отбор БД для обслуживания */
		declare @DB_current nvarchar(2000);
		if OBJECT_ID('tempdb..#src_ag_db') IS NOT NULL
			DROP TABLE #src_ag_db;
		CREATE TABLE #src_ag_db (DB nvarchar(2000), [db_id] int, [Role] nvarchar(2000), [PartnerReplica] nvarchar(2000), [PrimaryReplica] nvarchar(2000), sync_state nvarchar(2000), health nvarchar(2000), DB_State nvarchar(2000));
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
			;with cte_src_1 as
			(
				select top (@rowlimit) 
					SchemaName,TableName,IndexName,[PageCount],AVG_Fragm_percent,NotRunOnline
					,CAST( 
							([AVG_Fragm_percent])
							+ (100-[~PageUsed_perc])
							+ (cast([PageCount] as numeric(19,6)) / case when MAX ([PageCount]) over ()=0 then 1 else MAX ([PageCount]) over () end * 10)
							+ (datediff(day,LastRunDate,[LastUpdateStats])*[AVG_Fragm_percent]*0.01) 
							- (cast([ReindexCount] as numeric(19,6)) / case when MAX (ReindexCount) over ()=0 then 1 else MAX (ReindexCount) over () end * 10)
							as numeric(19,6)) as qt,
					[TableID] as obj_id, [IndexID] as ind_id,
					[~PageUsed_perc] as PageU_prc,
					[NoReorganize]
				from 
					[db_maintenance].[ReindexData]
				where 
					DBName=QUOTENAME(@DB_current)
					and (@TableFilter='' or TableName=QUOTENAME(@TableFilter))
					and CHARINDEX(TableName+';',@StopList_str)=0
					and ([LastUpdateStats] is not null or @fragm_tresh<0)
					and ([PageCount] is not null and (@filter_pages_min is null or [PageCount] >= @filter_pages_min))
					and ([PageCount] is not null and (@filter_pages_max is null or [PageCount] <= @filter_pages_max))
					and (
							(
								(AVG_Fragm_percent is not null and (@filter_fragm_min is null or AVG_Fragm_percent >= @filter_fragm_min))
								OR ([~PageUsed_perc]<@PageUsed_tresh AND AVG_Fragm_percent>0)
							)
							AND (AVG_Fragm_percent is not null and (@filter_fragm_max is null or AVG_Fragm_percent <= @filter_fragm_max))				
						)
					and (LastRunDate is null or (@filter_old_hours is null or DATEDIFF(HOUR,[LastRunDate],getdate()) >= @filter_old_hours))
					and (LastRunDate is null or (LastUpdateStats > LastRunDate) or @fragm_tresh<0)
				order by qt desc
			)
			insert into #T_RI (DB, SchemaName,TableName,IndexName, obj_id, ind_id, [PageCount],AVG_Fragm_percent,PageU_prc,NotRunOnline,qt,[NoReorganize])
			select
					@DB_current,SchemaName,TableName,IndexName, obj_id, ind_id, [PageCount],AVG_Fragm_percent,PageU_prc,NotRunOnline,qt,[NoReorganize]
			from 
				cte_src_1;
		
			FETCH NEXT FROM DB INTO @DB_current;
		END
		CLOSE DB;
		DEALLOCATE DB;

		--Создадим индекс по qt!
		--Для гарантированного отбора по Коэфициенту!
		CREATE CLUSTERED INDEX tmp_cix_qc01 ON #T_RI
		(
			[qt] DESC
		) ;

		if @only_show=1
			select SchemaName,TableName,IndexName,[PageCount],AVG_Fragm_percent,NotRunOnline,[NoReorganize]
			from
			(select  top (@rowlimit) SchemaName,TableName,IndexName,[PageCount],AVG_Fragm_percent,NotRunOnline,[NoReorganize] from #T_RI order by [qt] desc) t
			order by NEWID();
		else
		BEGIN

			declare C cursor for
			select DB, SchemaName,TableName,IndexName, obj_id, ind_id ,[PageCount],AVG_Fragm_percent,PageU_prc,NotRunOnline,[NoReorganize]
			from
			(select  top (@rowlimit) DB, SchemaName,TableName,IndexName, obj_id, ind_id,  [PageCount],AVG_Fragm_percent, PageU_prc, NotRunOnline,[NoReorganize] from #T_RI order by [qt] desc) t
			order by NEWID();
			open C
			fetch next from C into @DB_current, @SchemaName,  @TableName, @IndexName, @obj_id, @ind_id, @PageCount, @AVG_Fragm_percent,@PageU_prc,@NotRunOnline,@NoReorganize;
			while @@fetch_status=0
			begin
				--Проверяем TimeOut, если время вышло - пишем в лог HS и выходим!
				IF @timeout_sec is not null AND @timeout_sec>0
				BEGIN
					set @time_elapsed_sec=DATEDIFF(second,@tt_start_usp, CAST(SYSDATETIME() AS datetime2(2)));
					IF @time_elapsed_sec>@timeout_sec
					BEGIN
						set @commant_text_log='Достигнут TimeOut в usp_reindex_run. @TimeOut_sec='+cast(@TimeOut_sec as varchar(30))+'; @time_elapsed_sec='+cast(@time_elapsed_sec as varchar(30));
						--Логгируем в историю Обслуживания БД:
						EXEC sputnik.db_maintenance.usp_WriteHS 
							@DB_ID=@db_id_check,
							@Command_Type=100, --100-TimeOut for Reindex (usp_reindex_run)
							@Command_Text_1000=@commant_text_log,
							@tt_start=@tt_start_usp,
							@Status=0; --0-Success
						BREAK; --выход из текущего цикла.
					END
				END

				set @db_id_check=DB_ID(@DB_current);
				select 
					@MirrorState=mirroring_state_desc
				from 
					sys.database_mirroring
				where
					mirroring_guid is not null
					and database_id=@db_id_check
	
				if @PauseMirroring=1 and @MirrorState in ('SYNCHRONIZED','SYNCHRONIZING')
					exec(N'alter database ['+@DB_current+'] set partner suspend');

				if (isnull(@MaxDop,-1)<0)
					exec @MaxDop_set = [db_maintenance].[usp_getMaxDop] @PageCount;

				set @check_set_online=@set_online;
				IF (@check_set_online='ON' and @NotRunOnline=1 and @policy_offline=1)
				BEGIN
					--Пропустить этот индекс
					set @command_type=8;
					set @commant_text_log='Этот индекс пропущен, т.к. @NotRunOnline=1 и @policy_offline=1';
					set @i_cnt_skip += 1;
				END
				ELSE BEGIN
	
					if @check_set_online='ON' and @NotRunOnline=1 and @policy_offline=0
						set @check_set_online='OFF';	
						
					--rebuild делаем только если фрагментация меньше @fragm_tresh и если заполненость страницы более чем @PageUsed_tresh
					--в остальных случаях нужен reorginize!
					if (
						(@AVG_Fragm_percent <= @fragm_tresh AND (@PageU_prc>=@PageUsed_tresh)) 
						OR (@NotRunOnline=1 AND @check_set_online='ON' AND @policy_offline=2)
					) and isnull(@NoReorganize,0)=0
					begin
						set @command=N'alter index '+@IndexName+N' on '+@SchemaName+N'.'+@TableName+N' reorganize ';
						set @command_type=2;
					end
					else if @AVG_Fragm_percent > @fragm_tresh OR (@PageU_prc<@PageUsed_tresh and @AVG_Fragm_percent>0)
					begin
						if @check_set_online='ON' begin
							set @command_type=1;
							if isnull(@walp_max_duration,-1)>0
								set @walp_option=' (WAIT_AT_LOW_PRIORITY (MAX_DURATION = '+cast(@walp_max_duration as varchar(5))+' minutes, ABORT_AFTER_WAIT = '+@walp_abort_after_wait+'))'
						end
						else
							set @command_type=0;
						set @command=N'alter index '+@IndexName+N' on '+@SchemaName+N'.'+@TableName+N' rebuild with ( sort_in_tempdb = '+@set_sortintempdb+', online = '+@check_set_online+@walp_option+N' , data_compression = '+@set_compression+', fillfactor='+cast(@set_fillfactor as varchar(5))+CASE WHEN @Ed='Ent' THEN ', MAXDOP = '+cast(@MaxDop_set as varchar(5)) ELSE '' END+')';

					end
					set @tsql=@tsql_handle+N'
		use '+QUOTENAME(@DB_current)+';
		'			+@mtHead
					+''''+convert(VARCHAR(32), HashBytes('MD5', concat(@DB_Current, '.', @SchemaName, '.', @TableName,'.', @IndexName)), 2)+''''
					+@mtBody
					+@command
					+@mtEnd;
			
					set @flag_fail=0;
					set @StrErr=NULL;
					BEGIN TRY
						--PRINT(@UpdCmd);
						set @tt_start=CAST(SYSDATETIME() AS datetime2(2));
						exec(@tsql);

						update [db_maintenance].[ReindexData] 
						set 
							[LastRunDate]=getdate(),
							[LastCommand]=@command,
							[ReindexCount]+=1
						where 
							DBName=QUOTENAME(@DB_current)
							AND SchemaName=@SchemaName
							AND TableName=@TableName
							AND IndexName=@IndexName;
					END TRY
					BEGIN CATCH
						set @flag_fail=1;
						set @StrErr=/*'Ошибка при обслуживании индексов через процедуру [usp_reindex_run]! Текст ошибки: '+*/COALESCE(ERROR_MESSAGE(),'null *Error message is not defined');
					END CATCH
					--НЕ будем тратить время на пересчет статистик. Тем более что пересчет статистик теперь выполняется в отдельной процедуре!
					--exec(N''use '+@DB_current+'; update statistics ''+ @SchemaName+N''.''+@TableName+N'' ''+@IndexName+N'' with fullscan'' );
			
					set @commant_text_log=@tsql_handle_log+@command;
	
					set @i_cnt+=1;
		
					waitfor delay @delayperiod;
				END;
		
			--Логгируем в историю Обслуживания БД:
				EXEC sputnik.db_maintenance.usp_WriteHS 
					@DB_ID=@db_id_check,
					@Object_ID=@obj_id,
					@Index_Stat_ID=@ind_id,
					@Index_Stat_Type=0, --0-Index
					@Command_Type=@command_type, --0-Rebuild Index Offline, 1-Rebuild Index Online, 2-Reorginize Index,8-Skip Offline Index
					@Command_Text_1000=@commant_text_log,
					@tt_start=@tt_start,
					@Status=@flag_fail, --0-Success, 1-Fail(Error)
					@Error_Text_1000=@StrErr;
				fetch next from C into @DB_current, @SchemaName,  @TableName, @IndexName, @obj_id, @ind_id, @PageCount, @AVG_Fragm_percent,@PageU_prc,@NotRunOnline,@NoReorganize;
			
			end
			close C;
			deallocate C;

			select 
				@MirrorState=mirroring_state_desc
			from 
				sys.database_mirroring
			where
				mirroring_guid is not null
				and database_id=DB_ID(@DB_current)
			if @PauseMirroring=1 and @MirrorState = 'SUSPENDED'
				exec(N'alter database ['+@DB_current+'] set partner resume');
	
			set @commant_text_log='Задача завершена: usp_reindex_run. Обработано объектов: '+CONVERT(VARCHAR(10),@i_cnt)+
			CASE WHEN @i_cnt_skip>0 THEN ' ; пропущено: '+CONVERT(VARCHAR(10),@i_cnt_skip)
				ELSE ''
			END
			+' . Параметры: @db_name='''+COALESCE(@db_name,'NULL')+''',@RowCount='+CONVERT(VARCHAR(10),@RowLimit);
			--Логгируем в историю Обслуживания БД:
			EXEC sputnik.db_maintenance.usp_WriteHS 
				@DB_ID=0,
				@Command_Type=200, --200-TaskCompleted for Reindex (usp_reindex_run)
				@Command_Text_1000=@commant_text_log,
				@tt_start=@tt_start_usp,
				@Status=0; --0-Success

		END
	END