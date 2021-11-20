

/* =============================================
-- Author:		Андрей Иванов (sqland1c)
-- Create date: 10.12.2013 (1.0)
-- Description: Процедура для сбора первичных данных для реиндексации таблиц в базе
				Параметр @db_name определяет для какой базы собирается статистика.
-- Update:		15.01.2014 (1.1)
				Размер текстовых переменных (nvarchar) увеличен.	
				26.02.2014 (1.2)
				Добавлено условие в отбор индексов - только Включенные индексы отбирать.
				Также в таблицу ReindexDataFor_ добавлены новый столбец ReindexCount - количество выполнений операций Перестроении или Дефрагментаций индекса.
				27.02.2014 (1.3)
				Добавлен список столбцов при вставке данных!
				Также добавлен алгоритм для получения данных о хранении LOB в индексе. Если такие есть, то ONLINE перестроение индекса запрещено (столбец NotRunOnline = 1).
				03.03.2014 (1.4)
				Добавлен новый столбец PrepareDate - определяет дату и время подготовки данных (то есть инициализации таблицы ReindexDataFor_).
				Также для всех столбцов с типом Дата изменён Тип на более новый (кот-й занимает меньше места) - datetime2 c 2 разрядами после секунды.
				05.03.2014 (1.41)
				Добавлена настройка сеанса Низкий приоритет взаимоблокировки и READ UNCOMMITTED в качестве уровня изоляции транзакций.
				24.06.2015 (2.00)
				Совершенно новый алгоритм сбора и хранения данных для Реиндексации: 1. Теперь будет одна таблица на все БД, 2. Теперь таблицы не будут пересоздаваться,
				а информация будет обновляться с помощью MERGE!
				10.09.2015 (2.02)
				Добавлено дополнительные условие в команду MERGE, чтобы не обновлять строки, если они не изменились.
				Такде добавлено условие, чтобы удалять строки только по текущей БД - этим исправлена существенная ошибка!
				14.12.2016 (2.030)
				Изменена таблица ReindexData и добавлен алгоритм логгирования в таблицу HS.
				16.11.2021 (2.040) added NoReorganize parameter - if page-level locks are disabled in the index
				20.11.2021 (2.045) added check for repeated updating of information on non-processed indexes
-- ============================================= */
CREATE PROCEDURE db_maintenance.usp_reindex_preparedata
	@db_name nvarchar(300),
	@updateLagInHours smallint = 24	--delay in hours for re-updating information for non-processed indexes
AS
BEGIN
	SET NOCOUNT ON;
	SET DEADLOCK_PRIORITY LOW;
	SET LOCK_TIMEOUT 30000;
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
	--Проверка АКТУАЛЬНОСТИ указанной базы данных:
	declare @db_name_check nvarchar(300);
	select @db_name_check = name from sys.databases where name = @db_name and state_desc='ONLINE'
	if @db_name_check is null
		return -1;
	declare @tsql Nvarchar(2400);
	--check table ReindexData - failed if it not exists!
	if object_id('db_maintenance.ReindexData') IS NULL begin
		print('Table [db_maintenance].[ReindexData] not exists!')
		return 0
	end
	--В отдельном пакете соберём всю исходную информацию о таблицах и индексах по базе данных
	--Вся полученная информация сохраняется во временной таблице #T_Source для дальнейшей обработки.
	if object_id('tempdb..#T_Source') is not null
		drop table #T_Source;
	create table #T_Source
		(
			DBName     nvarchar(300),
			SchemaName nvarchar(300),
			TableName  nvarchar(300),
			IndexName  nvarchar(300),
			TableID    int,
			IndexID	   int,
			IndexType  tinyint,
			SetFillFactor tinyint,
			TableCreateDate datetime2(2),
			TableModifyDate datetime2(2),
			PrepareDate datetime2(2),
			[PageCount]  bigint,
			AVG_Fragm_percent tinyint,
			[~PageUsed_perc] tinyint,
			[~Row_cnt] bigint,
			[~RowSize_Kb] numeric(9,3),
			LastUpdateStats datetime2(2),
			LastCommand nvarchar(500),
			LastRunDate datetime2(2),
			ReindexCount int default 0,
			NotRunOnline bit default 0,
			NoReorganize bit default 0
		);
	set @tsql ='use '+QUOTENAME(@db_name_check)+';
		SET NOCOUNT ON;
		SET DEADLOCK_PRIORITY LOW;
		SET LOCK_TIMEOUT 30000;
		SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
		insert into #T_Source (DBName, SchemaName, TableName, IndexName, TableID, IndexID, IndexType,SetFillFactor, TableCreateDate, TableModifyDate, PrepareDate, PageCount, AVG_Fragm_percent,[~PageUsed_perc],[~Row_cnt],[~RowSize_Kb], LastUpdateStats, LastCommand, LastRunDate,NotRunOnline, NoReorganize)
		select 
			'''+QUOTENAME(@db_name_check)+''' as DBName,QUOTENAME(S.name) as SchemaName, T.TableName, I.IndexName, 
			T.object_id as TableID, I.index_id as IndexID,
			 I.type as IndexType, 
			I.Fill_Factor as SetFillFactor, T.create_date as TableCreateDate, 
			T.modify_date as TableModifyDate, getdate() as PrepareDate, null as PageCount, 
			null as AVG_Fragm_percent, 
			NULL as [~PageUsed_perc],
			NULL as [~Row_cnt],
			NULL as [~RowSize_Kb],
			null as LastUpdateStats, 
			null as LastCommand, null as LastRunDate,
			case 
				when LOB.index_id is null then 0
				when LOB.index_id is not null then 1
			end as NotRunOnline,
			case 
				when I.allow_page_locks > 0 then 0
				else 1
			end as NoReorganize
		from
			(
				select 
					object_id, 
					QUOTENAME(name) as IndexName, 
					index_id, 
					type, 
					fill_factor,
					[allow_page_locks]
				from sys.indexes
				where 
					index_id>0  --Исключить Кучи.
					and is_disabled=0 --Только Включенные индексы
			) I
			inner join
			(	
				select 
					QUOTENAME(name) as TableName, 
					object_id,
					schema_id, 
					create_date, 
					modify_date 
				from 
					sys.tables
			) T
				on I.object_id=T.object_id
			inner join sys.schemas S 
				on T.schema_id=S.schema_id 
			left join
			(
				select 
					object_id, 
					index_id 
				from
					(
					select container_id as id
					from sys.allocation_units
					where type=2 --LOB_DATA
					) lob
				inner join sys.partitions p
					on lob.id=p.partition_id
			) LOB
				on LOB.object_id=I.object_id and LOB.index_id=I.index_id
			';
	declare @db_id int, @flag_fail bit, @StrErr varchar(2048), @tt_start datetime2(2), @command_text_log varchar(8000);
	set @flag_fail=0;
	set @StrErr=NULL;
	BEGIN TRY
		set @tt_start=CAST(SYSDATETIME() AS datetime2(2));
		exec (@tsql);
		--Теперь используя команду MERGE синхронизируем информацию по таблицам и индексам между фактическими данными (#T_Source) и таблицей ReindexData
		MERGE
			INTO db_maintenance.ReindexData AS target
			USING #T_Source AS source --(DBName, SchemaName, TableName, IndexName, IndexType,SetFillFactor, TableCreateDate, TableModifyDate, PrepareDate, PageCount, AVG_Fragm_percent, LastUpdateStats, LastCommand, LastRunDate,NotRunOnline)
			ON target.DBName=source.DBName AND target.SchemaName=source.SchemaName AND target.TableName=source.TableName AND target.IndexName=source.IndexName
			WHEN NOT MATCHED THEN
				INSERT (DBName, SchemaName, TableName, IndexName, TableID, IndexID, IndexType,SetFillFactor, TableCreateDate, TableModifyDate, PrepareDate, PageCount, AVG_Fragm_percent, [~PageUsed_perc],[~Row_cnt],[~RowSize_Kb], LastUpdateStats, LastCommand, LastRunDate,NotRunOnline, NoReorganize)
				VALUES (source.DBName, source.SchemaName, source.TableName, source.IndexName, source.TableID, source.IndexID, source.IndexType, source.SetFillFactor, source.TableCreateDate, source.TableModifyDate, source.PrepareDate, source.PageCount, source.AVG_Fragm_percent,source.[~PageUsed_perc],source.[~Row_cnt],source.[~RowSize_Kb], source.LastUpdateStats, source.LastCommand, source.LastRunDate, source.NotRunOnline, source.NoReorganize)
			WHEN NOT MATCHED BY source AND 
				--дополнительное условие - удаляем только строки по текущей БД!
				(target.DBName=QUOTENAME(@db_name_check)) THEN
					DELETE
			WHEN MATCHED AND 
				--дополнительные условия, чтобы не обновлять, если не было никаких изменений!
				(
					target.TableID<>source.TableID OR target.IndexID<>source.IndexID OR target.IndexType<>source.IndexType
					OR target.SetFillFactor<>source.SetFillFactor OR target.TableCreateDate<>source.TableCreateDate
					OR target.TableModifyDate<>source.TableModifyDate OR target.NotRunOnline<>source.NotRunOnline
					OR target.NoReorganize<>source.NoReorganize 
				) 
				--updating information only for already processed indexes or if more than @updateLagInHours have passed since the last update of information
				AND (
					target.LastUpdateStats is null or target.LastUpdateStats < target.LastRunDate
					or datediff(hour,target.LastUpdateStats,@tt_start)>@updateLagInHours
				)
			THEN
				UPDATE SET
					target.TableID=source.TableID, target.IndexID=source.IndexID, target.IndexType=source.IndexType, 
					target.SetFillFactor=source.SetFillFactor, target.TableCreateDate=source.TableCreateDate, 
					target.TableModifyDate=source.TableModifyDate, target.PrepareDate=source.PrepareDate, 
					target.LastUpdateStats=source.LastUpdateStats, target.NotRunOnline=source.NotRunOnline,
					target.NoReorganize=source.NoReorganize
		;	
	END TRY
	BEGIN CATCH
			set @flag_fail=1;
			set @StrErr=/*'Ошибка при подготовке данных по индексам через процедуру [usp_reindex_preparedata]! Текст ошибки: '+*/COALESCE(ERROR_MESSAGE(),'null *Error message is not defined');
	END CATCH
	set @db_id=DB_ID(@db_name_check);
	--Логгируем в историю Обслуживания БД
	set @command_text_log='exec [db_maintenance].[usp_reindex_preparedata] @db_name='''+@db_name_check+''';';
	EXEC db_maintenance.usp_WriteHS 
		@DB_ID=@db_id,
		@Index_Stat_Type=0, --0-Index
		@Command_Type=4, --4-Prepare data for ReIndex (usp_reindex_preparedata)
		@Command_Text_1000=@command_text_log,
		@tt_start=@tt_start,
		@Status=@flag_fail, --0-Success, 1-Fail(Error)
		@Error_Text_1000=@StrErr;

END