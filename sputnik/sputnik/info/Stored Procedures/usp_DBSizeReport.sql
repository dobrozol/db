
	/* =============================================
	-- Author:		Андрей Иванов (sqland1c)
	-- Create date: 13.03.2014
	-- Description:	Эта процедура выдаёт информацию о размерах всех БД на сервере в разрезе: 
				ИмяБазыДанных
				Состояние
				РежимВосстановления
				РазмерВсейБД 
				КоличествоФайловДанных 
				РазмерВсехФайловДанных 
				НеиспользуемоеПространствоВФайлахДанных 
				КоличествоФайловЛогов 
				РазмерВсехФайловЛогов
				НеиспользуемоеПространствоВФайлахЛогов 
				
	-- Update:		28.07.2014 (1.1)
				В запросы добавлено условие: name NOT LIKE 201%(%)% , для того чтобы исключить
				базы данных Обмена. Из-за этих баз периодически возникают ошибки в этом Отчёте.

				04.10.2014 (1.3)
				Существенное изменение алгоритма! Во-первых, теперь вся информация полностью
				берётся из представления sys.database_files. Во-вторых, добавлен учет размеров FILESTREAM.
				В-третьих, добавлены параметры @DBFilter - возможность фильтра по конкретной базе,
				и @details - будет выводится детализация по файлам и файловым группам.

				29.10.2014 (1.31)
				В запросе, где получаем список БД добавлено ещё одно  условие name NOT LIKE 'S201%(%)%',
				чтобы исключить базы данных обмена (начинающихся на S).

				22.08.2015 (1.35)
				Добавлен новый параметр @GetFreeSpaceMb - возвращает свободное место в дата-файлах БД (в Мб).
				Нужно например для мониторинга свободного места в базе tempdb.

				10.09.2015 (1.36)
				Получение свободного места по базе tempdb переделано на получение информации из счетчика производительности.
				10.09.2015 (1.37)
				Добавлены новые параметры @GetLogFilesSize и @GetLogFilesSizeUsed для получения места занимаемого файлами журналами транзакций в БД.
				Причем размер указывается в Гб и берется из счетчика производительности.

				05.08.2016 (1.40)
				Новая версия. Изменён алгоритм: сначала отбираем общие данные из sys.master_files.
				Затем по каждой БД получаем инфо о ФГ и о неиспользуемом месте из sys.database_files.
				Также в вывод добавлен столбец LogBusy - показывает для лог-файлов причину
				распухания (это столбец log_reuse_wait_desc представления sys.databases).

				26.08.2016 (1.41)
				Небольшое исправление - изменены имена возвращаемых столбцов.
				Также в результат добавлен DISTINCT, чтобы исключить дубликаты.

				29.08.2016 (1.42)
				Небольшое исправление - связь между master_files и database_files теперь по FILE_ID
				(до этого было по FileName).

				08.11.2016 (1.430)
				Исправление IIF на CASE для обратной совместимости с 2008R2 (и младше!)

				26.12.2016 (1.435)
				Добавлен новый параметр @GetAllSizeGb - получение общего размера для указанной БД в Гб.
			
				16.01.2017 (1.440)
				Используемый размер файлов БД теперь получаем из sys.database_files. Если же база или файл недоступен, тогда берётся из sys.master_files

				28.03.2017 (1.450)
				В вывод добавлена информация об заданном лимите файлов (MaxSizeMb). Если лимит не задан, то будет 0.00.
				При выводе без детализации (@details=0), MaxSizeMb будет представлен, как СУММА по типу файлов. Но если 
				для какого-то файла с этим же типом MaxSize=0.00, то и вся сумма будет тоже 0.00 (то есть лимит не задан).
				При выводе с детализацией (@details=1),  MaxSizeMb выводится для каждого файла.

				11.04.2017 (1.455)
				Если задан параметр @GetAllSizeGb и @DBFilter не указан - то будет показан размер по всем базам.

				11.04.2017 (1.470)
				Добавлены новые параметры @DBList и @DBList_delimeter: первый параметр определяет список баз
				в виде строки, разделённых Разделителем @DBList_delimeter (по умолчанию это Запятая).
				Теперь можно получать размеры по списку баз. Старый параметр @DBFilter - отбор по конкретной базе.
			
				17.11.2017 (1.471)
				Увеличены размеры строковых переменных, связанных с именем БД до 1000 символов, а для переменной @Str до MAX.

				29.12.2017 (1.472)
				Увеличены размеры строковых переменных (имя БД) до 2000 символов.

				24.01.2018 (1.473)
				В вывод добавлена информация о владельце базы (DBOwner).

				01.02.2018 (1.500)
				Изменён алгоритм получения актуальных данных из sys.database_files. Теперь если нет данных из sys.database_files,
				то такие данные удаляем из общей таблицы!
				Также добавлен новый ВЫХОДНОЙ параметр @ReturnNum. Теперь через него можно вернуть значение для @GetFreeSpaceMb и @GetAllSizeGb 
				(при этом  @ReturnNum также должен быть задан при вызове процедуры).
			
				23.02.2018 (1.502)
				Для определения правильного имени сервера SQL теперь 
				используется процедура info.usp_getHostname	
			
				09.04.2018 (1.505)
				Возврат значений для @GetLogFilesSizeUsed и @GetLogFilesSize через выходной параметр @ReturnNum		
	-- ============================================= */
	CREATE PROCEDURE info.usp_DBSizeReport
		@DBFilter nvarchar(2000)=null,@details bit = 0,
		@GetFreeSpaceMb bit=0, @GetLogFilesSize bit=0,
		@GetLogFilesSizeUsed bit=0, @GetAllSizeGb bit=0,
		@DBList NVARCHAR(MAX) = NULL,
		@DBList_delimeter NVARCHAR(10)=',',
		@ReturnNum numeric(19,3)=NULL OUTPUT
	AS
	BEGIN
		SET NOCOUNT ON;
		declare @sql NVARCHAR(MAX), @dblist_fmt NVARCHAR(MAX);
		--Здесь получаем правильное имя SQL Server:
		declare @servername nvarchar(510);
		exec info.usp_GetHostname @Servername=@Servername OUT;
		IF @DBList > N'' AND @DBList IS NOT NULL
		BEGIN
			--11.04.17 Новый алгоритм обработки @DBList (список БД в виде строки);
			set @dblist_fmt = 'N'''+REPLACE(@DBList, @DBList_delimeter,''' , N''')+'''';
			set @dblist_fmt = REPLACE(@dblist_fmt,'N'''' , ','');
			set @dblist_fmt = REPLACE(@dblist_fmt,' , N''''','');
		END
		CREATE TABLE #x(DB Nvarchar(2000));
		SET @sql = N'SELECT name FROM sys.databases WHERE 1=1'
		+ CASE WHEN @dblist_fmt IS NOT NULL THEN ' AND name IN (' + @dblist_fmt + ')'
			   ELSE '' 
		  END;
		INSERT #x EXEC sp_executesql @sql;

		IF (@GetFreeSpaceMb=1 /*OR @GetAllSizeGb=1*/) and (@DBFilter is null OR @DBFilter='')
			set @DBFilter='tempdb';
		--По базе TempDB быстро получаем свободное место из счетчика производительности:
		if @GetFreeSpaceMb=1 and @DBFilter='tempdb'
		begin
			select 
				cast(cast(cntr_value as numeric(19,2))/(1024.00*1024.00) as numeric(19,2)) as FreeSpaceGb
			from sys.dm_os_performance_counters
			where counter_name='Free Space in tempdb (KB)';
			return;
		end
		--Получаем информацию о размерах файлах журналов транзакций в базе из счетчика производительности
		if @GetLogFilesSize=1 or @GetLogFilesSizeUsed=1
		begin
			declare @logsize_res numeric(19,2);
			;with cte_src AS (
				select 
					QUOTENAME(@DBFilter)+' Log Files Size (Gb)' as counter_name,
					case rtrim(counter_name)
						when 'Log File(s) Size (KB)' then '_Total'
						when 'Log File(s) Used Size (KB)' then '_Used'
					end as instance_name,
					cast(cast(cntr_value as numeric(19,2))/(1024.00*1024.00) as numeric(19,2)) as value
				from sys.dm_os_performance_counters
				where counter_name like 'Log File(s)% Size (KB)%'
					and rtrim(instance_name)=@DBFilter
			)
			select @logsize_res=[value]
			from cte_src
			where instance_name=CASE WHEN @GetLogFilesSize=1 THEN '_Total' ELSE '_Used' END
			;	
			IF @ReturnNum is not null
				set @ReturnNum=@logsize_res;
			else
				select @logsize_res as [Gb];

			return;
		end

		--Первая часть: Создаем временную таблицу и в неё загружаем основную информацию о размерах из представлений sys.master_files и sys.Database_Files(в каждой базе своя).
		IF OBJECT_ID('tempdb..#FSizeAll') is not null
			DROP TABLE #FSizeAll;
		CREATE TABLE #FSizeAll
		(
	
			DB Nvarchar(2000),
			[DBID] int,
			F_ID int, -- File_ID
			FName SYSNAME,
			FType NVARCHAR(60),
			FState nvarchar(60),
			FG NVARCHAR(900),
			SizeMb DECIMAL(12,2),
			MaxSizeMb DECIMAL(12,2),
			FreeSpaceMB DECIMAL(12,2),
			LogBusy nvarchar(60),
			DBState nvarchar(60),
			DBCreateDate datetime,
			DBReadOnly bit,
			DBRecoveryModel nvarchar(60),
			DBOwner nvarchar(128)
		);

		--Сначала в итоговую таблицу запишем информацию из sys.master_files
		INSERT INTO #FSizeAll
			([DBID], DB, F_ID, FName, FType, [FState], FG, SizeMb, MaxSizeMB, FreeSpaceMB, LogBusy, [DBState], DBCreateDate, DBReadOnly, DBRecoveryModel,DBOwner)
		SELECT 
			DBinfo.database_id AS [DBID],
			DBinfo.name AS DB,
			mf.[file_id] as F_ID,
			mf.name as FName,
			type_desc AS FType,
			mf.[state_desc] AS [FState],
			NULL as FG,
			cast(size/128.0 as decimal(12,2)) as SizeMB,
			CASE 
				WHEN mf.max_size=-1 THEN 0 
				WHEN mf.type_desc='LOG' and mf.max_size=268435456 THEN 0
				ELSE cast(mf.max_size/128.0 as decimal(12,2))
			END as MaxSizeMB,
			NULL AS FreeSpaceMb,
			CASE 
				WHEN mf.type_desc<>'LOG' THEN ''
				WHEN DBinfo.log_reuse_wait_desc='NOTHING' THEN ''
				ELSE DBinfo.log_reuse_wait_desc
			END AS LogBusy,
			DBinfo.state_desc AS [DBState],
			DBinfo.create_date AS DBCreateDate,
			DBinfo.is_read_only AS DBReadOnly,
			DBinfo.recovery_model_desc AS DBRecoveryModel,
			suser_sname(DBinfo.owner_sid) as DBOwner
		FROM sys.master_files as mf 
		left join sys.databases as DBinfo
			ON mf.database_id=DBinfo.database_id
		inner join #x as DBList
			ON DBinfo.[name]=DBList.DB
		WHERE (DBinfo.name=@DBFilter OR @DBFilter is null)
			AND (DBinfo.name NOT LIKE '201%(%)%' and DBinfo.name NOT LIKE 'S201%(%)%')
		;

		IF @GetAllSizeGb=1
		BEGIN
			Select TOP 1 cast(SUM(SizeMB)/1024.0 as decimal(12,3)) AS AllSizeGb
			From #FSizeAll;
			IF @ReturnNum is not null
				Select TOP 1 @ReturnNum=cast(SUM(SizeMB)/1024.0 as decimal(12,3))
				From #FSizeAll;
			return;
		END

		--Теперь по каждой базе получаем информацию из sys.database_files (если база и файлы доступны)
		-- и обновляем временную таблицу #FSizeAll!
		DECLARE @db Nvarchar(2000), @Str NVARCHAR(MAX);
		DECLARE Db CURSOR FOR
		SELECT DISTINCT DB AS name
		FROM #FSizeAll F
		WHERE [FState]='ONLINE'
			AND [DBState]='ONLINE'
		OPEN Db
		FETCH NEXT FROM Db INTO @db
		WHILE @@FETCH_STATUS=0
		BEGIN
			SELECT @Str=N'USE ['+@db+N'];
			DECLARE @DBFiles TABLE (F_ID int, FName nvarchar(128), FG nvarchar(128), FreeSpaceMb decimal(19,2), SizeMb decimal(19,2),MaxSizeMB decimal(19,2));
			INSERT INTO @DBFiles (F_ID, FName, FG, FreeSpaceMb, SizeMb, MaxSizeMB)
			SELECT 
				[file_id] as F_ID,
				name as FName,
				ISNULL(FILEGROUP_NAME(data_space_id),'''') as FG,
				CASE
					WHEN type_desc=''FILESTREAM'' THEN 0.00
					ELSE cast(size/128.0 as decimal(19,2)) - cast(FILEPROPERTY(name,''SpaceUsed'')/128.0 as decimal(19,2))
				END AS FreeSpaceMb,
				cast(size/128.0 as decimal(12,2)) as SizeMb,
				CASE 
					WHEN max_size=-1 THEN 0 
					WHEN type_desc=''LOG'' and max_size=268435456 THEN 0
					ELSE cast(max_size/128.0 as decimal(19,2))
				END as MaxSizeMB
			FROM sys.database_files;
			--Обновляем существующие данные в таблице из sys.database_files:
			UPDATE Upd
			SET Upd.FG=F.FG, Upd.FreeSpaceMb=F.FreeSpaceMb, Upd.SizeMb=F.SizeMb, Upd.MaxSizeMB=F.MaxSizeMB
			FROM #FSizeAll Upd
			INNER JOIN @DBFiles F
				ON Upd.F_ID=F.F_ID
			WHERE Upd.DB='''+@db+N'''
			;
			--Удаляем данные в таблице, которых нет в sys.database_files
			DELETE Del
			FROM #FSizeAll Del
			LEFT JOIN @DBFiles F
				ON Del.F_ID=F.F_ID
			WHERE Del.DB='''+@db+N''' AND F.F_ID IS NULL
			;';
			EXEC sp_executesql @Str;
			FETCH NEXT FROM Db INTO @db
		END
		CLOSE Db;
		DEALLOCATE Db;
		--Часть вторая: строим результирующую выборку на основании полученных данных во временной таблицы.
		--Причем можно получить сгруппированную по типам файлов информацию или детализированную информацию (с выводом Файловых групп и имен файлов).
	
		IF @details=0
			IF @GetFreeSpaceMb=0
				select distinct
					@Servername as SQLServerName, 
					DBSize.DB, DBSize.DBCreateDate as CreateDate, 
					DBSize.DBState as [State], DBSize.DBReadOnly as [ReadOnly], 
					ISNULL(Mrr.mirroring_role_desc,'') as MirroringRole,
					cast(SUM(SizeMB) OVER (PARTITION BY DBSize.[DBID])/1024.0 as decimal(12,3)) AS AllSizeGb,
					DBSize.DBRecoveryModel as RecoveryModel, DBSize.FType as FilesType, 
					SUM(DBSize.SizeMb) OVER (PARTITION BY DBSize.[DBID], DBSize.FType ) AS SizeMb,
					SUM(DBSize.FreeSpaceMB) OVER (PARTITION BY DBSize.[DBID], DBSize.FType ) AS FreeSpaceMB,
					CASE 
						WHEN MIN(DBSize.MaxSizeMB) OVER (PARTITION BY DBSize.[DBID], DBSize.FType )=0.00 THEN 0.00
						ELSE SUM(DBSize.MaxSizeMB) OVER (PARTITION BY DBSize.[DBID], DBSize.FType )
					END AS MaxSizeMB,
					DBSize.LogBusy,
					DBSize.DBOwner
				from #FSizeAll as DBSize
				left join sys.database_mirroring Mrr
					ON Mrr.database_id=DBSize.[DBID] and mirroring_guid is not null
				order by DBSize.DB,DBSize.FType
			ELSE
			BEGIN
				Select distinct CAST((SUM(FreeSpaceMb) over (partition by DB, FType))/1024.00 as numeric(19,2)) as FreeSpaceGb
				From #FSizeAll
				Where FType='ROWS';
				IF @ReturnNum is not null
					Select TOP 1 @ReturnNum=CAST((SUM(FreeSpaceMb) over (partition by DB, FType))/1024.000 as numeric(19,3))
					From #FSizeAll
					Where FType='ROWS';	
			END
		ELSE
			select distinct
				@Servername as SQLServerName, 
				DBSize.DB, DBSize.DBCreateDate AS CreateDate, 
				DBSize.DBState AS [State], DBSize.DBReadOnly AS [ReadOnly], 
				ISNULL(Mrr.mirroring_role_desc,'') as MirroringRole,
				cast(SUM(SizeMB) OVER (PARTITION BY DBSize.[DBID])/1024.0 as decimal(12,3)) AS AllSizeGb,
				DBSize.DBRecoveryModel AS RecoveryModel, DBSize.FType as FilesType, 
				DBSize.FG, DBSize.FName, DBSize.FState,
				SUM(DBSize.SizeMb) OVER (PARTITION BY DBSize.[DBID], DBSize.FName ) AS SizeMb,
				SUM(DBSize.FreeSpaceMB) OVER (PARTITION BY DBSize.[DBID], DBSize.FName ) AS FreeSpaceMB,
				SUM(DBSize.MaxSizeMB) OVER (PARTITION BY DBSize.[DBID], DBSize.FName ) AS MaxSizeMB,
				DBSize.LogBusy,
				DBSize.DBOwner
			from #FSizeAll as DBSize
			left join sys.database_mirroring Mrr
				ON Mrr.database_id=DBSize.[DBID] and mirroring_guid is not null
			order by DBSize.DB,DBSize.FG,DBSize.FName
	END
GO
GRANT EXECUTE
    ON OBJECT::[info].[usp_DBSizeReport] TO [zabbix]
    AS [dbo];

