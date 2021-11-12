
	/* =============================================
	-- Author:		Андрей Иванов (sqland1c)
	-- Create date: 10.12.2013 (1.0)
	-- Description: Процедура для обслуживания БД! Для указанной в параметре @db_name 
					базы данных производит shrinkfile и изменяет настройки 
					(автоприращение).
					Необязательные параметры @SetSizeMb - до какого размера сжимать в Мб (по умолчанию 1 Гб)
					и  @FileGrowthMb - установка приращений в Мб, если не указан, то изменение настроек не производится.
					Необязательный параметр @Disk- позволяет обработать файлы только на указанном диске (если не задан, то все файлы)!
	-- Update:		17.12.2013 (1.1) - Изменён алгоритм сжатия. Теперь блок try...catch включен в команду, передаваемую в exec.
										А также если произошла ошибка при сжатии, в исключении происходит запись ошибки в журнал SQL Server.
										Теперь в первую очередь будут обработаны файлы с большим размером!
					17.12.2013 (1.2) - Изменён алгоритм отбора файлов. Теперь размеры файлов берутся из системной функции dm_io_virtual_file_stats
									   которая возвращает реальный размер файлов.
					25.04.2014 (1.3) - Добавлен параметр @SetMaxSizeGB - теперь можно установить Limit для файлов в Гб. По умолчанию не используется.
										Также задержка между обработкой файлов снижена до 1 сек (вместо 3).	
					28.04.2014 (1.31) - Добавлена сортировка файлов в случайном порядке!
					30.04.2014 (1.35) - Исправлена критическая ошибка во второй части процедуры. А также добавлен алгоритм получения файлов с большим
										количеством свободных экстентов - только для БД TempDB.
					09.06.2014 (1.4)  - Новый параметр и новый алгоритм. Параметр @AutoShrink (по умолчанию 0), задаёт автоматическое определение нужного
										размера для каждого Файла в базе данных! В случае если этот параметр задан, то параметр @SetSizeMb не используется!
					19.02.2015 (1.41) - Изменил параметры местами. Параметр @AutoShrink поставлен самым последним. 
										Добавлен вывод обработки текущего файла.
					01.03.2016 (1.42) - Добавлен новый параметр @truncateonly - если задан, то при сжатии файлов используется опция TRUNCATEONLY - 
										освобождает все свободное место в КОНЦЕ файла, не перемещает страницы данных внутри файла.
										Аргумент target_size не обрабатывается - параметры @SetSizeMb и @AutoShrink обнуляются!
	-- ============================================= */
	CREATE PROCEDURE [db_maintenance].[usp_ShrinkDB]
		@db_name nvarchar(50),
		@SetSizeMb int = 1024,
		@FileGrowthMb int = null,
		@Disk char(1) = null,
		@SetMaxSizeGB int = NULL,
		@AutoShrink BIT = 0,
		@truncateonly BIT = 0
	AS
	BEGIN
		SET NOCOUNT ON;
		IF @truncateonly=1
		BEGIN
			SET @SetSizeMb=0;
			SET @AutoShrink=0;
		END
		IF @AutoShrink=1
		BEGIN
			CREATE TABLE #FSizeUsed
			(
	
				FName NVARCHAR(100),
				UsedSpaceMB INT
			)
			DECLARE @db NVARCHAR(300), @Str NVARCHAR(1000);
			SELECT @db=name
			FROM sys.databases
			WHERE state_desc='ONLINE'
				AND name=@db_name;
			SELECT @Str=N'USE ['+@db+N']
				INSERT INTO #FSizeUsed
				SELECT 
					name as FName,
					CAST(ROUND(FILEPROPERTY(name, ''SpaceUsed'')/128.00 + 0.5, 0) as INT) AS UsedSpaceMB
				FROM sys.database_files';
			EXEC sp_executesql @Str;
		END

		declare @tstr nvarchar(500), @name nvarchar(30), @tstrMaxSize NVARCHAR(100)='';
		DECLARE @T TABLE (File_ID INT);
		IF @db_name = 'TempDB'
		BEGIN
			--Отдельно для БД TempDB: проверяем количество свободных экстентов в файлах данных.
			INSERT INTO @T
				SELECT file_id
				FROM TempDB.SYS.dm_db_file_space_usage
				WHERE unallocated_extent_page_count > (user_object_reserved_page_count + internal_object_reserved_page_count + version_store_reserved_page_count + mixed_extent_page_count) * 10
		END
		declare tc cursor for
		select 
			mf.Name
		from 
			(
			select database_id, file_id, name, type_desc
			from sys.master_files
			where
				database_id=DB_ID(@db_name) 
				and (@Disk is null or LEFT(physical_name,1)=@Disk) --Зачистка файлов только на указанном диске!
			)mf
		left join sys.dm_io_virtual_file_stats(DEFAULT, DEFAULT) fs			--Отсюда получим реальные размеры файлов БД.
			on mf.database_id = fs.database_id and mf.file_id = fs.file_id
		where
			fs.size_on_disk_bytes / (1024.0*1024.0) > @SetSizeMb	--отбирать только файлы Более @SetSizeMb.
			AND ((@db_name='TempDB' AND mf.type_desc='ROWS' AND mf.file_id IN (SELECT File_ID FROM @T)) OR (@db_name<>'TempDB') OR (mf.type_desc<>'ROWS'))
		order by newid()	--сортировка в случайном порядке!!

		open tc
		fetch next from tc into @name
		while @@FETCH_STATUS = 0 
		begin
			--Сжать все файлы до @SetSizeMb.
			--Если возникла ошибка при работе с текущим файлом - Записать имя файла в ЖУрнал !
		
			IF @AutoShrink=1
				SELECT @SetSizeMb=UsedSpaceMB
				FROM #FSizeUsed
				WHERE FName=@name;

			set @tstr = 'use ['+@db_name+'];
			declare @terror nvarchar(500);
			BEGIN TRY
				PRINT(''Сжатие файла ['+@name+'] в БД ['+@db_name+']'');
				DBCC SHRINKFILE (N''' + @name + ''', '+CASE WHEN @truncateonly = 0 THEN cast(@SetSizeMb as varchar(25)) ELSE ' TRUNCATEONLY ' END+') --Сжатие файлов БД из ХП [db_maintenance].[usp_ShrinkDB]
			END TRY
			BEGIN CATCH
				set @terror=N''Ошибка при сжатии файла ' + @name+ N' базы данных ' + @db_name + N' . Причина : ''+error_message();
				RAISERROR(@terror ,11,1) WITH LOG;
			END CATCH';
			--print @tstr
			exec( @tstr );
			fetch next from tc into @name
			waitfor delay '00:00:01'	--Если всё ОК. то задержка 1 сек перед след. файлом.
		end
		close tc
		deallocate tc

		if @FileGrowthMb is not null
		begin
			--Во второй части запроса происходит принудительная настройка всех файлов в БД TempDB - приращение файла установить в @FileGrowthMb.
			declare tc cursor for
			select name
			from sys.master_files
			where
				database_id=DB_ID(@db_name) 
				and (growth*8)/1024 <> @FileGrowthMb  --Установленное приращение файла не равно FileGrowthMb
				and (@Disk is null or LEFT(physical_name,1)=@Disk) --Зачистка файлов только на указанном диске!
			order by physical_name

			open tc

			fetch next from tc into @name
			while @@FETCH_STATUS = 0 
			begin
				IF @SetMaxSizeGB IS NOT NULL
					SET @tstrMaxSize=N', MAXSIZE = '+CAST(@SetMaxSizeGB AS NVARCHAR(25))+'GB ';
				set @tstr='ALTER DATABASE ['+@db_name+'] MODIFY FILE ( NAME = '''+@name+''', FILEGROWTH = '+cast(@FileGrowthMb as nvarchar(25))+'MB '+@tstrMaxSize+')';
				--print @tstr;
				EXEC( @tstr )
				fetch next from tc into @name
			end
			close tc
			deallocate tc
		end

	END