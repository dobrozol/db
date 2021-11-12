
	/* =============================================
	-- Author:		Андрей Иванов (sqland1c)
	-- Create date: 22.02.2017 (1.0)
	-- Description:	Эта процедура нужна для обратной подмены боевых таблиц в процессе обновления базы!
					Реализует второй шаг:
					Шаг 2: "После обновления" - возврат боевых таблиц в бой. Перенос изменений в столбцах.
					Переносятся новые столбцы на боевую таблицу.
					Пустая таблица (с именем боевой) переименуется в *_empty
					Боевая таблица (*_prod) возвращается в бой.
					Переносятся новые индексы на боевую таблицу.
	-- Update:		
	-- ============================================= */
	CREATE PROCEDURE anz.usp_retab_step2
		@DB_Name nvarchar(600),	
		@table1c nvarchar(1000),
		@debug bit = 0
	AS
	BEGIN

		--ЧАСТЬ 2 - ПОСЛЕ Обновления
		--**********************************************
		--ВНИМАНИЕ! Этот скрипт нужно выполнить после завершения обновления в 1С И ДО запуска служб 1С (фон и бой)!!!
		--**********************************************

		SET NOCOUNT ON;

			--Вывод на экран,Начало процедуры
		declare @spid varchar(30), @login_time varchar(30), @host varchar(100), @host_ip varchar(100),
		@host_Pid varchar(30), @login nvarchar(400), @program nvarchar(600);
	
		select top 1 
			@spid=cast(a.session_id as varchar(30)), 
			@login_time=convert(varchar(30),a.login_time,126), 
			@host=a.host_name, 
			@host_pid=a.host_process_id, 
			@host_ip=b.client_net_address,
			@login=a.login_name,
			@program=a.program_name
		from sys.dm_exec_sessions a , sys.dm_exec_connections b 
		where a.session_id=b.session_id and a.session_id=@@SPID;

		PRINT(' <<< Запуск процедуры anz.usp_retab_step2. tt='+convert(varchar(30),sysdatetime(),126));

		PRINT('Информации о сессии: SPID='+@spid+'; Login=['+@login+']; LoginTime='+@login_time+'; HostName='+@host+'; HostIP='+@host_ip+'; HostProcessID='+@host_pid+'; Program=['+@program+'];');


		--Проверка БД:
		IF DB_ID(@DB_Name) IS NULL
		BEGIN
			PRINT('НЕ НАЙДЕНА БАЗА ДАННЫХ : '+@DB_Name);
			PRINT ('Укажите верное имя БД в параметре @DB_Name');
			PRINT(' >>> Завершение процедуры anz.usp_retab_step2. tt='+convert(varchar(30),sysdatetime(),126));
			RETURN -1;
		END

		declare @StrErr nvarchar(2000);
		declare @sqlstr nvarchar(max),@sqlstr2 nvarchar(max);
		declare @tt datetime2(2),@tt_end datetime2(2),@elapsed_format varchar(20), @tt_str varchar(60);
		set @tt=sysdatetime();
		set @tt_str=replace(convert(varchar(60),@tt,120),' ','_');
		set @tt_str=replace(@tt_str,'-','');
		set @tt_str=replace(@tt_str,':','');
		--print @tt_str;

		declare @Tname nvarchar(600), @paramsstr nvarchar(800);
	
		set @sqlstr='use ['+@DB_Name+']; select @tablesql_OUT=REPLACE(TABLE_NAME,''_prod'','''') from INFORMATION_SCHEMA.VIEW_TABLE_USAGE WHERE VIEW_NAME = @table1c_IN ;';
		set @paramsstr='@table1c_IN nvarchar(1000), @tablesql_OUT nvarchar(600) OUT';
		exec sp_executesql
			@stmt=@sqlstr,
			@params=@paramsstr,
			@table1c_IN=@table1c,
			@tablesql_OUT=@Tname OUT;


		IF @Tname IS NOT NULL
		BEGIN
			--Проверка: существует ли таблица *_prod:
			declare @Tname_prod nvarchar(800);
			set @Tname_prod=QUOTENAME(@DB_Name)+'.[dbo].'+QUOTENAME(@Tname+'_prod');
			IF OBJECT_ID(@Tname_prod) IS NULL
			BEGIN
				PRINT('НЕ НАЙДЕНА ТАБЛИЦА : '+@Tname_prod);
				PRINT('Возможные варианты: неправильно указано имя таблицы 1С, не выполнена процедура anz.usp_retab_step1, или процедура anz.usp_retab_step2 запущена повторно. Обратитесь к Администратору БД, если возникли трудности!');
				PRINT(' >>> Завершение процедуры anz.usp_retab_step2. tt='+convert(varchar(30),sysdatetime(),126));
				RETURN -2;
			END	

			--ИЗМЕНЕНИЯ В СТОЛБЦАХ
			--в таблицу *_prod добавить новый столбец из *
			--также учитываем изменения в типах

			if object_id('tempdb.dbo.#tmp_clmns') is not null
				drop table #tmp_clmns;
			create table #tmp_clmns(ColumnName nvarchar(600), cmd_SQL nvarchar(max));
			SET @sqlstr='use ['+@DB_Name+'];
			INSERT INTO #tmp_clmns(ColumnName,cmd_SQL)
			SELECT c_new.name as ColumnName,
				CASE 
					WHEN c_prod.column_id IS NULL THEN ''ALTER TABLE [dbo].['+@Tname+'_prod] ADD [''+c_new.name+''] ''+c_new.type+c_new.type_length+c_new.ColNull+c_new.ColDef+'';''
					ELSE ''ALTER TABLE [dbo].['+@Tname+'_prod] ALTER COLUMN [''+c_new.name+''] ''+c_new.type+c_new.type_length+c_prod.ColNull+'';''
				END as cmd_SQL
			FROM
				(select c.name,c.column_id,ct.name as type,
					CASE 
							WHEN ct.name like ''n%char'' THEN ''(''+CAST(c.max_length/2 as varchar(30))+'')''
							WHEN ct.name IN (''char'',''varchar'',''binary'') THEN ''(''+CAST(c.max_length as varchar(30))+'')''
							WHEN ct.name like ''numeric'' THEN ''(''+CAST(c.precision as varchar(30))+'',''+CAST(c.scale as varchar(30))+'')''
							ELSE ''''
						END AS type_length,
					CASE 
						WHEN c.is_nullable=0 THEN '' NOT NULL ''
						ELSE '' NULL ''
					END AS ColNull
				 from sys.columns c inner join sys.types ct on c.user_type_id=ct.user_type_id  where object_id=OBJECT_ID('''+@Tname+'_prod'')) c_prod
			FULL OUTER JOIN 
				(select c.name,c.column_id, ct.name as type, 
					CASE 
						WHEN ct.name like ''n%char'' THEN ''(''+CAST(c.max_length/2 as varchar(30))+'')''
						WHEN ct.name IN (''char'',''varchar'',''binary'') THEN ''(''+CAST(c.max_length as varchar(30))+'')''
						WHEN ct.name like ''numeric'' THEN ''(''+CAST(c.precision as varchar(30))+'',''+CAST(c.scale as varchar(30))+'')''
						ELSE ''''
					END AS type_length,
					CASE 
							WHEN c.is_nullable=1 THEN ''''
							WHEN ct.name like ''date%'' THEN '' default ''''1753-01-01 00:00:00.000'''' ''
							WHEN ct.name like ''binary'' THEN '' default 0x0 ''
							WHEN ct.name like ''%char'' THEN '' default '''''''' ''
							WHEN ct.name like ''%int'' OR ct.name in (''numeric'', ''bit'') THEN '' default 0 ''
							ELSE ''''
					END AS ColDef,
					CASE 
						WHEN c.is_nullable=0 THEN '' NOT NULL ''
						ELSE '' NULL ''
					END AS ColNull
						from sys.columns c
						inner join sys.types ct
						on c.user_type_id=ct.user_type_id
					where object_id=OBJECT_ID('''+@Tname+''')
			)c_new
				ON c_prod.name=c_new.name
			WHERE
				(c_prod.column_id IS NULL OR (c_prod.type<>c_new.type or c_prod.type_length<>c_new.type_length))
				AND OBJECT_ID('''+@Tname+''') IS NOT NULL;
			';
			EXEC(@sqlstr);

			IF EXISTS (select top 1 * from #tmp_clmns)
			BEGIN
				IF @debug=1
				BEGIN
					PRINT '/* ИЗМЕНЕНИЯ В СТОЛБЦАХ ::*/';
				END
			
					declare @c_name nvarchar(200), @c_sqlcmd nvarchar(2000);
					declare C CURSOR FOR
					select * from #tmp_clmns;
					OPEN C;
					FETCH NEXT FROM C INTO @c_name, @c_sqlcmd;
					WHILE @@FETCH_STATUS=0
					BEGIN
						set @c_sqlcmd='use ['+@DB_Name+'];
						'+@c_sqlcmd;

						IF @debug=1
						BEGIN 
							PRINT @c_sqlcmd;
						END
						ELSE 
						BEGIN
							BEGIN TRY
								set @tt=SYSDATETIME() ;
								PRINT 'Попытка: обновляем столбец ['+@c_name+'] на боевой таблице ['+@Tname+'_prod]';
								EXEC(@c_sqlcmd);
								set @tt_end=SYSDATETIME() ;
								set @elapsed_format=CAST(DATEADD(second,(datediff(second,@tt,@tt_end)), CAST('00:00:00' AS TIME(0))) as VARCHAR(20));
								PRINT 'Выполнено ['+@elapsed_format+']: обновлён столбец ['+@c_name+'] на боевой таблице ['+@Tname+'_prod]';
							END TRY
							BEGIN CATCH
								set @StrErr='Ошибка при обновлении столбца ['+@c_name+'] на боевой таблице ['+@Tname+'_prod]! Текст ошибки: '+COALESCE(ERROR_MESSAGE(),'null *Error message is not defined');
								RAISERROR(@StrErr,11,1) WITH LOG
								set @StrErr='Возникла критическая ошибка! Сохраните весь Вывод в текстовый файл и срочно обратитесь к Администратору БД!';
								RAISERROR(@StrErr,11,1) WITH LOG
								PRINT(' >>> Завершение процедуры anz.usp_retab_step2. tt='+convert(varchar(30),sysdatetime(),126));
								RETURN -3;
							END CATCH
						END
						FETCH NEXT FROM C INTO @c_name, @c_sqlcmd;
					END
					CLOSE C;
					DEALLOCATE C;
			
			END
			ELSE
			BEGIN
				PRINT ('Изменений в столбцах не обнаружено!');
			END

				--Перенос новых НЕкластерных индексов в бой!:
			if object_id('tempdb.dbo.#tmp_idxs') is not null
				drop table #tmp_idxs;
			create table #tmp_idxs(DB nvarchar(400), TableName nvarchar(600), index_create_statement nvarchar(max), index_id int, index_name nvarchar(600));
			SET @sqlstr='use ['+@DB_Name+'];
			declare @t1 nvarchar(600), @t2 nvarchar(600);
			select @t1='''+@Tname+'_prod'', @t2='''+@Tname+''';
		
			;with src1 as(
				select OBJECT_NAME(object_id) as TableName, count(*) over (partition by object_id) as Index_cnt, name as IndexName, index_id as IndexID, type as IndexType
				from sys.indexes 
				where object_id=OBJECT_ID(@t1) 
					and index_id>1 
					and name not like ''_Add%''
				--order by name;
			),
			src2 as(
				select OBJECT_NAME(object_id) as TableName, count(*) over (partition by object_id) as Index_cnt, name as IndexName, index_id as IndexID, type as IndexType
				from sys.indexes 
				where object_id=OBJECT_ID(@t2) 
					and index_id>1 
					and name not like ''_Add%''
				--order by name;
			)
			INSERT INTO #tmp_idxs (DB, TableName, index_create_statement,index_id,index_name)
			SELECT 
				DB_NAME() AS DB,
				sc.name + ''.'' + @t1 AS TableName,
				CASE si.index_id WHEN 0 THEN ''/* No create statement (Heap) */''
				ELSE 
					CASE is_primary_key WHEN 1 THEN
						''ALTER TABLE '' + QUOTENAME(sc.name) + ''.'' + QUOTENAME(@t1) + '' ADD CONSTRAINT '' + QUOTENAME(si.name) + '' PRIMARY KEY '' +
							CASE WHEN si.index_id > 1 THEN ''NON'' ELSE '''' END + ''CLUSTERED ''
						ELSE ''CREATE '' + 
							CASE WHEN si.is_unique = 1 then ''UNIQUE '' ELSE '''' END +
							CASE WHEN si.index_id > 1 THEN ''NON'' ELSE '''' END + ''CLUSTERED '' +
							''INDEX '' + QUOTENAME(si.name) + '' ON '' + QUOTENAME(sc.name) + ''.'' + QUOTENAME(@t1) + '' ''
					END +
					/* key def */ ''('' + key_definition + '')'' +
					/* includes */ CASE WHEN include_definition IS NOT NULL THEN 
						'' INCLUDE ('' + include_definition + '')''
						ELSE ''''
					END +
					/* filters */ CASE WHEN filter_definition IS NOT NULL THEN 
						'' WHERE '' + filter_definition ELSE ''''
					END +
					/* with clause - compression goes here */
					CASE WHEN row_compression_partition_list IS NOT NULL OR page_compression_partition_list IS NOT NULL 
						THEN '' WITH ('' +
							CASE WHEN row_compression_partition_list IS NOT NULL THEN
								''DATA_COMPRESSION = ROW '' + CASE WHEN psc.name IS NULL THEN '''' ELSE + '' ON PARTITIONS ('' + row_compression_partition_list + '')'' END
							ELSE '''' END +
							CASE WHEN row_compression_partition_list IS NOT NULL AND page_compression_partition_list IS NOT NULL THEN '', '' ELSE '''' END +
							CASE WHEN page_compression_partition_list IS NOT NULL THEN
								''DATA_COMPRESSION = PAGE '' + CASE WHEN psc.name IS NULL THEN '''' ELSE + '' ON PARTITIONS ('' + page_compression_partition_list + '')'' END
							ELSE '''' END
						+ '')''
						ELSE ''''
					END +
					/* ON where? filegroup? partition scheme? */
					'' ON '' + CASE WHEN psc.name is null 
						THEN ISNULL(QUOTENAME(fg.name),'''')
						ELSE psc.name + '' ('' + partitioning_column.column_name + '')'' 
						END
					+ '';''
				END AS index_create_statement,
				si.index_id,
				si.name AS index_name
			FROM 
			(
				select s2.TableName, s2.IndexName, s2.IndexID, s2.IndexType
					--, s1.IndexID as IndexID_old
				from src2 as s2
				left join src1 as s1
					on s2.IndexName=s1.IndexName
				where
					s1.IndexName IS NULL
					--OR s2.IndexID<>s1.IndexID
			) src_fin';
			set @sqlstr2='
			INNER JOIN sys.indexes AS si
				ON src_fin.IndexID=si.index_id AND src_fin.TableName=OBJECT_NAME(si.object_id)
			JOIN sys.tables AS t ON si.object_id=t.object_id
			JOIN sys.schemas AS sc ON t.schema_id=sc.schema_id
			LEFT JOIN sys.dm_db_index_usage_stats AS stat ON 
				stat.database_id = DB_ID() 
				and si.object_id=stat.object_id 
				and si.index_id=stat.index_id
			LEFT JOIN sys.partition_schemes AS psc ON si.data_space_id=psc.data_space_id
			LEFT JOIN sys.partition_functions AS pf ON psc.function_id=pf.function_id
			LEFT JOIN sys.filegroups AS fg ON si.data_space_id=fg.data_space_id
			/* Key list */ OUTER APPLY ( SELECT STUFF (
				(SELECT '', '' + QUOTENAME(c.name) +
					CASE ic.is_descending_key WHEN 1 then '' DESC'' ELSE '''' END
				FROM sys.index_columns AS ic 
				JOIN sys.columns AS c ON 
					ic.column_id=c.column_id  
					and ic.object_id=c.object_id
				WHERE ic.object_id = si.object_id
					and ic.index_id=si.index_id
					and ic.key_ordinal > 0
				ORDER BY ic.key_ordinal FOR XML PATH(''''), TYPE).value(''.'', ''NVARCHAR(MAX)''),1,2,'''')) AS keys ( key_definition )
			/* Partitioning Ordinal */ OUTER APPLY (
				SELECT MAX(QUOTENAME(c.name)) AS column_name
				FROM sys.index_columns AS ic 
				JOIN sys.columns AS c ON 
					ic.column_id=c.column_id  
					and ic.object_id=c.object_id
				WHERE ic.object_id = si.object_id
					and ic.index_id=si.index_id
					and ic.partition_ordinal = 1) AS partitioning_column
			/* Include list */ OUTER APPLY ( SELECT STUFF (
				(SELECT '', '' + QUOTENAME(c.name)
				FROM sys.index_columns AS ic 
				JOIN sys.columns AS c ON 
					ic.column_id=c.column_id  
					and ic.object_id=c.object_id
				WHERE ic.object_id = si.object_id
					and ic.index_id=si.index_id
					and ic.is_included_column = 1
				ORDER BY c.name FOR XML PATH(''''), TYPE).value(''.'', ''NVARCHAR(MAX)''),1,2,'''')) AS includes ( include_definition )
			/* Partitions */ OUTER APPLY ( 
				SELECT 
					COUNT(*) AS partition_count,
					CAST(SUM(ps.in_row_reserved_page_count)*8./1024./1024. AS NUMERIC(32,1)) AS reserved_in_row_GB,
					CAST(SUM(ps.lob_reserved_page_count)*8./1024./1024. AS NUMERIC(32,1)) AS reserved_LOB_GB,
					SUM(ps.row_count) AS row_count
				FROM sys.partitions AS p
				JOIN sys.dm_db_partition_stats AS ps ON
					p.partition_id=ps.partition_id
				WHERE p.object_id = si.object_id
					and p.index_id=si.index_id
				) AS partition_sums
			/* row compression list by partition */ OUTER APPLY ( SELECT STUFF (
				(SELECT '', '' + CAST(p.partition_number AS VARCHAR(32))
				FROM sys.partitions AS p
				WHERE p.object_id = si.object_id
					and p.index_id=si.index_id
					and p.data_compression = 1
				ORDER BY p.partition_number FOR XML PATH(''''), TYPE).value(''.'', ''NVARCHAR(MAX)''),1,2,'''')) AS row_compression_clause ( row_compression_partition_list )
			/* data compression list by partition */ OUTER APPLY ( SELECT STUFF (
				(SELECT '', '' + CAST(p.partition_number AS VARCHAR(32))
				FROM sys.partitions AS p
				WHERE p.object_id = si.object_id
					and p.index_id=si.index_id
					and p.data_compression = 2
				ORDER BY p.partition_number FOR XML PATH(''''), TYPE).value(''.'', ''NVARCHAR(MAX)''),1,2,'''')) AS page_compression_clause ( page_compression_partition_list )
			WHERE 
				si.type IN (/*0,1,*/2) /* heap, clustered, nonclustered */
			OPTION (RECOMPILE);';
			--SELECT(@sqlstr+@sqlstr2);
			EXEC(@sqlstr+@sqlstr2);
			IF NOT EXISTS(select top 1 * from #tmp_idxs)
			BEGIN
				PRINT ('Изменений в индексах не обнаружено!');
			END
			ELSE
			BEGIN
				IF @debug=1
				BEGIN
					PRINT '/* ИЗМЕНЕНИЯ В ИНДЕКСАХ ::*/';
				END
				DECLARE @TableName nvarchar(600),@index_name nvarchar(600), @index_cmd nvarchar(max);
				DECLARE ix CURSOR FOR
				SELECT DISTINCT 
					TableName,index_name, index_create_statement
				FROM #tmp_idxs;
				OPEN ix;
				FETCH NEXT FROM ix INTO @TableName,@index_name, @index_cmd;
				WHILE @@FETCH_STATUS=0
				BEGIN
					set @index_cmd='use ['+@DB_Name+'];
					'+@index_cmd;
					IF @debug=1
					BEGIN
						PRINT('--Перенос индекса '+QUOTENAME(@index_name)+' для таблицы '+QUOTENAME(@TableName));
						PRINT(@index_cmd);
					END
					ELSE
					BEGIN
						BEGIN TRY
							set @tt=SYSDATETIME() ;
							PRINT 'Попытка: добавляем индекс '+QUOTENAME(@index_name)+' на боевую таблицу '+QUOTENAME(@TableName);
							EXEC(@index_cmd);
							set @tt_end=SYSDATETIME() ;
							set @elapsed_format=CAST(DATEADD(second,(datediff(second,@tt,@tt_end)), CAST('00:00:00' AS TIME(0))) as VARCHAR(20));
							PRINT 'Выполнено ['+@elapsed_format+']: добавлен индекс '+QUOTENAME(@index_name)+' на боевую таблицу '+QUOTENAME(@TableName);
						END TRY
						BEGIN CATCH
							set @StrErr='Ошибка при добавлении индекса '+QUOTENAME(@index_name)+' на боевую таблицу '+QUOTENAME(@TableName)+'! Текст ошибки: '+COALESCE(ERROR_MESSAGE(),'null *Error message is not defined');
							RAISERROR(@StrErr,11,1) WITH LOG
							set @StrErr='Возникла критическая ошибка! Сохраните весь Вывод в текстовый файл и срочно обратитесь к Администратору БД!';
								RAISERROR(@StrErr,11,1) WITH LOG
								PRINT(' >>> Завершение процедуры anz.usp_retab_step2. tt='+convert(varchar(30),sysdatetime(),126));
								RETURN -4;
						END CATCH
					END
					FETCH NEXT FROM ix INTO @TableName,@index_name, @index_cmd;
				END
				CLOSE ix;
				DEALLOCATE ix;
			END

			IF @debug=1
			BEGIN
				PRINT ' ';
				PRINT '/* УБИРАЕМ ПУСТУЮ ТАБЛИЦУ В АРХИВ */';
			END
			--Убираем пустые таблицы, они больше не нужны!
			SET @sqlstr='use ['+@DB_Name+'];
					if object_id(''[dbo].['+@Tname+'_empty]'') is not null
						exec sp_rename @objname = '''+@Tname+'_empty'' ,  @newname =  '''+@Tname+'_archive_empty_'+@tt_str+''',
						@objtype = ''object'' ;
					exec sp_rename @objname = '''+@Tname+''' ,  @newname =  '''+@Tname+'_empty'',
						@objtype = ''object'' ;
				';
			IF @debug=1
			BEGIN
				PRINT @sqlstr;
				PRINT ' ';
				PRINT '/* ВЕРНУТЬ ТАБЛИЦЫ В БОЙ */';
			END
			ELSE
			BEGIN 
				BEGIN TRY
					set @tt=SYSDATETIME() ;
					PRINT 'Попытка: меняем имя пустой копии '+QUOTENAME(@Tname)+' >>> ['+@Tname+'_empty]';
					EXEC(@sqlstr);
					set @tt_end=SYSDATETIME() ;
					set @elapsed_format=CAST(DATEADD(second,(datediff(second,@tt,@tt_end)), CAST('00:00:00' AS TIME(0))) as VARCHAR(20));
					PRINT 'Выполнено ['+@elapsed_format+']: изменено имя пустой копии ['+@Tname+'_empty]';
				END TRY
				BEGIN CATCH
					set @StrErr='Ошибка при изменении имени пустой копии через процедуру [sp_rename]! Текст ошибки: '+COALESCE(ERROR_MESSAGE(),'null *Error message is not defined');
					RAISERROR(@StrErr,11,1) WITH LOG
					set @StrErr='Возникла критическая ошибка! Сохраните весь Вывод в текстовый файл и срочно обратитесь к Администратору БД!';
					RAISERROR(@StrErr,11,1) WITH LOG
					PRINT(' >>> Завершение процедуры anz.usp_retab_step2. tt='+convert(varchar(30),sysdatetime(),126));
					RETURN -5;
				END CATCH
			END
			
			--Вернуть таблицы в БОЙ: переименовать *_prod в *
			SET @sqlstr='use ['+@DB_Name+'];
				IF OBJECT_ID('''+@Tname+'_prod'') IS NOT NULL
					exec sp_rename @objname = '''+@Tname+'_prod'' ,  @newname =  '''+@Tname+''' ,
						@objtype = ''object'' ;
			';
			IF @debug=1
			BEGIN
				PRINT @sqlstr;
			END
			ELSE
			BEGIN 
				BEGIN TRY
					set @tt=SYSDATETIME() ;
					PRINT 'Попытка: меняем имя боевой таблицы (обратно) ['+@Tname+'_prod] >>> '+QUOTENAME(@Tname);
					EXEC(@sqlstr);
					set @tt_end=SYSDATETIME() ;
					set @elapsed_format=CAST(DATEADD(second,(datediff(second,@tt,@tt_end)), CAST('00:00:00' AS TIME(0))) as VARCHAR(20));
					PRINT 'Выполнено ['+@elapsed_format+']: изменено имя боевой таблицы (обратно) '+QUOTENAME(@Tname);
				END TRY
				BEGIN CATCH
					set @StrErr='Ошибка при изменении имени боевой таблицы (обратно) через процедуру [sp_rename]! Текст ошибки: '+COALESCE(ERROR_MESSAGE(),'null *Error message is not defined');
					RAISERROR(@StrErr,11,1) WITH LOG
					set @StrErr='Возникла критическая ошибка! Сохраните весь Вывод в текстовый файл и срочно обратитесь к Администратору БД!';
					RAISERROR(@StrErr,11,1) WITH LOG
					PRINT(' >>> Завершение процедуры anz.usp_retab_step2. tt='+convert(varchar(30),sysdatetime(),126));
					RETURN -5;
				END CATCH
			END
		END
		ELSE
		BEGIN
			PRINT ('ТАБЛИЦА ['+@table1c+'] В SQL НЕ НАЙДЕНА!!!');
			PRINT ('Укажите верное имя таблицы в параметре @Table1C');
		END
		PRINT(' >>> Завершение процедуры anz.usp_retab_step2. tt='+convert(varchar(30),sysdatetime(),126));
	END