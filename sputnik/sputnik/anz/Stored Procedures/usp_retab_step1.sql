
	/* =============================================
	-- Author:		Андрей Иванов (sqland1c)
	-- Create date: 22.02.2017 (1.0)
	-- Description:	Эта процедура нужна для проведения подмены боевых таблиц на пустые в процессе обновления базы!
					Реализует первый шаг:
					Шаг 1: "До обновления" - подмена боевых таблиц на пустые.
					Вместо боевой таблицы создается пустая (без индексов).
					Боевая таблица переименуется в *_prod.

	-- Update:		
	-- ============================================= */
	CREATE PROCEDURE anz.usp_retab_step1
		@DB_Name nvarchar(600),	
		@table1c nvarchar(1000),
		@debug bit = 0
	AS
	BEGIN
		--ЧАСТЬ 1 ДО ОБНОВЛЕНИЯ!
		--**********************************************
		--ВНИМАНИЕ! ДО ВЫПОЛНЕНИЯ ЭТОГО СКРИПТА ДОЛЖНЫ БЫТЬ ОСТАНОВЛЕНЫ СЛУЖБЫ 1С (Бой и Фон)!!!
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

		PRINT(' <<< Запуск процедуры anz.usp_retab_step1. tt='+convert(varchar(30),sysdatetime(),126));

		PRINT('Информации о сессии: SPID='+@spid+'; Login=['+@login+']; LoginTime='+@login_time+'; HostName='+@host+'; HostIP='+@host_ip+'; HostProcessID='+@host_pid+'; Program=['+@program+'];');

		--Проверка БД:
		IF DB_ID(@DB_Name) IS NULL
		BEGIN
			PRINT('НЕ НАЙДЕНА БАЗА ДАННЫХ : '+@DB_Name);
			PRINT ('Укажите верное имя БД в параметре @DB_Name');
			PRINT(' >>> Завершение процедуры anz.usp_retab_step1. tt='+convert(varchar(30),sysdatetime(),126));
			RETURN -1;
		END

		declare @tt datetime2(2),@tt_end datetime2(2),@elapsed_format varchar(20);
		declare @tablesql nvarchar(600);
		declare @StrErr nvarchar(2000);
		declare @tt_str varchar(60);
		declare @sqlstr nvarchar(4000),@paramsstr nvarchar(800);
		set @tt=sysdatetime();
		set @tt_str=replace(convert(varchar(60),@tt,126),' ','_');
		set @tt_str=replace(@tt_str,'-','');
		set @tt_str=replace(@tt_str,':','');
		--print @tt_str;

		set @sqlstr=N'use ['+@DB_Name+']; select @tablesql_OUT=TABLE_NAME from INFORMATION_SCHEMA.VIEW_TABLE_USAGE WHERE VIEW_NAME = @table1c_IN ;';
		set @paramsstr='@table1c_IN nvarchar(1000), @tablesql_OUT nvarchar(600) OUT';
		exec sp_executesql
			@stmt=@sqlstr,
			@params=@paramsstr,
			@table1c_IN=@table1c,
			@tablesql_OUT=@tablesql OUT;

		IF @tablesql is not null
		BEGIN
			--Проверка: существует ли таблица *_prod.
			--Если существует проверим сколько в ней строк и индексов...
			declare @Tname_prod nvarchar(800);
			set @Tname_prod=QUOTENAME(@DB_Name)+'.[dbo].'+QUOTENAME(@tablesql+CASE WHEN @tablesql LIKE '%#_prod' ESCAPE '#' THEN '' ELSE '_prod' END);
			IF OBJECT_ID(@Tname_prod) IS NOT NULL
			BEGIN
				declare @prod_cnt bigint;
				set @sqlstr=N'select @prod_cnt_OUT=count_big(*) from '+@Tname_prod;
				set @paramsstr='@prod_cnt_OUT bigint OUT';
				exec sp_executesql
					@stmt=@sqlstr,
					@params=@paramsstr,
					@prod_cnt_OUT=@prod_cnt OUT;
				IF @prod_cnt>0 
				BEGIN
					PRINT('Таблица '+@Tname_prod+' уже существует. Количество_строк: '+cast(@prod_cnt as varchar(36)));
					PRINT('Возможные варианты: для таблицы 1С ['+@table1c+'] уже выполнили эту процедуру! Теперь нужно выполнить процедуру anz.usp_retab_step2 для обратной замены таблиц! Обратитесь к администратору БД, если возникли трудности!');
					PRINT(' >>> Завершение процедуры anz.usp_retab_step1. tt='+convert(varchar(30),sysdatetime(),126));
					RETURN -2;
				END
			END
			--Переименуем боевую таблицу в _prod
			set @sqlstr='use ['+@DB_Name+'];
				if object_id(''[dbo].['+@tablesql+'_prod]'') is not null
					exec sp_rename @objname = '''+@tablesql+'_prod'' ,  @newname =  '''+@tablesql+'_archive_prod_'+@tt_str+''' ,
					@objtype = ''object'' ;
				exec sp_rename @objname = '''+@tablesql+''' ,  @newname =  '''+@tablesql+'_prod'' ,
					@objtype = ''object'' ;
			'
			IF @debug=1
			BEGIN
				PRINT '/* Переименуем боевую таблицу в _prod */';
				print @sqlstr;
			END
			ELSE
			BEGIN TRY
				set @tt=SYSDATETIME() ;
				PRINT 'Попытка: меняем имя боевой таблицы ['+@tablesql+'] >>> ['+@tablesql+'_prod]';
				EXEC (@sqlstr);
				set @tt_end=SYSDATETIME() ;
				set @elapsed_format=CAST(DATEADD(second,(datediff(second,@tt,@tt_end)), CAST('00:00:00' AS TIME(0))) as VARCHAR(20));
				PRINT 'Выполнено ['+@elapsed_format+']: изменено имя боевой таблицы ['+@tablesql+'_prod]';
			END TRY
			BEGIN CATCH
				set @StrErr='Ошибка при изменении имени боевой таблицы через процедуру [sp_rename]! Текст ошибки: '+COALESCE(ERROR_MESSAGE(),'null *Error message is not defined');
				RAISERROR(@StrErr,11,1) WITH LOG
			END CATCH

			--Создаём пустую копию боевой таблицы (и называем её как боевая)
			set @sqlstr='use ['+@DB_Name+'];
				SELECT *
				INTO [dbo].['+@tablesql+']
				FROM [dbo].['+@tablesql+'_prod]
				WHERE 1=0;
			';
			IF @debug=1
			BEGIN
				PRINT '/* Создаём пустую копию боевой таблицы (и называем её как боевая) */';
				print @sqlstr;
			END
			ELSE
			BEGIN
				BEGIN TRY
					set @tt=SYSDATETIME() ;
					PRINT 'Попытка: создаём пустую копию ['+@tablesql+'] ';
					EXEC (@sqlstr);
					set @tt_end=SYSDATETIME() ;
					set @elapsed_format=CAST(DATEADD(second,(datediff(second,@tt,@tt_end)), CAST('00:00:00' AS TIME(0))) as VARCHAR(20));
					PRINT 'Выполнено ['+@elapsed_format+']: создана пустая копия ['+@tablesql+']';
				END TRY
				BEGIN CATCH
					set @StrErr='Ошибка при создании пустой копии боевой таблицы (select * from ... where 1=0)! Текст ошибки: '+COALESCE(ERROR_MESSAGE(),'null *Error message is not defined');
					RAISERROR(@StrErr,11,1) WITH LOG
				END CATCH
			END
		END;
		ELSE
		BEGIN
			PRINT ('ТАБЛИЦА ['+@table1c+'] В SQL НЕ НАЙДЕНА!!!');
			PRINT ('Укажите верное имя таблицы в параметре @Table1C');
		END
		PRINT(' >>> Завершение процедуры anz.usp_retab_step1. tt='+convert(varchar(30),sysdatetime(),126));
	END