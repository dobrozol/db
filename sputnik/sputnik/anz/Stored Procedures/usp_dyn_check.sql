
	/* =============================================
	-- Author:		Андрей Иванов (sqland1c)
	-- Create date: 23.10.2017 (1.0)
	-- Description:	Эта процедура нужна для проверки и очистки служебной таблицы dbo.Config в базе 1С!
					Небходима для оказания помощи в обновлении 1С.
					При запуске нужно обязательно указать имя БД в которой проводим обновление.
	-- Update:		
	-- ============================================= */
	CREATE PROCEDURE [anz].[usp_dyn_check]
		@DB_Name nvarchar(600),	
		@debug bit = 0
	AS
	BEGIN

		--**********************************************
		--ВНИМАНИЕ! Этот скрипт нужно выполнять только по согласованию с 1С специалистами, когда они производят обновления 1С!!!
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

		PRINT(' <<< Запуск процедуры [anz].[usp_dyn_check]. tt='+convert(varchar(30),sysdatetime(),126));

		PRINT('Информации о сессии: SPID='+@spid+'; Login=['+@login+']; LoginTime='+@login_time+'; HostName='+@host+'; HostIP='+@host_ip+'; HostProcessID='+@host_pid+'; Program=['+@program+'];');


		--Проверка БД:
		IF DB_ID(@DB_Name) IS NULL
		BEGIN
			PRINT('НЕ НАЙДЕНА БАЗА ДАННЫХ : '+@DB_Name);
			PRINT ('Укажите верное имя БД в параметре @DB_Name');
			PRINT(' >>> Завершение процедуры [anz].[usp_dyn_check]. tt='+convert(varchar(30),sysdatetime(),126));
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

		set @sqlstr='use ['+@DB_Name+']; 
		IF NOT EXISTS (SELECT TOP 1 [FileName] FROM [dbo].[Config] WHERE [FileName] = ''DynamicallyUpdated'')
		BEGIN
			IF OBJECT_ID(''dbo.Config_cleaned'') IS NOT NULL
				DROP TABLE dbo.Config_cleaned;
			SELECT * INTO dbo.Config_cleaned FROM dbo.Config; 
			IF EXISTS (SELECT TOP 1 [FileName] FROM [dbo].[Config] WHERE [FileName] LIKE ''%dyn%'')
			BEGIN
				DELETE FROM dbo.Config WHERE [FileName] LIKE ''%dyn%'';
				PRINT(''--Очистка таблицы dbo.Config успешно выполнена.'');
			END
			ELSE
			BEGIN
				PRINT(''--Очистка таблицы dbo.Config пропущена по причине: '');
				PRINT(''---Записей "%dyn%" нет в таблице dbo.Config, база: ['+@DB_Name+']'');
			END
		END
		ELSE
		BEGIN
			PRINT(''--Очистка таблицы dbo.Config пропущена по причине: '');
			PRINT(''---Обнаружена запись "DynamicallyUpdated" в таблице dbo.Config, база: ['+@DB_Name+']'');
		END
		';

		IF @debug=1
		BEGIN 
			PRINT @sqlstr;
		END
		ELSE 
		BEGIN
			BEGIN TRY
				EXEC(@sqlstr);
			END TRY
			BEGIN CATCH
				set @StrErr='Ошибка при очистке таблица dbo.Config в базе ['+@DB_Name+']. Текст ошибки: '+COALESCE(ERROR_MESSAGE(),'null *Error message is not defined');
				RAISERROR(@StrErr,11,1) WITH LOG
				set @StrErr='Возникла критическая ошибка! Сохраните весь Вывод в текстовый файл и срочно обратитесь к Администратору БД!';
				RAISERROR(@StrErr,11,1) WITH LOG
				PRINT(' >>> Завершение процедуры [anz].[usp_dyn_check]. tt='+convert(varchar(30),sysdatetime(),126));
				RETURN -2;
			END CATCH
		END
		PRINT(' >>> Завершение процедуры [anz].[usp_dyn_check]. tt='+convert(varchar(30),sysdatetime(),126));
	END