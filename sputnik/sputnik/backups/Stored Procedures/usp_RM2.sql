
	/* =============================================
	-- Author:		Иванов Андрей (sql1c)
	-- Create date: 05.08.2014 (2.0)
	-- Description:	usp_RM - REPAIR MIRROR - настройка/ремонт Зеркалирования для указанной БД.
					В основе алгоритма - backup/restore базы с помощью функционала 
	   Update:
					05.08.2014 (2.0)
					Совершенно новая версия ХП usp_RM! Теперь используется новая ХП usp_GC2 для поднятия копии БД на Зеркальном сервере!
					А также добавлены дополнительные проверки! Кроме этого, важные операции завернуты в блок try...catch!
				
					06.10.2014 (2.01)
					Добавлен новый параметр @DisableMirroring. Теперь можно через эту процедуру отключить Зеркалирование для указанной БД.

					06.05.2015 (2.05)
					Для учета именованных экземпляров SQL Server изменен алгоритм определения имени серверов. 
					Теперь в параметры @ServerProd и @ServerMirror нужно передавать имена серверов SQL Server
					без указания полного доменного имени (например, pecom.local не нужно)!
					Также оптимизирован механизм определения имени локального сервера SQL Server.

					20.11.2015 (2.07)
					Добавлены новые параметры @MoveFilesTo и @MoveLogFilesTo для указания, где будут размещены 
					файлы зеркальной БД. Если не задано, тогда автоматом будет выбран диск, где больше всего
					свободного места.

					10.11.2017 (2.080)
					Добавлен новый параметр @UseFreshDiffBack - определяет возможность использования свежих
					Дифф. бэкапов при восстановлении зеркальной БД, а также разрешает создавать новые
					Дифф. бэкапы (может повлиять на производительность основного сервера).
					По умолчанию включен.
	-- ============================================= */
	CREATE PROCEDURE backups.usp_RM2
		@ServerProd NVARCHAR(200),
		@ServerMirror NVARCHAR(200),
		@DB NVARCHAR(300),
		@ForceRM bit = 0,
		@DisableMirroring bit = 0,
		@MoveFilesTo nvarchar(500)=NULL,
		@MoveLogFilesTo nvarchar(500)=NULL,
		@UseFreshDiffBack bit = 1
	AS
	BEGIN
		SET NOCOUNT ON;
		--Выполнять алгоритм от имени [sa], так как Linked Server должен быть привязан на этот логин.
		EXECUTE AS LOGIN = 'sa';
		DECLARE @StrErr NVARCHAR(900);
		--Сначала производим дополнительную проверку: если состояние зеркалирования в норме, тогда НИЧЕГО НЕ ДЕЛАЕМ!
		--Если задан параметр @ForceRM=1, тогда Зеркалирование будет принудительно перенастроено!
		DECLARE @MirrorState VARCHAR(30);
		select @MirrorState=mirroring_state_desc
		from sys.database_mirroring
		where DB_NAME(database_id) =@DB
			and mirroring_guid is not null;
		IF @MirrorState IN ('SYNCHRONIZING','SYNCHRONIZED') AND @ForceRM=0 AND @DisableMirroring=0
		begin
			set @StrErr=N'Выполнение ХП backups.usp_RM2 прервано, т.к. для БД ['+@DB+'] состояние Зеркалирования в норме ('+@MirrorState+').
						  При необходимости, запустите ХП backups.usp_RM2 с параметром @ForceRM=1';
			PRINT(@StrErr);
			return 0;
		end

		--Также дополнительно проверим, что для БД установлен Full Recovery Model!
		DECLARE @RecMod varchar(25);
		SELECT @RecMod=recovery_model_desc
		FROM sys.databases
		WHERE name = @DB
		IF @RecMod<>'FULL' AND @DisableMirroring=0
		begin
			set @StrErr=N'Выполнение ХП backups.usp_RM2 прервано, т.к. для БД ['+@DB+'] режим восстановления <> "FULL" ('+@MirrorState+').
						  Сначала нужно установить Recovery Model=FULL!';
			PRINT(@StrErr);
			return 0;
		end
	
		DECLARE @LocalServer NVARCHAR(600);
		SELECT @LocalServer=CAST(SERVERPROPERTY('MachineName') AS NVARCHAR(300))+COALESCE('\'+CAST(SERVERPROPERTY('InstanceName') AS NVARCHAR(300)),'');
	
		--Отключаем Зеркалирование на 1-м и 2-м серверах;
		EXEC('
				EXEC
				(''IF EXISTS(SELECT database_id FROM sys.database_mirroring WHERE mirroring_guid IS NOT NULL AND database_id=DB_ID('''''+@DB+'''''))
						ALTER DATABASE ['+@DB+'] SET PARTNER OFF;
				'')
				AT ['+@ServerMirror+']
			');

		EXEC('
			IF EXISTS(SELECT database_id FROM sys.database_mirroring WHERE mirroring_guid IS NOT NULL AND database_id=DB_ID('''+@DB+'''))
					ALTER DATABASE ['+@DB+'] SET PARTNER OFF;
			');
		IF @DisableMirroring=1
			return 0;

		--Проверяем настройки для бэкапа LOg - если их Нет, то добавить!
		IF NOT EXISTS(SELECT [DBName] FROM backups.BackConf WHERE Kind IN ('Log','XLog') AND DBName=@DB)
		BEGIN
			INSERT INTO backups.BackConf ([DBName], LocalDir, NetDir, LocalDays, NetDays, Kind)
			SELECT TOP 1
				[DBName], LocalDir+'LOG\', NetDir+'LOG\', 2, 2, 'Log' AS Kind
			FROM backups.BackConf
			WHERE DBName=@DB
		END;

		--Отключаем Бэкапы Лога для базы на Боевом сервере на время поднятия Копии БД на Зеркальном сервере и настройки Зеркала!
		UPDATE [backups].[BackConf]
			SET [Kind]='XLog'
			WHERE [DBName]=@DB AND [Kind]='Log';
	
		--Восстанавливаем Копию БД на зеркальном сервере с помощью новой ХП usp_GC2
		declare @MoveFilesTo_str nvarchar(500), @MoveLogFilesTo_str nvarchar(500);
		select
			@MoveFilesTo_str=
			CASE 
				WHEN @MoveFilesTo IS NULL THEN ''
				ELSE ', @MoveFilesTo=N'''''+@MoveFilesTo+''''''
			END,
			@MoveLogFilesTo_str=
			CASE 
				WHEN @MoveLogFilesTo IS NOT NULL AND @MoveFilesTo IS NOT NULL THEN ', @MoveLogFilesTo=N'''''+@MoveLogFilesTo+''''''
				ELSE ''
			END;

		DECLARE @UseFreshDiffBack_char CHAR(1);
		SET @UseFreshDiffBack_char=CASE @UseFreshDiffBack WHEN 1 THEN '1' ELSE '0' END;
		begin try
			EXEC('
				EXEC
				(''exec backups.usp_GC2 @ServerSource=N'''''+@LocalServer+''''', @DBNameSource=N'''''+@DB+''''', @DBNameTarget=N'''''+@DB+''''',
												@FromCopy=1, @NoRecovery=1, @RunNewBackIfNeed=1, @FreshBack='+@UseFreshDiffBack_char+', @RM=1'+@MoveFilesTo_str+@MoveLogFilesTo_str+',@RunNewDiffBackIfNeed='+@UseFreshDiffBack_char+';
				'')
				AT ['+@ServerMirror+']
			');
		end try
		begin catch
			SET @StrErr=N'Ошибка при выполнении ХП [usp_GC2] для восстановлении копии базы данных на зеркальном сервере! Текст ошибки: '+ERROR_MESSAGE();
			RAISERROR(@StrErr,11,1) WITH LOG
		end catch

		--Создаём EndPoint для Зеркалирования (если его нет) на 1-м и 2-м серверах.
		EXEC('
				IF NOT EXISTS (SELECT endpoint_id FROM sys.endpoints WHERE name = ''EndPoint_Mirroring'')
					CREATE ENDPOINT [EndPoint_Mirroring]
						STATE=started
						AS TCP (listener_port=5022, listener_ip=all)
						FOR database_mirroring (role=partner, authentication=windows negotiate, encryption=required algorithm AES);
			');
		EXEC('
				EXEC(''
					IF NOT EXISTS (SELECT endpoint_id FROM sys.endpoints WHERE name = ''''EndPoint_Mirroring'''')
						CREATE ENDPOINT [EndPoint_Mirroring]
							STATE=started
							AS TCP (listener_port=5022, listener_ip=all)
							FOR database_mirroring (role=partner, authentication=windows negotiate, encryption=required algorithm AES);
					'')
				AT ['+@ServerMirror+'];
			');

		--Включаем Зеркалирование сначала на зеркальном сервере, а затем на боевом:
		begin try
			--Имя конечной точки формируется с учетом имени компьютера (сервера),
			--а имя компьютера формируется из имени SQL Server переданных в параметрах
			--отбрасывается имя экземпляра SQL Server (если оно есть).
			DECLARE @MachineName nvarchar(300);
			SET @MachineName=SUBSTRING(@ServerProd,0,CASE CHARINDEX('\',@ServerProd) WHEN 0 THEN LEN(@ServerProd)+1 ELSE CHARINDEX('\',@ServerProd) END)
			EXEC('
					EXEC
					(''
						ALTER DATABASE ['+@DB+'] SET PARTNER = N''''TCP://'+@MachineName+'.PECOM.LOCAL:5022'''';
					'')
					AT ['+@ServerMirror+']
			');
			SET @MachineName=SUBSTRING(@ServerMirror,0,CASE CHARINDEX('\',@ServerMirror) WHEN 0 THEN LEN(@ServerMirror)+1 ELSE CHARINDEX('\',@ServerMirror) END)
			EXEC
				('
					ALTER DATABASE ['+@DB+'] SET PARTNER = N''TCP://'+@MachineName+'.PECOM.LOCAL:5022'';
				');
		end try
		begin catch
			SET @StrErr=N'Ошибка в ХП [usp_RM2] при попытке включить Зеркалирование! Текст ошибки: '+ERROR_MESSAGE();
			RAISERROR(@StrErr,11,1) WITH LOG
		end catch 

		--Включаем Бэкапы Лога для боевой базы на 2-м сервере!
		UPDATE [backups].[BackConf]
			SET [Kind]='Log'
			WHERE [DBName]=@DB AND [Kind]='XLog';

	END