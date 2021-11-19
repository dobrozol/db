
/* =============================================
-- Author:		Иванов Андрей (sql1c)
-- Create date: 20.04.2014 (1.0)
-- Description:	usp_RM - REPAIR MIRROR - настройка/ремонт Зеркалирования для указанной БД.
				В основе алгоритма - backup/restore базы с помощью функционала 
   Update:
				03.06.2014 (1.2)
				Переделан алгоритм восстановления зеркальной базы. Теперь используются Дифф. бэкапы, вместо полных.
				Что должно существенно экономить место на дисках обоих серверов и, самое главное, позволяет значительно
				ускорить процесс восстановления! Кроме того, в самом конце

				04.06.2014 (1.22)
				Добавлен алгоритм удаления информации о новом дифф. бэкапе из истории бэкапов! 
				А также в условие очистки добавлена проверка на существование настроек о дифф. бэкапе, которые
				могли остаться с прошлого неудачного запуска этой процедуры.

				10.07.2014 (1.3)
				Существенное изменение алгоритма создания бэкапов/восстановления зеркальной БД из бэкапов:
				Добавлена проверка существования ПОлного бэкапа (причём проверяется возраст: должен быть не старше 7 дней).
				Если полного бэкапа (свежего) нет, тогда он будет создан здесь же. ПРИ ЭТОМ Дифф. бэкап не создаётся!
				Иначе создаётся дифф. бэкап. Также в зависимости от свежести полного бэкапа: восстановление будет из полного 
				либо из полного и дифф. бэкапов.
				
				10.07.2014 (1.32)
				Добавлена проверка настроек Log бэкапов, если их нет, то они добавляются (на основе настроек Full бэкапов).

				31.01.2014 (1.35)
				Изменен алгоритм получения информации о полном бэкапе. Теперь эта информация берётся из новой ХП info.usp_GetLastBackups.
-- ============================================= */
CREATE PROCEDURE backups.usp_RM 
	@ServerProd NVARCHAR(200),
	@ServerMirror NVARCHAR(200),
	@DB NVARCHAR(300)
AS
BEGIN
	SET NOCOUNT ON;
	--Выполнять алгоритм от имени [sa], так как Linked Server должен быть привязан на этот логин.

	EXECUTE AS LOGIN = 'sa';
	
	--Отключаем Зеркалирование на 1-м и 2-м серверах;
	EXEC('
			EXEC
			(''IF EXISTS(SELECT database_id FROM sys.database_mirroring WHERE mirroring_guid IS NOT NULL AND database_id=DB_ID('''''+@DB+'''''))
					ALTER DATABASE ['+@DB+'] SET PARTNER OFF;
			'')
			AT ['+@ServerMirror+'.PECOM.LOCAL]
		');

	EXEC('
		IF EXISTS(SELECT database_id FROM sys.database_mirroring WHERE mirroring_guid IS NOT NULL AND database_id=DB_ID('''+@DB+'''))
				ALTER DATABASE ['+@DB+'] SET PARTNER OFF;
		');
	
	--Проверяем настройки для бэкапа LOg - если их Нет, то добавить!
	IF NOT EXISTS(SELECT [DBName] FROM backups.BackConf WHERE Kind='Log' AND DBName=@DB)
	BEGIN
		INSERT INTO backups.BackConf ([DBName], LocalDir, NetDir, LocalDays, NetDays, Kind)
		SELECT TOP 1
			[DBName], LocalDir+'LOG\', NetDir+'LOG\', 2, 2, 'Log' AS Kind
		FROM backups.BackConf
		WHERE DBName=@DB
	END;

	--Делаем Бэкап Лога Боевой базы на 2-м сервере!
	EXEC	[backups].[usp_StartBackup]
			@type = N'Log',
			@DBFilter = @DB;
	--Сжимаем журнал транзакций сразу после Бэкапа на 2-м сервере:
	EXEC	[db_maintenance].[usp_ShrinkLogFile]
			@db_name = @DB,
			@SetSizeMb = 2048,
			@FileGrowthMb = 128
	--Отключаем Бэкапы Лога для базы на 2-м сервере!
	UPDATE [backups].[BackConf]
		SET [Kind]='XLog'
		WHERE [DBName]=@DB AND [Kind]='Log';

	--Проверяем: есть ли полный бэкап!
	DECLARE @TT TABLE ([DB_name] NVARCHAR(400), Backup_Type VARCHAR(4), BackupFile NVARCHAR(500), ID INT, BackupDate DATETIME2(2), LocalDir NVARCHAR(500), NetDir NVARCHAR(500), CheckLocalFile BIT, CheckNetFile BIT);
	INSERT INTO @TT
		EXEC info.usp_GetLastBackups @DBName=@DB, @Backup_type='Full';

	DECLARE @FullBackupNetFile NVARCHAR(800);
	SELECT @FullBackupNetFile=NetDir+BackupFile
	FROM @TT
	WHERE 
		CheckNetFile = 1
		AND DATEDIFF(minute,BackupDate,SYSDATETIME()) < 10081
		
	DECLARE @NewConf bit = 0;
	--Если Полного Бэкапа нет, то создаём его!
	IF @FullBackupNetFile IS NULL
		EXEC	[backups].[usp_StartBackup]
			@type = N'Full',
			@DBFilter = @DB
	ELSE
	BEGIN
		/*	А если полный бэкап УЖЕ ЕСТЬ:
			Тогда вместо полного бэкапа делаем Дифф. Бэкап.
			При этом сначала проверяем и при необходимости создаём новые настройки
			(на основе настроек Full) для Дифф. Бэкапа в базе sputnik
		*/
		IF NOT EXISTS(SELECT [DBName] FROM backups.BackConf WHERE Kind='Diff' AND DBName=@DB)
		BEGIN
			INSERT INTO backups.BackConf ([DBName], LocalDir, NetDir, LocalDays, NetDays, Kind)
			SELECT [DBName], LocalDir, NetDir, 0, 0, 'Diff' AS Kind
			FROM backups.BackConf
			WHERE  Kind='Full' AND DBName=@DB
			SET @NewConf=1;
		END
		--Делаем Дифф. Бэкап Боевой базы на 2-м сервере!
		EXEC	[backups].[usp_StartBackup]
			@type = N'Diff',
			@DBFilter = @DB
	END
	
	--Выполняем восстановление из Полной копии + Дифф. копии на первом сервере 
	--(в зависимости от того, когда был выполнен Полный бэкап: в этой процедуре или ранее!)
	DECLARE @FullDir NVARCHAR(500),@DiffDir NVARCHAR(500), @FullFile NVARCHAR(300), @DiffFile NVARCHAR(300), @CMD NVARCHAR(1100);
	IF @FullBackupNetFile IS NULL 
	BEGIN
		SELECT	@FullDir=NetDir
		FROM	backups.BackConf
		WHERE	DBName = @DB AND Kind = 'Full';
		SELECT @FullFile=M.[Backup_File]+'.BAK'      
		FROM [backups].[BackupHistory] M
		INNER JOIN
			(
					SELECT [DB_Name], [Backup_Type], MAX(backup_finish_date) AS [BackupDate]
					FROM [backups].[BackupHistory]
					WHERE [DB_Name]=@DB AND [Backup_Type] = 'Full'
					GROUP BY [DB_Name], [Backup_Type]
			) G
		ON M.[DB_Name]=G.[DB_Name] AND M.Backup_Type=G.Backup_Type AND M.backup_finish_date=G.[BackupDate]
		SET @FullBackupNetFile=@FullDir+@FullFile;
	END
	ELSE
	BEGIN
	--Используем восстановление из Дифф. бэкапа, только если Полный бэкап уже был создан раньше, а не в этой процедуре.
		SELECT	@DiffDir=NetDir
		FROM	backups.BackConf
		WHERE	DBName = @DB AND Kind = 'Diff';
		SELECT @DiffFile=M.[Backup_File]+'.BAK'      
		FROM [backups].[BackupHistory] M
		INNER JOIN
			(
					SELECT [DB_Name], [Backup_Type], MAX(backup_finish_date) AS [BackupDate]
					FROM [backups].[BackupHistory]
					WHERE [DB_Name]=@DB AND [Backup_Type] = 'Diff'
					GROUP BY [DB_Name], [Backup_Type]
			) G
		ON M.[DB_Name]=G.[DB_Name] AND M.Backup_Type=G.Backup_Type AND M.backup_finish_date=G.[BackupDate]
	END
	--Восстановление из Полного Бэкапа
	EXEC('
			EXEC
			(''
				EXEC [backups].[usp_RestoreDB_simple] 
				@DBNameTarget=N'''''+@DB+''''', 
				@FromDisk=N'''''+@FullBackupNetFile+''''',
				@NoRecovery=1 	
			'')
			AT ['+@ServerMirror+'.PECOM.LOCAL]
		');
	IF @DiffFile IS NOT NULL
		--Восстановление из Дифф Бэкапа
		EXEC('
				EXEC
				(''
					EXEC [backups].[usp_RestoreDB_simple] 
					@DBNameTarget=N'''''+@DB+''''', 
					@FromDisk=N'''''+@DiffDir+@DiffFile+''''',
					@NoRecovery=1,
					@DiffBack=1 	
				'')
				AT ['+@ServerMirror+'.PECOM.LOCAL]
			');

	--Делаем Бэкап Лога БОЕВОЙ базы на 2-м сервере!
	--Включаем Бэкапы Лога для боевой базы на 2-м сервере!
	UPDATE [backups].[BackConf]
		SET [Kind]='Log'
		WHERE [DBName]=@DB AND [Kind]='XLog';
	EXEC	[backups].[usp_StartBackup]
			@type = N'Log',
			@DBFilter = @DB;
	--Отключаем Бэкапы Лога для базы на 2-м сервере!
	UPDATE [backups].[BackConf]
		SET [Kind]='XLog'
		WHERE [DBName]=@DB AND [Kind]='Log';
	--Восстанавливаем последний Бэкап Лога на первом сервере!
	DECLARE @Dir NVARCHAR(500), @File NVARCHAR(300);
	SELECT
		  @File=[Backup_File]+'.BAK'     
	FROM [backups].[BackupHistory]
	WHERE
		ID = (
				SELECT ID
				FROM [backups].[BackupHistory]
				WHERE
					[DB_Name]=@DB
					AND [Backup_Type]='Log' 
					AND [backup_finish_date] = (SELECT 
													MAX(backup_finish_date)
												FROM [backups].[BackupHistory]
												WHERE
													[DB_Name]=@DB
													AND [Backup_Type]='Log' 
												)
			)
	SELECT	@Dir=NetDir
	FROM	backups.BackConf
	WHERE	DBName = @DB AND Kind = 'XLog';
	EXEC('
			EXEC
			(''
				EXEC [backups].[usp_RestoreDB_simple] 
				@DBNameTarget=N'''''+@DB+''''', 
				@FromLog=N'''''+@Dir+@File+''''',
				@NoRecovery=1 	
			'')
			AT ['+@ServerMirror+'.PECOM.LOCAL]
		')
	
	--Создаём  EndPoint для Зеркалирования (если его нет) на 1-м и 2-м серверах.
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
			AT ['+@ServerMirror+'.PECOM.LOCAL];
		');

	--Включаем Зеркалирование сначала на 1-м сервере, а затем на 2-м (боевом):
	EXEC('
			EXEC
			(''
				ALTER DATABASE ['+@DB+'] SET PARTNER = N''''TCP://'+@ServerProd+'.PECOM.LOCAL:5022'''';
			'')
			AT ['+@ServerMirror+'.PECOM.LOCAL]
	');
	EXEC
		('
			ALTER DATABASE ['+@DB+'] SET PARTNER = N''TCP://'+@ServerMirror+'.PECOM.LOCAL:5022'';
		');

	--Включаем Бэкапы Лога для боевой базы на 2-м сервере!
	UPDATE [backups].[BackConf]
		SET [Kind]='Log'
		WHERE [DBName]=@DB AND [Kind]='XLog';

	--В самом конце делаем очистку от Дифф. бэкапов и удаляем настройки, 
	--только если они были созданы в этой же процедуре ИЛИ существует точно такая же настройка с предыдущего неудачного запуска!

	IF @NewConf=1 OR EXISTS(SELECT DBName FROM backups.BackConf WHERE DBName=@DB AND Kind='Diff' AND LocalDays=0 AND NetDays=0 )
	BEGIN
		EXEC backups.[usp_CleaningBack] @DBFilter=@DB, @type='Diff';
		
		DELETE 
		FROM backups.BackConf
		WHERE DBName=@DB AND Kind='Diff';

		--Дополнительно удаляем из истории Бэкапов информацию о созданном Дифф. бэкапе
		DELETE [backups].[BackupHistory]
		WHERE [ID] = 
		(	SELECT M.[ID]      
			FROM [backups].[BackupHistory] M
			INNER JOIN
			(
					SELECT [DB_Name], [Backup_Type], MAX(backup_finish_date) AS [BackupDate]
					FROM [backups].[BackupHistory]
					WHERE [DB_Name]=@DB AND [Backup_Type] = 'Diff'
					GROUP BY [DB_Name], [Backup_Type]
			) G
			ON M.[DB_Name]=G.[DB_Name] AND M.Backup_Type=G.Backup_Type AND M.backup_finish_date=G.[BackupDate]
		);
	
	END
END