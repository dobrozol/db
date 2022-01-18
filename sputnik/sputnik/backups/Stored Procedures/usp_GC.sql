
/* =============================================
-- Author:		Андрей Иванов (sqland1c)
-- Create date: 23.04.2014 (1.0)
-- Description: usp_GC - GetCopy. Эта процедура позволяет поднять копию базы на локальном сервере на указанный момент времени.
				В основе - восстановление по процедуре usp_RestoreDB_simple (восстановление из Полного бэкапа и из бэкапов Логов).
				Используется информация из базы sputnik, чтобы определить необходимую цепочку бэкапов для восстановления.
				То есть, для успешного выполнения вся необходимая информация должна быть в базе 
				Параметры:
					@DBNameSource - обязательный параметр, это имя базы источника (откуда делаем копию данных);
					@DBNameTarget - необязательный параметр, это имя базы назначения (куда загружам данные). Если не задан, то имя будет
					сформировано автоматически по следующими принципу: @DBNameTarget + дата и время последнего загруженого бэкапа.
					@ServerTarget ???
					@ToDate - необязательный параметр. Это дата и время, на которое нужно сделать копию данных. Если не задан, то текущее
					системное время.
-- Update:
				25.04.2014 (1.1)
				Добавлен оптимизированный алгоритм получения наиболее подходящего каталога с бэкапами через новую функцию info.uf_GetBackConf.

-- ============================================= */
CREATE PROCEDURE [backups].[usp_GC]  
	@DBNameSource nvarchar(300),
	@DBNameTarget nvarchar(300)=NULL,
	@ToDate datetime2(2)=NULL
AS
BEGIN
	SET NOCOUNT ON;
	IF @ToDate IS NULL
		SET @ToDate=SYSDATETIME();
	DECLARE @Dir NVARCHAR(500), @File NVARCHAR(300), @FullPath NVARCHAR(800), @BackupID INT, @BackupFinishDate DATETIME2(2), @BackupStartDate DATETIME2(2), @NetDir NVARCHAR(500);
	DECLARE @WeekDay tinyint, @MonthDay tinyint;

	--Получаем каталог и файл полного бэкапа (по информации из базы sputnik на боевом сервере).
	SELECT
		  @File=[Backup_File]+'.BAK', @BackupID=ID, @BackupFinishDate=backup_finish_date, @BackupStartDate=backup_start_date   
	FROM [backups].[BackupHistory]
	WHERE
		ID = (
				SELECT ID
				FROM [backups].[BackupHistory]
				WHERE
					[DB_Name]=@DBNameSource
					AND [Backup_Type]='Full' 
					AND [backup_finish_date] = (SELECT 
													MAX(backup_finish_date)
												FROM [backups].[BackupHistory]
												WHERE
													[DB_Name]=@DBNameSource
													AND [Backup_Type]='Full'
													AND [backup_finish_date] <= @ToDate
												)
			);

	--Получаем наиболее подходящий Каталог с полными бэкапами через новую функцию
	SELECT @Dir=LocalDir, @NetDir=NetDir
	FROM info.uf_GetBackConf (@DBNameSource,'Full', @BackupFinishDate);
	
	SET @FullPath=@Dir+@File;
	
	--Получение цепочки бэкапов Логов для восстановления (опять же из базы sputnik боевого сервера)
	SELECT BACKUP_File, Backup_Finish_Date
	INTO #T_Logs
	FROM [backups].[BackupHistory]
	WHERE
		DB_NAME=@DBNameSource 
		AND database_backup_lsn=(
			SELECT MIN(database_backup_lsn)
			FROM [backups].[BackupHistory]
			WHERE DB_NAME=@DBNameSource
			AND database_backup_lsn>
				(
					SELECT [database_backup_LSN]
					FROM [backups].[BackupHistory]
					WHERE ID=@BackupID
				)
			)
		AND [Backup_Type]='Log'
		AND [Backup_Finish_Date]<=@ToDate
	ORDER BY last_LSN;
	
	--Восстановление. Если Бэкапы Логов не обнаружены, то просто восстанавливаем полный бэкап.
	IF NOT EXISTS(SELECT [Backup_File] FROM #T_Logs)
	BEGIN
		--Формирование имени новой базы (если не задана)
		IF @DBNameTarget IS NULL
			SET @DBNameTarget=@DBNameSource+'_COPY_'+CONVERT(VARCHAR(8), @BackupFinishDate, 112)+'_'+REPLACE(CONVERT(VARCHAR(20), @BackupFinishDate, 108), ':', '');

		EXEC [backups].[usp_RestoreDB_simple] 
			@DBNameTarget=@DBNameTarget, 
			@FromDisk=@FullPath;
	END
	ELSE
	BEGIN
		--Восстановление из полного бэкапа и цепочки бэкапов Логов.
		IF @DBNameTarget IS NULL
		BEGIN
			--Формирование имени новой базы (если не задана)
			SELECT @BackupFinishDate=MAX([Backup_Finish_Date])
			FROM #T_Logs;
			SET @DBNameTarget=@DBNameSource+'_COPY_'+CONVERT(VARCHAR(8), @BackupFinishDate, 112)+'_'+REPLACE(CONVERT(VARCHAR(20), @BackupFinishDate, 108), ':', '');
		END
		PRINT ('********************************
				Восстановление ПОЛНОГО Бэкапа: '+@FullPath);
		EXEC [backups].[usp_RestoreDB_simple] 
			@DBNameTarget=@DBNameTarget, 
			@FromDisk=@FullPath,
			@NoRecovery=1;
			 	
		DECLARE RE CURSOR FOR
			SELECT [Backup_File]+'.BAK' AS [BackupFile],
					[Backup_Finish_Date]
			FROM #T_Logs;
		OPEN RE;
		FETCH NEXT FROM RE 
		INTO @File, @BackupFinishDate;
		WHILE @@FETCH_STATUS=0
		BEGIN
			--Получаем наиболее подходящий Каталог с полными бэкапами через новую функцию
			SELECT @Dir=LocalDir, @NetDir=NetDir
			FROM info.uf_GetBackConf (@DBNameSource,'Log', @BackupFinishDate);

			SET @FullPath=@Dir+@File;

			PRINT ('********************************
					Восстановление Бэкапа Лога: '+@FullPath);

			EXEC [backups].[usp_RestoreDB_simple] 
				@DBNameTarget=@DBNameTarget, 
				@FromLog=@FullPath,
				@NoRecovery=1
			FETCH NEXT FROM RE 
			INTO @File, @BackupFinishDate;
		END
		CLOSE RE;
		DEALLOCATE RE;
		--Перевод базы данных в режим RECOVERY
		PRINT ('********************************
				Перевод новой базы в режим ONLINE');
		EXEC [backups].[usp_RestoreDB_simple] 
			@DBNameTarget=@DBNameTarget, 
			@ForceRecovery=1
	END;
END