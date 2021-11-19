
/* =============================================
-- Author:		Андрей Иванов (sqland1c)
-- Create date: 04.08.2014
-- Description:	Эта процедура возвращает цепочку Бэкапов Логов из БД Спутник Начиная с указанного Полного Бэкапа (Если задан @BackupFullID).
				Чтобы учесть, что мог быть сделан Дифф. бэкап, или для отбора бэкапов Лога для наката в LSE 
				производится дополнительный отбор: отбираются более поздние бэкапы Логов, чем бэкап, заданный в @FilterBackupID.
				
-- Update:		21.10.2015 (1.03)
				Добавлен параметр @top - позволяет вывести только указанное количество строк.
				Также добавлен параметр @GetBackupFile - позволяет в результате в столбце BackupFile вывести только имя файла (без полного пути и расширения).
-- ============================================= */
CREATE PROCEDURE info.usp_GetChainLogs
	@DBName NVARCHAR(300),
	@BackupFullID INT = NULL,
	@FilterBackupID INT = NULL,
	@top INT = NULL,
	@ToDate DATETIME2(2) = NULL,
	@fromcopy bit = 0,
	@GetBackupFile bit = 0
AS
BEGIN
	SET NOCOUNT ON;
	declare @DB_LSN numeric(25,0)=NULL;
	IF @BackupFullID IS NOT NULL
	BEGIN
		SELECT @DB_LSN=MIN(database_backup_lsn)
		FROM [backups].[BackupHistory]
		WHERE DB_NAME=@DBName
		AND database_backup_lsn>
			(
				SELECT [database_backup_LSN]
				FROM [backups].[BackupHistory]
				WHERE ID=@BackupFullID
			)
	END
	IF @top IS NULL
		SET @top=1000000000;
	select TOP (@top) 
		case 
			when @GetBackupFile=1 then BACKUP_File					
			when @fromcopy=0 or CatalogInfo.NetDir is null or CatalogInfo.NetDir='' then CatalogInfo.LocalDir+BACKUP_File+'.BAK' 
			else CatalogInfo.NetDir+BACKUP_File+'.BAK' 
		end as BackupFile,
		'Log' as BackupType,
		[ID],
		Backup_Finish_Date
	from
	(
		SELECT DB_NAME, BACKUP_File, Backup_Finish_Date, backup_start_date, ID, last_LSN
		FROM [backups].[BackupHistory]
		WHERE
			DB_NAME=@DBName 
			AND [Backup_Type]='Log'
			AND ([database_backup_lsn]=@DB_LSN OR @DB_LSN IS NULL)
			AND ([ID]>@FilterBackupID OR @FilterBackupID IS NULL) 
			AND ([Backup_Finish_Date]<=@ToDate or @ToDate IS NULL)
	) AS LogBackups
	CROSS APPLY info.uf_GetBackConf(LogBackups.[DB_Name], 'Log', LogBackups.backup_start_date) AS CatalogInfo
	order by LogBackups.last_LSN;
END