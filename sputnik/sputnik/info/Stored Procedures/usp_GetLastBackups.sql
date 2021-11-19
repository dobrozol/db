
/* =============================================
-- Author:		Андрей Иванов (sqland1c)
-- Create date: 29.07.2014 (1.0)
-- Description:	Эта процедура возвращает информацию по последним выполненным бэкапам из БД Спутник!
				При этом сразу проверяется доступность файлов бэкапов!
-- Update:		25.08.2014 (1.01)
				Добавлена проверка: БД, по которой получаем Инфо существует на сервере и её
				состояние = Online и эта БД не для чтения.
				20.11.2014 (1.05)
				Изменен алгоритм дополнительной проверки БД (база online и не для чтения!). Во-первых, теперь по-умолчанию
				это проверка не производится. Во-вторых, чтобы проверка производилась, нужно задать новый параметр @CheckOnline!
				11.01.2016 (1.10)
				Расширен алгоритм проверки бэкапов: добавлены проверки существования каталогов и файла ONLY.
-- ============================================= */
CREATE PROCEDURE info.usp_GetLastBackups
	@DBName NVARCHAR(400) = NULL,
	@Backup_Type VARCHAR(4) = NULL,
	@ToDate DATETIME2(2) = NULL,
	@CheckOnline bit = 0
AS
BEGIN
	SET NOCOUNT ON;
	;with cte_1 AS
	(
		SELECT
			  AllBackups.[DB_name],
			  LastBackups.Backup_Type, 
			  AllBackups.Backup_File+'.BAK' AS BackupFile, 
			  LastBackups.ID, 
			  LastBackups.backup_finish_date as BackupDate,
			  CatalogInfo.LocalDir,
			  CatalogInfo.NetDir,
			  info.uf_checkfile(CatalogInfo.LocalDir) as CheckLocalDir,
			  info.uf_checkfile(CatalogInfo.NetDir) as CheckNetDir,
			  info.uf_checkfile(CatalogInfo.LocalDir + AllBackups.Backup_File+'.BAK') as CheckLocalFile,
			  info.uf_checkfile(CatalogInfo.LocalDir + AllBackups.Backup_File+'.ONLY') as CheckLocalFileOnly, 
			  info.uf_checkfile(CatalogInfo.NetDir + AllBackups.Backup_File+'.BAK') as CheckNetFile
		FROM
		(
			SELECT [Backup_Type], 
				MAX(ID) AS [ID],
				MAX(backup_finish_date) AS [backup_finish_date]
			FROM
				[backups].[BackupHistory] bh
			WHERE
				(@DBName IS NULL OR [DB_Name]=@DBName)
				AND (@CheckOnline=0 OR [DB_Name] IN (select name from sys.databases where [state]=0 and is_read_only=0))
				AND (@Backup_Type IS NULL OR [Backup_Type]=@Backup_Type)
				AND (@ToDate IS NULL OR [backup_finish_date] <= @ToDate)
			GROUP BY [DB_Name], [Backup_Type]
		) LastBackups
		INNER JOIN
			[backups].[BackupHistory] AllBackups ON LastBackups.ID=AllBackups.ID
		CROSS APPLY
			info.uf_GetBackConf(AllBackups.[DB_Name], LastBackups.Backup_Type, AllBackups.backup_start_date) AS CatalogInfo
	)
	SELECT 
		[DB_name],Backup_Type,BackupFile,ID,BackupDate,
		LocalDir,NetDir,
		CheckLocalDir,CheckNetDir,
		CheckLocalFile,
		CheckLocalFileOnly,
		CheckNetFile
	FROM cte_1
	ORDER BY
		[DB_name], BackupDate
END