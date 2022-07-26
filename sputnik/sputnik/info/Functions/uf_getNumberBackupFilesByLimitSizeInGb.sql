CREATE FUNCTION [info].[uf_getNumberBackupFilesByLimitSizeInGb]
(
	@dbName varchar(500),
	@backupType varchar(4),
	@limitSizeInGb int,
	@skipFilesFull int = 1,
	@skipFilesDiff int = 5,
	@skipFilesLog int = 720
)
RETURNS INT
AS
BEGIN
	declare @result int;
	select @result = max(skipFiles) 
	from (
		select iif(isnull(count(*),0)<1, 1, count(*)) as skipFiles
		from (select backup_start_date, backup_size_Mb,
			sum(backup_size_Mb) OVER(PARTITION BY db_name, backup_type
						ORDER BY backup_start_date desc
						ROWS BETWEEN UNBOUNDED PRECEDING
								 AND CURRENT ROW) AS backup_sum
			from backups.BackupHistory
			where db_name = @dbName and backup_type = @backupType
		) b
		where backup_sum <= @limitSizeInGb * 1024
		union 
			select skipFiles
			from (values 
				(@skipFilesFull, 'Full'), 
				(@skipFilesFull, 'Diff'),
				(@skipFilesLog, 'Log')
			)
			as s(skipFiles, backupType)
			where backupType = @backupType
	) as pz;

	return @result;
END
