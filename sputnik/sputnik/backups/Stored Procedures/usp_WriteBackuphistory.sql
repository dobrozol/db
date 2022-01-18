
	create proc backups.usp_WriteBackuphistory 
		@db_name nvarchar(300),
		@fg nvarchar(1000)=null,
		@backup_type varchar(4),
		@backup_file nvarchar(260),
		@backup_file_fullname nvarchar(260)
	as
	begin
		set nocount on;
		insert into backups.BackupHistory (DB_Name,FG,Backup_Type,Backup_File,backup_start_date,backup_finish_date,first_LSN,last_LSN,database_backup_LSN,diff_base_LSN,backup_size_Mb,backup_compress_ratio)
		select 
			@db_name, case when @fg='' then NULL else @fg end as FG, @backup_type, @backup_file, 
			BS.backup_start_date,BS.backup_finish_date,BS.first_LSN,BS.last_lsn,BS.database_backup_LSN,BS.differential_base_lsn,
			CASE 
				WHEN BS.compressed_backup_size IS NULL 
					THEN cast(BS.backup_size/(1024*1024) as decimal(19,3))
				ELSE cast(BS.compressed_backup_size/(1024*1024) as decimal(19,3)) 
			END backup_size_Mb,
			CASE 
				WHEN BS.compressed_backup_size IS NULL 
					THEN 0
				ELSE cast(round(BS.backup_size/BS.compressed_backup_size,2) as decimal(5,2)) 
			END as backup_compress_ratio
		from
			msdb.dbo.backupmediafamily BMF
		inner join msdb.dbo.backupset BS
			on BMF.media_set_id=BS.media_set_id
		where
			physical_device_name=@backup_file_fullname
	end