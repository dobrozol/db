
	/* =============================================
	-- Author:		Андрей Иванов (sqland1c)
	-- Create date: 29.11.2013 (1.0)
	-- Description: Returns information about the latest backups (full and log) from the database sputnik
	-- Update:

	-- ============================================= */
	CREATE PROCEDURE info.usp_BackupReport
		@Backup_type varchar(4) = null,
		@xp tinyint = 0,
		@OnlyProblems bit = 0,
		@DBFilter nvarchar(200) = null
	AS
	BEGIN
		SET NOCOUNT ON;
		declare @SQLServer nvarchar(510);
		exec info.usp_GetHostname @Servername=@SQLServer OUT;
		if OBJECT_ID('tempdb..DB') IS NOT NULL
			DROP TABLE #DB;
		CREATE TABLE #DB (name nvarchar(800), [id] int, model nvarchar(800), BackupTypeNeed nvarchar(800));
		if OBJECT_ID('tempdb..src_ag_db') IS NOT NULL
			DROP TABLE #src_ag_db;
		CREATE TABLE #src_ag_db (DB nvarchar(800), [db_id] int, [Role] nvarchar(800), [PartnerReplica] nvarchar(800), [PrimaryReplica] nvarchar(800), sync_state nvarchar(800), health nvarchar(800), DB_State nvarchar(800));
		IF CAST (LEFT (CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(2)),2) AS SMALLINT) >= 11
			--detect AlwaysON AG secondary databases in this server:
			INSERT INTO #src_ag_db (DB, [db_id], [Role], [PartnerReplica], [PrimaryReplica], sync_state, health, DB_State)
				SELECT
					DB_NAME(ag_db.database_id) as DB,
					ag_db.database_id as [db_id],
					ISNULL(arstates.role_desc, '') AS [Role],
					ISNULL(AR.replica_server_name, '') as [PartnerReplica],
					ISNULL(agstates.primary_replica, '') AS [PrimaryReplica],
					ag_db.synchronization_state_desc as sync_state,
					ag_db.synchronization_health_desc as health,
					ag_db.database_state_desc as DB_State 
				FROM sys.dm_hadr_database_replica_states ag_db
				LEFT JOIN sys.dm_hadr_availability_group_states as agstates
					on ag_db.group_id=agstates.group_id	
				LEFT JOIN sys.dm_hadr_availability_replica_states AS arstates
					ON ag_db.replica_id = arstates.replica_id
						and ag_db.group_id=arstates.group_id
				LEFT JOIN sys.availability_replicas as AR
					ON ag_db.replica_id=AR.replica_id
						and ag_db.group_id=AR.group_id
				WHERE ag_db.is_local=1 
					AND ISNULL(arstates.role_desc, '') = 'SECONDARY'
		IF CAST (LEFT (CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(2)),2) AS SMALLINT) >= 11
			INSERT INTO #DB(name,id,model,BackupTypeNeed)
			select 
				name,database_id as id,recovery_model_desc as model,'Full' as BackupTypeNeed
			from sys.databases db
			left join #src_ag_db as src_ag_db 
				ON db.database_id=src_ag_db.[db_id]
			where 
				is_read_only=0 and name not in ('tempdb')
				AND src_ag_db.[db_id] IS NULL 
			UNION ALL
			select 
				name,database_id as id,recovery_model_desc as model,'Log' as BackupTypeNeed
			from sys.databases
			where 
				is_read_only=0 and name not in ('tempdb', 'model')
				and recovery_model_desc<>'SIMPLE'
				AND (sys.fn_hadr_backup_is_preferred_replica(name)=1);
		ELSE
			INSERT INTO #DB(name,id,model,BackupTypeNeed)
			select 
				name,database_id as id,recovery_model_desc as model,'Full' as BackupTypeNeed
			from sys.databases db
			left join #src_ag_db as src_ag_db 
				ON db.database_id=src_ag_db.[db_id]
			where 
				is_read_only=0 and name not in ('tempdb')
				AND src_ag_db.[db_id] IS NULL 
			UNION ALL
			select 
				name,database_id as id,recovery_model_desc as model,'Log' as BackupTypeNeed
			from sys.databases
			where 
				is_read_only=0 and name not in ('tempdb', 'model')
				and recovery_model_desc<>'SIMPLE'
				
		--select * from #DB

		if @xp in (0,2)
		begin
			IF DB_ID('sputnik') is not null
			BEGIN
				DECLARE @TT TABLE ([DB_name] NVARCHAR(400), Backup_Type VARCHAR(4), BackupFile NVARCHAR(500), ID INT, BackupDate DATETIME2(2), LocalDir NVARCHAR(500), NetDir NVARCHAR(500), CheckLocalDir BIT, CheckNetDir BIT, CheckLocalFile BIT, CheckLocalFileOnly BIT, CheckNetFile BIT);
				INSERT INTO @TT
					EXEC info.usp_GetLastBackups @DBName=@DBFilter, @Backup_type=@Backup_type, @CheckOnline=1;

				IF @xp=0
					select DISTINCT 
						@SQLServer as SQLServer, 
						CAST(SYSDATETIME() AS datetime) AS ServerTime,
						[DB_name] AS DBName, 
						CAST(BackupDate AS datetime) as BackupDate, 
						Backup_Type, 
						CASE 
							WHEN CheckLocalDir=0 THEN 'Directrory "'+LocalDir+'" is not available!'
							WHEN CheckLocalFile=0 AND CheckLocalFileOnly=1 THEN LocalDir+REPLACE(BackupFile,'.BAK','.ONLY')
							ELSE LocalDir+BackupFile 
						END AS LocalFile,
						CASE 
							WHEN CheckLocalFile=0 THEN CheckLocalFileOnly
							ELSE CheckLocalFile
						END AS CheckLocalFile,
						CASE
							WHEN NetDir is NULL OR NetDir='' OR LocalDir=NetDir THEN 'Сopying backups disabled'
							WHEN CheckNetDir=0 THEN 'Directrory "'+NetDir+'" is not available!'
							WHEN CheckLocalFile=0 AND CheckLocalFileOnly=1 THEN 'Copy of backup file is not available!'
							ELSE NetDir+BackupFile
						END AS NetFile,
						case
							when NetDir is NULL OR NetDir='' OR LocalDir=NetDir then CheckLocalFile
							else CheckNetFile
						end as [CheckNetFile],
						DATEDIFF(minute,BackupDate,SYSDATETIME()) as BackupAgeInMinutes
					from @TT t
					where LocalDir is not null
				ELSE IF @xp=2
					select DISTINCT 
						getdate() AS tt,
						@SQLServer as SQLServer, 
						Bases.name AS DBName, 
						CAST(t.BackupDate AS datetime) as BackupDate, 
						CASE
							WHEN t.Backup_Type IS NULL THEN Bases.BackupTypeNeed 
							ELSE t.Backup_Type
						END as Backup_Type, 
						CASE 
							WHEN t.[DB_name] IS NULL THEN 'Backup not configured!'
							WHEN t.CheckLocalDir=0 THEN 'Directrory "'+t.LocalDir+'" is not available!'
							WHEN t.CheckLocalFile=0 AND t.CheckLocalFileOnly=1 THEN t.LocalDir+REPLACE(t.BackupFile,'.BAK','.ONLY')
							ELSE t.LocalDir+t.BackupFile 
						END AS LocalFile,
						CASE 
							WHEN t.CheckLocalFile=0 THEN t.CheckLocalFileOnly
							ELSE t.CheckLocalFile
						END AS CheckLocalFile,
						CASE
							WHEN t.[DB_name] IS NULL THEN NULL
							WHEN t.NetDir is NULL OR t.NetDir='' OR t.LocalDir=t.NetDir THEN 'Сopying backups disabled'
							WHEN t.CheckNetDir=0 THEN 'Directrory "'+t.NetDir+'" is not available!'
							WHEN t.CheckLocalFile=0 AND t.CheckLocalFileOnly=1 THEN 'Copy of backup file is not available!'
							ELSE t.NetDir+t.BackupFile
						END AS NetFile,
						case
							when t.NetDir is NULL OR t.NetDir='' OR t.LocalDir=t.NetDir then CheckLocalFile
							else t.CheckNetFile
						end as [CheckNetFile],
						CAST(AllBackups.backup_size_Mb/1024.000 as numeric(9,3)) AS backup_size_Gb,
						AllBackups.backup_compress_ratio,
						DATEDIFF(second, AllBackups.backup_start_date, AllBackups.backup_finish_date) as backup_elapsed_sec
					from #DB as Bases
					LEFT JOIN @TT t ON Bases.name=t.[DB_name] AND (Bases.BackupTypeNeed=t.Backup_Type OR (t.Backup_Type='DIFF' AND Bases.BackupTypeNeed='FULL'))
					LEFT JOIN [backups].[BackupHistory] AllBackups ON t.ID=AllBackups.ID
					WHERE (Bases.BackupTypeNeed=@Backup_type or @Backup_type is NULL) 
			END
		end
		else
		--Report backups for all databases (even if they are not configured in the sputnik database)
		begin
			select DISTINCT
				@SQLServer as SQLServer, 
				CAST(SYSDATETIME() AS datetime) AS ServerTime,
				Bases.name as DBName, Bases.id as DBID, Bases.model, 
				BackupTypeNeed,
				Backups.BackupType as BackupTypeFact,
				Backups.BackupDate, REPLACE(Backups.BackupFile,'.Only','.BAK') as BackupFile,
				info.uf_checkfile(REPLACE(Backups.BackupFile,'.Only','.BAK')) as CheckBackupFile,
				DATEDIFF(minute,Backups.BackupDate,getdate()) as BackupAgeInMinutes,
				CAST(Backups.backup_size_Mb/1024.000 as numeric(9,3)) AS backup_size_Gb
			from
			#DB as Bases
			left join
			(
				select DISTINCT
					BS.database_name as DBName,
					case BS.type 
						when 'D' then 'Full'
						when 'I' then 'Diff'
						when 'L' then 'Log'
					end as BackupType,
					BS.backup_finish_date as BackupDate,
					MAX(MF.physical_device_name) over (partition by BS.database_name,BS.[type],BS.backup_finish_date) as BackupFile,
					CASE 
						WHEN BS.compressed_backup_size IS NULL 
							THEN cast(BS.backup_size/(1024*1024) as decimal(19,3))
						ELSE cast(BS.compressed_backup_size/(1024*1024) as decimal(19,3)) 
					END backup_size_Mb
				from msdb.dbo.backupset BS
				inner join msdb.dbo.backupmediafamily MF
					on BS.media_set_id=MF.media_set_id
				inner join
					(
						select distinct
							database_name,type,max(backup_finish_date) as BackupDate
						from
							msdb.dbo.backupset
						group by
							database_name,type
					) BSmax
					on BS.database_name=BSmax.database_name 
						and (BS.type=BSmax.type)
						and BS.backup_finish_date=BSmax.BackupDate
			)Backups
				on Bases.name=Backups.DBName and (Bases.BackupTypeNeed=Backups.BackupType OR (Bases.BackupTypeNeed='Full' and Backups.BackupType='Diff'))
			where
				(@Backup_type is null or (Bases.BackupTypeNeed=@Backup_type))
				AND (@DBFilter is null or Bases.name=@DBFilter)
				AND (@OnlyProblems = 0 or info.uf_checkfile(REPLACE(Backups.BackupFile,'.Only','.BAK')) <> 1 or ((Backups.BackupType='Log' and DATEDIFF(minute,Backups.BackupDate,getdate())>60) or (Backups.BackupType<>'Log' and DATEDIFF(minute,Backups.BackupDate,getdate())>1600)))

		end;	
	END