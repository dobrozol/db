
	/* =============================================
	-- Author:		Андрей Иванов (sqland1c)
	-- Create date: 29.11.2013 (1.0)
	-- Description: Возвращает информацию о последних бэкапах (полный и лога) из БД sputnik.
	-- Update:
					04.02.2014 (2.0)
					Полностью переписан алгоритм процедуры. Теперь показывает все данные, имена файлов бэкапов локально и копии в сети, а также производится проверка файла через спец. процедуру.
					Добавлен параметр @Backup_type, если задан, то отчёт будет только по указанному типу бэкапа.
					Расширенные возможности! Добавлен параметр @xp, если задан 1, то Отчёт строится не по базе sputnik. А по всем базам через системные таблицы базы msdb! 
						При этом если будет задан тип бэкапа, тогда Отчёт будет построен только по созданным бэкапам указанного типа.
					05.02.2014 (2.1)
					Добавлен новый параметр @OnlyProblems. Если задан 1, то в Отчёт будут попадать только Проблемы ( файла бэкапа нет, или дата бэкапа очень старая)!
					По умолчанию 0. Исправления в алгоритме отбора по виду бэкапа.
					17.02.2014 (2.2)
					Добавлен новый алгоритм для более точного определения информации при получении НЕрасширенного отчёта. Используется новая функция uf_GetWeekDay, которая
					определяет день недели для Даты бэкапа. А также доработан алгоритм существования Копии файла (если путь для копии НЕ задан, то определяется существование
					основного файла бэкапа). Кроме этого, доработан алгоритм получения Расширенного отчёта - теперь учитываются Дифф. бэкапы (на равне с полными).					18.02.2014 (2.21)
					Добавлена проверка на состояние базы ReadOnly=0 при получении расширенного отчёта.
					21.02.2014 (2.5)
					Полностью изменён алгоритм получения НЕрасширенного отчета. Теперь используется CTE и один большой запрос разбит на два.
					Всё это сделано, чтобы исправить задвоение в результатах (из-за использования таблицы Daily и Weekly).
					27.05.2014 (2.51) 
					Добавлено DISTINCT в запрос по базе sputnik (нерасширенный вариант). Чтобы исключить появление дубликатов 
					в результатах (когда в таблице реально есть дубликаты).
					10.07.2014 (2.55)
					Добавлен новый параметр @DBFilter - позволяет получить отчёт только по указанной БД.
					28.07.2014 (2.56)
					Добавлено DISTINCT в расширенный отчёт. Чтобы исключить появление дубликатов.
					30.07.2014 (2.7)
					Полностью переделан алгоритм получения НЕрасширенного отчёта (по данным из БД sputnik). Теперь получение всей информации
					происходит из новой ХП info.usp_GetLastBackups.
					30.07.2014 (2.72)
					Учтены входные параметры @DBFilter и @Backup_type при работе НЕрасширенного отчёта. При этом отбор происходит на самом
					раннем этапе (при получении данных из ХП info.usp_GetLastBackups), что ускоряет выполнение всего отчёта
					31.07.2014 (2.73)
					Изменен алгоритм получения имени сервера для НЕрасширенного отчёта: теперь используется SERVERPROPERTY + явная конвертация в nvarchar.
					31.07.2014 (2.74)
					Внесено небольшое изменение: для работы отчета из 1С, нужно чтобы возвращаемые поля с датами были СТАРОГО ТИПА datetime!
					17.11.2014 (2.75)
					В расширенный отчёт добавлен дополнительный отбор для исключения временных баз Обмена 1СПегас!
					20.11.2014 (2.76)
					В вызов ХП usp_GetLastBackups добавлен новый параметр @CheckOnline - чтобы проверять только существующие БД (причём у которых 
					состояние=Online и ReadOnly=False).
					01.12.2015 (2.78)
					SQLServer теперь формируется правильно из SERVERPROPERTY + учитывается именованный экземпляр.
					Также внесён небольшой FIX в расширенный отчет - получение самого последнего имени файла в разрезе БД,типа бэкапа и даты бэкапа.
					Чтобы исключить подобные задвоения.
					11.01.2016 (2.80)
					Расширены проверки и результаты при работе нерасширенного отчета!
					24.05.2016 (2.85)
					Добавлен новый режим @xp=2 - это нерасширенный отчет + дополнительные сведения по бэкапу (время выполнения, размер,
					коэфициент сжатия). Для этого изменён тип параметра @xp - c bit на tinyint.
					Также исправлено формирование имени сервера (теперь правильно учитывается имя сервера и имя экземпляра).
					16.06.2016 (2.86)
					Для режима @xp2 размер бэкапа возвращается теперь в Гб (ранее было в Мб).
					24.08.2016 (2.90)
					Доработан режим работы @xp2, теперь показывает полную информацию (в том числе теперь проверяется полный 
					бэкап для базы model!).
					29.09.2016 (2.92)
					Добавлено новое исключение - для вторичных реплик AlwaysOn теперь не требуется Полный бэкап.
					А для бэкапов Логов проверяем, что бэкапы должны выполняться на текущем сервере через
					функцию: fn_hadr_backup_is_preferred_replica.
					17.10.2016 (2.95)
					Алгоритм процедуры изменён - теперь есть возможность работы на старых экземплярах SQL Server 2008(R2).
					18.10.2016 (2.96)
					Небольшое исправление - фильтр по @Backup_type не работал в режиме @xp=2.
					24.11.2016 (2.963)
					Для расширенного решима (@xp1) теперь также выводиться размер последнего бэкапа.
					23.02.2018 (2.965)
					Для определения правильного имени сервера SQL теперь 
					используется процедура info.usp_getHostname	
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
		exec sputnik.info.usp_GetHostname @Servername=@SQLServer OUT;
		if OBJECT_ID('tempdb..DB') IS NOT NULL
			DROP TABLE #DB;
		CREATE TABLE #DB (name nvarchar(800), [id] int, model nvarchar(800), BackupTypeNeed nvarchar(800));
		if OBJECT_ID('tempdb..src_ag_db') IS NOT NULL
			DROP TABLE #src_ag_db;
		CREATE TABLE #src_ag_db (DB nvarchar(800), [db_id] int, [Role] nvarchar(800), [PartnerReplica] nvarchar(800), [PrimaryReplica] nvarchar(800), sync_state nvarchar(800), health nvarchar(800), DB_State nvarchar(800));
		IF CAST (LEFT (CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(2)),2) AS SMALLINT) >= 11
			--Информация о вторичных репликах AlwaysON AG на текущем сервере:
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
				/*state_desc='ONLINE' and */is_read_only=0 and name not in ('tempdb'/*, 'model'*/)
				AND (name NOT LIKE '201%(%)%' and name NOT LIKE 'S201%(%)%')
				AND src_ag_db.[db_id] IS NULL 
			UNION ALL
			select 
				name,database_id as id,recovery_model_desc as model,'Log' as BackupTypeNeed
			from sys.databases
			where 
				/*state_desc='ONLINE' and */ is_read_only=0 and name not in ('tempdb', 'model')	and recovery_model_desc<>'SIMPLE'
				AND (name NOT LIKE '201%(%)%' and name NOT LIKE 'S201%(%)%') 
				AND (sys.fn_hadr_backup_is_preferred_replica(name)=1);
		ELSE
			INSERT INTO #DB(name,id,model,BackupTypeNeed)
			select 
				name,database_id as id,recovery_model_desc as model,'Full' as BackupTypeNeed
			from sys.databases db
			left join #src_ag_db as src_ag_db 
				ON db.database_id=src_ag_db.[db_id]
			where 
				/*state_desc='ONLINE' and */is_read_only=0 and name not in ('tempdb'/*, 'model'*/)
				AND (name NOT LIKE '201%(%)%' and name NOT LIKE 'S201%(%)%')
				AND src_ag_db.[db_id] IS NULL 
			UNION ALL
			select 
				name,database_id as id,recovery_model_desc as model,'Log' as BackupTypeNeed
			from sys.databases
			where 
				/*state_desc='ONLINE' and */ is_read_only=0 and name not in ('tempdb', 'model')	and recovery_model_desc<>'SIMPLE'
				AND (name NOT LIKE '201%(%)%' and name NOT LIKE 'S201%(%)%')
	
		--select * from #DB

		if @xp in (0,2)
		begin
			IF DB_ID('sputnik') is not null
			BEGIN
				DECLARE @TT TABLE ([DB_name] NVARCHAR(400), Backup_Type VARCHAR(4), BackupFile NVARCHAR(500), ID INT, BackupDate DATETIME2(2), LocalDir NVARCHAR(500), NetDir NVARCHAR(500), CheckLocalDir BIT, CheckNetDir BIT, CheckLocalFile BIT, CheckLocalFileOnly BIT, CheckNetFile BIT);
				INSERT INTO @TT
					EXEC sputnik.info.usp_GetLastBackups @DBName=@DBFilter, @Backup_type=@Backup_type, @CheckOnline=1;

				IF @xp=0
					select DISTINCT 
						@SQLServer as SQLServer, 
						CAST(SYSDATETIME() AS datetime) AS ServerTime,
						[DB_name] AS DBName, 
						CAST(BackupDate AS datetime) as BackupDate, 
						Backup_Type, 
						CASE 
							WHEN CheckLocalDir=0 THEN 'Каталог "'+LocalDir+'" НЕДОСТУПЕН!'
							WHEN CheckLocalFile=0 AND CheckLocalFileOnly=1 THEN LocalDir+REPLACE(BackupFile,'.BAK','.ONLY')
							ELSE LocalDir+BackupFile 
						END AS LocalFile,
						CASE 
							WHEN CheckLocalFile=0 THEN CheckLocalFileOnly
							ELSE CheckLocalFile
						END AS CheckLocalFile,
						CASE
							WHEN NetDir is NULL OR NetDir='' OR LocalDir=NetDir THEN 'Копии бэкапа отключены'
							WHEN CheckNetDir=0 THEN 'Каталог "'+NetDir+'" НЕДОСТУПЕН!'
							WHEN CheckLocalFile=0 AND CheckLocalFileOnly=1 THEN 'Копия бэкапа НЕДОСТУПНА!'
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
							WHEN t.[DB_name] IS NULL THEN '!Бэкапы не настроены/нет первого бэкапа!'
							WHEN t.CheckLocalDir=0 THEN 'Каталог "'+t.LocalDir+'" НЕДОСТУПЕН!'
							WHEN t.CheckLocalFile=0 AND t.CheckLocalFileOnly=1 THEN t.LocalDir+REPLACE(t.BackupFile,'.BAK','.ONLY')
							ELSE t.LocalDir+t.BackupFile 
						END AS LocalFile,
						CASE 
							WHEN t.CheckLocalFile=0 THEN t.CheckLocalFileOnly
							ELSE t.CheckLocalFile
						END AS CheckLocalFile,
						CASE
							WHEN t.[DB_name] IS NULL THEN NULL
							WHEN t.NetDir is NULL OR t.NetDir='' OR t.LocalDir=t.NetDir THEN 'Копии бэкапа отключены'
							WHEN t.CheckNetDir=0 THEN 'Каталог "'+t.NetDir+'" НЕДОСТУПЕН!'
							WHEN t.CheckLocalFile=0 AND t.CheckLocalFileOnly=1 THEN 'Копия бэкапа НЕДОСТУПНА!'
							ELSE t.NetDir+t.BackupFile
						END AS NetFile,
						case
							when t.NetDir is NULL OR t.NetDir='' OR t.LocalDir=t.NetDir then CheckLocalFile
							else t.CheckNetFile
						end as [CheckNetFile],
						--DATEDIFF(minute,BackupDate,SYSDATETIME()) as BackupAgeInMinutes
						CAST(AllBackups.backup_size_Mb/1024.000 as numeric(9,3)) AS backup_size_Gb,
						AllBackups.backup_compress_ratio,
						DATEDIFF(second, AllBackups.backup_start_date, AllBackups.backup_finish_date) as backup_elapsed_sec
					from #DB as Bases
					LEFT JOIN @TT t ON Bases.name=t.[DB_name] AND (Bases.BackupTypeNeed=t.Backup_Type OR (t.Backup_Type='DIFF' AND Bases.BackupTypeNeed='FULL'))
					LEFT JOIN [sputnik].[backups].[BackupHistory] AllBackups ON t.ID=AllBackups.ID
					WHERE (Bases.BackupTypeNeed=@Backup_type or @Backup_type is NULL) 
			END
		end
		else
		--Расширенный мониторинг для всех баз (а не только тех, что прописаны в базе sputnik).
		begin
			select DISTINCT
				@SQLServer as SQLServer, 
				CAST(SYSDATETIME() AS datetime) AS ServerTime,
				Bases.name as DBName, Bases.id as DBID, Bases.model, 
				BackupTypeNeed,
				Backups.BackupType as BackupTypeFact,
				Backups.BackupDate, REPLACE(Backups.BackupFile,'.Only','.BAK') as BackupFile,
				sputnik.info.uf_checkfile(REPLACE(Backups.BackupFile,'.Only','.BAK')) as CheckBackupFile,
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
				AND (@OnlyProblems = 0 or sputnik.info.uf_checkfile(REPLACE(Backups.BackupFile,'.Only','.BAK')) <> 1 or ((Backups.BackupType='Log' and DATEDIFF(minute,Backups.BackupDate,getdate())>60) or (Backups.BackupType<>'Log' and DATEDIFF(minute,Backups.BackupDate,getdate())>1600)))

		end;	
	END