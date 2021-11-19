
	/* =============================================
	-- Author:		Андрей Иванов (sqland1c)
	-- Create date: 24.01.2014
	-- Description:	Эта процедура возвращает информацию о зеркальном отображении БД. 
					Параметр @zabbix - если задан 1, тогда  будет возвращена информация о Сбоях в зеркалировании!
						Если ничего не возвращено, тогда всё ок. По умолчанию 0.
	-- Update:
					13.02.2013 (1.1)
					В результат добавлен ещё один столбец - Имя базы данных, для которой работает Зеркалирование.
					Также изменен алгоритм для получения ServerName (теперь получаем точное физическое имя сервера!
					т.к. свойство @@SERVERNAME - имя экземпляра SQL Server может быть неточным, после переименования Сервера)
					19.05.2015 (1.11)
					Исправлено определение имени сервера - теперь учитывается имя экземпляра!
					25.05.2015 (1.20)
					Новая версия алгоритма определения состояния Зеркала. Теперь учитывается Базы готовые к Зеркалированию,
					но у которых нет ни Log Shipping , ни Зеркалирования (в таком случае состояние помечается как Candidate)!
					26.05.2015 (1.25)
					Доработан механизм определения состояния Зеркала. 
					Теперь учитываются базы, для которых в данный момент настраиваются Зеркалирование! Исходя из состояния XLog
					в таблице BackConf и текущего состояния Job по настройке Зеркала.
					26.05.2015 (1.26)
					Исправлен алгоритм определения состояния Зеркалирования - теперь состояние XLog не учитывается, учитывается
					только текущее состояние Job по настройке Зеркала!
					02.06.2015 (1.27)
					Небольшое исправление в алгоритме определения состояния Зеркалирования - если результат Job Successfully,
					то состояние Зеркала должно быть Candidate!
					28.08.2015 (1.29)
					Добавлена поддежрка мониторинга AlwaysON AG с основной реплики!
					02.09.2015 (1.30)
					Исправлен алгоритм получения информации для мониторинга AlwaysON AG.
					28.07.2016 (1.31)
					Снова подправлен алгоритм получения информации для мониторинга AlwaysON AG!
					24.08.2016 (1.32)
					Исправлен алгоритм получения БД - в список исключения добавлены системные БД 'master','msdb','model'!
					23.01.2018 (1.36)
					Доработан алгоритм для zabbix(возвращает выходной параметр!)
					26.01.2018 (1.37)
					Исправление для zabbix - при вызове с параметром @ResState процедура usp_JobMonitor не вызывается! 
					23.02.2018 (1.381)
					Для определения правильного имени сервера SQL теперь 
					используется процедура info.usp_getHostname	 
	-- ============================================= */
	CREATE PROCEDURE info.usp_CheckMirror 
		@zabbix bit = 0,
		@dbfilter nvarchar(2000)=null,
		@ResState nvarchar(100)=null OUTPUT
	as
	begin
		set nocount on;	
		declare @SQLServer nvarchar(510);
		exec info.usp_GetHostname @Servername=@SQLServer OUT;
		/* Старая версия
		select 
			@sqlserver  as ServerName, DB_NAME(database_id) as DBName, mirroring_role_desc as Role, mirroring_state_desc as State, mirroring_partner_instance as PartnerServerName
		from 
			sys.database_mirroring
		where
			mirroring_guid is not null
			and (@zabbix=0 or (mirroring_state_desc not in ('SYNCHRONIZING','SYNCHRONIZED')))
		*/
		--Выключаем параметр ANSI_WARNINGS чтобы вставлять длинные строки в таблицу с обрезанием!
		SET ANSI_WARNINGS OFF;
		DECLARE @T_Jobs TABLE (SQLServerName sql_variant, [Job] NVARCHAR(200), [Step] NVARCHAR(1), Info CHAR(1), RUN_STATUS VARCHAR(30), DateTimeRun DateTime, Duration VARCHAR(10));
		IF @ResState is null
			insert into @T_Jobs
			exec info.usp_JobMonitor @Activity=1;

		Declare @T_Result TABLE (servername nvarchar(1000), DBName nvarchar(2000), [Role] nvarchar(100), [State] nvarchar(100), PartnerServerName nvarchar(1000));
		insert into @T_Result(servername,DBName,[Role],[State],PartnerServerName)
		SELECT 
			@sqlserver  as ServerName, 
			DB.DB as DBName, 
			CASE 
				WHEN alwayson_ag.[Role] IS NOT NULL AND alwayson_ag.[PrimaryReplica] COLLATE Cyrillic_General_CI_AS<>@sqlserver THEN N'AlwaysON '+QUOTENAME(alwayson_ag.[Role] COLLATE Cyrillic_General_CI_AS)
				WHEN alwayson_ag.[Role] IS NOT NULL AND alwayson_ag.[PrimaryReplica] COLLATE Cyrillic_General_CI_AS=@sqlserver THEN N'AlwaysON [PRIMARY]'
				WHEN Mir.mirroring_state_desc IS NULL THEN N'PRIMARY'
				ELSE Mir.mirroring_role_desc
			END AS [Role],
			CASE 
				WHEN alwayson_ag.[Role] IS NOT NULL AND alwayson_ag.health=N'HEALTHY' THEN alwayson_ag.sync_state
				WHEN alwayson_ag.[Role] IS NOT NULL AND alwayson_ag.health<>N'HEALTHY' THEN alwayson_ag.sync_state+' '+QUOTENAME(alwayson_ag.health COLLATE Cyrillic_General_CI_AS)
				WHEN Mir.mirroring_state_desc IS NULL AND (Jobs.[RUN_STATUS] IS NULL OR Jobs.[RUN_STATUS] COLLATE Cyrillic_General_CI_AS='Successfully') THEN 'Candidate'
				WHEN Mir.mirroring_state_desc IS NULL AND Jobs.[RUN_STATUS] IS NOT NULL THEN 'Setup '+Jobs.RUN_STATUS COLLATE Cyrillic_General_CI_AS
				ELSE Mir.mirroring_state_desc
			END AS [State],
			CASE
				WHEN alwayson_ag.[Role] IS NOT NULL AND (alwayson_ag.[PrimaryReplica] COLLATE Cyrillic_General_CI_AS=@sqlserver
					AND alwayson_ag.[PartnerReplica] COLLATE Cyrillic_General_CI_AS <> alwayson_ag.[PrimaryReplica] COLLATE Cyrillic_General_CI_AS) THEN N'SECONDARY: '+QUOTENAME(alwayson_ag.[PartnerReplica] COLLATE Cyrillic_General_CI_AS)
				WHEN alwayson_ag.[Role] IS NOT NULL AND (alwayson_ag.[PrimaryReplica] COLLATE Cyrillic_General_CI_AS<>@sqlserver
					OR alwayson_ag.[PartnerReplica] COLLATE Cyrillic_General_CI_AS = alwayson_ag.[PrimaryReplica] COLLATE Cyrillic_General_CI_AS) THEN N'PRIMARY: '+QUOTENAME(alwayson_ag.[PrimaryReplica] COLLATE Cyrillic_General_CI_AS)
				--WHEN alwayson_ag.[Role] IS NOT NULL AND alwayson_ag.[Role] COLLATE Cyrillic_General_CI_AS<>'PRIMARY' THEN N'Primary: '+QUOTENAME(alwayson_ag.PrimaryReplica)
				WHEN Mir.mirroring_state_desc IS NULL THEN ''
				ELSE Mir.mirroring_partner_instance
			END AS [PartnerServerName]
		FROM
		(
			SELECT DB --, CASE WHEN [Log]=0 THEN 'XLog' ELSE 'Candidate' END AS [Status] 
			FROM info.vGetQuickBackConf
			WHERE [state]='ONLINE' AND RecoveryModel='FULL' AND DB NOT IN ('master','msdb','model')
			--Проверять Зеркало нужно для всех БД, у которых Full Recovery Model (ДАЖЕ ЕСЛИ НЕТ БЭКАПОВ В sputnik). Поэтому след.строка закоментирована:
			--AND (([Full]=1 OR [FullWeekly]=1) AND ([Log]=1 OR DB IN (SELECT DBName FROM backups.BackConf WHERE [Kind] IN ('XLog', 'Log_Secondary'))))		
		) DB
		LEFT JOIN sys.database_mirroring Mir 
			ON DB.DB=DB_NAME(Mir.database_id)
		LEFT JOIN (
				SELECT
					DB_NAME(ag_db.database_id) as DB,
					ISNULL(arstates.role_desc, '') AS [Role],
					ISNULL(AR.replica_server_name, '') as [PartnerReplica],
					ISNULL(agstates.primary_replica, '') AS [PrimaryReplica],
					ag_db.synchronization_state_desc as sync_state,
					ag_db.synchronization_health_desc as health
				FROM sys.dm_hadr_database_replica_states ag_db
				LEFT JOIN sys.dm_hadr_availability_group_states as agstates
					on ag_db.group_id=agstates.group_id	
				LEFT JOIN sys.dm_hadr_availability_replica_states AS arstates
					ON ag_db.replica_id = arstates.replica_id
						and ag_db.group_id=arstates.group_id
				LEFT JOIN sys.availability_replicas as AR
					ON ag_db.replica_id=AR.replica_id
						and ag_db.group_id=AR.group_id
				--WHERE ag_db.is_local=1 OR
				--	ISNULL(AR.replica_server_name, '') COLLATE Cyrillic_General_CI_AS<>@sqlserver
			) as alwayson_ag
			ON DB.DB=alwayson_ag.DB
		LEFT JOIN (select REPLACE([Job],'RM ','') as DB,[RUN_STATUS] from @T_Jobs where [Job] LIKE 'RM %') as Jobs
			ON DB.DB=Jobs.DB-- AND DB.[Status]='XLog'
		WHERE
			(Mir.mirroring_state_desc IS NOT NULL OR alwayson_ag.[Role] IS NOT NULL OR DB.DB NOT IN (select DBNameTarget from lse.SourceConfig))
			AND (@zabbix=0 or (Mir.mirroring_state_desc not in ('SYNCHRONIZING','SYNCHRONIZED')));
		if @ResState is null or @dbfilter is null
			select * from @T_Result where (DBName = @dbfilter or @dbfilter is null);
		else
		begin
			declare @restemp nvarchar(100);
		
			select top 1 @restemp='mi '+CASE WHEN [State] IN ('SYNCHRONIZING','SYNCHRONIZED') THEN '1' ELSE '2' END from @T_Result where (DBName = @dbfilter) and [State]<>'Candidate' and [Role] not like 'AlwaysON %';

			if @restemp is null
				select top 1 @restemp='ao 2'
				from @T_Result where (DBName = @dbfilter) and [Role] like 'AlwaysON %' and [state] NOT IN ('SYNCHRONIZING','SYNCHRONIZED');
			if @restemp is null
				select top 1 @restemp='ao 1'
				from @T_Result where (DBName = @dbfilter) and [Role] like 'AlwaysON %' and [state] IN ('SYNCHRONIZING','SYNCHRONIZED');
			if @restemp is null
				select top 1 @restemp='0'
				from @T_Result where (DBName = @dbfilter) and [State]='Candidate';			

			set @ResState=COALESCE(@restemp,'-1');
		end

	end
GO
GRANT EXECUTE
    ON OBJECT::[info].[usp_CheckMirror] TO [zabbix]
    AS [dbo];

