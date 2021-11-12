
	/* =============================================
	-- Author:		Андрей Иванов (sqland1c)
	-- Create date: 04.08.2014 (1.0)
	-- Description: Эта процедура используется как часть модуля lse - Log Shipping Easy.
					Сначала открываются все настройки для LSE и производится вызов ХП usp_RunRolling для наката бэкапов Логов по каждой БД.
					Если же целевая БД ещё не проинициализирована (или был задан режим переинициализации), тогда для неё будет вызов ХП для инициализации копии - usp_GC2.
					Возможен отбор по конкретной целевой БД, для этого должен быть задан параметр @DBNameTarget.

	-- Update:		07.08.2014 (1.1) реализован алгоритм, которые позволяет запускать LSE в многопоточном режиме через модуль pp.
					За это отвечает новый параметр @pp.
					18.08.2014 (1.2) реализована дополнительная проверка при работе в многопоточном режиме для минимизации
					нагрузки на сервер: В очередь сообщения будут добавлены только в том случае, eсли процессорами usp_ExecProcessor
					сейчас ничего не выполняется + инициализация производится только по одной БД! 
					18.03.2015 (1.23) Добавлена новая переменная @StandBy_File - для поддержки режима STANDBY (read-only) для восстанавливаемой БД.
					Это полный путь к файлу отката standby. Соответственно в таблице sputnik.lse.TargetConfig должен быть новый столбец StandBy_File.
					30.03.2016 (1.30) Доработка процедуры: Добавлены 2 проверки перед помещением в очередь Service Broker.
					А также добавлен новый параметр @execute - для того чтобы чётко разделить запуск из Joba (помещения в очередь)
					и запуски из обработчика очереди Service Broker (здесь как раз @execute=1).
					25.07.2016 (1.31) Доработка процедуры: добавлен учёт новой возможности - расположение лог-файлов 
					на отдельным диске(CatalogLogFiles).
					09.09.2016 (1.32) Доработка процедуры: Исправлена небольшая ошибка при определении курсора не хватало 1го нового столбца.
					13.11.2017 (1.330) Доработка процедуры: добавлен алгоритм обработки нового параметра из настроек - UseFreshDiffBack. Действует для тех баз, которые нужно инициализировать!
	-- ============================================= */
	CREATE PROCEDURE [lse].[usp_RollForwardRecovery]  
		@DBName nvarchar(300)=NULL,
		@pp bit=0,
		@execute bit=0
	AS
		SET NOCOUNT ON;
		if @pp=1 and @execute=0--and @DBName is null
		begin

			declare @DlgHandle UNIQUEIDENTIFIER, @MsgRequest XML;
			declare @tt table (db nvarchar(300));
			declare @count_q_pp int, @dbname_cur nvarchar(300);
			--Накат журналов транзакций для всех БД
			insert into @tt
			select distinct DBNameTarget as db
			from sputnik.lse.TargetConfig
			where [suspend]=0 AND [InitDate] is not null
				AND (DBNameTarget=@DBName OR @DBName IS NULL);

			--Производим дополнительную проверку, чтобы минимально нагружать сервер:
			--Инициализацию производим, только если процессорами usp_ExecProcessor сейчас ничего не исполняется
			IF NOT EXISTS(select queue_id from sys.dm_broker_activated_tasks where procedure_name='[pp].[usp_ExecProcessor]')
			BEGIN			
				--А Инициализация производится только по одной БД (опять же для минимальной нагрузки на сервер)!
				insert into @tt
				select TOP 1 DBNameTarget as db
				from sputnik.lse.TargetConfig
				where [suspend]=0 AND [InitDate] is null
					AND (DBNameTarget=@DBName OR @DBName IS NULL);
			END

			if exists(select top 1 db from @tt)
			begin
				declare LSE cursor for
				select cast('<DB>'+db+'</DB>' as xml) as DB,
					db as DBname
				from @tt;
				--Начинаем диалог для LSE!
				BEGIN DIALOG CONVERSATION @DlgHandle
					FROM SERVICE [pp_Commander]
					TO SERVICE N'pp_Processor'
					ON CONTRACT [pp_lse_Contract]
					WITH ENCRYPTION = OFF;
				open LSE;
				fetch next from LSE into @MsgRequest, @dbname_cur;
				while @@FETCH_STATUS=0
				begin
					--29.03.2016 Добавлена проверка: существует ли УЖЕ в очереди наше сообщение!
					--Добавляем в очередь такое же сообщение, если его ещё нет в Очереди!
					;WITH XMLNAMESPACES
					('http://pecom.ru/pegasExchange/types/' as ns),
					Q AS 
					(
						select CAST(message_body as XML) as msg_xml
						from pp.ProcessorQueue
						where message_type_name='pp_lse_Request'
					)
					select @count_q_pp=count(*)
					from Q
					where msg_xml.exist(N'(/db/text()[.=sql:variable("@dbname_cur")])')=1;

					--30.03.2016 Ещё одна проверка- выполняется ли в данный момент
					--обработчиком Service Broker восстановление в текущей БД
					if OBJECT_ID('tempdb..#broker_tasks') is not null
						DROP TABLE #broker_tasks;
					select 
						br_t.spid, s.login_time, s.status as Session_Status, s.open_transaction_count as Open_tran_cnt, DB_NAME(s.database_id) as DB,
						r.status as command_status, r.command as command_type, r.percent_complete as [%],
						--r.sql_handle, r.statement_start_offset, r.statement_end_offset,
						--sql_text.text as sql_text,
						(SELECT TOP 1 SUBSTRING(sql_text.text,r.statement_start_offset / 2+1 , 
						  ((CASE WHEN r.statement_end_offset = -1 THEN (LEN(CONVERT(nvarchar(max),sql_text.text)) * 2) 
							 ELSE r.statement_end_offset 
							END)  - r.statement_start_offset) / 2+1))  AS sql_statement
					INTO #broker_tasks
					from sys.dm_broker_activated_tasks br_t
					left join sys.dm_exec_sessions s
						on s.session_id = br_t.spid
					left join sys.dm_exec_requests r
						on r.session_id =s.session_id
					CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) AS sql_text
					where 
						br_t.procedure_name='[pp].[usp_ExecProcessor]'
						and r.command LIKE 'RESTORE %'
					--select * from #broker_tasks
					--if not exists(select spid from #broker_tasks where sql_statement like '%^['+'Booh2014'+'^]%' ESCAPE '^')
					--	select 1;
					IF (@count_q_pp=0 OR @count_q_pp IS NULL) 
						AND not exists(select spid from #broker_tasks where sql_statement like '%^['+@dbname_cur+'^]%' ESCAPE '^')
					BEGIN
						--отправляем сообщение в очередь!
						SEND ON CONVERSATION @DlgHandle
							MESSAGE TYPE [pp_lse_Request]
							(@MsgRequest);
					END				
					fetch next from LSE into @MsgRequest, @dbname_cur;
				end	
				close LSE;
				deallocate LSE;
			end
			return 1;
		end
		--Сначала получаем только те БД для наката, которые не приостановлены и для которых выполнена Инициализация.
		declare @ServerSource nvarchar(300), @DBNameSource nvarchar(300), @DBNameTarget nvarchar(300), @FromCopy bit, @BackupID int, @ConfigID smallint, @CatalogFilesDB nvarchar(800), @CatalogLogFiles nvarchar(800), @StandBy_File nvarchar(500), @UseFreshDiffBack bit;
		declare LSE cursor for
		select distinct c.id as ConfigID, c.CatalogFilesDB, c.CatalogLogFiles,
			case when c.InitBackupHS_id<h.MaxBackupHS_id then h.MaxBackupHS_id
				else c.InitBackupHS_id
			end BackupID
		from sputnik.lse.TargetConfig c
		left join (select distinct config_id, max(BackupHS_id) over (partition by config_id) as MaxBackupHS_id from sputnik.lse.HS) h
			on c.id=h.config_id		
		where [Suspend]=0 AND [InitDate] IS NOT NULL AND (DBNameTarget=@DBName OR @DBName IS NULL);
		open LSE;
		fetch next from LSE into @ConfigID, @CatalogFilesDB, @CatalogLogFiles, @BackupID;
		while @@FETCH_STATUS=0
		begin
			exec sputnik.lse.usp_RunRolling @ConfigID=@ConfigID, @BackupID=@BackupID, @MoveFilesTo=@CatalogFilesDB, @MoveLogFilesTo=@CatalogLogFiles, @pp=@pp;
			fetch next from LSE into @ConfigID, @CatalogFilesDB, @CatalogLogFiles, @BackupID;
		end
		close LSE;
		deallocate LSE;
		--Теперь получаем настройки, которые нужно проинициализировать!
		declare LSE cursor for
		select ServerSource, DBNameSource, DBNameTarget, FromCopy, CatalogFilesDB, CatalogLogFiles, StandBy_File, COALESCE(UseFreshDiffBack,0) as UseFreshDiffBack 
		from sputnik.lse.TargetConfig
		where [InitDate] is null AND (DBNameTarget=@DBName OR @DBName IS NULL);
		open LSE;
		fetch next from LSE into @ServerSource, @DBNameSource, @DBNameTarget, @FromCopy, @CatalogFilesDB, @CatalogLogFiles, @StandBy_File, @UseFreshDiffBack;
		while @@FETCH_STATUS=0
		begin
			--сформируем заново путь к файлам БД (если не задан в настройках!)
			if @CatalogFilesDB is NULL and @DBNameSource is not null
			begin
				DECLARE @MaxDrive CHAR(1);
				exec sputnik.info.usp_GetDrives @GetMaxFree=1, @MaxFreeDrive=@MaxDrive OUTPUT;
				SET @CatalogFilesDB=@MaxDrive+':\DATA\lse\'+@DBNameTarget+'\';
				UPDATE sputnik.lse.TargetConfig
				SET CatalogFilesDB=@CatalogFilesDB, CatalogLogFiles=NULL 
				WHERE DBNameTarget=@DBNameTarget;
			end
			exec sputnik.backups.usp_GC2 
				@ServerSource=@ServerSource, @DBNameSource=@DBNameSource, 
				@DBNameTarget=@DBNameTarget, @FromCopy=@FromCopy, @MoveFilesTo=@CatalogFilesDB, @MoveLogFilesTo=@CatalogLogFiles,
				@NoRecovery=1,@RunNewBackIfNeed=1, @FreshBack=@UseFreshDiffBack,@lse=1, @pp=@pp, @StandBy_File=@StandBy_File, @RunNewDiffBackIfNeed=@UseFreshDiffBack;
			fetch next from LSE into @ServerSource, @DBNameSource, @DBNameTarget, @FromCopy, @CatalogFilesDB, @CatalogLogFiles, @StandBy_File, @UseFreshDiffBack;
		end
		close LSE;
		deallocate LSE;