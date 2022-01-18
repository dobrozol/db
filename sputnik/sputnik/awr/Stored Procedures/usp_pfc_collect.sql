
	/* =============================================
	-- Author:		Андрей Иванов (sqland1c)
	-- Create date: 24.08.2015 (1.0)
	-- Description: Процедура для сбора и сохранения в базе sputnik данным мониторинга по наиболее важным
					счетчикам производительности SQL SERVER.
				
	-- Update:		01.09.2015 (1.1)
					Добавлена поддержка именованных экземпляров (переменная @instance_name).
					Если экземпляр именованных то в [object_name] будет имя экзепляра, а не SQLServer.  
					01.09.2015 (1.15)								
					Счетчик CPU Usage % теперь собираем из системного Extended Event (причем два счетчика: общая загрузка CPU и загрузка CPU текущим экземпляром SQL Server).
					14.09.2015 (1.2)
					Добавлен сбор нестантартных счетчиков по памяти, а также по времени выполнения активных запросов sp_whoisactive.
					23.09.2015 (1.3)
					Добавлен сбор нестандартных ДИНАМИЧЕСКИХ счетчиков, таких как Logical Disk (в разрезе по каждому диску).
					28.09.2015 (1.31)
					Включение и настройка xEvents [xe_DiskInfo] перенесена сюда (в начало) из процедуры usp_get_DiskMetr.
					19.11.2015 (1.32)
					Добавлен сбор информации о Uptime для SQL Server в часах.
					10.03.2016 (1.33)
					Оптимизация - при обращении к awr.pfc_data используем фильтр по полю tt (чтобы задействовать индекс).
					Результаты: длительность ДО:  ~25 сек, ПОСЛЕ: ~2 сек. Кол-во чтений стало на порядок меньше!
					27.05.2016 (1.34)
					Добавлен собственный счетчик Response time(ms)
					12.12.2016 (1.350)
					Добавлен собственный счетчик Tran_RunTime_min (время выполнения активных транзакций в мин.)
					24.04.2017 (1.371)
					Добавлены новые счетчики (по регламентным заданиям SQL).
					Также частично выполнена оптимизация при сборе - удалено обращение к awr.pfc_data - зачем сюда обращаться, если нужно просто вставить новые значения?
					19.03.2018 (1.380)
					Добавлены новые счётчики (по использованию TempDB).
					23.03.2018 (1.382)
					Добавлен новый счётчик Sleeping_tran- это кол-во зависших сесссий с открытыми транзакциями.
					02.04.2018 (1.383)
					Исправление сбора данных по использованию TempDB.
	-- ============================================= */
	CREATE PROCEDURE awr.usp_pfc_collect

	AS
	begin
		set nocount on;
		set LOCK_TIMEOUT 10000;
		declare @tt datetime, @instance_name nvarchar(128);
		declare @T1 table(id smallint, counter_type int, value numeric(19,2));
		IF NOT EXISTS (SELECT * FROM sys.server_event_sessions WHERE name = 'xe_DiskInfo')
		BEGIN
		--Настраиваем и включаем сборщик данных через Extended Events!
		--xEvents сессия для сбора информации о счетчиках группы Logical Disk (инфо обновляется каждые 15 сек.)
		--Данные сохраняются в кольцевой буфер и хранятся тут совсем недолго.
		--Эти данные нужно успеть захватить и обработать и положить в схему awr в базу 
			CREATE EVENT SESSION [xe_DiskInfo] ON SERVER 
				ADD EVENT sqlserver.perfobject_logicaldisk 
				ADD TARGET package0.ring_buffer(SET max_events_limit=(128),max_memory=(32768))
			WITH (MAX_MEMORY=4096 KB,EVENT_RETENTION_MODE=ALLOW_SINGLE_EVENT_LOSS,MAX_DISPATCH_LATENCY=10 SECONDS,MAX_EVENT_SIZE=0 KB,MEMORY_PARTITION_MODE=NONE,TRACK_CAUSALITY=OFF,STARTUP_STATE=ON);
			ALTER EVENT SESSION [xe_DiskInfo] ON SERVER STATE = START;
			--геренируем задержку в 30 сек, чтобы данные успелись собраться!
			--если не успеет собраться, тогда соберем в след. раз!
			waitfor delay '00:00:30.000';
		END
		declare @xml_data xml;
		SELECT @xml_data=CAST(xet.target_data AS xml)
				FROM sys.dm_xe_session_targets AS xet
				JOIN sys.dm_xe_sessions AS xe
				   ON (xe.address = xet.event_session_address)
				WHERE xe.name = 'xe_DiskInfo'
					and xet.target_name = 'ring_buffer';
		--select @xml_data;
		IF @xml_data is null
			--Если возвращен NULL, значит скорее всего Сессия не включена!
			ALTER EVENT SESSION [xe_DiskInfo] ON SERVER STATE = START;


		set @tt=getdate();
		select @instance_name=IIF(serverproperty('instancename') IS NULL, 'SQLServer', 'MSSQL$'+CAST(serverproperty('instancename') as varchar(128)));
		insert into @T1 (id, counter_type, value)
		select ph.id, ph.counter_type, CAST(pd.cntr_value as numeric(19,2)) as value
		from
		(
			select id, [object_name], counter_name, instance_name, counter_type
			from awr.pfc_handle
			where counter_type is not null
			union
			select id, [object_name], counter_name+' base', instance_name, null as counter_type
			from awr.pfc_handle
			where counter_type=537003264 --для этого типа счетчика нужно также получить базовое значение!
		) ph
		inner join sys.dm_os_performance_counters pd 
			on REPLACE(ph.[object_name],'#instance#',@instance_name)=rtrim(pd.[object_name]) and ph.[counter_name]=rtrim(pd.[counter_name])
				and (/*ph.counter_name like 'CPU usage %' or */ph.instance_name=pd.instance_name);
	
		--Сбор нестандартных счетчиков:
		--1. Кол-во запросов в sp_whoisactive;
		declare @T2 table(tt datetime null,id int null,cnt numeric(19,2));
		declare @T_tmp table(tt datetime null, counter_name nvarchar(300) null, instance_name nvarchar(300), value numeric(19,2));
		declare @id_spwhoisactive_count smallint=null, @id_sleeptran_count smallint=null;
		select @id_spwhoisactive_count=id from awr.pfc_handle where [object_name]='awr' and counter_name='Active sessions (sp_whoisactive)' and instance_name='_Total';
		select @id_sleeptran_count=id from awr.pfc_handle where [object_name]='awr' and counter_name='Active sessions (sp_whoisactive)' and instance_name='Sleep_transactions';
		if @id_spwhoisactive_count is not null
		begin tran
			insert into @T2(cnt)
			exec info.sp_who3 @busy_minutes=0,@get_count=1;
			update @T2
			set id=@id_spwhoisactive_count,tt=@tt
			where id is null
		commit
		if @id_sleeptran_count is not null
		begin tran
			insert into @T2(cnt)
			exec info.sp_who3 @busy_minutes=0,@get_count=1, @only_sleep_tran=1;;
			update @T2
			set id=@id_sleeptran_count,tt=@tt
			where id is null
		commit

		--2. Всего установлено памяти на сервере в Мб и всего использовано памяти в Мб из системного dmv:
		if exists(select id from awr.pfc_handle where [object_name]='awr' and counter_name IN ('Physical Memory (Gb)','Physical Memory (Mb)'))
		begin
			;with cte_1 AS(
				SELECT 
					@tt as tt,
					cast(total_physical_memory_kb/(1024.00*1024.00) as numeric(19,2)) as _Total, 
					cast((total_physical_memory_kb-available_physical_memory_kb)/(1024.00*1024.00) as numeric(19,2)) as _Used,
					cast(available_physical_memory_kb/1024.00 as numeric(19,2)) as _Free 
				FROM sys.dm_os_sys_memory
			),
			cte_2 AS(
				SELECT 
					tt,
					instance_name,	
					value
				FROM cte_1
				unpivot(value for instance_name in ([_Total],[_Used],[_Free])
				)unpvt
			),
			cte_3 AS (
				select id,h.instance_name
				from awr.pfc_handle h
				where [object_name]='awr' and counter_name IN ('Physical Memory (Gb)','Physical Memory (Mb)')
			)
			insert into @T2(tt,id,cnt)
			select 
				cte_2.tt,
				cte_3.id,	
				cte_2.value
			from cte_2
			inner join cte_3
				on cte_2.instance_name=cte_3.instance_name
			;
		end

		--3. Загрузка информации о текущем времени выполнения запросов (sp_whoisactive) 
		if exists(select id from awr.pfc_handle where [object_name]='awr' and counter_name='Run_time_query_min')
		begin
			delete @T_tmp;
			insert into @T_tmp (tt,counter_name,instance_name, value)
			exec info.usp_SQLMon @Filter_Reads=0, @Filter_DurSec=0, @interval_sec=0, @get_runtime_metr=1;

			;with cte_src as (
				select tt, instance_name, value
				from @T_tmp
			),cte_pfc as
			(
				select distinct h.id,h.instance_name,max(d.tt) over (partition by h.id) as tt
				from awr.pfc_handle h
				left join awr.pfc_data d 
					on h.id=d.pfc_id  and d.tt>dateadd(minute,-10,@tt) --оптимизация - фильтруем, используем индекс чтобы быстро искать!
				where [object_name]='awr' and counter_name='Run_time_query_min'
			)
			insert into @T2(tt,id,cnt)
			select 
				cte_src.tt,
				cte_pfc.id,	
				cte_src.value
			from cte_src
			inner join cte_pfc
				on cte_src.instance_name=cte_pfc.instance_name
				and (cte_src.tt>cte_pfc.tt or cte_pfc.tt is null)
			;		
		end
		
		--4. Загрузка CPU общая и текущим экземпляров SQL Server из системного Extended Events
		if exists(select id from awr.pfc_handle where [object_name]='awr' and counter_name='CPU usage %')
		begin
			DECLARE @ts_now bigint = (SELECT cpu_ticks/(cpu_ticks/ms_ticks)FROM sys.dm_os_sys_info); 
			;WITH cte_1 AS(
				SELECT [timestamp], convert(xml, record) AS [record] 
				FROM sys.dm_os_ring_buffers 
				WHERE ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR' 
				AND record LIKE '%<SystemHealth>%'
			),
			cte_2 AS(   
				SELECT 
					100-record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') AS [_Total], 
		 			record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]','int') AS [SQL], 
					CAST(DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) as datetime2(0)) AS [tt] 
				FROM cte_1
				WHERE [timestamp]=(SELECT MAX([timestamp]) FROM cte_1)
			),
			cte_3 AS(
				select 
					tt,
					instance_name,	
					value
				from cte_2
				unpivot(value for instance_name in ([SQL],[_Total])
				)unpvt
			),
			cte_4 AS(
				select distinct h.id,h.instance_name,max(d.tt) as tt
				from awr.pfc_handle h
				left join awr.pfc_data d 
					on h.id=d.pfc_id  and d.tt>dateadd(minute,-10,@tt) --оптимизация - фильтруем, используем индекс чтобы быстро искать!
				where [object_name]='awr' and counter_name='CPU usage %'
				group by h.id, h.instance_name
			)
			insert into @T2(tt,id,cnt)
			select 
				cte_3.tt,
				cte_4.id,	
				cte_3.value
			from cte_3
			inner join cte_4
				on cte_3.instance_name=cte_4.instance_name
				and (cte_3.tt>cte_4.tt or cte_4.tt is null)
			;
		end

		--5. Подготовка и загрузка динамических данных (например, данных по каждому дисковому разделу)
		declare @T_dyn table(tt datetime null, id smallint null, cnt numeric(19,2));
		if exists(select top 1 id from awr.pfc_handle where [object_name]='awr' and instance_name='#dynamic#')
		begin
			delete @T_tmp;
			insert into @T_tmp (tt,counter_name,instance_name, value)
			exec info.usp_get_DiskMetr;
			--сначала загрузим информацию о заголовках (в таблицу pfc_handle_dyn), по тем данным, которых ещё нет!
			;with cte_dyn_h as (
				select h.id, T.instance_name
				from @T_tmp T
				inner join awr.pfc_handle h
					on T.counter_name=h.counter_name and h.[object_name]='awr' and h.instance_name='#dynamic#'
			)
			MERGE
			INTO awr.pfc_handle_dyn as Target
			USING cte_dyn_h as Source
			ON (Source.id=Target.pfc_id and Source.instance_name=Target.instance_name)
			WHEN NOT MATCHED THEN
				INSERT (pfc_id,instance_name)
				VALUES (Source.id,Source.instance_name)
			;

			--теперь загрузим информацию о данных (сначала в @T_dyn, а в конце процедуры в таблицу pfc_data_dyn)!
			;with cte_1 as (
				select T.tt, dyn_h.id, T.value
				from @T_tmp T
				inner join awr.pfc_handle h
					on T.counter_name=h.counter_name and h.[object_name]='awr' and h.instance_name='#dynamic#'
				inner join awr.pfc_handle_dyn dyn_h
					on h.id=dyn_h.pfc_id and T.instance_name=dyn_h.instance_name
			),
			cte_2 as (
				select distinct h.id, max(d.tt) as tt
				from cte_1 h
				left join awr.pfc_data_dyn d 
					on h.id=d.pfc_dyn_id  and d.tt>dateadd(minute,-10,@tt) --оптимизация - фильтруем, используем индекс чтобы быстро искать!
				group by h.id
			)
			insert into @T_dyn(tt,id,cnt)
			select 
				cte_1.tt, cte_1.id,	cte_1.value
			from cte_1
			inner join cte_2
				on cte_2.id=cte_1.id
				and (cte_1.tt>cte_2.tt or cte_2.tt is null)
			;
		end

		--6. Загрузка информации о текущем Uptime для SQL Server в часах
		if exists(select id from awr.pfc_handle where [object_name]='awr' and counter_name='SQL Server Uptime (hours)')
		begin

			;with cte_1 AS(
				select top 1
					datediff(hour,create_date,getdate()) as value
				from sys.databases
				where name='tempdb'
			),
			cte_2 AS (
				select id
				from awr.pfc_handle h
				where [object_name]='awr' and counter_name IN ('SQL Server Uptime (hours)')
			)
			insert into @T2(tt,id,cnt)
			select 
				@tt,
				(select id from cte_2) as id,	
				(select value from cte_1) as value
			from cte_1
			;
		end

		--7. Загрузка информации о Времени отклика (в милисек.)
		if exists(select id from awr.pfc_handle where [object_name]='awr' and counter_name='Response time (ms)')
		begin
			declare @tt_start datetime2, @tt_end datetime2, @t float, @i smallint=0;
			set @tt_start=SYSDATETIME();
			if object_id('tempdb.dbo.#t') is not null
				drop table #t;
			create table #t(t float);
			while (@i<100)
			begin
				insert into #t(t)
				select square((rand()/PI()) * log(rand()) * cos(rand())) as t option(maxdop 0);
				insert into #t(t)
				select square((rand()/2) * log10(rand()) * sin(rand())) as t option(maxdop 0);
				insert into #t(t)
				select square((rand()/3) * log(rand()) * tan(rand())) as t option(maxdop 0);
				insert into #t(t)
				select square((rand()/4) * log10(rand()) * abs(rand())) as t option(maxdop 0);
				insert into #t(t)
				select square((rand()/5) * log(rand()) * cos(rand())) as t option(maxdop 0);
				set @i+=1;
			end
			select @t=avg(t) from #t option(maxdop 0, recompile);
			select @t=max(t) from #t option(maxdop 0, recompile);
			select @t=min(t) from #t option(maxdop 0, recompile);
			select @t=sum(t) from #t option(maxdop 0, recompile);
			set @tt_end=SYSDATETIME();

			;with cte_1 AS(
				select datediff(MILLISECOND,@tt_start, @tt_end) as value
				--option(maxdop 0)
			),
			cte_2 AS (
				select id
				from awr.pfc_handle h
				where [object_name]='awr' and counter_name IN ('Response time (ms)')
			)
			insert into @T2(tt,id,cnt)
			select 
				@tt,
				(select id from cte_2) as id,	
				(select value from cte_1) as value
			from cte_1
			;
		end

		--8. Загрузка информации о текущем времени выполнения Транзакций (sys.dm_tran_active_transactions) 
		if exists(select id from awr.pfc_handle where [object_name]='awr' and counter_name='Tran_RunTime_min')
		begin
			delete @T_tmp;
			;with cte_src1 as(
				select transaction_id as tran_id, [name],transaction_type as tran_type,transaction_state as tran_state, datediff(minute,transaction_begin_time,@tt) as dur_sec,
					row_number() over (order by transaction_begin_time asc, transaction_id asc) as rnk
				from sys.dm_tran_active_transactions
				where transaction_id>1000 and transaction_type<>2
			)
			, cte_src2 as (
				select distinct sum(dur_sec) over () as top5
				from cte_src1
				where rnk<=5
			)
			insert into @T_tmp (tt,counter_name,instance_name, value)
			select distinct @tt as tt, 'Tran_RunTime_min' as counter_name, '_All' as instance_name, sum(dur_sec) over () as [value]
			from cte_src1
			UNION ALL
			select distinct @tt as tt, 'Tran_RunTime_min' as counter_name, '_Max' as instance_name, max(dur_sec) over () as [value]
			from cte_src1
			UNION ALL
			select distinct @tt as tt, 'Tran_RunTime_min' as counter_name, '_Avg' as instance_name, avg(dur_sec) over () as [value]
			from cte_src1
			UNION ALL
			select distinct @tt as tt, 'Tran_RunTime_min' as counter_name, '_Top5' as instance_name, top5 as [value]
			from cte_src2;

			;with cte_src as (
				select tt, instance_name, value
				from @T_tmp
			),cte_pfc as
			(
				select distinct h.id,h.instance_name
				from awr.pfc_handle h
				where [object_name]='awr' and counter_name='Tran_RunTime_min'
			)
			insert into @T2(tt,id,cnt)
			select 
				cte_src.tt,
				cte_pfc.id,	
				cte_src.value
			from cte_src
			inner join cte_pfc
				on cte_src.instance_name=cte_pfc.instance_name
			;		
		end

		--9. Загрузка информации о sp_whoisactive: сколько строк возвращает, и за сколько мсек. отрабатывает 
		if exists(select id from awr.pfc_handle where [object_name]='awr' and counter_name='sp_whoisactive')
		begin
			delete @T_tmp;
			declare @cnt bigint;
			--сформируем схему таблицу и создадим таблицу [tempdb].[dbo].[sp_whoisactive_collect]
			--drop table [tempdb].[dbo].[sp_whoisactive_collect];
			if OBJECT_ID('[tempdb].[dbo].[sp_whoisactive_collect]') IS NULL
			BEGIN
				declare @schema nvarchar(MAX);
				exec sp_whoisactive @schema=@schema output, @return_schema=1,@output_column_list = '[start_time][dd hh:mm:ss.mss][session_id][sql_text][host_name][login_name][program_name][database_name][wait_info][CPU][tempdb%][block%][reads][writes][physical_reads][used_memory][status][open_tran_count][percent_complete][collection_time]'
				set @schema=replace(@schema,'<table_name>', '[tempdb].[dbo].[sp_whoisactive_collect]');
				set @schema=replace(@schema,'[dd hh:mm:ss.mss] varchar(8000)', '[dd hh:mm:ss.mss] varchar(50)');
				set @schema=replace(@schema,'varchar(4000)', 'varchar(500)');
				--select @schema;
				exec(@schema);
			END;
			TRUNCATE TABLE [tempdb].[dbo].[sp_whoisactive_collect];
			set @tt_start=SYSDATETIME();
			EXEC sp_WhoIsActive 
				@output_column_list = '[start_time][dd hh:mm:ss.mss][session_id][sql_text][host_name][login_name][program_name][database_name][wait_info][CPU][tempdb%][block%][reads][writes][physical_reads][used_memory][status][open_tran_count][percent_complete][collection_time]'
				,@destination_table = '[tempdb].[dbo].[sp_whoisactive_collect]';
			select @cnt=COUNT_BIG(*) from [tempdb].[dbo].[sp_whoisactive_collect]; 
			set @tt_end=SYSDATETIME();
	
			insert into @T_tmp (tt,counter_name,instance_name, value)
			select distinct @tt as tt, 'sp_whoisactive' as counter_name, 'cnt' as instance_name, @cnt as [value]
			UNION ALL
			select distinct @tt as tt, 'sp_whoisactive' as counter_name, 'elapsed_time_ms' as instance_name, datediff(MILLISECOND,@tt_start, @tt_end) as [value];

			;with cte_src as (
				select tt, instance_name, value
				from @T_tmp
			),cte_pfc as
			(
				select distinct h.id,h.instance_name
				from awr.pfc_handle h
				where [object_name]='awr' and counter_name='sp_whoisactive'
			)
			insert into @T2(tt,id,cnt)
			select 
				cte_src.tt,
				cte_pfc.id,	
				cte_src.value
			from cte_src
			inner join cte_pfc
				on cte_src.instance_name=cte_pfc.instance_name
			;		
		end

		--10. Загрузка информации о регламентых заданиях (Jobs) sputnik 
		if exists(select id from awr.pfc_handle where [object_name]='awr' and counter_name LIKE 'Job$_%' ESCAPE '$')
		begin
			delete @T_tmp;
			declare @TJ TABLE (Job nvarchar(2000), Step nvarchar(2000),RUN_STATUS varchar(50), Duration_min numeric(19,2));
			insert into @TJ (Job,Step,RUN_STATUS,Duration_min)
			exec info.usp_JobMonitor @Lite=1, @OnlyEnabled=0;

			insert into @T_tmp (tt,counter_name,instance_name, [value])
			select distinct @tt as tt, 'Job_Backup_min' as counter_name, 'Full_or_Diff' as instance_name, CASE WHEN RUN_STATUS='Running' THEN [Duration_min] ELSE 0 END as [value]
			from @TJ
			where Job LIKE '%BackupFull Всех БД%'
			UNION ALL
			select distinct @tt as tt, 'Job_Backup_min' as counter_name, 'Log' as instance_name, CASE WHEN RUN_STATUS='Running' THEN [Duration_min] ELSE 0 END as [value]
			from @TJ
			where Job LIKE '%BackupLog Всех БД%'
			UNION ALL
			select distinct @tt as tt, 'Job_Index_Stats_min' as counter_name, 'GetInfo' as instance_name, CASE WHEN RUN_STATUS='Running' THEN [Duration_min] ELSE 0 END as [value]
			from @TJ
			where Job LIKE '%Обслуживание индексов. Сбор статистик%'
			UNION ALL
			select distinct @tt as tt, 'Job_Index_Stats_min' as counter_name, 'ReIndex' as instance_name, CASE WHEN RUN_STATUS='Running' AND Step='1.rebuild indexes' THEN [Duration_min] ELSE 0 END as [value]
			from @TJ
			where Job LIKE '%Обслуживание индексов. Реиндексация%'
			UNION ALL
			select distinct @tt as tt, 'Job_Index_Stats_min' as counter_name, 'UpdStats' as instance_name, CASE WHEN RUN_STATUS='Running' AND Step='2.recompute stats' THEN [Duration_min] ELSE 0 END as [value]
			from @TJ
			where Job LIKE '%Обслуживание индексов. Реиндексация%'
			UNION ALL
			select distinct @tt as tt, 'Job_Index_Stats_min' as counter_name, 'UpdStatsOpt' as instance_name, CASE WHEN RUN_STATUS='Running' THEN [Duration_min] ELSE 0 END as [value]
			from @TJ
			where Job LIKE '%Обслуживание индексов. Optimizing updatestats%'
			UNION ALL
			select distinct @tt as tt, 'Job_RM_min' as counter_name, '' as instance_name, CASE WHEN RUN_STATUS='Running' THEN sum([Duration_min]) over() ELSE 0 END as [value]
			from @TJ
			where Job LIKE 'RM%'
			UNION ALL
			select distinct @tt as tt, 'Job_ShrinkTempDB_min' as counter_name, '' as instance_name, CASE WHEN RUN_STATUS='Running' THEN [Duration_min] ELSE 0 END as [value]
			from @TJ
			where Job LIKE '%Зачистка TempDb%'
			UNION ALL
			select distinct @tt as tt, 'Job_Cntr' as counter_name, 'All' as instance_name, COUNT_BIG(DISTINCT Job) as [value]
			from @TJ
			UNION ALL
			select distinct @tt as tt, 'Job_Cntr' as counter_name, 'Active' as instance_name, COUNT_BIG(DISTINCT Job) as [value]
			from @TJ
			where RUN_STATUS='Running' and Job not like '% Сбор данных awr. pfc%'
			UNION ALL
			select distinct @tt as tt, 'Job_Cntr' as counter_name, 'Failed_or_Cancel' as instance_name, COUNT_BIG(DISTINCT Job) as [value]
			from @TJ
			where RUN_STATUS IN ('Failed','Canceled by user')
			;

			;with cte_pfc as
			(
				select distinct h.id, h.instance_name, h.counter_name
				from awr.pfc_handle h
				where [object_name]='awr' and counter_name LIKE 'Job$_%' ESCAPE '$'
			)
			insert into @T2(tt,id,cnt)
			select 
				src.tt,
				cte_pfc.id,	
				src.[value]
			from @T_tmp as src
			inner join cte_pfc
				on src.counter_name=cte_pfc.counter_name AND src.instance_name=cte_pfc.instance_name
			;		
		end

		--11. Загрузка информации об использовании TempDB в Мб
		if exists(select id from awr.pfc_handle where [object_name]='awr' and counter_name = 'tempdb_using_Mb')
		begin
			delete @T_tmp;
			;with cte_src1 as(
				select distinct
					CASE pr 
						WHEN 'rowver_mb' THEN 'Row-versions'
						WHEN 'user_mb' THEN 'User-objects'
						WHEN 'internal_mb' THEN 'Internal'
						WHEN 'mixed_mb' THEN 'Mixed-extents'
						WHEN 'total_mb' THEN '_Total'
						ELSE NULL
					END as instance_name,	 
					vl as [value]
				from info.vtempusing
			)
			insert into @T_tmp (tt,counter_name,instance_name, value)
			select @tt as tt, 'tempdb_using_Mb' as counter_name, instance_name, [value]
			from cte_src1;

			;with cte_src as (
				select tt, instance_name, [value]
				from @T_tmp
			),cte_pfc as
			(
				select distinct h.id,h.instance_name
				from awr.pfc_handle h
				where [object_name]='awr' and counter_name='tempdb_using_Mb'
			)
			insert into @T2(tt,id,cnt)
			select 
				cte_src.tt,
				cte_pfc.id,	
				cte_src.value
			from cte_src
			inner join cte_pfc
				on cte_src.instance_name=cte_pfc.instance_name
			;		
		end


		--Загружаем в таблицу в БД sputnik все собранные данные Счетчиков производительности из dm_os_performance_counters и другие
		insert into awr.pfc_data(tt,pfc_id,value)
		select distinct @tt as tt, T.id as pfc_id,
			case 
				when T_base.value_base=0.00 then 0.00
				when T.counter_type=537003264 and T_base.value_base is not null then cast(t.value / T_base.value_base * 100.00  as numeric(19,2))
				else value
			end as value
		from 
		(
			select distinct id, counter_type,
				case 
					when counter_type=537003264 then SUM(value) over (partition by id)
					else value
				end as value
			from @T1
			where counter_type is not null
		) as T
		left join
		(
			select id, MAX(value) over (partition by id,counter_type) as value_base
			from @T1
			where counter_type is null
		) T_base
			on T.id=T_base.id
		UNION
		select tt, id as pfc_id, cnt as value
		from @T2
		;
	
		--Загружаем в таблицу в БД sputnik все собранные ДИНАМИЧЕСКИЕ данные 
		insert into awr.pfc_data_dyn(tt,pfc_dyn_id,value)
		select distinct tt, id as pfc_dyn_id, cnt as value
		from @T_dyn; 

	end