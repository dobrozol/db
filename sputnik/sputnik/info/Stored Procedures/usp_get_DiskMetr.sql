
/* =============================================
-- Author:		Андрей Иванов (sqland1c)
-- Create date: 09.09.2015 (1.0)
-- Description: Получаем данные о счетчике производительности Logical Disk из xEvents xe_DiskInfo (собирает каждые 15 сек.)
				xEvents xe_DiskInfo должен быть включен и должен собирать информацию.
				Вывод результатов основан на предположении что все значения получаемых счетчиков (кроме free_megabytes и cur_disk_queue_len) из xEvents (Logical Disk) куммулятивные (то есть постоянно накапливаются)
				Значения большинство показателей высчитываются как среднее примерно за последнюю минуту (учитывается 4 последних сборов данных)
				Значения free_megabytes и cur_disk_queue_len берется из последнего сбора (самые свежие данные)
				
-- Update:		23.09.2015 (1.03)
				Добавлен алгоритм включения и настройки Extended Events "xe_DiskInfo" если он ещё не был настроен!
				Также добавлено исправление если из кольцевого буфера получаем NULL - здесь делаем включение
				сессии xEvents и возвращаем пустой набор данных.
				28.09.2015 (1.04)
				Включение и настройка xEvents [xe_DiskInfo] перенесена в начало ХП usp_pfc_collect,  а здесь закомментировано.
				При получении данных из xml расширен тип для столбцов % использования и простоя диска с numeric(19,2) до numeric(38,2).
				19.10.2015 (1.05)
				Небольшое исправление: включение сессии закоментировано (т.к. все равно это невозможно внутри транзакции).
-- ============================================= */
CREATE PROCEDURE info.usp_get_DiskMetr

AS
begin
	--IF NOT EXISTS (SELECT * FROM sys.server_event_sessions WHERE name = 'xe_DiskInfo')
	--BEGIN
	----Настраиваем и включаем сборщик данных через Extended Events!
	----xEvents сессия для сбора информации о счетчиках группы Logical Disk (инфо обновляется каждые 15 сек.)
	----Данные сохраняются в кольцевой буфер и хранятся тут совсем недолго.
	----Эти данные нужно успеть захватить и обработать и положить в схему awr в базу 
	--	CREATE EVENT SESSION [xe_DiskInfo] ON SERVER 
	--		ADD EVENT sqlserver.perfobject_logicaldisk 
	--		ADD TARGET package0.ring_buffer(SET max_events_limit=(128),max_memory=(32768))
	--	WITH (MAX_MEMORY=4096 KB,EVENT_RETENTION_MODE=ALLOW_SINGLE_EVENT_LOSS,MAX_DISPATCH_LATENCY=10 SECONDS,MAX_EVENT_SIZE=0 KB,MEMORY_PARTITION_MODE=NONE,TRACK_CAUSALITY=OFF,STARTUP_STATE=ON);
	--	ALTER EVENT SESSION [xe_DiskInfo] ON SERVER STATE = START;
	--	--геренируем задержку в 30 сек, чтобы данные успелись собраться!
	--	--если не успеет собраться, тогда соберем в след. раз!
	--	waitfor delay '00:00:30.000';
	--END
	declare @xml_data xml, @tz_offset smallint=datepart(TZoffset,SYSDATETIMEOFFSET());
	SELECT @xml_data=CAST(xet.target_data AS xml)
			FROM sys.dm_xe_session_targets AS xet
			JOIN sys.dm_xe_sessions AS xe
			   ON (xe.address = xet.event_session_address)
			WHERE xe.name = 'xe_DiskInfo'
				and xet.target_name = 'ring_buffer';
	--select @xml_data;
	IF @xml_data is null
	begin
		--Если возвращен NULL, значит скорее всего Сессия не включена!
		--ALTER EVENT SESSION [xe_DiskInfo] ON SERVER STATE = START;
		select null as tt, null as counter_name, null as instance_name, null as value
		where 1=0;
	end
	else
	begin 
		;with data_xml as
		(	SELECT 
				xed.event_data.value('(@timestamp)[1]', 'datetime') AS [tt_utc],
				xed.event_data.value('(data[@name="instance_name"]/value)[1]', 'varchar(50)') AS instance_name,
				xed.event_data.value('(data[@name="free_megabytes"]/value)[1]', 'numeric(19,2)') AS free_megabytes,
				xed.event_data.value('(data[@name="disk_writes_per_second"]/value)[1]', 'numeric(19,2)') AS disk_writes_per_second,
				xed.event_data.value('(data[@name="disk_reads_per_second"]/value)[1]', 'numeric(19,2)') AS disk_reads_per_second,
				xed.event_data.value('(data[@name="disk_write_bytes_per_second"]/value)[1]', 'numeric(19,2)') AS disk_write_bytes_per_second,
				xed.event_data.value('(data[@name="disk_read_bytes_per_second"]/value)[1]', 'numeric(19,2)') AS disk_read_bytes_per_second,
				xed.event_data.value('(data[@name="current_disk_queue_length"]/value)[1]', 'numeric(19,2)') AS current_disk_queue_length,
				xed.event_data.value('(data[@name="percent_idle_time"]/value)[1]', 'numeric(38,2)') AS percent_idle_time,
				xed.event_data.value('(data[@name="percent_disk_read_time"]/value)[1]', 'numeric(38,2)') AS percent_disk_read_time,
				xed.event_data.value('(data[@name="percent_disk_write_time"]/value)[1]', 'numeric(38,2)') AS percent_disk_write_time
			FROM (select @xml_data as data) as data
				CROSS APPLY data.nodes('//RingBufferTarget/event') AS xed (event_data)
		),
		--SELECT * FROM @T_data;
		 data_tt_rank as (
			select distinct tt_utc, DENSE_RANK() over (order by tt_utc desc) as tt_rank
			from data_xml
		),
		data_tt as (
			select distinct
				max(tt_utc) over () as tt_max,
				min(tt_utc) over () as tt_p
			from data_tt_rank 
			where tt_rank between 1 and 4
		),
		data_results_1 as (
			select 
				t_max.tt_utc as tt_utc,
				datediff(second,t_p.tt_utc,t_max.tt_utc) as sec_delta,
				t_max.instance_name,
				t_max.free_megabytes as Free_Mb,
				t_max.disk_writes_per_second-t_p.disk_writes_per_second as disk_writes_sec_Delta,
				t_max.disk_reads_per_second-t_p.disk_reads_per_second as disk_reads_sec_Delta,
				(t_max.disk_write_bytes_per_second-t_p.disk_write_bytes_per_second) as disk_write_B_sec_Delta,
				(t_max.disk_read_bytes_per_second-t_p.disk_read_bytes_per_second) as disk_read_B_sec_Delta,
				t_max.current_disk_queue_length as cur_disk_queue_len,
				t_max.percent_idle_time-t_p.percent_idle_time as Disk_IdleTime,
				t_max.percent_disk_read_time-t_p.percent_disk_read_time as Disk_ReadTime,
				t_max.percent_disk_write_time-t_p.percent_disk_write_time as Disk_WriteTime
			from data_xml as t_max
			inner join data_xml as t_p
				on t_p.instance_name=t_max.instance_name
			where t_max.tt_utc=(select tt_max from data_tt)
				and t_p.tt_utc=(select tt_p from data_tt)
				and t_max.instance_name like '%:' 
		),
		data_result_2 as (
			select 
				dateadd(minute,@tz_offset,tt_utc) as tt,
				sec_delta, 
				instance_name, Free_Mb, 
				cast(disk_reads_sec_Delta/sec_delta as numeric(19,2)) as avg_disk_reads_sec, 
				cast(disk_writes_sec_Delta/sec_delta as numeric(19,2)) as avg_disk_writes_sec, 
				cast((disk_read_B_sec_Delta/sec_delta)/1024.00 as numeric(19,2)) as avg_disk_read_Kb_sec,
				cast((disk_write_B_sec_Delta/sec_delta)/1024.00 as numeric(19,2)) as avg_disk_write_Kb_sec,
				cur_disk_queue_len,
				cast(Disk_IdleTime/(Disk_IdleTime+Disk_ReadTime+Disk_WriteTime)*100.00 as numeric (19,2)) as Disk_IdleTime_perc,
				cast(Disk_ReadTime/(Disk_IdleTime+Disk_ReadTime+Disk_WriteTime)*100.00 as numeric (19,2)) as Disk_ReadTime_perc,
				cast(Disk_WriteTime/(Disk_IdleTime+Disk_ReadTime+Disk_WriteTime)*100.00 as numeric (19,2)) as Disk_WriteTime_perc
			from data_results_1
			where sec_delta>=44
		)
		select tt,counter_name,instance_name, value
		from data_result_2
		unpivot(value for counter_name in ([Free_Mb],[avg_disk_reads_sec],[avg_disk_writes_sec],[avg_disk_read_Kb_sec],[avg_disk_write_Kb_sec],
			[cur_disk_queue_len], [Disk_IdleTime_perc], [Disk_ReadTime_perc], [Disk_WriteTime_perc])
		)unpvt
		order by counter_name,instance_name
		;	
	end
end