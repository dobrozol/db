
	/* =============================================
	-- Author:		Андрей Иванов (sqland1c)
	-- Create date: 23.03.2018 (1.0)
	-- Description: Процедура для сбора и сохранения в базе sputnik данных по использованию базы TempDB. 
					А также прибивание самых долгих и тяжёлых сессий, которые больше всех используют TempDB.
					Все параметры для прибивания сессий берутся из таблицы config.params в БД sputnik.
	-- Update:		30.03.2018 (1.005)
					Изменён алгоритм определения для прибивания тяжёлых сессий:
					максимум общего использования tempdb это параметр @tempusing_total_gb	
					общее использование tempdb(когда open_tran=0) это параметр @Kill_opentran0_tempusing_gb	
	*/
	CREATE PROCEDURE awr.usp_tempusing_do
	AS
	BEGIN
		set nocount on;
		set QUOTED_IDENTIFIER ON;
		declare @tt datetime2(2)=SYSDATETIME(), @spid smallint,@cmd nvarchar(100);
		declare @kill_enabled bit, @Kill_tempusing_gb numeric(9,0), @Kill_opentran0_tempusing_gb numeric(9,0), @kill_rq_elapsed_sec numeric(9,0), @kill_userinternalusing_gb numeric(9,0),@kill_rowver_tranactive_sec numeric(9,0);
		select @kill_enabled=cast(vl as bit)
		from config.params where obj='awr.tempusing' and pr='kill: enabled';
		select @Kill_tempusing_gb=cast(vl as numeric(9,0))
		from config.params where obj='awr.tempusing' and pr='kill: total tempdb using(Gb)>';
		select @Kill_opentran0_tempusing_gb=cast(vl as numeric(9,0))
		from config.params where obj='awr.tempusing' and pr='kill: opentran=0/total tempdb using(Gb)>';
		select @kill_rq_elapsed_sec=cast(vl as numeric(9,0))
		from config.params where obj='awr.tempusing' and pr='kill: query elapsed(sec)>';
		select @kill_userinternalusing_gb=cast(vl as numeric(9,0))
		from config.params where obj='awr.tempusing' and pr='kill: user+internal using(gb)>';
		select @kill_rowver_tranactive_sec=cast(vl as numeric(9,0))
		from config.params where obj='awr.tempusing' and pr='kill: row-verions tran active (sec)>';
		if object_id('tempdb..#tmp01') is not null
			drop table #tmp01;
		create table #tmp01(
				tt datetime2(2),
				[spid] smallint not null,
				[login_time] datetime not null,
				rq_start_time datetime null,
				user_mb numeric(9,0) null,
				internal_mb numeric(9,0) null,
				sleep_status bit not null,
				rq_elapsed_sec numeric(9,0) null,
				rowver_tran_sec numeric(9,0) null,
				open_tran_flag bit not null,
				kill_flag bit not null
		)
		;with cte01 as (
		select 
			@tt as tt, 
			ss.[spid],ss.[login_time],ss.[rq_start_time],
			ss.[user_mb],ss.[internal_mb], 
			case when ss.[status]='sleeping' then 1 else 0 end as sleep_status,
			ss.[elapsed_sec] as rq_elapsed_sec,
			ver.[tran_active_sec] as [rowver_tran_sec],
			case when ss.[open_tran_cnt]>0 then 1 else 0 end as [open_tran_flag],
			0 as kill_flag
		from info.vtempusing_ss as ss
		left join info.vtempusing_ver as ver
			on ss.spid=ver.spid and ss.[login_time]=ver.[login_time] and ver.[tran_active_sec]>300
		where ss.elapsed_sec>150)
		,cte02 as(
		select 
			@tt as tt, 
			ver.[spid],ver.[login_time],null as [rq_start_time],
			null as [user_mb], null as [internal_mb],
			case when [status]='sleeping' then 1 else 0 end as sleep_status,
			null as rq_elapsed_sec,
			ver.[tran_active_sec] as [rowver_tran_sec],
			case when [open_tran_cnt]>0 then 1 else 0 end as [open_tran_flag],
			0 as kill_flag
		from info.vtempusing_ver as ver
		left join cte01 
			on ver.spid=cte01.spid and ver.[login_time]=cte01.[login_time]
		where [tran_active_sec]>300 and cte01.spid is null
		)
		insert into #tmp01(tt,[spid], [login_time], [rq_start_time],[user_mb],[internal_mb],sleep_status, rq_elapsed_sec, [rowver_tran_sec], open_tran_flag,kill_flag)
		select tt,[spid], [login_time], [rq_start_time],[user_mb],[internal_mb],sleep_status, rq_elapsed_sec, [rowver_tran_sec], open_tran_flag,kill_flag from cte01
		union all
		select tt,[spid], [login_time], [rq_start_time],[user_mb],[internal_mb],sleep_status, rq_elapsed_sec, [rowver_tran_sec], open_tran_flag,kill_flag from cte02;
		declare @tempusing_total_gb numeric(9,0);
		select top 1 @tempusing_total_gb=cast((vl/1024) as numeric(9,0)) from info.vtempusing where pr='total_mb';
		if @kill_enabled=1
		begin
			declare kl cursor for
				select spid 
				from #tmp01
				where 
					(
						(open_tran_flag=0 and @tempusing_total_gb>@Kill_opentran0_tempusing_gb) or @tempusing_total_gb>@Kill_tempusing_gb
					)
					and (
						(rq_elapsed_sec>@kill_rq_elapsed_sec and [user_mb]+[internal_mb]>@kill_userinternalusing_gb*1024)
						or ([rowver_tran_sec]>@kill_rowver_tranactive_sec)
					);
			open kl;
			fetch next from kl into @spid;
			while @@fetch_status=0
			begin
				set @cmd='kill '+cast(@spid as varchar(7));
				exec(@cmd);
				update #tmp01
				set kill_flag=1
				where spid=@spid and kill_flag=0;
				fetch next from kl into @spid;
			end
			close kl;
			deallocate kl;
		end
		insert into awr.tempusing (tt,[spid], [login_time], [rq_start_time],[user_mb],[internal_mb],sleep_status, rq_elapsed_sec, [rowver_tran_sec], open_tran_flag,kill_flag)
		select tt,[spid], [login_time], [rq_start_time],[user_mb],[internal_mb],sleep_status, rq_elapsed_sec, [rowver_tran_sec], open_tran_flag,kill_flag
		from #tmp01;
		if object_id('tempdb..#tmp01') is not null
			drop table #tmp01;	
	END