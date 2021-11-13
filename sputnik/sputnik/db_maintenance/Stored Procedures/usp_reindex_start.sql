	/* =============================================
	-- Author:		Андрей Иванов (sqland1c)
	-- Create date: 24.01.2013 (1.0)
	-- Description: Процедура для запуска Реиндексации по всем настроенным в таблице ReindexConf базам.
					Параметр @DBFilter - здесь можно указать для какой конкретно базы будет выполнен запуск.
					Параметр @StartUpdateStats - если задан 1, то запускается Сбор статистик по индексам. Если 0, то запускается
						Реиндексация. По умолчанию 0.
	-- Update:		
					21.02.2014 (1.1) Получения дня недели реализовано с помощью отдельной функции uf_GetWeekDay
					26.02.2014 (1.11) Изменения столбца filter_old_hours.
					11.03.2014 (1.2) В таблице ReindexConf добавлено новое поле PauseMirroring (тип bit). Значение по умолчанию 0.
									 Если значение 1, тогда на время Реиндексации Сеанс Зеркалирования для базы должен быть приостановлен!
					12.03.2014 (1.22) Небольшое исправление - теперь при вызове процедуры usp_reindex_run Параметры указываются через ИмяПараметра=Значение.
					15.10.2014 (1.25) Добавлен новый параметр @TableFilter - теперь можно запустить сбор статистик и реиндексацию для конкретной таблицы!!
					01.03.2015 (1.27) Добавлен вызов ХП usp_ReComputeStats - Интеллектуальный пересчет статистик распределения сразу после Реиндексации.
									  Будут пересчитываться только те статистики, которые не пересчитались при Реиндексации!
					02.03.2015 (1.28) Вызов модуля usp_freeproccache (сброс процедурного кэша) перенесён в конец этого модуля из модуля usp_reindex_run.
					23.06.2015 (2.00) Полная оптимизация схемы Реиндексации. Процедура запуска значительно переделана под новую схему.
					07.07.2015 (2.01) Исправлен алгоритм выбора окна обслуживания!
					09.07.2015 (2.02) Добавлен параметр @only_show - просмотр данных для обслуживания (без обслуживания)!
									 Также исправлен вызов Реиндексации - был пропущен параметр @UniqueName_SL.
					07.09.2015 (2.03) Исправлен алгоритм выборка окна обслуживания.
					16.12.2015 (2.04) Исправлен алгоритм выбора окна обслуживания. Неправильно отрабатывал, когда задано окно в два дня (сегодня и завтра).
					10.08.2016 (2.05) Исправлен алгоритм запуска обновления данных по индексам (@StartUpdateStats=1). При запуске задан параметр @oldupdhours=3
					чтобы обновлять информацию о состоянии индексов каждые 3 часа.
					18.08.2016 (2.06) Исправлен алгоритм запуска обновления данных по индексам (@StartUpdateStats=1). 
					При запуске задан новый параметр @rowlimit_max=1000 - за 1 запуск обновляем только 1000 самых устаревших статистик .
					02.12.2016 (2.102) В новой версии поддерживается реиндексация и обновление статистик по нескольким БД.
					19.07.2017 (2.104) Небольшой ПАТЧ - добавлена возможность использования параметров filter_DataUsedMb_min и filter_DataUsedMb_max
					в алгоритме запуска Пересчёта статистик распределения.
					02.12.2017 (2.105) Расширены строковые переменные (БД).
					14.11.2018 (2.110) Добавлена совместимость с 2008 версией (iif заменены на case).
					13.11.2021 (2.112) maxdop option was added for using in reindex operations
	-- ============================================= */
	CREATE PROCEDURE db_maintenance.usp_reindex_start
		@DBFilter nvarchar(2000) = null,
		@StartUpdateStats bit = 0,
		@TableFilter nvarchar(2000) = null,
		@StartRecomputeStats bit = 0,
		@only_show bit = 0
	as
	begin
		set nocount on;
		declare @getdate datetime, @gettime time,  @WeekDay tinyint;
		declare @DBName nvarchar(2000), @RowLimit smallint, @delayperiod char(12), @filter_pages_min int , @filter_pages_max int , @UniqueName_SL nvarchar(200),
		@filter_fragm_min tinyint, @filter_fragm_max tinyint, @filter_old_hours tinyint, 
		@fragm_tresh tinyint, @set_fillfactor tinyint, @set_compression char(4), @set_online char(3), @set_sortintempdb char(3), @PauseMirroring bit,
		@DeadLck_PR smallint, @Lck_Timeout int, @filter_rows_min int, @filter_rows_max int, @filter_perc_min decimal(18,2), @filter_perc_max decimal(18,2),
		@policy_scan varchar(100), @timeout_sec int, @set_maxdop smallint;
		declare @mv_Name nvarchar(200);
		declare @filter_DataUsedMb_min numeric(9,1), @filter_DataUsedMb_max numeric(9,1);
		set @getdate=GETDATE();
		set @gettime=CAST(@getdate as TIME);
		--Определяем текущий день недели!
		select @WeekDay=sputnik.info.uf_GetWeekDay(@getdate);

		--Определяем наиболее подходящее окно обслуживания для текущего времени!
		--insert into @mv(Name)
		select top 1 @mv_Name=UniqueName
		from sputnik.db_maintenance.mw as mw
		where 
				(@getdate >= mw.DateOpen OR mw.DateOpen IS NULL)
			  and (@getdate <= mw.DateClose OR mw.DateClose IS NULL)
			  and (
				(
					(@gettime >= case when mw.TimeOpen<mw.TimeClose then mw.TimeOpen else '00:00:00' end OR mw.TimeOpen IS NULL)
					 and (@gettime <= mw.TimeClose OR mw.TimeClose IS NULL)
				) 
				OR	
				(	(@gettime >= mw.TimeOpen OR mw.TimeOpen IS NULL)
					and (@gettime <= case when mw.TimeOpen<mw.TimeClose then mw.TimeClose else '23:59:59.999' end OR mw.TimeClose IS NULL)
				)
			  )
			  and (CHARINDEX(CAST(@WeekDay as CHAR(1)),mw.WeekDays/*,CAST(@WeekDay as CHAR(1))*/)>0 OR mw.WeekDays IS NULL)
			  /*and UniqueName IN (select UniqueName_MW from sputnik.db_maintenance.ReindexConf)*/
		order by case when TimeClose<TimeOpen then DATEDIFF(minute, TimeOpen, '23:59:59')+1+DATEDIFF(minute, '00:00:00', TimeClose) else DATEDIFF (minute, TimeOpen,TimeClose) end;
		--select @mv_Name as MW_Name;

		if @StartRecomputeStats=0
		BEGIN
			--Определяем настройки Реиндексации на основании окна обслуживания!
			--Проверяем что эти базы существуют, и их состояние ONLINE, и это не системные БД!
			declare INDEXs cursor
			for
				select Conf.*
				from
				(
					select [DBName]
							--,[UniqueName_MW]
							,[UniqueName_SL]
							,[RowLimit]
							,[delayperiod]
							,[filter_pages_min]
							,[filter_pages_max]
							,[filter_fragm_min]
							,[filter_fragm_max]
							,[filter_old_hours]
							,[fragm_tresh]
							,[set_fillfactor]
							,[set_compression]
							,[set_online]
							,[set_sortintempdb]
							,[PauseMirroring]
							,[DeadLck_PR]
							,[Lck_Timeout]
							,[timeout_sec]
							,[set_maxdop]
					from sputnik.db_maintenance.ReindexConf
					where
						(@DBFilter is null or DBName=@DBFilter)
						and (UniqueName_MW = @mv_Name ) 
				) Conf
				inner join
				(
					select name as dbname
					from sys.databases
					where 
					state_desc='ONLINE' and is_in_standby=0 and is_read_only=0
					and name not in ('master','msdb','model','tempdb')
					union all 
					select NULL as dbname
				) DBFact
				on Conf.dbname=DBFact.dbname OR (DBFact.dbname IS NULL and Conf.DBName IS NULL)
		END
		else 
		BEGIN
			--Определяем настройки Пересчета статистик на основании окна обслуживания!
			--Проверяем что эти базы существуют, и их состояние ONLINE, и это не системные БД!
			declare ReStats cursor
			for
				select Conf.*
				from
				(
					select  [DBName]
						  --,[UniqueName_MW]
						  ,[UniqueName_SL]
						  ,[RowLimit]
						  ,[delayperiod]
						  ,[filter_rows_min]
						  ,[filter_rows_max]
						  ,filter_DataUsedMb_min , filter_DataUsedMb_max 
						  ,[filter_perc_min]
						  ,[filter_perc_max]
						  ,[filter_old_hours]
						  ,[policy_scan]
						  ,[PauseMirroring]
						  ,[DeadLck_PR]
						  ,[Lck_Timeout]
						  ,[timeout_sec]
					from sputnik.db_maintenance.RecomputeStatsConf
					where
						(@DBFilter is null or DBName=@DBFilter)
						and (UniqueName_MW = @mv_Name ) 
				) Conf
				inner join
				(
					select name as dbname
					from sys.databases
					where 
					state_desc='ONLINE' and is_in_standby=0 and is_read_only=0
					and name not in ('master','msdb','model','tempdb')
					union all 
					select NULL as dbname
				) DBFact
				on Conf.dbname=DBFact.dbname OR (DBFact.dbname IS NULL and Conf.DBName IS NULL)
		END
	
		if @StartRecomputeStats=0 --Запуск Реиндексации или сбора информации по таблицам и индексам.
		BEGIN
			open INDEXs
			fetch next from INDEXs
				into @DBName, @UniqueName_SL, @RowLimit, @delayperiod, @filter_pages_min, @filter_pages_max, @filter_fragm_min, @filter_fragm_max,
					@filter_old_hours, @fragm_tresh, @set_fillfactor, @set_compression, @set_online, @set_sortintempdb, @PauseMirroring, @DeadLck_PR,
					@Lck_Timeout,@timeout_sec, @set_maxdop;
			while @@FETCH_STATUS=0
			begin
				if @StartUpdateStats=0
					--Запуск Реиндексации!
					EXEC db_maintenance.usp_reindex_run 
						@db_name=@DBName, 
						@UniqueName_SL=@UniqueName_SL,
						@RowLimit=@RowLimit, 
						@delayperiod=@delayperiod, 
						@filter_pages_min=@filter_pages_min,
						@filter_pages_max=@filter_pages_max, 
						@filter_fragm_min=@filter_fragm_min,
						@filter_fragm_max=@filter_fragm_max,
						@filter_old_hours=@filter_old_hours, 
						@fragm_tresh=@fragm_tresh, 
						@set_fillfactor=@set_fillfactor, 
						@set_compression=@set_compression, 
						@set_online=@set_online, 
						@set_sortintempdb=@set_sortintempdb,
						@PauseMirroring=@PauseMirroring,
						@TableFilter=@TableFilter,
						@DeadLck_PR=@DeadLck_PR,
						@Lck_Timeout=@Lck_Timeout,
						@only_show=@only_show,
						@timeout_sec=@timeout_sec,
						@MaxDop = @set_maxdop;
				else
				BEGIN
					--Запуск Сбора статистик(информации) по индексам и таблицам!
					--Указываем @oldupdhours=3 - означает, что обновляем только те данные, которые старше 3 часов.
					--@rowlimit_max=1000 - за 1 запуск обновляем только 1000 самых устаревших статистик .
				
					--для отладки
					--select @DBName as DBName, @rowlimit as rowlimit, @delayperiod as DelayPeriod,
					--	   3 as OldUpdhours, @TableFilter as TableFilter;
					exec db_maintenance.usp_reindex_updatestats 
						@db_name=@DBName, 
						@rowlimit=@rowlimit,
						@delayperiod=@delayperiod,
						@oldupdhours=3,
						@TableFilter=@TableFilter,
						@rowlimit_max=3000;
				END
				fetch next from INDEXs
				into @DBName, @UniqueName_SL, @RowLimit, @delayperiod, @filter_pages_min, @filter_pages_max, @filter_fragm_min, @filter_fragm_max,
					@filter_old_hours, @fragm_tresh, @set_fillfactor, @set_compression, @set_online, @set_sortintempdb, @PauseMirroring, @DeadLck_PR,
					@Lck_Timeout,@timeout_sec;
			end;
			CLOSE INDEXs;
			DEALLOCATE INDEXs;
		END
		else if @StartRecomputeStats=1
		BEGIN
		--Запуск пересчета статистик распределения теперь тоже осуществляется из этой процедуре
		--также как и запуск Реиндексации - на основании окна обслуживания!
			open ReStats
			fetch next from ReStats
				into @DBName, @UniqueName_SL, @RowLimit, @delayperiod, @filter_rows_min, @filter_rows_max, @filter_DataUsedMb_min, @filter_DataUsedMb_max,
				 @filter_perc_min, @filter_perc_max, @filter_old_hours, @policy_scan, @PauseMirroring, @DeadLck_PR, @Lck_Timeout,@timeout_sec;
			while @@FETCH_STATUS=0
			begin
				exec db_maintenance.usp_RecomputeStats
					@DBName=@DBName, 
					@UniqueName_SL=@UniqueName_SL,
					@RowLimit=@RowLimit,
					@delayperiod=@delayperiod,
					@filter_rows_min=@filter_rows_min,
					@filter_rows_max=@filter_rows_max,
					@filter_DataUsedMb_min=@filter_DataUsedMb_min,
					@filter_DataUsedMb_max=@filter_DataUsedMb_max,
					@filter_perc_min=@filter_perc_min,
					@filter_perc_max=@filter_perc_max,
					@filter_old_hours=@filter_old_hours,
					@policy_scan=@policy_scan,
					@PauseMirroring=@PauseMirroring,
					@DeadLck_PR=@DeadLck_PR,
					@Lck_Timeout=@Lck_Timeout,
					@only_show=@only_show,
					@timeout_sec=@timeout_sec;
		
				fetch next from ReStats
				into @DBName, @UniqueName_SL, @RowLimit, @delayperiod, @filter_rows_min, @filter_rows_max, @filter_DataUsedMb_min, @filter_DataUsedMb_max,
				 @filter_perc_min, @filter_perc_max, @filter_old_hours, @policy_scan, @PauseMirroring, @DeadLck_PR, @Lck_Timeout,@timeout_sec;
			end
			CLOSE ReStats;
			DEALLOCATE ReStats;
		END
	end