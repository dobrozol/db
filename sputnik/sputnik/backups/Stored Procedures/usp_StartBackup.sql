
	/* =============================================
	-- Author:		Андрей Иванов (sqland1c)
	-- Create date: 10.12.2013 (1.0)
	-- Description: Запуск резервного копирования для баз, указанных в таблицах BackConf и BackConfWeekly
					В качестве параметров можно указать имя конкретной базы данных!
					А также Тип бэкапа!
	-- Update:		26.12.2013 (1.1)
					Теперь Выборка баз для резервного копирования осуществляется сразу из двух таблиц BackConf и BackConfWeekly.
					Также переделан запрос для получения имён баз для резервного копирования!
					Теперь базы проверяются на существование (они могли быть удалены) и что состояние этих баз ONLINE
					16.01.2014 (1.2)
					Теперь для всех бэкапов новый алгоритм: сначала создаются все бэкапы локально, а затем уже происходит копирование всех файлов на сетевой ресурс.
					Также тип курсора изменён на scroll, чтобы можно было переходить по курсору во все стороны (и получить первую запись).
					18.01.2014 (1.22)
					Добавлены новын параметры
						@OnlyCopy - если будет 1, то для всех баз будет запушен скрипт копирования всех файлов бэкапов на сетевой ресурс.
						При этом создание самого бэкапа не будет. По умолчанию 0.
					17.03.2014 (1.25)
					При выборке Имя базы для создания бэкапов добавлен параметр DISTINCT.
					16.04.2014 (1.3)
					Добавлена поддержка Резервного копирования в Always ON! В зависимости от настроек AlwaysOn проверяется нужно ли выполнять
					Бэкап с текущей реплики. Сработает только для SQL Server 2012 и старше.
					13.10.2014 (1.33)
					Добавлен новый параметр @NoStats - если задан 1, то при выполнении бэкапа не будет выводиться дополнительная информация о 
					выполненном %. Также изменен алгоритм запуска процедуры usp_RunBack - при указании параметров теперь используется имена параметров
					(ранее параметры передавались без имени - по позиции). 
					Также для параметра @type задано значение по умолчанию - Full(полный бэкап!)
					11.11.2014 (1.40)
					Полностью изменен алгоритм копирования файлов бэкапов - теперь это выполняется через PowerShell в новом отдельном модуле usp_CopyBack.
					Причем копироваться будут только бэкапы сделанные только что! Чтобы копировать все бэкапы нужно задать параметр @OnlyCopy!
					21.11.2014 (1.41)
					Для бэкапов ЛОГОВ при копировании бэкапов изменен отбор по дате - не с текущего момента, а за последние сутки! 
					18.01.2015 (1.50)
					Добавлена возможность обработки бэкапов в многопоточном режиме (через Service Broker).
					Работа в многопоточном режиме определяется в столбце pp в таблице backups.config
					18.01.2015 (1.51)
					Многопоточный режим бэкапов теперь работает через новую очередь - [ProcessorQueueHard].
					Это очередь создана специально для тяжелых операций. Кол-во потоков - максимум 2.
					13.07.2015 (1.52)
					Резерное копирование в AlwaysOn с вторичных реплик - пока добавлена поддержка только Log бэкапов.
					31.12.2015 (1.55) 
					Добавлена проверка для режима pp! Добавляем в очередь сообщение о бэкапе, только если его ещё нет в Очереди!
					01.04.2016 (1.56) 
					Оператор IIF заменён на CASE для нормальной работы на SQL Server версий  < 2012!
					25.08.2016 (1.57) 
					Добавлена возможность бэкапить системную базу model.
					25.10.2016 (1.580)
					Добавлен новый параметр @ForceFULL - принудительно выполнить полный бэкап (даже если по расписанию должен быть DIFF).
					Действует, когда в параметре @type задан 'Full'.
					02.11.2016 (1.585)
					Добавлена проверка для Log-бэкапов: пропускать базу, если для неё установлен recovery_model=SIMPLE.

					05.06.2017 (1.590)
					Добавлены новые параметры @DBList и @DBList_delimeter: первый параметр определяет список баз
					в виде строки, разделённых Разделителем @DBList_delimeter (по умолчанию это Запятая).
					Теперь можно сделать бэкапы по списку баз. Старый параметр @DBFilter - отбор по конкретной базе.

					29.10.2018 (1.591)
					Когда определён только DIFF (без FULL): при выборке из vGetAllBackConf добавлено условие, чтобы учитывать
					такой бэкап.

					29.10.2020 (1.592)
					Резерное копирование в AlwaysOn с вторичных реплик - добавлена поддержка для Full и Diff бэкапов.

					23.02.2021 (1.593)
					Fixed check: is it possible to make a backup on this server (AlwaysOn)
	-- ============================================= */
	CREATE proc [backups].[usp_StartBackup] 
		@type varchar(4) = 'Full' --тип бэкапа Log или Full или Diff.
		,@DBFilter nvarchar(400) = null
		,@OnlyCopy bit = 0
		,@NoStats bit = 1
		,@pp bit = 0 --определяет, что этот модуль уже запущен в многопоточном режиме (через вызов из Service Broker).
		,@ForceFULL bit = 0 --принудительно сделать ПолныйБэкап, даже если сейчас по расписанию должен быть DIFF.
		,@DBList NVARCHAR(MAX) = NULL
		,@DBList_delimeter NVARCHAR(10)=','
	as
	begin
		set nocount on;
		declare @dbname nvarchar(400), @DateStart datetime=null, @pp_config bit=0;
		DECLARE @BackupHere BIT, @DlgHandle UNIQUEIDENTIFIER, @MsgRequest XML, @count_q_pp int;
		declare @sql NVARCHAR(MAX), @dblist_fmt NVARCHAR(MAX);
		IF @DBList > N'' AND @DBList IS NOT NULL
		BEGIN
			--11.04.17 Новый алгоритм обработки @DBList (список БД в виде строки);
			set @dblist_fmt = 'N'''+REPLACE(@DBList, @DBList_delimeter,''' , N''')+'''';
			set @dblist_fmt = REPLACE(@dblist_fmt,'N'''' , ','');
			set @dblist_fmt = REPLACE(@dblist_fmt,' , N''''','');
		END
		CREATE TABLE #x(DB NVARCHAR(600));
		SET @sql = N'SELECT name FROM sys.databases WHERE 1=1'
		+ CASE WHEN @dblist_fmt IS NOT NULL THEN ' AND name IN (' + @dblist_fmt + ')'
			   ELSE '' 
		  END;
		INSERT #x EXEC sp_executesql @sql;


		--Здесь Определяем отбор для копирования бэкапов по дате - только бэкапы сделанные только что!
		--А для бэкапов Логов - в течении последних суток!
		--или вообще все бэкапы (если задан параметр @OnlyCopy)
		IF @OnlyCopy=0
		BEGIN
			IF @type='Log'
				set @DateStart=DATEADD(day,-1,GETDATE());
			ELSE
				set @DateStart=GETDATE();				
		END;

		IF @type<>'Full'
			set @ForceFULL=0;

		--Определяем Базы для резервного копирования!
		--Проверяем что эти базы существуют, и их состояние ONLINE!
		--Если делаем Log бэкапы, то проверяем что модель восстановления<>simple
		declare BACKUPS scroll cursor
		for 
			select Conf.dbname
			from
			(
				select distinct DBName
				from info.vGetAllBackConf
				where
					(@DBFilter is null or DBName=@DBFilter)
					and (@type is null or (Kind=@type) or (Kind in ('Full','Diff') and @type='Full'))
			) Conf
			inner join
			(
				select name as dbname
				from sys.databases
				where 
				state_desc='ONLINE' and is_in_standby=0 and is_read_only=0
				and name <> 'tempdb'
				and (@type<>'Log' OR recovery_model_desc<>'SIMPLE')
			) DBFact
			inner join #x X
				ON DBFact.dbname=X.DB
			on Conf.dbname=DBFact.dbname
	
		--Определяем настройки многопоточности!
		if @pp=0
		begin
			select @pp_config=[pp] 
			from backups.Config 
			where [pp] is not null and DateConfig=(select max(DateConfig) from backups.Config where [pp] is not null);
			if @pp_config=1
				--Открываем новый диалог для Backups!
				BEGIN DIALOG CONVERSATION @DlgHandle
					FROM SERVICE [pp_Commander]
					TO SERVICE N'pp_ProcessorHard'
					ON CONTRACT [pp_back_Contract]
					WITH ENCRYPTION = OFF;
		end;

		--Часть 1. Сначала выполним всех бэкапы локально (для всех баз!), без копирования файлов бэкапов
		open BACKUPS
		if @OnlyCopy=0
		begin
			fetch next from BACKUPS
				into @dbname
			while @@FETCH_STATUS=0
			begin
				--check: is it possible to make a backup on this server
				IF CAST (LEFT (CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(2)),2) AS SMALLINT) >= 11
					SET @BackupHere=sys.fn_hadr_backup_is_preferred_replica(@dbname);
				ELSE
					SET @BackupHere=1;
				IF @BackupHere=1
				BEGIN
					--Если многопоточность настроена, тогда добавляем все БД и типы бэкапов в очередь 
					--а всё остальное сделает Service Broker.
					if @pp=0 and @pp_config=1
					begin
						--формируем сообщение в очередь в виде XML.
						set @MsgRequest=cast('<backups>
												<db>'+@dbname+'</db>
												<type>'+@type+'</type>
											  </backups>' as xml);
					
						--31.12.2015 Добавлена проверка: существует ли УЖЕ в очереди наше сообщение!
						--Добавляем в очередь такое сообщение, если его ещё нет в Очереди!
						;WITH XMLNAMESPACES
						('http://pecom.ru/pegasExchange/types/' as ns),
						Q AS 
						(
							select CAST(message_body as XML) as msg_xml
							from pp.ProcessorQueueHard
							where message_type_name='pp_back_Request'
						)
						select @count_q_pp=count(*)
						from Q
						where msg_xml.exist(N'(/backups/db/text()[.=sql:variable("@dbname")])')=1
							AND msg_xml.exist(N'(/backups/type/text()[.=sql:variable("@type")])')=1;
						IF @count_q_pp=0 OR @count_q_pp IS NULL
						BEGIN
							--отправляем сообщение в очередь!
							SEND ON CONVERSATION @DlgHandle
								MESSAGE TYPE [pp_back_Request]
								(@MsgRequest);
						END
					end
					else
						--Запуск команды Бэкап теперь всё через одну новую процедуру backups.usp_RunBack!
						exec backups.usp_RunBack @DBName_IN=@dbname, @TypeBack=@type, @NoStats=@NoStats, @OnlyFull=@ForceFULL;
				END

				fetch next from BACKUPS
					into @dbname
			end
		end
		CLOSE BACKUPS;
		DEALLOCATE BACKUPS;

		if @pp=1 or @pp_config=0
		begin
			--Часть 2. Теперь выполним копирование всех файлов бэкапов, если это нужно
			--через новый отдельный модуль usp_CopyBack

			--check: is it possible to make a backup on this server
			IF CAST (LEFT (CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(2)),2) AS SMALLINT) >= 11
				SET @BackupHere=sys.fn_hadr_backup_is_preferred_replica(@dbname);
			ELSE
				SET @BackupHere=1;
			IF @BackupHere=1
			BEGIN
				--Запускаем новый модуль копирования файлов бэкапов!
				exec backups.usp_CopyBack @DBFilter=@DBFilter,@type=@type, @DateStart=@DateStart;
			END;
		end;
	end