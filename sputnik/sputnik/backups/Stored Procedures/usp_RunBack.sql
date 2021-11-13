	/* =============================================
	-- Author:		Андрей Иванов (sqland1c)
	-- Create date: 23.12.2013 (1.0)
	-- Description: Эта процедура выполняет конкретный бэкап для конкретной базы данных.
					Тип бэкапа и все настройки хранятся в таблицу sputnik.backups.BackConf.
					После выполнения команды BACKUP происходит копирование бэкапа на удалённый ресурс (если он задан и отличается от локального).
					Эта процедура создана на основе процедур usp_PegLogBack и usp_PegFullBack и полностью заменяет обе эти процедуры, 
					добавляя расширенный функционал!
	-- Update:		25.12.2013 (1.1)
					Добавлен алгоритм определения Расширения файла в зависимости от наличия NetDir. Если NetDir не задан, или он равен LocalDir,
					то расширения файла сразу будет BAK , без переименования файла в конце.
					25.12.2013 (1.2)
					Добавлен алгоритм для отдельного резервного копирования Недельных бэкапов. Определяется текущий день недели и осуществляется
					поиск настройки из таблицы BackConfWeekly, если ничего не найдено, то поиск будет осуществляться в таблице BackConf. Также
					добавлена проверка - если из таблиц BackConf и BackConfWeekly не получена никакая информация - ничего не делать!
					26.12.2013 (1.3)
					Добавлен алгоритм для Дифференциального бэкапа!
					30.12.2013 (1.35)
					Исправлен алгоритм определения дня недели!
					09.01.2014 (1.36)
					Для переменной, которая хранит путь к файлу бэкапа увеличен размер строки с 200 до 900.
					16.01.2014 (1.4)
					Добавлен новые параметры:
							@NoCopy - если установлен в 1, тогда после создания бэкапа, копирование файлов бэкапа НЕ происходит.
						По умолчанию 0, т.е. копирование происходит после создания бэкапа.
							@ForceCopy - если установлен в 1, тогда Создание бэкапа НЕ происходит, а происходит принудительное копирование файлов бэкапа
						на сетевой ресурс. По умолчанию 0, т.е. сначала происходит создание Бэкапа, а потом уже (параметр @NoCopy устанавливается в 0) копирование
						файлов бэкапа на сетевой ресурс.
					Также увеличены размеры всех текстовых переменных (nvarchar).
					06.02.2014 (1.5)
						Добавлен новый алгоритм. Теперь если тип бэкапа задан Full, то осуществляется бэкап Full/Diff в зависимости от настроек в таблицах BackConfWeekly и
							BackConf
					17.02.2014 (1.6)
						Оптимизирован алгоритм получения информации дня недели. Теперь для этого создана новая функция uf_GetWeekDay.

					17.03.2014 (1.65)
						Добавлен алгоритм создания Бэкапов 1 раз в месяц. Для этого в таблицу BackConfWeekly добавлен новый столбец ДеньМесяца!

					31.07.2014 (1.7)
						Новый параметр @OnlyFull. Если этот параметр задан, то будет принудительно выполнена Полная копия (без учёта дифф!).
						Соответственно изменен алгоритм получения настроек (добавлено получение настроек без учета дня месяца и дня недели).

					07.08.2014 (1.72) Новый параметр @NoStats, если задан тогда в процессе резервного копирования не будет выводится кол-во выполненных процентов
					( это оператор STATS в команде BACKUP).

					06.10.2014 (1.73) Доработан алгоритм переименования расширения файлов бэкапов в самом конце. Теперь если задан параметр
						принудительного копирования файлов (без выполнения бэкапа) - @ForceCopy, то переименование расширения тоже выполняется принудительно.

					11.11.2014 (1.80) Теперь эта процедура выполняет только бэкапы. 
					Весь алгоритм копирования и переименования бэкапов вынесен в новый отдельный модуль - usp_CopyBack.Поэтому отсюда удален весь старый код, касающийся этого процесса. 
					Также алгоритм сжатие файлов журналов транзакций теперь закомментирован (т.к. в программе Хьюстон теперь работает механизм автоматического сжатия файлов БД).

					13.11.2014 (1.81) Восстановлен параметр @ForceCopy! При этом логика изменена: если задан этот параметр, тогда после выполнения бэкапа в конце сразу происходит 
					копирование этого бэкапа. Такое поведение нужно только при запуске модуля [usp_RunBack] из [usp_GC2]! В остальных случаях копирование бэкапов
					производится из [usp_StartBack] сразу для всех сделанных бэкапов!

					03.12.2014 (1.82) В команды полного и дифф. бэкапов добавлены опции BUFFERCOUNT и MAXTRANSFERSIZE с более оптимальными значениями для больших БД.
				
					29.12.2014 (1.83) Исправлена небольшая ошибка - в команду создания каталогов (если их нет) добавлены кавычки (защита от пробелов в имени).

					22.06.2015 (1.85) Добавлен учёт редакции экземпляра. Если Express Edition, тогда сжатие бэкапа не выполняется (слово COMPRESSION отсутствует)!

					15.08.2015 (2.00) Реализована возможность резервного копирования по Файловым Группам!

					31.05.2016 (2.01) Оператор IIF заменён на CASE для нормальной работы на SQL Server версий  < 2012!

					22.12.2017 (2.020) Добавлена защита при формировании пути к бэкапу - в конце каталога должен быть символ "\".
				
					29.08.2019 (2.021) Добавлен новый параметр @debug - если он задан, то процедура ничего не делает, просто выводит все команды через PRINT для отладки.
	-- ============================================= */
	CREATE PROCEDURE [backups].[usp_RunBack] 
		@DBNAME_in nvarchar(300)
		,@TypeBack varchar(4)
		,@OnlyFull bit = 0 
		,@NoStats bit = 0
		,@ForceCopy bit = 0
		,@debug bit = 0
	AS
	BEGIN
		set nocount on;
		DECLARE @DBName NVARCHAR(300), @LocalDir NVARCHAR(500), @NetDir NVARCHAR(500), @LocalDays int, @NetDays INT, @tint int, @getdate datetime, @Extension nvarchar (20), @Edition nvarchar(100), @Compression nvarchar(50), @FG nvarchar(1000)=NULL, @str_FG nvarchar(2000)='', @str_FG_fname nvarchar(1000)='';
		DECLARE @tstr NVARCHAR(1100), @tcmd NVARCHAR(400),@rc int,@backup_file nvarchar(500), @WeekDay tinyint, @DynTypeBack varchar(4), @MonthDay tinyint, @StrStats NVARCHAR(20)='';
		SET @Edition=CAST(SERVERPROPERTY('Edition') as nvarchar(100));
		SELECT @Compression=CASE WHEN CHARINDEX('Express',@Edition)>0 THEN '' ELSE ', COMPRESSION' END;	
		--SELECT @Edition, @Compression;
		SET DATEFORMAT YMD;
		set @getdate=GETDATE();
		IF @NoStats=0
			SET @StrStats=N',STATS=25';
		--Определяем текущий день недели! Сначала ищем настройку в таблице BackConfWeekly для недельных бэкапов по текущему дню недели.
		--Если ничего нет, тогда ищем настройку в обычной таблице BackConf.
		select @WeekDay=sputnik.info.uf_GetWeekDay(@getdate);
		select @MonthDay=DATEPART ( DAY , @getdate );
		--update: теперь, если задан Тип Full. То проверяем и Full и Diff.
		--	Алгоритм такой: 1. ищем Full в Weekly. 2. Ищем Diff в Weekly. 3.Ищем Full в Daily 4. Ищем Diff в Daily	

		SELECT TOP 1 @DBName=DBName, @LocalDir=LocalDir, @NetDir=NetDir, @LocalDays=LocalDays, @NetDays=NetDays, @DynTypeBack=Kind, @FG=FG 
		FROM sputnik.backups.BackConfWeekly
		WHERE Kind=@TypeBack and DBName=@DBNAME_in and MonthDay=@MonthDay;
		if @DBName is null and @TypeBack='Full' and @OnlyFull=0
			SELECT TOP 1 @DBName=DBName, @LocalDir=LocalDir, @NetDir=NetDir, @LocalDays=LocalDays, @NetDays=NetDays, @DynTypeBack=Kind, @FG=FG  
			FROM sputnik.backups.BackConfWeekly
			WHERE Kind='Diff' and DBName=@DBNAME_in and MonthDay=@MonthDay;

		if @DBName is null
		begin
			SELECT TOP 1 @DBName=DBName, @LocalDir=LocalDir, @NetDir=NetDir, @LocalDays=LocalDays, @NetDays=NetDays, @DynTypeBack=Kind, @FG=FG 
			FROM sputnik.backups.BackConfWeekly
			WHERE Kind=@TypeBack and DBName=@DBNAME_in and WeekDay=@WeekDay;
			if @DBName is null and @TypeBack='Full' and @OnlyFull=0
				SELECT TOP 1 @DBName=DBName, @LocalDir=LocalDir, @NetDir=NetDir, @LocalDays=LocalDays, @NetDays=NetDays, @DynTypeBack=Kind, @FG=FG  
				FROM sputnik.backups.BackConfWeekly
				WHERE Kind='Diff' and DBName=@DBNAME_in and WeekDay=@WeekDay;
		end
		if @DBName is null
		begin
			SELECT TOP 1 @DBName=DBName, @LocalDir=LocalDir, @NetDir=NetDir, @LocalDays=LocalDays, @NetDays=NetDays, @DynTypeBack=Kind, @FG=FG 
			FROM sputnik.backups.BackConf 
			WHERE Kind=@TypeBack and DBName=@DBNAME_in
			if @DBName is null and @TypeBack='Full' and @OnlyFull=0
				SELECT TOP 1 @DBName=DBName, @LocalDir=LocalDir, @NetDir=NetDir, @LocalDays=LocalDays, @NetDays=NetDays, @DynTypeBack=Kind, @FG=FG  
				FROM sputnik.backups.BackConf
				WHERE Kind='Diff' and DBName=@DBNAME_in;
		end
		--Новый алгоритм (только для полных бэкапов!): если использован параметр @OnlyFull и настройки бэкапов не были найдены
		--тогда нужно найти подходящие настройки для Full из таблицы BackConfWeekly без учета дня месяца и дня недели!
		if @DBName is null and @OnlyFull=1 and @TypeBack='Full'
		begin
			DECLARE @T TABLE (DBName NVARCHAR(300), LocalDir NVARCHAR(500), NetDir NVARCHAR(500), LocalDays INT, NetDays INT, Kind VARCHAR(4), Ord tinyint);
			INSERT INTO @T
			SELECT TOP 1 DBName, LocalDir, NetDir, LocalDays, NetDays, Kind, 1 AS Ord
			FROM sputnik.backups.BackConfWeekly
			WHERE Kind='Full' and DBName=@DBNAME_in and MonthDay BETWEEN 1 AND 31
			UNION ALL
			SELECT TOP 1 DBName, LocalDir, NetDir, LocalDays, NetDays, Kind, 2 AS Ord
			FROM sputnik.backups.BackConfWeekly
			WHERE Kind='Full' and DBName=@DBNAME_in and WeekDay BETWEEN 1 AND 7;
			SELECT @DBName=DBName, @LocalDir=LocalDir, @NetDir=NetDir, @LocalDays=LocalDays, @NetDays=NetDays, @DynTypeBack=Kind
			FROM @T
			WHERE Ord=(SELECT MIN(Ord) FROM @T);
		end		

		if @DBName is not null
		begin
			-- если каталогов нет, создать их 
			SET @tcmd='md "' + @LocalDir+'"';
			if @debug=1 print 'xp_cmdshell '''+@tcmd+'';
			else	EXEC xp_cmdshell @tcmd, no_output
			IF @LocalDir<>@NetDir and @NetDir is not null and @NetDir<>''
			begin
				SET @tcmd='md "' + @NetDir+'"';
				if @debug=1 print 'xp_cmdshell '''+@tcmd+'';
				else	EXEC xp_cmdshell @tcmd, no_output
				--Установка Расширения файла @Extension в only - будем копировать в NetDir и потом менять на BAK
				set @Extension = '.ONLY';
			end
			else
				--Установка Расширения файла @Extension в BAK - копировать файл бэкапа НЕ НУЖНО!
				set @Extension = '.BAK';
		
			IF @FG IS NOT NULL AND @FG<>'' AND @DynTypeBack IN ('Full', 'Diff')
			BEGIN
				--Реализуем бэкапы по файловым группам!!
				--Сначала определяем имена ФГ которые попадают в текущий бэкап
				--Затем формируем строку для команды BACKUP где будут перечислены ФГ
				--Также формируем строку для вставки в имя файла, где будут указаны ФГ.
				declare @tsql_fg nvarchar(2000), @c_fg_cur nvarchar(300);
				if OBJECT_ID('tempdb.dbo.#t_fg') is not null
					drop table #t_fg;
				create table #t_fg (fg nvarchar(300));
				set @tsql_fg='USE ['+@DBName+'];
					insert into #t_fg (fg)			 
					select name as fg
					from sys.filegroups
					where charindex(QUOTENAME(name),'''+@FG+''')>0;'
				EXEC (@tsql_fg);
			
				declare C_fg cursor for
				select distinct fg
				from #t_fg;
				open C_fg;
				fetch next from C_fg into @c_fg_cur;
				while @@FETCH_STATUS=0
				begin
					if LEN(@str_FG)>0
						SET @str_FG=@str_FG+',';
					if LEN(@str_FG_fname)=0
						SET @str_FG_fname='FG_';
					SET @str_FG=@str_FG+' FILEGROUP = '''+@c_fg_cur+''' ';
					SET @str_FG_fname=@str_FG_fname+@c_fg_cur+'_';
					fetch next from C_fg into @c_fg_cur;
				end
				close C_fg;
				deallocate C_fg; 	  
			END
		
			set @backup_file=@DBName+'_'+@DynTypeBack+'_'+@str_FG_fname+REPLACE(REPLACE(SUBSTRING(CONVERT(NVARCHAR(max),@getdate,126),1,19),'T','_'),':','.');
			SET @tstr=@LocalDir+CASE WHEN RIGHT(@LocalDir,1)='\' THEN '' ELSE '\' END+@backup_file+@Extension;

			if @DynTypeBack='Full'
				if @debug=1 PRINT(N'BACKUP DATABASE ['+@DBName+'] '+@str_FG+' TO  DISK = N'''+@tstr+''' WITH FORMAT, INIT, SKIP, NOREWIND, NOUNLOAD'+@Compression+@StrStats+', CHECKSUM, BUFFERCOUNT=64, MAXTRANSFERSIZE=2097152');
				else EXEC(N'BACKUP DATABASE ['+@DBName+'] '+@str_FG+' TO  DISK = N'''+@tstr+''' WITH FORMAT, INIT, SKIP, NOREWIND, NOUNLOAD'+@Compression+@StrStats+', CHECKSUM, BUFFERCOUNT=64, MAXTRANSFERSIZE=2097152');
			else if @DynTypeBack='Diff'
				if @debug=1 PRINT(N'BACKUP DATABASE ['+@DBName+'] '+@str_FG+' TO  DISK = N'''+@tstr+''' WITH DIFFERENTIAL,FORMAT, INIT, SKIP, NOREWIND, NOUNLOAD'+@Compression+@StrStats+', CHECKSUM, BUFFERCOUNT=64, MAXTRANSFERSIZE=2097152');
				else EXEC(N'BACKUP DATABASE ['+@DBName+'] '+@str_FG+' TO  DISK = N'''+@tstr+''' WITH DIFFERENTIAL,FORMAT, INIT, SKIP, NOREWIND, NOUNLOAD'+@Compression+@StrStats+', CHECKSUM, BUFFERCOUNT=64, MAXTRANSFERSIZE=2097152');
			else if @DynTypeBack='Log'
			begin
				if @debug=1 PRINT(N'BACKUP LOG ['+@DBName+'] TO  DISK = N'''+@tstr+''' WITH NOFORMAT, INIT, SKIP, NOREWIND, NOUNLOAD'+@Compression+@StrStats+', CHECKSUM');
				else EXEC(N'BACKUP LOG ['+@DBName+'] TO  DISK = N'''+@tstr+''' WITH NOFORMAT, INIT, SKIP, NOREWIND, NOUNLOAD'+@Compression+@StrStats+', CHECKSUM');
				/*	Теперь не нужно здесь сжимать файлы ЖТ! Работает механизм авто-сжатия файлов БД из программы Хьюстон!
					Закомментировано. Оставлено на всякий случай.
					--if datepart(hh,@getdate)=23		--выполнить сжатие файлов LOG с 23 до 00.
					--	begin try
					--		exec db_maintenance.usp_ShrinkLogFile @DBName
					--	end try
					--	begin catch
					--		RAISERROR('Ошибка при попытке сжать (shrinkfile) файлы журналов транзакций через процедуру usp_ShrinkLogs !',11,1) WITH LOG
					--	end catch
				*/
			end
			begin try
				if @debug=0 exec backups.usp_WriteBackuphistory @DBName, @FG, @DynTypeBack,@backup_file,@tstr
			end try
			begin catch
				RAISERROR('Ошибка при записи в таблицу История резервных копий через процедуру usp_WriteBackuphistory !',11,1) WITH LOG
			end catch

			--Если задан параметр @ForceCopy то нужно сразу же произвести КОПИРОВАНИЕ сделанного бэкапа через новый модуль!
			IF @ForceCopy=1
				if @debug=0 exec backups.usp_CopyBack @DBFilter=@DBName,@type=@DynTypeBack,@DateStart=@getdate;			
		end
	END
