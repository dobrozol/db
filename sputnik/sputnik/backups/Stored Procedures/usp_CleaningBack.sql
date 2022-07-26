
	/* =============================================
	-- Author:		Андрей Иванов (sqland1c)
	-- Create date: 11.12.2013 (1.0)
	-- Description: Эта процедура производит очистку от старых бэкапов на серверах c расширением заданным в параметре @rez (по умолчанию bak)
					Производится формирование скрипта PowerShell и в конце производится запуск этого сценария.
	-- Update:		25.12.2013 (1.1)
					Так как теперь есть таблица BackConfWeekly для недельных бэкапов, необходимо получать из неё информацию для очистки!
					09.01.2014 (1.15)
					Для переменных (текстовых), хранящих информацию о каталогах бэкапов и команды, увеличен размер на 1000 знаков.
					09.01.2014 (1.2)
					При формировании файла PowerShell-скрипта применяется кодировка 650001 (UTF-8).
					12.02.2014 (1.3)
					Улучшен алгоритм Ротации - теперь для Удаления будут отбираться файлы не только по расширению и старше определённой Даты, но и содержащие в Имени файла нужные Имя Базы и Тип Бэкапа.
						Это позволит избежать неправильного удаления, когда бэкапы разных баз или разных типов сохраняются в одном каталоге.
					Также оптимизирован алгоритм PS скрипта - фильтрация данных WHERE теперь происходит в одном месте конвеера с использованием оператора AND.
					17.03.2014 (1.35)
					Исправлен алгоритм Ротации - поиск старых файлов бэкапов! Условие Меньше Или Равно (-le) изменено на Меньше (-lt)!
					То есть теперь, удаляться будут файлы, которые именно Старше заданного количества дней!
					21.03.2014 (1.37)
					Внесены исправления: кодировка для файла изменена с 65001(UTF8) на 1251(ANSI-Кириллица). Увеличены размеры всех строковых переменных. Для всех строковых литералов добавлен
						символ N'' (поддержка UNICODE символов).
					21.11.2014 (1.39)
					Добавлен новый алгоритм зачистки старых файлов скриптов PowerShell, сформированных из модуля usp_CopyBack (копирование файлов бэкапов).
					23.06.2015 (1.40)
					Добавлен новый алгоритм формирования имени файлов скриптов, теперь в имени файлов будет имя экземпляра, тип бэкапов, время. Всё это нужно
					чтобы избежать пересечения файлов скриптов (данный алгоритм аналогичен алгоритму из процедуры usp_CopyBack.
					Также добавлен алгоритм зачистки старых файлов скриптов PowerShell, сформированных из модуля [usp_CleaningBack]
					12.02.2016 (2.00)
					Новый алгоритм реализущий очень нужную возможность - ротацию бэкапов по количеству файлов (а не по возрасту файлов).
					Но и старый способ ротации также остается.
					Причем для каждого бэкапа можно сделать выбор (поле [Policy_CountFiles]) - какую политику ротации использовать!
					Вся логика реализована на Powershell!
					12.02.2016 (2.01)
					Доработка нового алгоритма ротации. Теперь доступны 3 режима: классический(0), по количествоу файлов(1) и гибридный(2).
					Причем можно задавать разные режимы для бэкапов и для копий бэкапов! 
					22.12.2017 (2.020)
					Добавлен алгоритм экранирования спец.символов при поиске каталогов.
					01.02.2018 (2.021)
					Доработан алгоритм экранирования: чтобы работало нужно использовать одинарные кавычки вместо двойных.
					23.03.2018 (2.025)
					Изменена политика ротаций: классический(0) - теперь хранит x-дней + 3-файла, гибридный(2) - теперь хранит x-дней + x-файлов.
					13.04.2018 (2.027)
					Доработан поиск бэкапов для удаления по имени файла. 
					19.04.2018 (2.028)
					Добавлена проверка при отборе бэкапов - только для существующих online баз!
					05.07.2022 (2.030)
					added new rotation policy mode 3 by size of backup files in Gb
	-- ============================================= */
	create PROCEDURE backups.[usp_CleaningBack]  
		@type varchar(4) =null --тип бэкапа Log или Full.
		,@DBFilter nvarchar(100) = null
		,@rez nvarchar(10) = 'bak'
	AS
	BEGIN
		SET NOCOUNT ON;
		declare @PS_command nvarchar(4000), @CMD nvarchar(4000),@rc bit, @LocalDir nvarchar(1600), @NetDir nvarchar(1600), @LocalDays int, @NetDays int, @skipBackupFiles int;
		declare @Kind varchar(4),@DBName nvarchar(400), @InstanceName nvarchar(128),@PSFile nvarchar(300),@ErrMsg nvarchar(900), @LocalPolicy tinyint, @NetPolicy tinyint, @PS_Filter_Policy nvarchar(4000);
		declare C cursor for
		select bc.LocalDir, bc.NetDir, bc.LocalDays, bc.NetDays, bc.DBName, bc.Kind, bc.[LocalPolicy], bc.[NetPolicy]
		from backups.BackConf bc
		inner join sys.databases sdb on bc.DBName=sdb.[name] and sdb.state_desc='ONLINE'
		where
			(@DBFilter is null or bc.DBName=@DBFilter)
			and (@type is null or bc.Kind=@type)
			and (bc.Kind in ('Full','Diff','Log')) --на всякий случай ограничение. Удалять только файлы полных копий, дифф. копий и Лога.
		UNION
		select bc.LocalDir, bc.NetDir, bc.LocalDays, bc.NetDays, bc.DBName, bc.Kind, bc.[LocalPolicy], bc.[NetPolicy]
		from backups.BackConfWeekly bc
		inner join sys.databases sdb on bc.DBName=sdb.[name] and sdb.state_desc='ONLINE'
		where
			(@DBFilter is null or bc.DBName=@DBFilter)
			and (@type is null or bc.Kind=@type)
			and (bc.Kind in ('Full','Diff','Log')) --на всякий случай ограничение. Удалять только файлы полных копий, дифф. копий и Лога.
	
		--формируем Имя файла скрипта.
		--Добавляем текущее время к имени файла. Также добавляется тип бэкапов, которые будут ротироваться или ALL
		--ещё учитывается имя экземпляра SQL Server и добавляется в имя файла.
		SELECT @InstanceName=COALESCE('_'+CAST(SERVERPROPERTY('InstanceName') as nvarchar(128)),'');
		set @PSFile='usp_CleaningBack'+@InstanceName;
		if @type is not null
			set @PSFile=@PSFile+'_'+@type;
		else
			set @PSFile=@PSFile+'_ALL';
		set @PSFile=@PSFile+'_'+REPLACE(REPLACE(SUBSTRING(CONVERT(NVARCHAR(23),GETDATE(),126),1,23),'T','_'),':','.');
		set @PSFile=@PSFile+'.ps1';

		--1. Формируем файл скрипта PowerShell
		set @CMD = N'cmd /k chcp 1251 && echo | echo #PowerShell-Script for cleaning old backups. Created by Job on local SQL Server. Date: '+convert(varchar(20),getdate(),120)+ '> %temp%\'+@PSFile;
		EXEC @rc=xp_cmdshell @CMD , no_output
		if @rc=1
		begin
			SET @ErrMsg=N'Ошибка при записи файла (PowerShell скрипт) %temp%\'+@PSFile;
			RAISERROR(@ErrMsg,11,1) WITH LOG
		end
		open C
		fetch next from C
		into @LocalDir, @NetDir, @LocalDays, @NetDays, @DBName, @Kind, @LocalPolicy, @NetPolicy
		while @@FETCH_STATUS=0
		begin
			--Экранируем символы [ и ], чтобы PowerShell смог обнаружить каталоги (если такие символы использованы):
			IF CHARINDEX('[', @LocalDir)>0
				SET @LocalDir=REPLACE(@LocalDir,'[','``[');
			IF CHARINDEX(']', @LocalDir)>0
				SET @LocalDir=REPLACE(@LocalDir,']','``]');
			IF CHARINDEX(']', @NetDir)>0
				SET @NetDir=REPLACE(@NetDir,']','``]');
			IF CHARINDEX(']', @NetDir)>0
				SET @NetDir=REPLACE(@NetDir,']','``]');

			--Определяем политику ротации - все это средствами Powershell!
			if @LocalPolicy=1 --Режим ротации по количеству файлов! 
				set @PS_Filter_Policy= N' ^^^| Sort-Object -property lastwritetime -descending ^^^| Select-Object -skip '+cast(@LocalDays as varchar(5));
			else if @LocalPolicy=0 --Режим ротации классический (по возрасту файлов)! + теперь оставляем 3 файла, чтобы гарантировано сохранить бэкапы!
				set @PS_Filter_Policy= N' ^^^| Sort-Object -property lastwritetime -descending ^^^| Select-Object -skip 3 ^^^| where {$_.lastwritetime -lt ((get-date).AddDays(-'+cast(@LocalDays as varchar(5))+N'))}';
			else if @LocalPolicy=2 --Режим ротации гибридный (всегда оставляем x-файлов и удаляем по возрасту файлов старше x-дней )!
				set @PS_Filter_Policy= N' ^^^| Sort-Object -property lastwritetime -descending ^^^| Select-Object -skip '+cast(@LocalDays as varchar(5))+' ^^^| where {$_.lastwritetime -lt ((get-date).AddDays(-'+cast(@LocalDays as varchar(5))+N'))}';
			else if @LocalPolicy=3 --Rotation mode according to the specified file size + leave minimum files to ensure backups are saved!
			begin
				--get the number of backup files we are saving
				select @skipBackupFiles = [info].[uf_getNumberBackupFilesByLimitSizeInGb] (@DBName, @Kind, @LocalDays, default, default, default)
				
				set @PS_Filter_Policy= N' ^^^| Sort-Object -property lastwritetime -descending ^^^| Select-Object -skip '+cast(@skipBackupFiles as varchar(5));
			end
			set @PS_command=N'get-childitem -path '''+@LocalDir+N''' ^^^| where {$_.extension -eq ''.'+@rez+N''' -and $_.Name -like '''+@DBName+'_'+@Kind+'_*''} '+@PS_Filter_Policy+N' ^^^| remove-item';
			set @CMD = N'cmd /k chcp 1251 && echo | echo '+@PS_command+N' >> %temp%\'+@PSFile;
			--для отладки:
			--PRINT @CMD;
			EXEC @rc=xp_cmdshell @CMD , no_output
			if @rc=1
			begin
				SET @ErrMsg=N'Ошибка при записи файла (PowerShell скрипт) %temp%\'+@PSFile;
				RAISERROR(@ErrMsg,11,1) WITH LOG
			end
			--Определяем политику ротации - все это средствами Powershell!
			if @NetPolicy=1 --Режим ротации по количеству файлов! 
				set @PS_Filter_Policy= N' ^^^| Sort-Object -property lastwritetime -descending ^^^| Select-Object -skip '+cast(@NetDays as varchar(5));
			else if @NetPolicy=0 --Режим ротации классический (по возрасту файлов)! + теперь оставляем 3 файла, чтобы гарантировано сохранить бэкапы!
				set @PS_Filter_Policy= N' ^^^| Sort-Object -property lastwritetime -descending ^^^| Select-Object -skip 3 ^^^| where {$_.lastwritetime -lt ((get-date).AddDays(-'+cast(@NetDays as varchar(5))+N'))}';
			else if @NetPolicy=2 --Режим ротации гибридный (всегда оставляем x-файлов и удаляем по возрасту файлов старше x-дней )!
				set @PS_Filter_Policy= N' ^^^| Sort-Object -property lastwritetime -descending ^^^| Select-Object -skip '+cast(@NetDays as varchar(5))+' ^^^| where {$_.lastwritetime -lt ((get-date).AddDays(-'+cast(@NetDays as varchar(5))+N'))}';
			else if @NetPolicy=3 --Rotation mode according to the specified file size + leave minimum files to ensure backups are saved!
			begin
				--get the number of backup files we are saving
				select @skipBackupFiles = [info].[uf_getNumberBackupFilesByLimitSizeInGb] (@DBName, @Kind, @NetDays, default, default, default)
				
				set @PS_Filter_Policy= N' ^^^| Sort-Object -property lastwritetime -descending ^^^| Select-Object -skip '+cast(@skipBackupFiles as varchar(5));
			end
			set @PS_command=N'get-childitem -path '''+@NetDir+N''' ^^^| where {$_.extension -eq ''.'+@rez+N''' -and $_.Name -like '''+@DBName+'_'+@Kind+'_*''} '+@PS_Filter_Policy+N' ^^^| remove-item';
			set @CMD = N'cmd /k chcp 1251 && echo | echo '+@PS_command+N' >> %temp%\'+@PSFile;
			--для отладки:
			--PRINT @CMD;
			EXEC @rc=xp_cmdshell @CMD , no_output

			if @rc=1
			begin
				SET @ErrMsg=N'Ошибка при записи файла (PowerShell скрипт) %temp%\'+@PSFile;
				RAISERROR(@ErrMsg,11,1) WITH LOG
			end
			fetch next from C
			into @LocalDir, @NetDir, @LocalDays, @NetDays, @DBName, @Kind, @LocalPolicy, @NetPolicy
		end
		close C;
		deallocate C;

		--2.1 Добавляем команды PowerShell для зачистки старых файлов скриптов usp_CopyBack (копирование бэкапов).
		set @PS_command=N'Get-ChildItem -Path $env:TEMP\usp_CopyBack*.ps1 ^^^| where {$_.lastwritetime -lt ((get-date).AddDays(-3))} ^^^| remove-item'
		set @CMD = N'cmd /k chcp 1251 && echo | echo '+@PS_command+N' >> %temp%\'+@PSFile;
		EXEC @rc=xp_cmdshell @CMD , no_output
		if @rc=1
		begin
			SET @ErrMsg=N'Ошибка при записи файла (PowerShell скрипт) %temp%\'+@PSFile;
			RAISERROR(@ErrMsg,11,1) WITH LOG
		end

		--2.2 Добавляем команды PowerShell для зачистки старых файлов скриптов usp_CleaningBack (очистка старых бэкапов).
		set @PS_command=N'Get-ChildItem -Path $env:TEMP\usp_CleaningBack*.ps1 ^^^| where {$_.lastwritetime -lt ((get-date).AddDays(-3))} ^^^| remove-item'
		set @CMD = N'cmd /k chcp 1251 && echo | echo '+@PS_command+N' >> %temp%\'+@PSFile;
		EXEC @rc=xp_cmdshell @CMD , no_output
		if @rc=1
		begin
			SET @ErrMsg=N'Ошибка при записи файла (PowerShell скрипт) %temp%\'+@PSFile;
			RAISERROR(@ErrMsg,11,1) WITH LOG
		end

		--3. Запуск скрипта PowerShell из файла
		set @CMD = 'powershell %temp%\'+@PSFile;
		EXEC @rc=xp_cmdshell @CMD , no_output
		if @rc=1
		begin
			SET @ErrMsg=N'Ошибка при запуске PowerShell-скрипта из файла %temp%\'+@PSFile;
			RAISERROR(@ErrMsg,11,1) WITH LOG
		end
		else
			PRINT N'Команда Очистки старых бэкапов успешно выполнена!';
	end