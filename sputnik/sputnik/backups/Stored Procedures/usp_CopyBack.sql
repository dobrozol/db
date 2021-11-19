
	/* =============================================
	-- Author:		Андрей Иванов (sqland1c)
	-- Create date: 09.11.2014 (1.0)
	-- Description: Эта процедура производит копирование файлов резервных копий (бэкапов) в каталог для копий бэкапов (если такой каталог определён!).
					Все пути к резервным копиям и к копиям бэкапов должны быть определены в базе sputnik!
					После того, как файл бэкапа будет скопирован, расширения файлов переименовывается из .ONLY в .BAK
					Производится формирование сценария PowerShell для поиска, копирования и переименования файлов бэкапов, и в конце производится запуск этого сценария.

	-- Update:		16.11.2014 (1.01)
					Если в параметре @type задан Full, тогда сразу учитывается и Full и Diff бэкапы!
					21.11.2014 (1.05)
					Новый алгоритм - теперь файлы скриптов PowerShell формируется динамически, чтобы исключить вероятное пересечение!
					А очистка от старых файлов скриптов будет производиться в модуле [usp_CleaningBack].
					Также изменен алгоритм формирование исключений - теперь текст ошибки формируется через переменную @ErrMsg.
					31.12.2014 (1.06)
					Добавлена проверка наличие обрабатываемых данных перед выполнением!
					23.06.2015 (1.07)
					Добавлен учет имени экземпляра в алгоритм формирования файлов скриптов.
					21.10.2015 (1.10)
					Добавлена дополнительная проверка при переименовании расширения в локальном каталоге - 
					переименовываем только если в каталоге Копий есть такой файл! 
					А также добавлен новый параметр @FilterBackupID - позволяет найти файл бэкапа по ИД и копировать только его
					(добавляется ещё одно условие в отбор файлов).
					01.07.2016 (1.11)
					Добавлен новый параметр @XLastHours - Определяет фильтр по дате (за последние X указанных часов). 
					Не учитывается, если заданы @DateStart и @DateEnd. По умолчанию - за всё время (то есть фильтр не используется).
					22.12.2017 (1.120)
					Добавлен алгоритм экранирования спец.символов при поиске каталогов.
					19.04.2018 (1.121)
					Добавлена проверка при отборе бэкапов - только для существующих online баз!
					21.02.2019 (1.400)
					Изменён алгоритм: добавлено логгирование, запись файла скрипта через новую процедуру [usp_WriteToFile].
					12.11.2021 (1.402) combination of characters $ and ( used in sqlcmd for variables and breaks project publishing

	-- ============================================= */
	create PROCEDURE backups.[usp_CopyBack]  
		@type varchar(4) = null				--тип бэкапа Log, Diff или Full.
		,@DBFilter nvarchar(100) = null		--фильтр по конкретной БД.
		,@Force	bit = 0						--флаг принудительного копирования (копирует даже если расширение = .BAK).
		,@DateStart datetime = null			
		,@DateEnd   datetime = null			--фильтр по дате. Будут копироваться только бэкапы в указанный период (если он задан!)
		,@FilterBackupID int = null			--Копировать будем конкретный файл бэкапа. Определяем этот файл бэкапа по ID
		,@XLastHours smallint = null		--Новый параметр. Определяет фильтр по дате (за последние X указанных часов). Не учитывается, если заданы @DateStart и @DateEnd 	
	AS
	BEGIN
		SET NOCOUNT ON;
		declare @Find_command nvarchar(3600), @Filter_FileName nvarchar(500), @PS_command nvarchar(2400), @CMD nvarchar(2800),@rc bit, @LocalDir nvarchar(1600), @NetDir nvarchar(1600), @rez_filter nvarchar(75), @date_filter nvarchar(200)='';
		declare @PSFile nvarchar(300), @Kind varchar(4), @DBName nvarchar(400), @ErrMsg nvarchar(900), @InstanceName nvarchar(128), @LogFile nvarchar(300);
		declare @Log_command nvarchar(3600), @S char(2);
		set @S=CHAR(36)+CHAR(40);	-- symbols $ and (
		--Перед выполнением проверяем наличие обрабатываемых данных!
		if exists(
				select bc.DBName
				from info.vGetAllBackConf bc
				inner join sys.databases sdb on bc.DBName=sdb.[name] and sdb.state_desc='ONLINE'
				where
					(@DBFilter is null or bc.DBName=@DBFilter)
					and (@type is null or (@type='Full' and bc.Kind IN ('Full','Diff')) or bc.Kind=@type)	--Если задан Full, тогда нужно учесть и Full и Diff.
					and (bc.NetDir is not null and bc.NetDir<>'' and bc.LocalDir<>bc.NetDir) --обязательное условия для копирования файлов!
			)
		BEGIN
			--получаем все настройки бэкапов из БД sputnik и сохраняем в курсор!
			declare C cursor for
			select distinct bc.LocalDir, bc.NetDir, bc.DBName, bc.Kind
			from info.vGetAllBackConf bc
			inner join sys.databases sdb on bc.DBName=sdb.[name] and sdb.state_desc='ONLINE'
			where
				(@DBFilter is null or bc.DBName=@DBFilter)
				and (@type is null or (@type='Full' and bc.Kind IN ('Full','Diff')) or bc.Kind=@type)	--Если задан Full, тогда нужно учесть и Full и Diff.
				and (bc.NetDir is not null and bc.NetDir<>'' and bc.LocalDir<>bc.NetDir) --обязательное условия для копирования файлов!
			--Если задан конкретный ИД бэкапа тогда получим имя файла из таблицы и добавим это имя в условия поиска
			if @FilterBackupID is not null
			begin
				declare @BackupFile nvarchar(500);
				DECLARE @ChainBack TABLE (BackupFile NVARCHAR(800), BackupType VARCHAR(4), ID INT, BackupDate DATETIME2(2));
				INSERT INTO @ChainBack (BackupFile, BackupType, ID, BackupDate)
				EXEC info.usp_GetChainLogs @DBName=@DBFilter,@FilterBackupID=@FilterBackupID, @top=1, @FromCopy=0, @GetBackupFile=1;
			
				select top 1 @BackupFile=BackupFile from @ChainBack;
				if @BackupFile IS NOT NULL AND @BackupFile<>'' 
				begin
					set @Filter_FileName=' -and ($_.Name) -like "'+@BackupFile+'*"';
				end
				else
					set @Filter_FileName='';
			end
			else
				set @Filter_FileName='';
			--определяем поиск по расширению
			if @Force=0
				set @rez_filter='$_.extension -eq ".ONLY"';
			else
				set @rez_filter='($_.extension -eq ".ONLY" -or $_.extension -eq ".BAK")';
			--определяем поиск по датам
			if @DateStart is not null or @DateEnd is not null
			begin
				if @DateStart is not null
					set @date_filter='-and $_.lastwritetime -ge "'+convert(nvarchar(50),@DateStart,120)+'" ';
				if @DateEnd is not null
					set @date_filter=@date_filter+'-and $_.lastwritetime -le "'+convert(nvarchar(50),@DateEnd,120)+'" ';
			end
			else if @XLastHours is not null
			begin
				set @date_filter='-and $_.lastwritetime -ge "'+convert(nvarchar(50),DATEADD(HOUR,-@XLastHours,GETDATE()),120)+'" '+
								 '-and $_.lastwritetime -le "'+convert(nvarchar(50),GETDATE(),120)+'" ';
			end 
			--формируем Имя файла скрипта, в зависимости от параметра ТипБэкапа.
			--Также добавляем текущее время к имени файла.
			--Все это нужно, чтобы исключить вероятность пересечение разных бэкапов.
			--+ теперь ещё учитывается имя экземпляра SQL Server и добавляется в имя файла.
			SELECT @InstanceName=COALESCE('_'+CAST(SERVERPROPERTY('InstanceName') as nvarchar(128)),'');
			set @PSFile='usp_CopyBack'+@InstanceName;
			if @type is not null
				set @PSFile=@PSFile+'_'+@type;
			else
				set @PSFile=@PSFile+'_ALL';
			set @PSFile=@PSFile+'_'+REPLACE(REPLACE(SUBSTRING(CONVERT(NVARCHAR(23),GETDATE(),126),1,23),'T','_'),':','.');
			set @LogFile=@PSFile+'.log';
			set @PSFile='%temp%\'+@PSFile+'.ps1';
			--1. Формируем файл скрипта PowerShell
			set @CMD = N'#PowerShell-Script for COPY backups. Created by Job on local SQL Server. Date: '+convert(varchar(20),getdate(),120);
			exec dbo.[usp_WriteToFile] @CMD, @PSFile, 1;
			set @CMD = N'$ErrorActionPreference = ^^^"stop^^^"';
			exec dbo.[usp_WriteToFile] @CMD, @PSFile;
			set @CMD = N'#Logs for COPY backups. Created by Job on local SQL Server. Date: '+convert(varchar(20),getdate(),120);
			exec dbo.[usp_WriteToFile] @CMD, @LogFile, 1;
			open C
			fetch next from C
			into @LocalDir, @NetDir, @DBName, @Kind
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
				--Блок TRY:
				set @PS_command=N'Try {';
				exec dbo.[usp_WriteToFile] @PS_command, @PSFile;
				--Ищем файлы бэкапов локально
				set @Find_command=N'get-childitem -path "'+@LocalDir+N'" ^^^| where {'+@rez_filter+' -and $_.Name -like "*'+@DBName+'*'+@Kind+N'*" '+@date_filter+@Filter_FileName+'} ^^^|';
				--Логируем найденные файлы бэкапов:
				set @Log_command=N'echo "'+@S+'Get-Date -Format "dd-MM-yyyy HH:mm:ss.ms") INFO Начало копирования (база: '+@DBName+N', тип бэкапа: '+@Kind+') " ^^^>^^^> %temp%\'+@LogFile;
				exec dbo.[usp_WriteToFile] @Log_command, @PSFile;
				set @Log_command=N'echo "INFO Всего файлов: '+@S+'('+REPLACE(@Find_command,'^^^','')+N' measure).count) ; Размер_Гб: '+@S+'[math]::round((('+REPLACE(@Find_command,'^^^','')+N' measure length -sum).sum / 1gb),2)) ; Список файлов далее:" ^^^>^^^>  %temp%\'+@LogFile; 
				exec dbo.[usp_WriteToFile] @Log_command, @PSFile;
				set @Log_command=@Find_command+N' select Name -expand Name ^^^>^^^>  %temp%\'+@LogFile; 
				exec dbo.[usp_WriteToFile] @Log_command, @PSFile;
				--Копирование
				set @PS_command=@Find_command+' Copy-Item -Destination "'+@NetDir+'" -Force';
				exec dbo.[usp_WriteToFile] @PS_command, @PSFile;
				--Переименование расширений файлов в локальном каталоге!
				--Дополнительная проверка - переименовываем только если в каталоге Копий есть такой файл!
				set @Log_command=N'echo "'+@S+'Get-Date -Format "dd-MM-yyyy HH:mm:ss.ms") INFO Конец копирования " ^^^>^^^> %temp%\'+@LogFile;
				exec dbo.[usp_WriteToFile] @Log_command, @PSFile;
				set @Log_command=N'echo "'+@S+'Get-Date -Format "dd-MM-yyyy HH:mm:ss.ms") INFO Начало переименования (LocalDir, база: '+@DBName+N', тип бэкапа: '+@Kind+') .ONLY в .BAK " ^^^>^^^>  %temp%\'+@LogFile;
				exec dbo.[usp_WriteToFile] @Log_command, @PSFile;
				set @Log_command=@Find_command+N' select Name -expand Name ^^^>^^^>  %temp%\'+@LogFile; 
				exec dbo.[usp_WriteToFile] @Log_command, @PSFile;
				declare @Check_command nvarchar(500);
				set @Check_command=' WHERE {(Test-Path ("'+@NetDir+'"+$_.Name)) -eq $true} ^^^|';
				set @PS_command=@Find_command+@Check_command+' Rename-Item -newname { $_.name -replace "\.ONLY",".BAK" }';
				exec dbo.[usp_WriteToFile] @PS_command, @PSFile;
				--Переименование расширений файлов в каталоге копий бэкапов!
				set @Log_command=N'echo "'+@S+'Get-Date -Format "dd-MM-yyyy HH:mm:ss.ms") INFO Начало переименования (NetDir, база: '+@DBName+N', тип бэкапа: '+@Kind+') .ONLY в .BAK " ^^^>^^^>  %temp%\'+@LogFile;
				exec dbo.[usp_WriteToFile] @Log_command, @PSFile;
				set @Find_command=REPLACE(@Find_command, '-path "'+@LocalDir+N'"' , '-path "'+@NetDir+N'"'); 
				set @Log_command=@Find_command+N' select Name -expand Name ^^^>^^^>  %temp%\'+@LogFile; 
				exec dbo.[usp_WriteToFile] @Log_command, @PSFile;
				set @PS_command=@Find_command+' Rename-Item -newname { $_.name -replace "\.ONLY",".BAK" }';
				exec dbo.[usp_WriteToFile] @PS_command, @PSFile;
				--Логирование конца + закрываем скобку для Try
				set @Log_command=N'echo "'+@S+'Get-Date -Format "dd-MM-yyyy HH:mm:ss.ms") INFO Конец переименования " ^^^>^^^> %temp%\'+@LogFile;
				exec dbo.[usp_WriteToFile] @Log_command, @PSFile;
				set @Log_command=N'}'; 
				exec dbo.[usp_WriteToFile] @Log_command, @PSFile;
				set @PS_command=N'Catch {';
				exec dbo.[usp_WriteToFile] @PS_command, @PSFile;
				--Логирование + BREAK + закрываем скобку для Catch
				set @Log_command=N'echo "'+@S+'Get-Date -Format "dd-MM-yyyy HH:mm:ss.ms") ERROR '+@S+'$error[0].exception) " ^^^>^^^> %temp%\'+@LogFile;
				exec dbo.[usp_WriteToFile] @Log_command, @PSFile;
				set @Log_command=N'BREAK';
				exec dbo.[usp_WriteToFile] @Log_command, @PSFile;
				set @Log_command=N'}';
				exec dbo.[usp_WriteToFile] @Log_command, @PSFile;
				fetch next from C
				into @LocalDir, @NetDir, @DBName, @Kind
			end
			close C;
			deallocate C;
			--2. Запуск скрипта PowerShell из файла
			set @CMD = 'powershell '+@PSFile;
			EXEC @rc=xp_cmdshell @CMD , no_output
			if @rc=1
			BEGIN
				set @ErrMsg=N'Ошибка при запуске PowerShell-скрипта из файла %temp%\'+@PSFile;
				RAISERROR(@ErrMsg,11,1) WITH LOG
			END;
			else
				PRINT N'Команда копирования файлов бэкапов успешно выполнена!';
		END;
	end