
	/* =============================================
	-- Author:		Андрей Иванов (sqland1c)
	-- Create date: 17.12.2013 (1.0)
	-- Description: Эта процедура позволяет восстанавить указанную базу на локальном сервере из другой базы (и даже с другого сервера).
					Если восстановление происходит из базы другого сервера нужно, чтобы на локальном сервере была настроена связь
					через LinkedServer c удалённым сервером @ServerSource (желательно с привязкой к логину RestoreDB_Link.
					Также необходимо, чтобы файлы резервных копий существовали на том самом месте, куда они создавались!
					В параметре @dbowner нужно указать владельца (это логин на уровне SQL Server) для новой восстанавливаемой БД (для работы из 1С и других приложений)!
					В параметре @MoveFilesTo нужно задать путь для размещения всех файлов БД.
	-- Update:
					09.01.2014 (1.1)
						Новый параметр @ToDate - производит восстановление базы на указанную дату (не позже).
					17.01.2014 (1.2)
						Все текстовые параметры увеличены. Добавлена проверка: если не удалось найти резервную копию для Базы-Источника, то restore не происходит!	
					21.01.2014 (1.25)
						Добавлено создание каталога @MoveFilesTo, если его нет. Через команду xp_cmdshell.
					28.01.2014 (1.27)
						Добавлен новый параметр @NoRecovery - если указан 1, то база данных останется в состояние NoRecovery (для дальнейшего восстановления).
							По умолчанию 0.
						Также в команду восстановления добавлен параметр STATS=10 - показывать ход выполнения каждые 10 %.
					26.02.2014 (1.28)
						Исправлена ошибка - при создании каталога нужно использовать "", чтобы создать каталог с пробелами.
					28.02.2014 (1.4)
						Процедура существенно доработана: оптимизирован алгоритм получения информации о файлах БД (теперь вся информация берётся из бэкапа).
						А также добавлена возможность восстановления из указанного бэкапа - для этого нужно указать полный путь в новом параметре @FromDisk.
					04.03.2014 (1.41)
						В алгоритм определения файлов добавлено определения файла для Каталога Полнотекстового поиска.
					02.04.2014 (1.43)
						Небольшое исправление - замена ONLY на BAK в имени файла бэкапа вынесено наверх (сразу после получения соответствующих переменных).
					06.04.2014 (1.5)
						Теперь большинство параметров можно не задавать! Нужно только задать Имя БД назначения и источник (БД или файл бэкапа).
						При локальном источнике Имя сервера теперь не нужно задавать! Также путь для размещения базы теперь автоматически определяется -
						на диске, где больше всего свободного места создаётся каталог \DATA\Имя БД назначения.
					19.04.2014 (1.6)
						Новый параметр @FromLog - файл бэкапа Журнала транзакций. Соотвественно, теперь есть возможность продолжить восстановление
						из журнала транзакций!
					20.04.2014 (1.61)
						Оптимизирован алгоритм удаления существующей базы в самом начале процедуры.
					22.04.2014 (1.62)
						Добавлен алгоритм выполнения кода от имена sa в случае, если параметр @NoRecovery=1. Чтобы сделать владельцем восстанавливаемой
						базы sa, а не текущего пользователя.
					23.04.2014 (1.65)
						Добавлен параметр @ForceRecovery. Изменён алгоритм перевода базы после восстановления в режим RECOVERY. Добавлен алгоритм
						сжатия файлов Логов после восстановления и перевода базы в RECOVERY. Оптимизирован алгоритм определения пути, где будет лежать
						новая база.
					03.06.2014 (1.7)
						Добавлен параметр @DiffBack. Этот параметр говорит о том, что в параметре @FromDisk дифф. бэкап. Соответственно появилась
						восстановление из Дифф. бэкапа (аналогично восстановлению полного бэкапа).
					05.08.2014 (1.8)
						Новый параметр @NoFileList. Если задан 1, то информация о файлах берётся из уже восстановленной на сервере БД (из 
						представления sys.master_files. Иначе, информация о файлах берётся непосредственно из бэкапа.
						Этот параметр следует применять при повторном накате бэкапов на восстанавливаемую БД, чтобы ускорить процесс восстановления!
					07.08.2014 (1.81)
						Новый параметр @NoStats, если задан тогда в процессе восстановления не будет выводится кол-во выполненных процентов
						(оператор STATS в команде RESTORE).
					24.09.2014 (1.82)
						В блоке удаления существующей БД добавлен алгоритм отключения Зеркалирование с проверкой перед удалением существующей БД. 
						Также в этот же блок изменен блок обработки исключений - теперь будет выводиться ERROR_MESSAGE.
					18.03.2015 (1.85)
						Добавлен новый параметр @StandBy_File - для поддержки режима STANDBY (read-only) для восстанавливаемой БД. Может применяться
						в механизме lse.
					20.03.2015 (1.86)
						Доработан алгоритм восстановления БД в режиме STANDBY (read-only) - теперь перед восстановлением журналов транзакций БД будет
						переведена в режим single_user, чтобы закрыть коннекты с базой и начать восстановление.
					14.08.2015 (1.90)
						Добавлена поддержка FileStream-данных!
						Также добавлена поддержка восстановление БД из разных файловых групп!
						Сначала нужно восстановить полный бэкап или PRIMARY файловую группу с параметром @NoRecovery=1. 
						Затем уже восстановить остальные ФГ с указанием параметра @ContinueRecovery=1. Причем бэкап таких ФГ мог быть создан раньше
						(в случае если это ФГ READONLY!), чем Primary. Затем уже можно восстановить бэкапы журналов транзакций (если необходимо)!
					25.10.2015 (1.92)
						При восстановлении из Log-файлов теперь не формируется команды MoveFilesTo (их не должно быть в команде Restore Log...)
						Также добавлен параметр @MoveLogFilesTo - иногда требуется разместить Log-файлы на другом диске. Если параметр не задан,
						то все файлы будут размещены в месте указанном в параметре @MoveFilesTo
					15.12.2015 (1.93)
						Расширены длина строки для параметров  @tsql и @tsql2 чтобы избежать проблем обрезания строки когда в БД много файлов!
					10.09.2016 (1.94)
						Добавлен новый параметр @NoSetMultiUser!
					05.04.2017 (1.950)
						Добавлен новый параметр @ChangeDBOwner! Если =1, то процедура только поменяет владельца существующей БД, без восстановления. Если в базе уже есть такой пользователь, то ему назначается роль db_owner.
						Также сжатие лог-файла теперь будет до 128Мб (было 512).
					13.04.2017 (1.953)
						Изменён алгоритм получения данных из restore filelistonly. Вместо табличной переменной теперь временная таблица.
						Также здесь добавлена поддержка 2016 версии (появился новый столбец).
					22.02.2018 (1.957)
						Добавлен новый параметр @del_bak_hs - если 1, то будет очистка истории бэкапов для базы (если она есть).
						По умолчанию 0.
				07.08.2019 (1.961)
					Добавлена совместимость со старыми версиями SQL Server (<2012) в самом конце процедуры в алгоритме переопределения владельца БД.
				26.11.2020 (1.962)
					Added new column for FILELISTONLY (for compability with 2019)
-- ============================================= */
CREATE PROCEDURE [backups].[usp_RestoreDB_simple]  
	@DBNameTarget nvarchar(200),
	@DBNameSource nvarchar(200)=null,
	@ServerSource nvarchar(200)=null,
	@dbowner nvarchar(50)='sa',
	@MoveFilesTo nvarchar(500)=NULL,
	@MoveLogFilesTo nvarchar(500)=NULL,
	@ToDate datetime=null,
	@NoRecovery bit = 0,
	@FromDisk nvarchar(600)=null,
	@DiffBack bit = 0,
	@FromLog  nvarchar(600)=null,
	@ForceRecovery bit = 0,
	@NoFileList bit = 0,
	@NoStats bit = 0,
	@StandBy_File nvarchar(500)=null,
	@ContinueRecovery bit = 0,
	@NoSetMultiUser bit=0,
	@ChangeDBOwner bit = 0,
	@del_bak_hs bit = 0
AS
BEGIN
	--Определяем мажорную версию SQL Server
	declare @Ver nvarchar(128), @VerMain numeric(6,3);
	set @Ver=CAST(SERVERPROPERTY('ProductVersion') as nvarchar(128));
	set @VerMain=cast(LEFT(@Ver, CHARINDEX('.',@Ver, CHARINDEX('.', @Ver)+1)-1) as numeric(6,3));
	IF @ChangeDBOwner=0
	BEGIN
		IF @ForceRecovery=0
		BEGIN
			IF @NoRecovery=1
				EXECUTE AS LOGIN = 'sa';
			declare @LASTBACKUP nvarchar(600),@FULLBACKUP nvarchar(600), @tsql nvarchar(4000), @tsql2 nvarchar(4000)='', @type nvarchar(10), @typeF int, @ErrMsg nvarchar(800);
			declare @All smallint, @i smallint,	@File_Id int, @PrevFileID int, @DB_ID int, @FileName nvarchar(70), @NewFileName nvarchar (400),  @tmkdir nvarchar(550);
			declare @Tab TABLE (FileName nvarchar(128), FileID bigint, type tinyint);
			declare @RecoveryState nvarchar(650), @RESTORE VARCHAR(8), @SingleUserMode nvarchar(300)='', @MultiUserMode nvarchar(300)='';
			declare @StrStats NVARCHAR(20)='';
			IF @NoStats=0
				SET @StrStats = N',STATS=10';

			IF @FromLog IS NULL AND @DiffBack=0 AND @ContinueRecovery=0
			BEGIN
				IF DB_ID(@DBNameTarget) IS NOT NULL
				BEGIN
					BEGIN TRY
						IF @del_bak_hs=1
							EXEC msdb.dbo.sp_delete_database_backuphistory @database_name = @DBNameTarget;
						--Отключим зеркалирование (если оно включено) для базы, перед удалением!
						IF EXISTS(SELECT database_id FROM sys.database_mirroring WHERE mirroring_guid IS NOT NULL AND database_id=DB_ID(@DBNameTarget))
						BEGIN
							SET @tsql='ALTER DATABASE ['+@DBNameTarget+'] SET PARTNER OFF';
							EXEC (@tsql);
						END;
						--Перевод базы в режим SINGLE_USER с отключением всех соединений (если база в Online и в Multi_USer).
						IF EXISTS(SELECT database_id FROM SYS.DATABASES WHERE name = @DBNameTarget AND state_desc='ONLINE' AND user_access_desc='MULTI_USER')
						BEGIN
							SET @tsql='ALTER DATABASE ['+@DBNameTarget+'] SET SINGLE_USER WITH ROLLBACK IMMEDIATE';
							EXEC (@tsql);
						END
						SET @tsql='DROP DATABASE ['+@DBNameTarget+']';
						EXEC (@tsql);
					END TRY
					BEGIN CATCH 
						set @ErrMsg='usp_RestoreDB_simple: Ошибка при удалении базы и/или истории резервных копий по базе ! Подробности: '+ERROR_MESSAGE();
						RAISERROR(@ErrMsg,11,1) WITH LOG
					END CATCH
				END
			END

			IF @MoveFilesTo IS NULL AND @FromLog IS NULL
			BEGIN
				--Если путь к базе НЕ указан, определяем диск, где больше всего свободного места.
				DECLARE @MaxDrive CHAR(1);
				IF @FromLog IS NULL AND @DiffBack=0
					exec sputnik.info.usp_GetDrives @GetMaxFree=1, @MaxFreeDrive=@MaxDrive OUTPUT;
				ELSE
					SELECT TOP 1 @MaxDrive=LEFT(physical_name,1)
					FROM SYS.MASTER_FILES
					WHERE database_id = DB_ID(@DBNameTarget);
				SET @MoveFilesTo=@MaxDrive+':\DATA\'+@DBNameTarget+'\';
			END;

		
			if @FromDisk is null AND @FromLog IS NULL
			begin
				set @FULLBACKUP=''; 
	
				if @ToDate is null
					set @ToDate=getdate()

				IF @ServerSource IS NULL
					SET @ServerSource=@@SERVERNAME;

				IF @DBNameSource IS NULL
					SET @DBNameSource=@DBNameTarget;

				set @tsql=N'select @res1=
					ltrim(rtrim(MF.physical_device_name)), @res2=BS.type
				from
				(SELECT top 1
					BS.media_set_id,
					BS.name,
					BS.type
				FROM 
					['+@ServerSource+'].[msdb].[dbo].[backupset] as BS
				where
					BS.database_name = '''+@DBNameSource+'''
					and (BS.type in (''I'',''D''))
					and (BS.backup_finish_date <= '''+convert(nvarchar(23),@ToDate,109)+''')
				order by BS.backup_finish_date desc) as BS
				inner join ['+@ServerSource+'].[msdb].[dbo].[backupmediafamily] as MF on BS.media_set_id=MF.media_set_id';

				exec sp_executesql @tsql, N'@res1 nvarchar(300) OUTPUT,@res2 nvarchar(10) OUTPUT', @res1=@LASTBACKUP OUTPUT,@res2=@type OUTPUT


				IF @type='I'	--Если это Дифференциальная копия, то получить последнюю полную копию!
				BEGIN
					set @tsql=N'select @res1=
						ltrim(rtrim(MF.physical_device_name))
					from
					(SELECT top 1
						BS.media_set_id,
						BS.name
					FROM 
						['+@ServerSource+'].[msdb].[dbo].[backupset] as BS
					where
						BS.database_name = '''+@DBNameSource+'''
						and BS.type = ''D''
					order by BS.backup_finish_date desc) as BS
					inner join ['+@ServerSource+'].[msdb].[dbo].[backupmediafamily] as MF on BS.media_set_id=MF.media_set_id';

					exec sp_executesql @tsql, N'@res1 nvarchar(300) OUTPUT', @res1=@FULLBACKUP OUTPUT
				END
			end
			else
			begin
				IF @FromLog IS NULL
					set @LASTBACKUP=@FromDisk;
				ELSE
					set @LASTBACKUP=@FromLog;
				set @FULLBACKUP='';
			end
			set @LASTBACKUP=replace(@LASTBACKUP,'.only','.BAK');
			IF @FULLBACKUP<>''
				set @FULLBACKUP=replace(@FULLBACKUP,'.only','.BAK');
		
			--Получаем информацию о файлах БД и формируем команды MoveFilesTo только если восстанавливаем НЕ Log файлы
			--т.к. для команды Restore Log команды MoveFilesTo задавать нельзя.
			IF @FromLog IS NULL
			BEGIN 
				--Получить Все имена, ИД и типы файлов и записать их в таблицу @Tab
				if @NoFileList=1 AND EXISTS(select database_id from sys.databases where name=@DBNameTarget and state=1)
				--Если задан параметр @NoFileList=1, тогда информацию о файлах возьмём из представления sys.master_files
				--При этом предполагается, что целевая БД уже существует на сервере в состоянии RESTORING.
					insert into @Tab (FileName,FileID, type)
					select 
						name as FileName,
						file_id as FileID, 
						type
					from sys.master_files
					where database_id=db_id(@DBNameTarget);
				else
				begin	
					--В остальных случаях Данные взять из последнего бэкапа!
					if object_id('tempdb.dbo.#FileList') is not null
						drop table #FileList;
					create table #FileList
						(
							LogicalName nvarchar(128),
							PhysicalName nvarchar(260),
							Type char(1),
							FileGroupName nvarchar(128),
							Size numeric(20,0),
							MaxSize numeric(20,0),
							FileId bigint,
							CreateLSN numeric(25,0),
							DropLSN numeric(25,0) NULL,
							UniqueID  uniqueidentifier,
							ReadOnlyLSN numeric(25,0) NULL,
							ReadWriteLSN  numeric(25,0) NULL,
							BackupSizeInBytes bigint,
							SourceBlockSize int,
							FileGroupId int,
							LogGroupGUID uniqueidentifier NULL, 
							DifferentialBaseLSN  numeric(25,0) NULL,
							DifferentialBaseGUID uniqueidentifier NULL,
							isReadOnly bit,
							isPresent bit,
							TDEThumbrint varbinary(32),
							SnapshotURL nvarchar(360)
						)

					insert into #FileList
					exec(
							'restore filelistonly
							from disk=N'''+@LASTBACKUP+''''
						)
					insert into @Tab (FileName,FileID, type)
					select 
						LogicalName as FileName, 
						FileID, 
						case
							when Type='L' then 1
							when Type='D' then 0
							when Type='F' then 2
							when Type='S' then 3 --FileStream
						end as type
					from #FileList
				end
		
				--Формируем команду для Restore по перемещению файлов БД на новое место.
				declare CF Cursor for
				select FileName,FileID,type from @Tab;
				open CF;
				fetch next from CF into @FileName, @File_Id, @typeF;
				while @@Fetch_Status=0
				begin
					if @File_ID=1
						set @NewFileName=@DBNameTarget++'.MDF';
					else if @typeF=0
						set @NewFileName=@DBNameTarget+cast(@File_Id as nvarchar(10))+'.NDF';
					else if @typeF=1
						set @NewFileName=@DBNameTarget+'_log_'+cast(@File_Id as nvarchar(10))+'.LDF';
					else if @typeF=2
						set @NewFileName=@DBNameTarget+'_FullTextSearchCatalog_'+cast(@File_Id as nvarchar(10))+'.NDF';
					else if @typeF=3
					begin
						set @NewFileName=@DBNameTarget+'_FS_'+@FileName+'\';
						--Создать каталог для FileStream данных (если его не существует).
						set @tmkdir = N'mkdir "' + @MoveFilesTo+@NewFileName + '"';
						exec xp_cmdshell @tmkdir, no_output;
					end
					set @tsql2=@tsql2+N'
								MOVE N'''+@FileName+''' TO N'''+
								CASE 
									WHEN @MoveLogFilesTo IS NOT NULL AND @typeF=1 THEN @MoveLogFilesTo
									ELSE @MoveFilesTo
								END +
								@NewFileName+''',';
					fetch next from CF into @FileName, @File_Id, @typeF;
				end
				close CF;
				deallocate CF;
			END;

			IF @LASTBACKUP IS NOT NULL
			BEGIN
				--Создать каталог @MoveFilesTo (если его не существует).
				set @tmkdir = N'mkdir "' + @MoveFilesTo + '"';
				exec xp_cmdshell @tmkdir, no_output;
				if @MoveLogFilesTo is not null
				begin
					--Создать каталог @@MoveLogFilesTo (если он был задан и он не существует).
					set @tmkdir = N'mkdir "' + @MoveLogFilesTo + '"';
					exec xp_cmdshell @tmkdir, no_output;
				end
				--Формируем все команду для Restore.
				if @NoRecovery=1
					set @RecoveryState='NORECOVERY';
				else
					set @RecoveryState='RECOVERY';
				if @StandBy_File IS NOT NULL AND @FromLog IS NOT NULL
				BEGIN
					set @RecoveryState='STANDBY=N'''+@StandBy_File+'''';
					if exists (select database_id from sys.databases where name=@DBNameTarget and is_in_standby=1 and [state]=0)
					BEGIN
						set @SingleUserMode=' ALTER DATABASE ['+@DBNameTarget+'] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; ';
						if @NoSetMultiUser=0
							set @MultiUserMode=' ALTER DATABASE ['+@DBNameTarget+'] SET MULTI_USER; ';
					END	
				END
				if @FULLBACKUP=''
				begin
					IF @FromLog IS NULL
						SET @RESTORE='DATABASE';
					ELSE
						SET @RESTORE='LOG';

					set @tsql=@SingleUserMode+
					'RESTORE ' +@RESTORE+ ' ['+@DBNameTarget+']
						FROM  DISK = N'''+@LASTBACKUP+''' 
					 WITH  FILE = 1, '+@tsql2+'
					'+@RecoveryState+',NOUNLOAD,  REPLACE'+@StrStats+';'+
					+@MultiUserMode;
				end
				else
				begin
					set @tsql=
					'RESTORE DATABASE ['+@DBNameTarget+']
						FROM  DISK = N'''+@FULLBACKUP+''' 
					 WITH  FILE = 1, '+@tsql2+'
					NORECOVERY, NOUNLOAD,  REPLACE'+@StrStats;
					--print (@tsql);
					exec (@tsql);
					set @tsql=
					'RESTORE DATABASE ['+@DBNameTarget+']
						FROM  DISK = N'''+@LASTBACKUP+''' 
					 WITH  FILE = 1, '+@tsql2+'
					'+@RecoveryState+', NOUNLOAD,  REPLACE'+@StrStats;
				end;
				--print (@tsql);
				exec (@tsql);
			END
			ELSE
				select 'Не удалось найти резервную копию для базы!!!'
		END
		ELSE
		BEGIN
			set @tsql='RESTORE DATABASE ['+@DBNameTarget+']	WITH RECOVERY';
			EXEC (@tsql);	
		END
	END
	ELSE
		set @NoRecovery=0;
	
	IF @NoRecovery=0
	BEGIN
		--после восстановления изменить модель восстановления на простую 
		--и прописать владельца базы (пользователь, под которым идёт обращение из приложения)
		SET @tsql=N'USE [master]
		ALTER DATABASE ['+@DBNameTarget+'] SET RECOVERY SIMPLE WITH NO_WAIT;';
		--print (@tsql);
		EXEC (@tsql);
		SET @tsql=N'USE ['+@DBNameTarget+'];
			IF EXISTS(select principal_id from sys.database_principals where [name]='''+@dbowner+''')'+
			CASE WHEN @VerMain < 11 THEN '
				exec sp_addrolemember ''db_owner'','''+@dbowner+''';' ELSE '
				ALTER ROLE [db_owner] ADD MEMBER ['+@dbowner+'];' END+'
			ELSE
				ALTER AUTHORIZATION ON DATABASE::['+@DBNameTarget+'] TO ['+@dbowner+'];';
		--print (@tsql);
		EXEC (@tsql);

		--Сжатие файлов Логов принудительно для восстановленной БД до 128 Мб.:
		EXEC sputnik.db_maintenance.usp_ShrinkLogFile 
				@db_name=@DBNameTarget, 
				@SetSizeMb=128,
				@FileGrowthMb=64
	END
END