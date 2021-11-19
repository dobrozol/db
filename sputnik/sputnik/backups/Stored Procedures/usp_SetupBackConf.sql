
	/* =============================================
	-- Author:		Андрей Иванов (sqland1c)
	-- Create date: 12.02.2014 (1.0)
	-- Description: Эта процедура устанавливает настройки Резервного копирования в таблице backups.BackConf.
					Может установить настройку для конкретных баз данных или для всех баз вообще.
					Перед установкой настроек происходит проверка существующих настроек (по имени БД и по типу бэкапа в столбце Kind).
					Если такая настройка уже есть, тогда новая настройка НЕ устанавливается.
					Но если задан параметр @Replace = 1, тогда настройка переписывается.
					Если в качестве типа бэкапа используется Diff, то производится две настройки: Diff в таблицу backups.BackConf
						и Full в таблицу backups.BackConfWeekly. Кроме этого, происходит очистка настройки Full в таблице backups.BackConf для этих БД.
					В алгоритме используется новейшая инструкция MERGE и обобщённое табличное выражение CTE.
					Параметры:
						@DBNAME_in -имя БД, может быть не задано, тогда для всех БД на сервере;
						@TypeBack - тип бэкапа. Значение должно быть задано. Возможны след. значения: Full, Diff, Log.
						@Replace - если параметр задан = 1, то существующая настройка будет переписываться.
						@LocalDir - Каталог для бэкапов. Должен быть задан. 
						@NetDir - Каталог для копий бэкапов (обычно другой сетевой ресурс). Может быть НЕ задан.
						@LocalDays - сколько дней хранить файлы бэкапов в каталоге @LocalDir
						@NetDays - сколько дней хранить файлы копий бэкапов в каталоге @NetDir.
						@SubDirDBName - Если задан = 1, то к полному пути каталогов @LocalDir и @NetDir будет добавлен каталог с именем Базы 
							(поведение по умолчанию для случая, когда не задан параметр @DBNAME_in).
						@FullBackupDay - день недели для создания Полной копии БД. По умолчанию 7 - воскресенье. В остальные дни должны быть созданы Дифф. копии БД.
							Этот параметр необходим, только если в @TypeBack задан Diff.
		UPDATE:
				08.05.2014 (1.1)
					Добавлены новые параметры:
						@ForceSetWeek - Установка указанных настроек в таблицу BackConfWeek (пока без возможности месячных бэкапов).
						@LocalDaysWeek и @NetDaysWeek - Это настройки задаются в таблицу: сколько хранить бэкапы и копии бэкапов. 
												Нужно чтобы была возможность задать разные настройки для BackConf и BackConfWeek при
												установки Diff бэкапов.
				21.07.2014 (1.15)
					Новый параметр @ForceDelete - Если задан 1, то происходит удаление настроек (отбор по имени БД, типу бэкапа и в зависимости
					от параметров @ForceSetWeek и @ForceSetMonth удаляются настроки либо в BackConf, либо в BackConfWeek).
				22.07.2014 (1.20)
					Новый параметр @OnlyDiff - Если указан, то при установки настроек для Diff бэкапов, настройки для Full не будут
					прописаны в таблицу Weekly.
				27.07.2014 (1.22)
					Оптимизирован алгоритм удаления настроек (когда @ForceDelete=1). Добавлена возможность удаления конкретной настройки (если в @FullBackupDay
					задана эта настройка), или всех соотвествующих настроек (если @FullBackupDay=0).

				09.11.2014 (1.24)
					Изменения в алгоритме установки настроек для Weekly - теперь при установке или изменении настроек для Weekly, настройки Daily не удаляются
					- соответствующий код закомментирован. Т.к. теперь весь процесс контролируется программой Хьюстон.
				11.03.2016 (1.25)
					Обновления - добавлены параметры @LocalPolicy и @NetPolicy для установки политик ротаций в настройки бэкапов!
				25.08.2016 (1.26)
					Обновления - добавлена возможность установки бэкапов для системной базы model!
	-- ============================================= */
	CREATE PROCEDURE [backups].[usp_SetupBackConf] 
		@DBNAME_in nvarchar(300) = null
		,@TypeBack varchar(4)
		,@Replace bit = 0
		,@LocalDir nvarchar(500) = null
		,@NetDir nvarchar(500) = null
		,@LocalDays int = 6
		,@NetDays int = 5
		,@SubDirDBName bit=0
		,@FullBackupDay tinyint = 7
		,@ForceSetWeek bit=0
		,@ForceSetMonth bit=0
		,@LocalDaysWeek int = NULL
		,@NetDaysWeek int = NULL
		,@ForceDelete bit=0
		,@OnlyDiff bit=0 
		,@LocalPolicy smallint=0
		,@NetPolicy smallint=0
	AS
	BEGIN
		set nocount on;
		IF @ForceDelete=1 
		BEGIN
			IF @ForceSetMonth=0
			BEGIN
				IF @ForceSetWeek=0 
					DELETE FROM backups.BackConf
					WHERE DBName=@DBNAME_in AND Kind=@TypeBack
				ELSE
					DELETE FROM backups.BackConfWeekly
					WHERE DBName=@DBNAME_in AND Kind=@TypeBack 
						AND ((WeekDay=@FullBackupDay AND @FullBackupDay BETWEEN 1 AND 7) OR (@FullBackupDay=0 AND WeekDay BETWEEN 1 AND 7))
			END
			ELSE
				DELETE FROM backups.BackConfWeekly
				WHERE DBName=@DBNAME_in AND Kind=@TypeBack
					AND ((MonthDay=@FullBackupDay AND @FullBackupDay BETWEEN 1 AND 31) OR (@FullBackupDay=0 AND MonthDay BETWEEN 1 AND 31))
			return 1;
		END
		IF @LocalDir is null OR @LocalDir='' OR @TypeBack NOT IN ('Full','Diff','Log')
			return 0;
		IF CHARINDEX('\',@LocalDir,LEN(@LocalDir)-1)=0
			set @LocalDir=@LocalDir+'\';
		IF CHARINDEX('\',@NetDir,LEN(@NetDir)-1)=0
			set @NetDir=@NetDir+'\';
		IF @DBNAME_in = ''
			set @DBNAME_in = NULL;
		IF @LocalDaysWeek IS NULL 
			SET @LocalDaysWeek=@LocalDays;
		IF @NetDaysWeek IS NULL 
			SET @NetDaysWeek=@NetDays;

		IF @DBNAME_in is null
			set @SubDirDBName=1;
		IF @ForceSetWeek=0
		BEGIN
			WITH DBs 
			AS
				(
					select name, @TypeBack as Kind from sys.databases
					where (@DBNAME_in is null or name=@DBNAME_in) and (name not in ('tempdb')) and (state=0 and is_read_only=0)
							and (recovery_model_desc<>'SIMPLE' or @TypeBack<>'Log')
				)
			MERGE backups.BackConf AS target
			USING (	select 
						name, Kind,
						case when @SubDirDBName=1 then @LocalDir+name+'\' else @LocalDir end,
						case when @SubDirDBName=1 and @NetDir is not null  then @NetDir+name+'\' else @NetDir end,
						@LocalDays, @NetDays, @LocalPolicy, @NetPolicy from DBs
			) AS source (DBName,Kind,LocalDir,NetDir,LocalDays,NetDays, LocalPolicy, NetPolicy)
				ON (target.DBName=source.DBName and target.Kind=source.Kind)
			WHEN NOT MATCHED THEN
				INSERT (DBName,LocalDir,NetDir,LocalDays,NetDays,Kind, LocalPolicy, NetPolicy)
				VALUES (source.DBName,source.LocalDir,source.NetDir,source.LocalDays,source.NetDays,source.Kind, source.LocalPolicy, source.NetPolicy)
			WHEN MATCHED AND @Replace=1 THEN
				UPDATE SET LocalDir=source.LocalDir, NetDir=source.NetDir, LocalDays=source.LocalDays, NetDays=source.NetDays,
					LocalPolicy=source.LocalPolicy, NetPolicy=source.NetPolicy;
		END
		IF (@TypeBack='Diff' AND @OnlyDiff=0) OR @ForceSetWeek=1
		BEGIN
			WITH DBs 
			AS
				(
					select name, 'Full' as Kind from sys.databases
					where (@DBNAME_in is null or name=@DBNAME_in) and (name not in ('tempdb')) and (state=0 and is_read_only=0)
				)
			MERGE backups.BackConfWeekly AS target
			USING (	select 
						name, Kind,
						case when @SubDirDBName=1 then @LocalDir+name+'\' else @LocalDir end,
						case when @SubDirDBName=1 and @NetDir is not null  then @NetDir+name+'\' else @NetDir end,
						@LocalDaysWeek, @NetDaysWeek, @FullBackupDay, @LocalPolicy, @NetPolicy from DBs
			) AS source (DBName,Kind,LocalDir,NetDir,LocalDays,NetDays, WeekDay, LocalPolicy, NetPolicy)
				ON (target.DBName=source.DBName and target.Kind=source.Kind and target.WeekDay=source.WeekDay)
			WHEN NOT MATCHED THEN
				INSERT (DBName,LocalDir,NetDir,LocalDays,NetDays,Kind,WeekDay, LocalPolicy, NetPolicy)
				VALUES (source.DBName,source.LocalDir,source.NetDir,source.LocalDays,source.NetDays,source.Kind,source.WeekDay,source.LocalPolicy, source.NetPolicy)
			WHEN MATCHED AND @Replace=1 THEN
				UPDATE SET LocalDir=source.LocalDir, NetDir=source.NetDir, LocalDays=source.LocalDays, NetDays=source.NetDays,
					LocalPolicy=source.LocalPolicy, NetPolicy=source.NetPolicy;
		
			/*	При изменении или добавлении настроек в Weekly удалять соответствующие настройки из Daily теперь НЕ НУЖНО!
				Вся настройка производится из программы Хьюстон и полностью её контролируется!
				Поэтому код ниже закоментирован!
					WITH DBs 
					AS
						(
							select name, 'Full' as Kind from sys.databases
							where (@DBNAME_in is null or name=@DBNAME_in) and (name not in ('tempdb','model')) and (state=0 and is_read_only=0)
						)		
					DELETE Del
					FROM backups.BackConf AS Del
					INNER JOIN DBs
						ON Del.DBName=DBs.name and Del.Kind=DBs.Kind
			*/
		END
		return 1;
	END