
	/* =============================================
	-- Author:		Андрей Иванов (sqland1c)
	-- Create date: 04.08.2014 (1.0)
	-- Description: Процедура управления настройками по Log Shipping Easy (не стандартному, а работающему через спутник).
					Параметры: @ServerSource - имя сервера источника (где лежит БД источник), 
					@DBNameSource - имя БД источника, 
					@DBNameTarget - имя БД назначения (в эту базу будет выполняться восстановление).
					@FromCopy - Указывает из какого каталога будет выполняться восстановление: из самих бэкапов или из копий бэкапов.
					@ForceDelete - если 1, тогда заданная настройка удаляется!
					@ReInit - Если параметр указан, то для указанной настроки будет установлена Инициализация (процесс восстановления начнётся заново с полного бэкапа).
					@CatalogFilesDB - задаёт расположение (каталог) для всех Файлов целевой БД.
					@Suspend - Признак приостановки/паузы. Если задан 1, тогда накат бэкапов не будет выполняться для этой целевой БД.

					Алгоритм следующий: Процедура будет работать, только если на целевом Сервере правильно настроен Linked Server на сервер Primary!
					Так что перед запуском этой процедуры нужно настроить Linked Server! Но можно не указывать Linked Server - тогда LSE будет работать в
					рамках одного сервера SQL Server!
					Когда задан @ForceDelete -удаляем все настройки и историю, а также настройки на первичном сервере.
					В противном случае добавляем настройки (если этих нет). А если они есть обновляем значение FromCopy в настройках. При этом если значение параметра
					@ReInit=1 , тогда настраивается процесс Инициализации (обновляются соответствующие столбцы в таблице [TargetConfig].
					При успешном выполнении процедура должна вернуть 1.
	-- Update:		
					08.10.2014 (1.05)
					В блоки исключений добавлен вывод подробных ошибок через команду print! Также в алгоритме добавления новых настроек изменен порядок:
					сначала выполняется операция на удаленном сервере-источнике, затем уже локально.
					В алгоритме удаления конфигурации lse изменен порядок: сначала операция выполняется на удаленном сервере-источнике, а затем локально.
					Также в блок исключения добавлена проверка на открытые транзакции перед ROLLBACK (@@TRANCOUNT).
					И исправлен алгоритм удаления на удаленном сервере-источнике (были лишние кавычки в строке).
					19.03.2015 (1.06)
					Добавлен новый столбец и новый параметр @StandBy_File - реализует режим StandBy (read-only) для восстанавливаемой БД.
					25.07.2016 (1.07)
					Добавлен новый столбец и новый параметр @CatalogLogFiles - теперь можно расположить лог-файлы на отдельный диск.
					01.02.2017 (1.075)
					При удалении конфигурации добавлен алгоритм очистки неактуальных конфигураций на серверах источнике и приемнике.
					13.11.2017 (1.080)
					Добавлен новый параметр @UseFreshDiffBack - Разрешает использовать требование "свежести" к дифф.бэкапу, а также разрешает создать свежий дифф.бэкап, если его ещё нет!
	-- ============================================= */
	CREATE PROCEDURE [lse].[usp_SetLseConf]
		@ServerSource nvarchar(300) = null,
		@DBNameSource nvarchar(500) = null,
		@DBNameTarget nvarchar(500),
		@FromCopy bit = null,
		@ForceDelete bit = 0,
		@ReInit bit = 0,
		@CatalogFilesDB nvarchar(800) = null,
		@CatalogLogFiles nvarchar(800) = null,
		@Suspend bit = null,
		@StandBy_File nvarchar(600)=null,
		@UseFreshDiffBack bit = 1
	AS
	BEGIN
		SET NOCOUNT ON;
		DECLARE @errmsg NVARCHAR(600);
		IF EXISTS(select server_id from sys.servers where data_source=@ServerSource) OR @ServerSource IS NULL 
		BEGIN
			IF @ServerSource IS NULL
				SET @ServerSource=CAST(SERVERPROPERTY('ServerName') AS NVARCHAR(500));
			DECLARE @ServerTarget nvarchar(500)=CAST(SERVERPROPERTY('ServerName') AS NVARCHAR(500));
			IF @ForceDelete=1
			BEGIN
				declare @res bit;
				begin try
					EXEC('
								DELETE FROM ['+@ServerSource+'].sputnik.lse.SourceConfig
								WHERE ServerTarget='''+@ServerTarget+''' AND DBNameTarget='''+@DBNameTarget+''';
								--Дополнительно почистим НЕАКТУАЛЬНЫЕ конфигурации:
								DELETE FROM ['+@ServerSource+'].sputnik.lse.SourceConfig
								WHERE ServerTarget=(select [data_source] from ['+@ServerSource+'].master.sys.servers where is_linked=0);
								DELETE FROM ['+@ServerSource+'].sputnik.lse.TargetConfig
								WHERE ServerSource=(select [data_source] from ['+@ServerSource+'].master.sys.servers where is_linked=0);
								DELETE DelTab FROM ['+@ServerSource+'].sputnik.lse.HS DelTab
								LEFT JOIN ['+@ServerSource+'].sputnik.lse.TargetConfig tc
								ON DelTab.config_id=tc.id
								WHERE tc.id IS NULL;
							');
					begin tran
						DELETE 
						FROM [lse].[hs]
						WHERE config_id IN (SELECT id FROM [lse].[TargetConfig] WHERE DBNameTarget=@DBNameTarget);
						DELETE 
						FROM [lse].[TargetConfig]
						WHERE DBNameTarget=@DBNameTarget;
						--Дополнительно почистим НЕАКТУАЛЬНЫЕ конфигурации:
						DELETE FROM lse.SourceConfig
						WHERE ServerTarget=(select [data_source] from sys.servers where is_linked=0);
						DELETE FROM lse.TargetConfig
						WHERE ServerSource=(select [data_source] from sys.servers where is_linked=0);
						DELETE DelTab FROM lse.HS DelTab
						LEFT JOIN lse.TargetConfig tc
						ON DelTab.config_id=tc.id
						WHERE tc.id IS NULL;

					commit tran;
					set @res=1;
				end try
				begin catch
					IF @@TRANCOUNT > 0 
						rollback;
					set @res=0;
					set @errmsg=N'Ошибка при удалении старой конфигурации lse. Описание: '+ERROR_MESSAGE();
					print(@errmsg);
				end catch
				return @res;
			END;
			declare @t table (id smallint);
			INSERT INTO @t (id)
			SELECT id FROM sputnik.lse.TargetConfig WHERE DBNameTarget=@DBNameTarget
			IF EXISTS(select id from @t)
			BEGIN
				begin try
					IF @ReInit=1
					begin
						UPDATE sputnik.lse.TargetConfig
						SET InitDate=NULL, InitBackupHS_id=NULL, [Suspend]=0, UseFreshDiffBack=@UseFreshDiffBack
						WHERE DBNameTarget=@DBNameTarget;
						IF @CatalogFilesDB IS NOT NULL
							UPDATE sputnik.lse.TargetConfig
							SET CatalogFilesDB=@CatalogFilesDB,
								CatalogLogFiles=@CatalogLogFiles
							WHERE DBNameTarget=@DBNameTarget;
					end
					IF @FromCopy IS NOT NULL
						UPDATE sputnik.lse.TargetConfig
						SET FromCopy=@FromCopy
						WHERE DBNameTarget=@DBNameTarget;
					IF @Suspend IS NOT NULL
						UPDATE sputnik.lse.TargetConfig
						SET [Suspend]=@Suspend
						WHERE DBNameTarget=@DBNameTarget;	
					IF @StandBy_File IS NOT NULL
						UPDATE sputnik.lse.TargetConfig
						SET [StandBy_File]=@StandBy_File
						WHERE DBNameTarget=@DBNameTarget;
				end try
				begin catch
					set @errmsg=N'Ошибка при обновлении конфигурации lse (ReInit). Описание: '+ERROR_MESSAGE();
					print(@errmsg);
					return 0;
				end catch
			END;
			ELSE
			BEGIN
				begin try
					exec('
							DELETE FROM ['+@ServerSource+'].sputnik.lse.SourceConfig
								WHERE ServerTarget='''+@ServerTarget+''' AND DBNameTarget='''+@DBNameTarget+''';
							INSERT INTO ['+@ServerSource+'].sputnik.lse.SourceConfig (ServerTarget, DBNameSource, DBNameTarget)
								VALUES ('''+@ServerTarget+''', '''+@DBNameSource+''' ,'''+@DBNameTarget+''');
					');
					INSERT INTO sputnik.lse.TargetConfig (ServerSource, DBNameSource, DBNameTarget, FromCopy, CatalogFilesDB, CatalogLogFiles, [StandBy_File], UseFreshDiffBack)
					VALUES (@ServerSource, @DBNameSource,@DBNameTarget,@FromCopy, @CatalogFilesDB, @CatalogLogFiles, @StandBy_File, @UseFreshDiffBack);
				end try
				begin catch
					set @errmsg=N'Ошибка при добавлении новой конфигурации lse. Описание: '+ERROR_MESSAGE();
					print(@errmsg);
					return 0;
				end catch
			END	
			return 1;
		END;
		ELSE
			return -1;
	END