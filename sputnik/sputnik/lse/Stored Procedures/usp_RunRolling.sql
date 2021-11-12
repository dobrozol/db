
	/* =============================================
	-- Author:		Андрей Иванов (sqland1c)
	-- Create date: 04.08.2014 (1.0)
	-- Description: Эта процедура используется как часть модуля lse - Log Shipping Easy.
					Воспроизводит (накатывает) бэкапы логов на конкретную целевую БД. Непосредственное восстановление происходит через ХП [usp_RestoreDB_simple].

	-- Update:		05.08.2014 (1.1)
					Оптимизирован алгоритм восстановления через процедуру usp_RestoreDB_simple. Теперь при восстановления БД 
					используется новый параметр @NoFileList=1, который говорит о том, что все данные о файлах целевой БД будут получены 
					из системного представления sys.master_files, а не из бэкапов. Это должно ускорить восстановление каждого бэкапа!
				
					07.08.2014 (1.11) Добавлен новый параметр @pp - если задан, значит процесс запущен в многопоточном режиме через модуль pp.
					В этом случае нужно выводить как можно меньше сообщений пользователю!				

					19.08.2014 (1.2) Новый параметр @Maxi, который регулирует количество бэкапов логов обрабатываемых за 1 сеанс.
					По умолчанию 8. Для того, чтобы избежать высокой и постоянной нагрузки на сервере БД.

					18.03.2015 (1.23) Добавлен новая переменная @StandBy_File - для поддержки режима STANDBY (read-only) для восстанавливаемой БД.
					Это полный путь к файлу отката standby. Соответственно в таблице sputnik.lse.TargetConfig должен быть новый столбец StandBy_File.

					21.10.2015 (1.25) Добавлена дополнительная проверка и защита - если файл бэкапа не обнаружен в каталоге Копий, то 
					вызывается его повторное копирование.

					15.12.2015 (1.27) При накате логов, если происходит ошибка нужно вернуть базу в режим MULTI_USER!
					Также в этот режим база возвращается в самом конце успешного восстановления.
					Чтобы не было включение MULTI_USER после каждого наката лога используется параметр @NoSetMultiUser.

					25.07.2016 (1.28) Новый параметр @MoveLogFilesTo - теперь файлы логов можно расположить на отдельным диске.

					10.09.2016 (1.29) Исправлен алгоритм оптимизации при включении параметра Standby - теперь этот параметр
					будет включен только при накате последнего бэкапа лога. Также исправлен алгоритм при передаче параметра
					@NoSetMultiUser.

					12.03.2018 (1.305) 
					1.Параметр @Maxi: значение по умолчанию увеличено до 15. 
					2.Для определения правильного имени сервера MSSQL теперь используется процедура info.usp_getHostname		
	-- ============================================= */
	CREATE PROCEDURE [lse].[usp_RunRolling]  
		@ConfigID int,
		@BackupID int,
		@MoveFilesTo nvarchar(800),
		@MoveLogFilesTo nvarchar(800),
		@pp bit = 0,
		@Maxi smallint=15
	AS
		SET NOCOUNT ON;
		DECLARE @LocalServer NVARCHAR(510);
		exec sputnik.info.usp_GetHostname @Servername=@LocalServer OUT;
		--Получаем настройки для конкретной целевой БД из таблицы настроек lse
		declare @ServerSource nvarchar(300), @DBNameSource nvarchar(300), @DBNameTarget nvarchar(300), @FromCopy bit, @CatalogFilesDB nvarchar(800), @CatalogLogFiles nvarchar(800), @StandBy_File nvarchar(500)=null,@user_access_desc nvarchar(50), @state_desc nvarchar(100);
		select  @ServerSource=ServerSource, @DBNameSource=DBNameSource, @DBNameTarget=DBNameTarget, @FromCopy=FromCopy, @CatalogFilesDB=CatalogFilesDB, @CatalogLogFiles=CatalogLogFiles, @StandBy_File=StandBy_File
		from sputnik.lse.TargetConfig 
		where ID=@ConfigID AND [Suspend]=0 AND [InitDate] IS NOT NULL;

		--Получаем цепочку бэкапов логов для наката!
		DECLARE @ChainBack TABLE (BackupFile NVARCHAR(800), BackupType VARCHAR(4), ID INT, BackupDate DATETIME2(2));	
		IF @ServerSource IS NULL OR @ServerSource=@LocalServer
			INSERT INTO @ChainBack (BackupFile, BackupType, ID, BackupDate)
				EXEC sputnik.info.usp_GetChainLogs @DBName=@DBNameSource, @BackupFullID=null, @FilterBackupID=@BackupID, @ToDate=null, @fromcopy=@fromcopy;
		ELSE
		BEGIN
			DECLARE @strP VARCHAR(300);
			SET @strP='@ToDate=null, @BackupFullID=null, @FilterBackupID='+CAST(@BackupID AS NVARCHAR(40))+', @fromcopy='+CAST(@fromcopy AS VARCHAR(1));
			INSERT INTO @ChainBack (BackupFile, BackupType, ID, BackupDate)
				EXEC ('	SELECT *
						FROM OPENQUERY(['+@ServerSource+'], '' EXEC sputnik.info.usp_GetChainLogs @DBName=N'''''+@DBNameSource+''''','+@strP+';'')
					 ');
		END
		--Восстановление из полученной цепочки бэкапов.
		declare @str nvarchar(800);
		DECLARE @BF nvarchar(800), @BT varchar(4), @RestoreStart datetime2(2), @BackupLogID int, @CheckFile bit, @BackupDate datetime2(2);
		DECLARE RE CURSOR FOR
			SELECT TOP (@Maxi) BackupFile, BackupType, ID, sputnik.info.uf_CheckFile(BackupFile) as CheckFile, BackupDate
			FROM @ChainBack
			--ORDER BY ID;
		OPEN RE;
		declare @cur_cnt int, @cur_i int = 1;
		SET @cur_cnt=@@CURSOR_ROWS;
		FETCH NEXT FROM RE INTO @BF, @BT, @BackupLogID, @CheckFile, @BackupDate;
		WHILE @@FETCH_STATUS=0
		BEGIN
		
			if @CheckFile=0
			begin
				if @FromCopy=1
				begin
					begin try
						SET @strP='@DBFilter=N'''''+@DBNameSource+''''', @FilterBackupID='+CAST(@BackupID AS NVARCHAR(40))+', @Force=1';
						EXEC ('
							  EXEC(''
									EXEC sputnik.backups.usp_CopyBack '+@strP+';
							  '') AT ['+@ServerSource+']
						');
					end try
					begin catch
						set @str=N'Ошибка в ХП [usp_RunRolling]: не удалось скопировать файл бэкапа '+@BF+' (ИД='+CAST(@BackupID AS NVARCHAR(40))+')! Возможно соединение было сброшено!';
						if @pp=0
							PRINT(@str);
						RAISERROR(@str,11,1) WITH LOG
					end catch
				end
				else
				begin
					set @str=N'Ошибка в ХП [usp_RunRolling]: не обнаружен файл бэкапа '+@BF+' ! Возможно бэкап был удален или у SQL Server нет доступа!';
					if @pp=0
						PRINT(@str);
					RAISERROR(@str,11,1) WITH LOG
				end				
			end
			begin try
				set @RestoreStart=sysdatetime();
				IF @cur_i<@cur_cnt
					EXEC [sputnik].[backups].[usp_RestoreDB_simple] 
						@DBNameTarget=@DBNameTarget, 
						@FromLog=@BF,
						@MoveFilesTo=@CatalogFilesDB,
						@MoveLogFilesTo=@CatalogLogFiles,
						@NoRecovery=1,
						@NoFileList=1,
						@NoStats=@pp
						--,@StandBy_File=@StandBy_File
						,@NoSetMultiUser=1
					;
				ELSE
					EXEC [sputnik].[backups].[usp_RestoreDB_simple] 
						@DBNameTarget=@DBNameTarget, 
						@FromLog=@BF,
						@MoveFilesTo=@CatalogFilesDB,
						@MoveLogFilesTo=@CatalogLogFiles,
						@NoRecovery=1,
						@NoFileList=1,
						@NoStats=@pp
						,@StandBy_File=@StandBy_File
						,@NoSetMultiUser=1
					;
				insert into sputnik.lse.HS (config_id, BackupHS_id, StartRestore, CompleteRestore)
				values(@ConfigID, @BackupLogID, @RestoreStart, sysdatetime());
			end try
			begin catch
				--В случае Ошибки при восстановлении бэкапа лога, нужно вернуть базу в состояние MULTI_USER
				--Или в режим StandBy
				select top 1 @user_access_desc=user_access_desc, @state_desc=state_desc 
				from sys.databases
				where name=@DBNameTarget;
				IF @user_access_desc='SINGLE_USER' AND @state_desc='ONLINE'
					EXEC('ALTER DATABASE ['+@DBNameTarget+'] SET MULTI_USER;');
				--IF @state_desc<>'ONLINE' AND @StandBy_File IS NOT NULL
				--	EXEC [sputnik].[backups].[usp_RestoreDB_simple] 
				--		@DBNameTarget=@DBNameTarget, 
				--		@NoRecovery=1,
				--		@NoFileList=1,
				--		@NoStats=@pp,
				--		@StandBy_File=@StandBy_File,
				--		@ContinueRecovery=1;	
				set @str=N'Ошибка в ХП [usp_RunRolling]: при попытке накатить бэкап лога '+@BF+' на целевую БД ['+@DBNameTarget+']! Текст ошибки: '+ERROR_MESSAGE();
				if @pp=0
					PRINT(@str);
				RAISERROR(@str,11,1) WITH LOG;
			end catch
			set @cur_i+=1;
			FETCH NEXT FROM RE INTO @BF, @BT, @BackupLogID, @CheckFile, @BackupDate;
		END
		close RE;
		deallocate RE;
		--В самом конце вернем базу в MULTI_USER...Или в режим StandBy
		select top 1 @user_access_desc=user_access_desc, @state_desc=state_desc 
		from sys.databases
		where name=@DBNameTarget;
		IF @user_access_desc='SINGLE_USER' AND @state_desc='ONLINE'
			EXEC('ALTER DATABASE ['+@DBNameTarget+'] SET MULTI_USER;');