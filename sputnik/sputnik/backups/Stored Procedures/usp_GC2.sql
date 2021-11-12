
	/* =============================================
	-- Author:		Андрей Иванов (sqland1c)
	-- Create date: 04.08.2014 (2.0)
	-- Description: usp_GC2 - GetCopy (Новая!). Эта процедура позволяет поднять копию базы на локальном сервере на указанный момент времени.
					В новой версии появилась возможность поднимать копию Базы с удалённого сервера источника!! Кроме этого, в новой версии теперь используются Дифф. бэкапы!
					При необходимости создается новый бэкап (полный или дифф.) чтобы быстрее получиться копию БД на последний момент времени!

					В основе - восстановление по процедуре usp_RestoreDB_simple (восстановление из Полного бэкапа, Дифф. бэкапа и из бэкапов Логов).
					Используется информация из базы sputnik, чтобы определить необходимую цепочку бэкапов для восстановления.
					То есть, для успешного выполнения вся необходимая информация должна быть в базе sputnik (то есть резервное копирование
					для боевой БД должно быть настроено в базе sputnik).
					Параметры:
						@DBNameSource - обязательный параметр, это имя базы источника (откуда делаем копию данных);
						@DBNameTarget - необязательный параметр, это имя базы назначения (куда загружам данные). Если не задан, то имя будет
						сформировано автоматически по следующими принципу: @DBNameTarget + дата и время последнего загруженого бэкапа.
						@ServerSource - необязательный параметр, это имя сервера, где находится боевая БД, из которой нужно сделать копию!
						Должно быть настроено Linked Server для успешной работы этого механизма! Если этот параметр НЕ задан, предполагается
						что база данных источник и целевая база данных находятся на одном и том же сервере!
						@ToDate - необязательный параметр. Это дата и время, на которое нужно сделать копию данных. Если не задан, то текущее
						системное время.
						@FromCopy - указывает, что нужно использовать каталог с копиями бэкапов (NetDir), а не с самими бэкапами (LocalDir).
						@RunNewBackIfNeed - Необходимость создания новых бэкапов Полный или Дифф. для ускорения восстановления БД на последний
						момент времени.
						@FreshBack - указывает требования к "свежести" Дифф. бэкапа. Если дифф. бэкап был создан более 8 часов назад, то будет создан
						новый дифф. бэкап (если задан разрешающий параметр @RunNewBackIfNeed).
						@lse - специальный параметр, определяет, что вызов этой процедуры произведен из модуля LSE (Log Shipping Easy). В этом случае,
						будет записана информация о последней восстановленном бэкапе!
	-- Update:
						05.08.2014 (2.1)
						Оптимизирован алгоритм восстановления через процедуру usp_RestoreDB_simple. Теперь после восстановления БД из полного бэкапа
						все последующие бэкапы должны восстанавливаться быстрее, т.к. используется новый параметр @NoFileList=1, который говорит
						о том, что все данные о файлах целевой БД будут получены из системного представления sys.master_files, а не из бэкапов.
					
						05.08.2014 (2.12) Добавлен новый параметр @RM для запуска из процедуры Восстановления/Настройки Зеркалирования.
						Если этот параметр задан, тогда дополнительно будет создан (и восстановлен) последний бэкап Лога! При этом настройки
						для бэкапов Логов будут включены и снова отключены!

						05.08.2014 (2.13) Небольшое исправление при формировании информации о Дифф. бэкапе - добавлены дополительные проверки,
						чтобы определить есть ли Дифф. бэкап вообще.

						06.08.2014 (2.15) Изменен алгоритм включения/выключения настройки Бэкапов логов на удалённом сервере, если используется 
						параметр @RM. Теперь SELECT и UPDATE выполняются в двух разных инструкциях OPENQUERY. При этом на время обновления 
						устанавливается параметр XACT_ABORT=ON (нужен для выполнения UPDATE в OPENQUERY).

						07.08.2014 (2.17) Исправлена ошибка при формировании полного бэкапа (если его нет) на удаленном сервере!
						Также добавлен новый параметр @pp - если задан, значит процесс запущен в многопоточном режиме через модуль pp.
						В этом случае нужно выводить как можно меньше сообщений пользователю!

						13.11.2014 (2.18) Во все вызовы модуля [usp_RunBack] добавлен параметр @ForceCopy - чтобы после создания бэкапа
						сразу было произведено его КОПИРОВАНИЕ (теперь через новый модуль).

						18.03.2015 (2.20) Добавлен новый параметр @StandBy_File - для поддержки режима STANDBY (read-only) для восстанавливаемой БД. Может применяться
						в механизме lse.

						20.11.2015 (2.21) Добавлен новый параметр @MoveLogFilesTo, теперь Log-файлы можно расположить на отдельном диске.
						Этот параметр уже поддерживается в процедуре [usp_RestoreDB_simple].

						12.01.2016 (2.22) Небольшое исправление в связи с изменением зависимой процедуры usp_GetLastBackups

						28.02.2017 (2.230) Новый параметр @dbowner - теперь можно указать владельца новой базы (по умолчанию sa).

						10.11.2017 (2.233) Выполнены оптимизации в алгоритмах формирования новых бэкапов:
						Изменены требования к свежести полного и/или дифф. бэкапов: c 2 до 12 часов (полный), с 2 до 8 часов (дифф.)!
						Также внесены исправления в формирование настроек по дифф.бэкапу (если таких настроек ещё нет):
						теперь будет хранится 1 файл (политика ротации 1-по количеству файлов).
						Также сразу после создания Дифф.бэкапа добавлен запуск Ротации по Дифф.бэкапам, чтобы не засорять Дифф.бэкапами весь диск!

						10.11.2017 (2.234) Добавлен новый параметр @RunNewDiffBackIfNeed - разрешение на выполнение Дифф.бэкапа.
						Разрешение на выполнение полного бэкапа даёт уже существующий параметр @RunNewBackIfNeed
	-- ============================================= */
	CREATE PROCEDURE [backups].[usp_GC2]  
		@DBNameSource nvarchar(300),
		@DBNameTarget nvarchar(300)=NULL,
		@ServerSource nvarchar(300)=NULL,
		@ToDate datetime2(2)=NULL,
		@FromCopy bit=0,
		@RunNewBackIfNeed bit=0,
		@FreshBack bit=0,
		@NoRecovery bit=0,
		@MoveFilesTo nvarchar(500)=NULL,
		@MoveLogFilesTo nvarchar(500)=NULL,
		@lse bit=0,
		@RM bit=0,
		@pp bit=0,
		@StandBy_File nvarchar(500)=null,
		@dbowner nvarchar(50)='sa',
		@RunNewDiffBackIfNeed bit=0
	AS
	BEGIN
		SET NOCOUNT ON;
		DECLARE @LocalServer NVARCHAR(500)=CAST(SERVERPROPERTY('ServerName') AS NVARCHAR(500));
		DECLARE @lseInitDate datetime2(2)=sysdatetime();
		DECLARE @Dir NVARCHAR(500), @File NVARCHAR(300), @FullPath NVARCHAR(800),@DiffPath NVARCHAR(800), @BackupFullID INT, @BackupDiffID int, @BackupFullFinishDate DATETIME2(2), @BackupDiffFinishDate DATETIME2(2), @NetDir NVARCHAR(500);
		DECLARE @WeekDay tinyint, @MonthDay tinyint, @NewFB bit=0, @NewConf bit=0, @NewDB bit=0;
		DECLARE @OldBackupHH smallint;
		DECLARE @pToDate NVARCHAR(75);
		DECLARE @StrErr NVARCHAR(900);
		IF @ToDate IS NULL
			SET @pToDate='NULL';
		ELSE
			SET @pToDate=''''''+CONVERT(NVARCHAR(75),@ToDate,121)+'''''';
		--Получаем всю информацию о последних полном и дифф. (если есть) бэкаках (по информации из базы sputnik на боевом сервере).
		DECLARE @TT TABLE ([DB_name] NVARCHAR(400), Backup_Type VARCHAR(4), BackupFile NVARCHAR(500), ID INT, BackupDate DATETIME2(2), LocalDir NVARCHAR(500), NetDir NVARCHAR(500), CheckLocalDir BIT, CheckNetDir BIT, CheckLocalFile BIT, CheckLocalFileOnly BIT, CheckNetFile BIT);
		IF @ServerSource IS NULL OR @ServerSource=@LocalServer
			INSERT INTO @TT
				EXEC sputnik.info.usp_GetLastBackups @DBName=@DBNameSource, @ToDate=@ToDate;
		ELSE
			INSERT INTO @TT
				EXEC ('	SELECT *
						FROM OPENQUERY(['+@ServerSource+'], '' EXEC sputnik.info.usp_GetLastBackups @DBName=N'''''+@DBNameSource+''''', @ToDate='+@pToDate+';'')
					 ');
		SELECT @BackupFullID=ID, @BackupFullFinishDate=BackupDate,  
			@FullPath=case when @FromCopy=0 OR NetDir is null OR NetDir='' then LocalDir+BackupFile else NetDir+BackupFile end
		FROM @TT
		WHERE Backup_Type='Full'
			AND (((@FromCopy=0 OR NetDir is null OR NetDir='') AND CheckLocalFile=1) OR ((@FromCopy=1 AND CheckNetFile=1)));

		--Если полного бэкапа нет, тогда нужно его сделать (если включен разрешающий параметр)!!
		IF @FullPath IS NULL AND @RunNewBackIfNeed=1 AND @ToDate IS NULL
			begin try	
				IF @ServerSource IS NULL OR @ServerSource=@LocalServer
					EXEC sputnik.backups.usp_RunBack @DBName_in=@DBNameSource, @TypeBack='Full', @OnlyFull=1, @ForceCopy=1;
				ELSE
					EXEC ('
						  EXEC(''
								EXEC sputnik.backups.usp_RunBack @DBName_in=N'''''+@DBNameSource+''''', @TypeBack=''''Full'''', @OnlyFull=1, @ForceCopy=1;
						  '') AT ['+@ServerSource+']
					');
				SET @NewFB=1;	
			end try
			begin catch
				SET @StrErr=N'Ошибка при попытке выполнить полный бэкап в ХП [usp_GC2]! Текст ошибки: '+ERROR_MESSAGE();
				RAISERROR(@StrErr,11,1) WITH LOG
			end catch
		--Если полный бэкап уже есть, тогда попробуем найти бэкап DIFF, если такого нет, то нужно его сделать (если включен разрешающий параметр). 
		ELSE IF @FullPath IS NOT NULL
		BEGIN
			--Обработку дифф. бэкапа производится только, если Полный бэкап старше 12 часов! Иначе быстрее восстановить из полного без создания Дифф!
			IF @ToDate IS NULL
				SET @OldBackupHH = DATEDIFF(MINUTE,@BackupFullFinishDate,SYSDATETIME());
			ELSE
				SET @OldBackupHH = DATEDIFF(MINUTE,@BackupFullFinishDate,@ToDate);
			IF @OldBackupHH>12*60
			BEGIN
				SELECT @BackupDiffID=ID, @BackupDiffFinishDate=BackupDate,  
				@DiffPath=case when @FromCopy=0 OR NetDir is null OR NetDir='' then LocalDir+BackupFile else NetDir+BackupFile end
				FROM @TT
				WHERE Backup_Type='Diff' AND BackupDate > @BackupFullFinishDate
					AND (((@FromCopy=0 OR NetDir is null OR NetDir='') AND CheckLocalFile=1) OR ((@FromCopy=1 AND CheckNetFile=1)))
					AND (@FreshBack=0 OR DATEDIFF(MINUTE,BackupDate,SYSDATETIME())<8*60)
				IF @DiffPath IS NULL AND @RunNewDiffBackIfNeed=1 AND @ToDate IS NULL
					begin try	
						--если настроек по Diff нет, тогда временно создадим их!
						DECLARE @TTD TABLE (DB NVARCHAR(300));
						IF @ServerSource IS NULL OR @ServerSource=@LocalServer
							INSERT INTO @TTD
							SELECT [DBName] AS DB FROM sputnik.backups.BackConf WHERE Kind='Diff' AND DBName=@DBNameSource;
						ELSE
							INSERT INTO @TTD
							EXEC('SELECT [DBName] AS DB
								  FROM OPENQUERY(['+@ServerSource+'], ''SELECT [DBName] FROM sputnik.backups.BackConf WHERE Kind=''''Diff'''' AND DBName=N'''''+@DBNameSource+''''';'')
								  ');
						IF NOT EXISTS(SELECT DB FROM @TTD)
						BEGIN
							IF @ServerSource IS NULL OR @ServerSource=@LocalServer
								INSERT INTO sputnik.backups.BackConf ([DBName], LocalDir, NetDir, LocalDays, NetDays, Kind, [LocalPolicy],[NetPolicy])
								SELECT [DBName], LocalDir, NetDir, 1, 1, 'Diff' AS Kind, 1, 1
								FROM sputnik.backups.BackConf
								WHERE  Kind='Full' AND DBName=@DBNameSource
							ELSE
								EXEC ('
									EXEC(''
										INSERT INTO sputnik.backups.BackConf ([DBName], LocalDir, NetDir, LocalDays, NetDays, Kind, [LocalPolicy],[NetPolicy])
										SELECT [DBName], LocalDir, NetDir, 1, 1, ''''Diff'''' AS Kind, 1, 1
										FROM sputnik.backups.BackConf
										WHERE  Kind=''''Full'''' AND DBName=N'''''+@DBNameSource+''''';
									'') AT ['+@ServerSource+']
								');
							SET @NewConf=1;
						END
						--Делаем Дифф. Бэкап Боевой базы на локальном ИЛИ удалённом сервере:
						--При этом сразу после создания Бэкапа запустим Ротацию по Дифф.бэкапам, чтобы не занять весь диск!
						IF @ServerSource IS NULL OR @ServerSource=@LocalServer
						BEGIN
							EXEC sputnik.backups.usp_RunBack @DBName_in=@DBNameSource, @TypeBack='Diff', @ForceCopy=1;
							EXEC sputnik.backups.[usp_CleaningBack] @DBFilter=@DBNameSource, @type='Diff';
						END
						ELSE
							EXEC ('
								EXEC(''
									EXEC sputnik.backups.usp_RunBack @DBName_in=N'''''+@DBNameSource+''''', @TypeBack=''''Diff'''', @ForceCopy=1;
									EXEC sputnik.backups.[usp_CleaningBack] @DBFilter=N'''''+@DBNameSource+''''', @type=''''Diff'''';
								'') AT ['+@ServerSource+']
							');
						SET @NewDB=1;	
					end try
					begin catch
						SET @StrErr=N'Ошибка при попытке сформировать Дифф. бэкап в ХП [usp_GC2]! Текст ошибки: '+ERROR_MESSAGE();
						RAISERROR(@StrErr,11,1) WITH LOG
					end catch
			END
		END
		--Проверяем, если Полный или Дифф. бэкап создавались заново, нужно снова получить инфу о последних бэкапах!
		IF @NewDB=1 OR @NewFB=1
		BEGIN
			DELETE FROM @TT;
			IF @ServerSource IS NULL OR @ServerSource=@LocalServer
				INSERT INTO @TT
					EXEC sputnik.info.usp_GetLastBackups @DBName=@DBNameSource, @ToDate=@ToDate;
			ELSE
				INSERT INTO @TT
					EXEC ('	SELECT *
							FROM OPENQUERY(['+@ServerSource+'], ''EXEC sputnik.info.usp_GetLastBackups @DBName=N'''''+@DBNameSource+''''', @ToDate='+@pToDate+';'')
						');
			IF @NewFB=1
			BEGIN
				SELECT @File=BackupFile, @BackupFullID=ID, @BackupFullFinishDate=BackupDate,  
					@Dir=case when  @FromCopy=0 OR NetDir is null OR NetDir='' then LocalDir else NetDir end
				FROM @TT
				WHERE Backup_Type='Full'
					AND (((@FromCopy=0 OR NetDir is null OR NetDir='') AND CheckLocalFile=1) OR ((@FromCopy=1 AND CheckNetFile=1)));
				SET @FullPath=@Dir+@File;
			END
			IF @NewDB=1
			BEGIN 
				SELECT @File=BackupFile, @BackupDiffID=ID, @BackupDiffFinishDate=BackupDate,  
				@Dir=case when @FromCopy=0 OR NetDir is null OR NetDir='' then LocalDir else NetDir end
				FROM @TT
				WHERE Backup_Type='Diff' AND BackupDate > @BackupFullFinishDate
				AND (((@FromCopy=0 OR NetDir is null OR NetDir='') AND CheckLocalFile=1) OR ((@FromCopy=1 AND CheckNetFile=1)));
				IF @@ROWCOUNT>0
					SET @DiffPath=@Dir+@File;
				ELSE
					SET @DiffPath=NULL;
			END
		END			
	
		IF @FullPath IS NULL
			return 0;

		--Если выполняется Восстановление/Настройка Зеркалирования, нужно обязательно создать последний бэкап Лога!
		IF @RM=1
		BEGIN
			DECLARE @LogOff TABLE(DBName nvarchar(300));
			IF @ServerSource IS NULL OR @ServerSource=@LocalServer
			BEGIN
				begin try
					UPDATE [sputnik].[backups].[BackConf]
					SET [Kind]='Log'
					OUTPUT inserted.[DBName] into @LogOff
					WHERE [DBName]=@DBNameSource AND [Kind]='XLog';
					EXEC sputnik.backups.usp_RunBack @DBName_in=@DBNameSource, @TypeBack='Log', @ForceCopy=1;
				end try
				begin catch
					SET @StrErr=N'Ошибка при попытке сформировать последний бэкап Лога в ХП [usp_GC2]! Текст ошибки: '+ERROR_MESSAGE();
					RAISERROR(@StrErr,11,1) WITH LOG
				end catch
			END
			ELSE
				begin try
					INSERT INTO @LogOff
					EXEC ('	SELECT DBName
							FROM OPENQUERY(['+@ServerSource+'], ''SELECT DBName 
																  FROM [sputnik].[backups].[BackConf] 
																  WHERE [DBName]=N'''''+@DBNameSource+''''' AND [Kind]=''''XLog'''';'')
						');
					IF EXISTS(SELECT DBname FROM @LogOff)
					BEGIN
						SET XACT_ABORT ON;
						EXEC ('	UPDATE OPENQUERY(['+@ServerSource+'], ''SELECT [Kind]
																		FROM [sputnik].[backups].[BackConf] 
																		WHERE [DBName]=N'''''+@DBNameSource+''''' AND [Kind]=''''XLog'''';'')
								SET [Kind]=''Log''
			 			');
						SET XACT_ABORT OFF;
					END	
					EXEC ('
						EXEC(''
							EXEC sputnik.backups.usp_RunBack @DBName_in=N'''''+@DBNameSource+''''', @TypeBack=''''Log'''', @ForceCopy=1;
						'') AT ['+@ServerSource+']
					');
				end try
				begin catch
					SET @StrErr=N'Ошибка при попытке сформировать последний бэкап Лога в ХП [usp_GC2]! Текст ошибки: '+ERROR_MESSAGE();
					RAISERROR(@StrErr,11,1) WITH LOG
				end catch
		END

		--Формируем цепочку бэкапов для восстановления
		DECLARE @ChainBack TABLE (BackupFile NVARCHAR(800), BackupType VARCHAR(4), ID INT, BackupDate DATETIME2(2));	
		INSERT INTO @ChainBack (BackupFile, BackupType, ID, BackupDate)
		VALUES (@FullPath, 'Full', @BackupFullID, @BackupFullFinishDate);
		IF @DiffPath IS NOT NULL
			INSERT INTO @ChainBack (BackupFile, BackupType, ID, BackupDate)
			VALUES (@DiffPath, 'Diff', @BackupDiffID, @BackupDiffFinishDate);
		IF @ServerSource IS NULL OR @ServerSource=@LocalServer
			INSERT INTO @ChainBack (BackupFile, BackupType, ID, BackupDate)
				EXEC sputnik.info.usp_GetChainLogs @DBName=@DBNameSource, @BackupFullID=@BackupFullID, @FilterBackupID=@BackupDiffID, @ToDate=@ToDate, @fromcopy=@fromcopy;
		ELSE
		BEGIN
			DECLARE @strP VARCHAR(300);
			IF @BackupDiffID IS NOT NULL
				SET @strP='@BackupFullID='+CAST(@BackupFullID AS VARCHAR(40))+', @FilterBackupID='+CAST(@BackupDiffID AS NVARCHAR(40))+', @fromcopy='+CAST(@fromcopy AS VARCHAR(1));
			ELSE
				SET @strP='@BackupFullID='+CAST(@BackupFullID AS VARCHAR(40))+', @FilterBackupID=NULL, @fromcopy='+CAST(@fromcopy AS VARCHAR(1));
			INSERT INTO @ChainBack (BackupFile, BackupType, ID, BackupDate)
				EXEC ('	SELECT *
						FROM OPENQUERY(['+@ServerSource+'] ,''EXEC sputnik.info.usp_GetChainLogs @DBName=N'''''+@DBNameSource+''''',  @ToDate='+@pToDate+', '+@strP+';'')
					 ');
		END
		--После формирования цепочки бэкапов, для режима восстановления/настройки Зеркалирования отключим настройки бэкапов Логов
		IF @RM=1
		BEGIN
			IF EXISTS(SELECT DBName FROM @LogOff)
				IF @ServerSource IS NULL OR @ServerSource=@LocalServer
					UPDATE [sputnik].[backups].[BackConf]
					SET [Kind]='XLog'
					WHERE [DBName]=@DBNameSource AND [Kind]='Log';	
				ELSE
				BEGIN 
					SET XACT_ABORT ON;
					EXEC ('	UPDATE OPENQUERY(['+@ServerSource+'], ''SELECT [Kind]
																	FROM [sputnik].[backups].[BackConf] 
																	WHERE [DBName]=N'''''+@DBNameSource+''''' AND [Kind]=''''Log'''';'')
							SET [Kind]=''XLog''
			 			');
					SET XACT_ABORT OFF;
				END
		END

		--Формирование имени новой базы (если не задано)
		IF @DBNameTarget IS NULL
		begin
			declare @BackupFinishDate Datetime2(2);
	
			SELECT @BackupFinishDate=MAX([BackupDate])
			FROM @ChainBack;

			SET @DBNameTarget=@DBNameSource+'_COPY_'+CONVERT(VARCHAR(8), @BackupFinishDate, 112)+'_'+REPLACE(CONVERT(VARCHAR(20), @BackupFinishDate, 108), ':', '');
		END
		--Восстановление из полученной цепочки бэкапов.
		DECLARE @BF nvarchar(800), @BT varchar(4);
		DECLARE RE CURSOR FOR
			SELECT BackupFile, BackupType
			FROM @ChainBack
			--ORDER BY ID;
		OPEN RE;
		FETCH NEXT FROM RE INTO @BF, @BT;
		WHILE @@FETCH_STATUS=0
		BEGIN
			begin try
			IF @BT='Full' 
				BEGIN
					if @pp=0
						PRINT ('********************************
							Восстановление ['+@DBNameTarget+'] из ПОЛНОГО Бэкапа: '+@BF);
					EXEC [sputnik].[backups].[usp_RestoreDB_simple] 
						@DBNameTarget=@DBNameTarget, 
						@FromDisk=@BF,
						@MoveFilesTo=@MoveFilesTo,
						@MoveLogFilesTo=@MoveLogFilesTo,
						@NoRecovery=1,
						@NoStats=@pp,
						@StandBy_File=@StandBy_File
						--,@dbowner=@dbowner
					;
				END
				ELSE IF @BT='Diff' 
				BEGIN
					if @pp=0
						PRINT ('********************************
							Восстановление ['+@DBNameTarget+'] из ДИФФ. Бэкапа: '+@BF);
					EXEC [sputnik].[backups].[usp_RestoreDB_simple] 
						@DBNameTarget=@DBNameTarget, 
						@FromDisk=@BF,
						@MoveFilesTo=@MoveFilesTo,
						@MoveLogFilesTo=@MoveLogFilesTo,
						@NoRecovery=1,
						@DiffBack=1,
						@NoFileList=1,
						@NoStats=@pp,
						@StandBy_File=@StandBy_File
						--,@dbowner=@dbowner
					;
				END
				ELSE IF @BT='Log' 
				BEGIN
					if @pp=0
						PRINT ('********************************
							Восстановление ['+@DBNameTarget+'] из Бэкапа ЛОГА (ЖТ): '+@BF);
					EXEC [sputnik].[backups].[usp_RestoreDB_simple] 
						@DBNameTarget=@DBNameTarget, 
						@FromLog=@BF,
						@MoveFilesTo=@MoveFilesTo,
						@MoveLogFilesTo=@MoveLogFilesTo,
						@NoRecovery=1,
						@NoFileList=1,
						@NoStats=@pp,
						@StandBy_File=@StandBy_File
						--,@dbowner=@dbowner
					;
				END
			end try 
			begin catch
				SET @StrErr=N'Ошибка в ХП [usp_GC2] при попытке восстановить БД ['+@DBNameTarget+'] из '+@BT+'-Бэкапа ('+@BF+') ! Текст ошибки: '+ERROR_MESSAGE();
				RAISERROR(@StrErr,11,1) WITH LOG
			end catch
			FETCH NEXT FROM RE INTO @BF, @BT;
		END
		CLOSE RE;
		DEALLOCATE RE;
		--Перевод базы данных в режим RECOVERY
		IF @NoRecovery=0
		begin
			if @pp=0
				PRINT ('********************************
					Перевод новой базы в режим ONLINE + изменение db_owner');
			EXEC [sputnik].[backups].[usp_RestoreDB_simple] 
				@DBNameTarget=@DBNameTarget, 
				@ForceRecovery=1,
				@dbowner=@dbowner;
		end;
		--Если запуск произведен из модуля Log Shipping Easy,
		--тогда установим ID для последнего восстановленного бэкапа в sputnik.lse.TargetConfig
		IF @lse=1
		BEGIN
			UPDATE sputnik.lse.TargetConfig
			SET [InitBackupHS_id]=(SELECT MAX(ID) FROM @ChainBack),
				[InitDate]=@lseInitDate
			WHERE [DBNameTarget]=@DBNameTarget;
		END

		--В самом конце делаем очистку от Дифф. бэкапов и удаляем настройки, 
		--только если они были созданы в этой же процедуре ИЛИ существует точно такая же настройка с предыдущего неудачного запуска!
		begin try
			DELETE @TTD;
			IF @ServerSource IS NULL OR @ServerSource=@LocalServer
				INSERT INTO @TTD
				SELECT DBName AS DB 
				FROM sputnik.backups.BackConf 
				WHERE DBName=@DBNameSource AND Kind='Diff' AND LocalDays=1 AND NetDays=1
					AND [LocalPolicy]=1 AND [NetPolicy]=1
					AND EXISTS (SELECT DBName AS DB FROM sputnik.backups.BackConf as BakF WHERE BakF.Kind='Full' AND BakF.DBName=@DBNameSource);
			ELSE
				INSERT INTO @TTD
				EXEC('SELECT [DBName] AS DB
					  FROM OPENQUERY(['+@ServerSource+'], ''SELECT [DBName] FROM sputnik.backups.BackConf WHERE Kind=''''Diff''''  AND LocalDays=1 AND NetDays=1 AND DBName=N'''''+@DBNameSource+''''' AND [LocalPolicy]=1 AND [NetPolicy]=1 AND EXISTS (SELECT DBName AS DB FROM sputnik.backups.BackConf as BakF WHERE BakF.Kind=''''Full'''' AND BakF.DBName=N'''''+@DBNameSource+''''');'')
					');
			IF @NewConf=1 OR EXISTS(SELECT DB FROM @TTD )
			BEGIN
				IF @ServerSource IS NULL OR @ServerSource=@LocalServer
				BEGIN	
					EXEC sputnik.backups.[usp_CleaningBack] @DBFilter=@DBNameSource, @type='Diff';
					DELETE 
					FROM sputnik.backups.BackConf
					WHERE DBName=@DBNameSource AND Kind='Diff';
					--Дополнительно удаляем из истории Бэкапов информацию о созданном Дифф. бэкапе
					DELETE [sputnik].[backups].[BackupHistory]
					WHERE [ID] = @BackupDiffID;
				END
				ELSE
				BEGIN
					DECLARE @BackupDiffIDstr nvarchar(50);
					set @BackupDiffIDstr=CAST(@BackupDiffID AS NVARCHAR(50));
					EXEC ('
							EXEC(''
									EXEC sputnik.backups.[usp_CleaningBack] @DBFilter=N'''''+@DBNameSource+''''', @type=''''Diff'''';
									DELETE 
									FROM sputnik.backups.BackConf
									WHERE DBName=N'''''+@DBNameSource+''''' AND Kind=''''Diff'''';
									--Дополнительно удаляем из истории Бэкапов информацию о созданном Дифф. бэкапе
									DELETE [sputnik].[backups].[BackupHistory]
									WHERE [ID] = '+@BackupDiffIDstr+';
							'') AT ['+@ServerSource+']
						');
				END
			END
		end try
		begin catch
			SET @StrErr=N'Ошибка в ХП [usp_GC2] при попытке удалить настройку и историю по Дифф. бэкапам! Текст ошибки: '+ERROR_MESSAGE();
			RAISERROR(@StrErr,11,1) WITH LOG
		end catch
		return 1;
	END