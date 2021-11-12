
/* =============================================
-- Author:		Андрей Иванов (sqland1c)
-- Create date: 06.08.2014 (1.0)
-- Description: Эта процедура используется как процедура активации для целевой очереди!
				Предназначена для организации многопоточной обработки различных модулей (в зависимости от типа сообщения).
-- Update:		18.01.2015 (1.1)
				Добавлена возможность резервного копирования в многопоточном режиме (схема backups).
				18.01.2015 (1.11)
				Небольшое но важное исправление в алгоритме обработки исключений при работе над сообщениями backups-
				удалена команда завершения диалога (end conversation), чтобы другие бэкапы могли быть выполнены!!
				18.01.2015 (1.12)
				В алгоритме обработки исключений пр работе над сообщениями backups - 
				добавлена команда выхода из цикла (break), чтобы в случае ошибки остальные бэкапы могли 
				быть обработать в других потоках.
				31.03.2016 (1.14)
				Добавлен параметр @execute=1 для вызова [usp_RollForwardRecovery] - этот параметр говорит
				что выполняется обработчик очереди и будет выполнятся накат бэкапов на целевую базу lse.
-- ============================================= */
CREATE PROCEDURE [pp].[usp_ExecProcessor]
AS
	SET NOCOUNT ON;
	DECLARE @DlgHandle UNIQUEIDENTIFIER, @Msg VARBINARY(MAX), @MsgType sysname, @StrErr NVARCHAR(3000),@RunningBackupCount smallint;
	WHILE (1=1)
	BEGIN
		 DECLARE @DB NVARCHAR(300);
		 
		 WAITFOR
			(	RECEIVE TOP(1)
					@DlgHandle = conversation_handle,
					@MsgType   = message_type_name,
					@Msg	   = message_body
				FROM [pp].ProcessorQueue
		), TIMEOUT 1000;		--делаем небольшое ожидание 1 сек. нового сообщения.
		IF (@@ROWCOUNT = 0)
		BEGIN
			IF EXISTS(SELECT conversation_id FROM sys.conversation_endpoints WHERE conversation_handle=@DlgHandle AND state<>'CD')
				END CONVERSATION @DlgHandle;
			BREAK;
		END
		--Запуск модуля lse в многопоточном режиме!
		IF @MsgType='pp_lse_Request'
		BEGIN
			begin try
				--В самом сообщении должна быть База данных для обработки (в формате XML).
				SET	@DB = CAST(@Msg as XML).value('(/DB)[1]', 'NVARCHAR(300)');
				--Теперь запускаем процедуру [lse].[usp_RollForwardRecovery]
				--Для этой процедуры должен быть новый параметр @pp=1 (который определяет запуск в многопоточном режиме)
				--А также специальный флаг @execute=1
				EXEC [sputnik].[lse].[usp_RollForwardRecovery] @DBName=@DB, @pp=1, @execute=1;
			end try
			begin catch
				SET @StrErr=N'Ошибка при запуске [lse].[usp_RollForwardRecovery] в потоке [pp].usp_ExecProcessor. Текст ошибки: '+ERROR_MESSAGE();
				END CONVERSATION @DlgHandle WITH ERROR = 10 DESCRIPTION=@StrErr;
			end catch
		END
		--Запуск модуля backups в многопоточном режиме!
		IF @MsgType='pp_back_Request'
		BEGIN
			begin try
				DECLARE @TypeBack VARCHAR(4); --тип бэкапа!
				--В самом сообщении должна быть имя БД и Тип Бэкапа для обработки (в формате XML).
				SET	@DB = CAST(@Msg as XML).value('(/backups/db)[1]', 'NVARCHAR(300)');
				SET	@TypeBack = CAST(@Msg as XML).value('(/backups/type)[1]', 'VARCHAR(4)');
				--Теперь запускаем процедуру выполнения бэкапов [usp_StartBackup] для конкретной БД
				--с параметром @pp=1 - указывает что работа идёт в многопоточном режиме! 
				EXEC [sputnik].[backups].[usp_StartBackup] @DBFilter=@DB, @type=@TypeBack, @pp=1;
			end try
			begin catch
				SET @StrErr=N'Ошибка при запуске [backups].[usp_StartBackup] в потоке [pp].usp_ExecProcessor. Имя БД: ['+@DB+']; Тип бэкапа: ['+@TypeBack+']. Текст ошибки: '+ERROR_MESSAGE();
				--В этом случае не будем заканчивать Диалог принудительно, чтобы другие бэкапы были выполнены до конца!
				--Просто пишем ошибку с подробностями в журнал SQL Server (через print) и заканчиваем выполнения (break):
				PRINT(@StrErr);
				BREAK;
				--END CONVERSATION @DlgHandle WITH ERROR = 10 DESCRIPTION=@StrErr;
			end catch
		END
		--Если в сообщении КонецДиалога или Ошибка, тогда закрыть диалог!
		ELSE IF @MsgType=N'http://schemas.microsoft.com/SQL/ServiceBroker/EndDialog'
			END CONVERSATION @DlgHandle;
		ELSE IF @MsgType=N'http://schemas.microsoft.com/SQL/ServiceBroker/Error'
			END CONVERSATION @DlgHandle;
	END