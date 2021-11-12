/* =============================================
-- Author:		Андрей Иванов (sqland1c)
-- Create date: 17.04.2018 (1.000)
-- Description: Эта процедура используется как процедура-обработчик для очереди event_notification_q!
				Обрабатываются нужные события event_notifications для реализации различного аудита!

-- Update:		25.05.2018 (1.020)
				Добавлена проверка перед отправкой уведомлений по почте - если запись о такой же БД
				уже есть в таблице аудита, значит не будем отправлять уведомление.

				14.11.2018 (1.030)
				Теперь уведомление по почте происходят при событии drop database.
				
-- ============================================= */
create procedure [adt].[usp_eventnots_processor]
as
	set nocount on;
	declare @DlgHandle UNIQUEIDENTIFIER, @Msg VARBINARY(MAX), @MsgType sysname, @StrErr NVARCHAR(3000), @Msgxml XML;
	declare @tt datetime2(2), @event_type nvarchar(128), @db nvarchar(2000), @login varchar(128), @spid smallint, @sqltext nvarchar(max), @textdata nvarchar(2000), @host nvarchar(128)=null, @program nvarchar(128)=null;
	while (1=1)
	begin
		WAITFOR (
			RECEIVE TOP(1)
				@DlgHandle = [conversation_handle],
				@MsgType = message_type_name
			,	@Msg = message_body                    
			FROM	event_notification_q                 
		), TIMEOUT 1000
		IF (@@ROWCOUNT = 0)
		BEGIN
			--IF EXISTS(SELECT conversation_id FROM sys.conversation_endpoints WHERE conversation_handle=@DlgHandle AND state<>'CD')
			--	END CONVERSATION @DlgHandle;
			BREAK;
		END
		if (@MsgType='http://schemas.microsoft.com/SQL/Notifications/EventNotification')
		begin
			begin try
				set @Msgxml=cast(@Msg as XML);
				select 
					@tt=@Msgxml.value('(/EVENT_INSTANCE/PostTime)[1]', 'datetime2(2)'),
					@event_type=@Msgxml.value('(/EVENT_INSTANCE/EventType)[1]', 'nvarchar(128)'),
					@db=@Msgxml.value('(/EVENT_INSTANCE/DatabaseName)[1]', 'nvarchar(2000)'),
					@login=@Msgxml.value('(/EVENT_INSTANCE/LoginName)[1]', 'nvarchar(128)'),
					@spid=@Msgxml.value('(/EVENT_INSTANCE/SPID)[1]', 'smallint');
				select
					@sqltext=CASE WHEN @event_type in ('CREATE_DATABASE', 'ALTER_DATABASE', 'DROP_DATABASE') then @Msgxml.value('(/EVENT_INSTANCE/TSQLCommand/CommandText)[1]','nvarchar(max)') else @Msgxml.value('(/EVENT_INSTANCE/TextData)[1]','nvarchar(max)') END,
					@textdata=CASE WHEN @event_type in ('CREATE_DATABASE', 'ALTER_DATABASE', 'DROP_DATABASE') then @Msgxml.value('(/EVENT_INSTANCE/TextData)[1]', 'nvarchar(2000)') WHEN @event_type='AUDIT_BACKUP_RESTORE_EVENT' THEN CASE WHEN @Msgxml.value('(/EVENT_INSTANCE/Success)[1]','smallint') <> 1 THEN 'not completed' ELSE NULL END END ;
				if @event_type in ('CREATE_DATABASE', 'ALTER_DATABASE', 'DROP_DATABASE')
				begin
					if datediff(minute,@tt,sysdatetime())<=15
					begin
						select @host=[host_name], @program=[program_name] from sys.dm_exec_sessions where session_id=@spid and login_name=@login and login_time<=@tt;
					end
					else
						print('Невозможно определить host и program: прошло уже более 15 минут!');
				end
				else if @event_type='AUDIT_BACKUP_RESTORE_EVENT'
				begin
					select @host=@Msgxml.value('(/EVENT_INSTANCE/HostName)[1]', 'nvarchar(128)'),
						@program=@Msgxml.value('(/EVENT_INSTANCE/ApplicationName)[1]', 'nvarchar(128)')						
				end
				
				if (@event_type<>'AUDIT_BACKUP_RESTORE_EVENT' or @sqltext like 'restore%database%')
				begin
					declare @pr_event nvarchar(128);
					select @pr_event = CASE WHEN @event_type='AUDIT_BACKUP_RESTORE_EVENT' then 'restore_database' else @event_type end;
					print('event: '+@pr_event+'. database ['+@db+']. login: '+coalesce(@login,'')+' ; host: '+coalesce(@host,'')+' ; program: '+coalesce(@program,'')+' ; spid: '+coalesce(cast(@spid as varchar(10)),''));
				end

				--Проверка существования такой же БД: если есть записи в аудите, значит эта БД уже была и НЕ будем отправлять уведомление!
				declare @last_tt datetime2(2); 
				SELECT @last_tt=max(tt)
				FROM [sputnik].[adt].[instance_hs]
				WHERE tt between dateadd(hour,-24,@tt) and @tt 
					and db=@db;

				insert into sputnik.adt.instance_hs (tt,event_type,db,spid,[login],[host],program,sqltext,textdata)
				values(@tt,@event_type,@db,@spid,@login,@host,@program,@sqltext,@textdata);

				if (@event_type='CREATE_DATABASE' or (@event_type='AUDIT_BACKUP_RESTORE_EVENT' and @sqltext like 'restore%database%')) and @last_tt is null
					exec [sputnik].[adt].[usp_mail_eventdb] @dbname=@db, @tt=@tt, @login=@login, @hostname=@host, @program=@program, @sqlcommand=@sqltext;

				if (@event_type='DROP_DATABASE')
					exec [sputnik].[adt].[usp_mail_eventdb] @dbname=@db, @tt=@tt, @login=@login, @hostname=@host, @program=@program, @sqlcommand=@sqltext, @dropdb=1;

				set @host=null;set @program=null;
			end try
			begin catch
				SET @StrErr=N'Ошибка при обработке очереди event_notification_q в потоке [adt].[usp_eventnots_processor]. Имя БД: ['+@db+']; Тип события: ['+@event_type+'], время события(tt): ['+convert(varchar(30),@tt,120)+']. Текст ошибки: '+ERROR_MESSAGE();
				--В этом случае не будем заканчивать Диалог принудительно, чтобы другие бэкапы были выполнены до конца!
				--Просто пишем ошибку с подробностями в журнал SQL Server (через print) и заканчиваем выполнения (break):
				PRINT(@StrErr);
				BREAK;
				--END CONVERSATION @DlgHandle WITH ERROR = 10 DESCRIPTION=@StrErr;
			end catch
		end
		----Если в сообщении КонецДиалога или Ошибка, тогда закрыть диалог!
		--ELSE IF @MsgType=N'http://schemas.microsoft.com/SQL/ServiceBroker/EndDialog'
		--	END CONVERSATION @DlgHandle;
		--ELSE IF @MsgType=N'http://schemas.microsoft.com/SQL/ServiceBroker/Error'
		--	END CONVERSATION @DlgHandle;
	end