
/* =============================================
-- Author:		Андрей Иванов (sqland1c)
-- Create date: 07.08.2014 (1.0)
-- Description: 
-- Update:		Эта процедура используется как процедура активации для очереди инициатора!
				Здесь должны обрабатываться ответы (Тип сообщения pp_модуль_Reply) от целевой очереди (Processor)!
				...
				
-- ============================================= */
CREATE PROCEDURE [pp].[usp_CheckCommander]
AS
	SET NOCOUNT ON;
	DECLARE @DlgHandle UNIQUEIDENTIFIER, @Msg VARBINARY(MAX), @MsgType sysname, @StrErr NVARCHAR(3000);
	WHILE (1=1)
	BEGIN
		 WAITFOR
			(	RECEIVE TOP(1)
					@DlgHandle = conversation_handle,
					@MsgType   = message_type_name,
					@Msg	   = message_body
				FROM [pp].[CommanderQueue]
		), TIMEOUT 1000;		--делаем небольшое ожидание 1 сек. нового сообщения.
		IF (@@ROWCOUNT = 0)
		BEGIN
			IF EXISTS(SELECT conversation_id FROM sys.conversation_endpoints WHERE conversation_handle=@DlgHandle AND state<>'CD')
				END CONVERSATION @DlgHandle;
			BREAK;
		END
		--Если в сообщении КонецДиалога или Ошибка, тогда нужно закрыть диалог!
		IF @MsgType=N'http://schemas.microsoft.com/SQL/ServiceBroker/EndDialog'
			END CONVERSATION @DlgHandle;
		ELSE IF @MsgType=N'http://schemas.microsoft.com/SQL/ServiceBroker/Error'
			END CONVERSATION @DlgHandle;
	END