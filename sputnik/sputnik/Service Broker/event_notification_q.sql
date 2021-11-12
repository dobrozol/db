CREATE QUEUE [adt].[event_notification_q]
    WITH ACTIVATION (STATUS = ON, PROCEDURE_NAME = [adt].[usp_eventnots_processor], MAX_QUEUE_READERS = 1, EXECUTE AS OWNER);

