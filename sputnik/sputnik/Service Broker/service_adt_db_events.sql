CREATE SERVICE [service_adt_db_events]
    AUTHORIZATION [dbo]
    ON QUEUE [adt].[event_notification_q]
    ([http://schemas.microsoft.com/SQL/Notifications/PostEventNotification]);

