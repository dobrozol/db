
/* =============================================
-- Author:		Андрей Иванов (sqland1c)
-- Create date: 28.02.2014 (1.0)
-- Description: Процедура для выполнения очистки процедурного Кэша.
				Выполняется на основании настроек, заданных в таблице db_maintenance_FreeProcCache
-- Update:		25.03.2014 (1.05)
				Добавлен параметр Force - если задан 1, то принудительно будет запущен FREEPROCCACHE.
					По умолчанию 0.
-- ============================================= */
CREATE PROCEDURE db_maintenance.usp_FreeProcCache 
	@Force bit = 0
AS
BEGIN
	declare @WeekDay tinyint, @getdate datetime2(2), @PeriodHours tinyint, @LastRunDate datetime2(2);
	set nocount on;
	set @getdate=GETDATE();
	select @WeekDay=info.uf_GetWeekDay(@getdate);

	select top 1 
		@PeriodHours=[PeriodHours], 
		@LastRunDate=[LastRunDate]
	from 
		[db_maintenance].[FreeProcCache]
	where 
		(WeekDay is null or WeekDay=@WeekDay)
	
	if (@LastRunDate is null) or (datediff(hour,@LastRunDate,@getdate)>=@PeriodHours) or (@Force=1)
	begin
		DBCC FREEPROCCACHE;
		update [db_maintenance].[FreeProcCache]
		set [LastRunDate]=@getdate;
	end
END