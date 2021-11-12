
/* =============================================
-- Author:		Андрей Иванов (sqland1c)
-- Create date: 17.02.2014
-- Description:	Эта функция возвращает день недели для заданной даты.
				
-- Update:

-- ============================================= */
CREATE FUNCTION info.uf_GetWeekDay
(
	@date datetime
)
RETURNS int
AS
BEGIN
	declare @Return tinyint=0;
	if @@DATEFIRST=7
	begin
		set @Return = datepart(weekday,@date) - 1;
		if @Return=0
			set @Return=7
	end
	else if @@DATEFIRST=1
		set @Return= datepart(weekday,@date);
	
	return @Return
END