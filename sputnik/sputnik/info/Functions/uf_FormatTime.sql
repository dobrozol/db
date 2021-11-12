
	/* =============================================
	-- Author:		Андрей Иванов (sqland1c)
	-- Create date: 01.12.2017
	-- Description:	Эта функция вернёт форматированную строку (время) в днях, часах, минутах, секунда. Входной параметр число - секунды
				
	-- Update:

	-- ============================================= */
	CREATE FUNCTION info.uf_FormatTime
	(
		@seconds bigint
	)
	RETURNS varchar(20)
	AS
	BEGIN
		declare @Return varchar(20);
		--проверка:
		--select @seconds/(3600*24) as dd,(@seconds%(3600*24))/3600 as hh,((@seconds%(3600*24))%3600) / 60 as mm, @seconds % 60 as ss

		SET @Return=CASE WHEN @seconds is null THEN '' ELSE 
			CASE 
				WHEN @seconds/(3600*24)>=1 THEN
					RIGHT (CONVERT(varchar(6), @seconds/(3600*24)),3)+ 'd:'
				ELSE ''
			END 
			+ RIGHT ('0' + CONVERT(varchar(6), (@seconds%(3600*24))/3600),2)
			+ ':' + RIGHT('0' + CONVERT(varchar(2), ((@seconds%(3600*24))%3600) / 60), 2)
			+ ':' + RIGHT('0' + CONVERT(varchar(2), @seconds % 60), 2)
		END;
	
		return @Return
	END