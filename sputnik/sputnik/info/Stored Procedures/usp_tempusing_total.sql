﻿
	/* =============================================
	-- Author:		Андрей Иванов (sqland1c)
	-- Create date: 09.04.2018
	-- Description:	Эта процедура возвращает данные из представления v_tempusing. Процедура создана для zabbix!

	-- Update:		
	-- ============================================= */
	CREATE PROC [info].[usp_tempusing_total] 
	AS
	BEGIN
		set nocount on;
		select [vl] from info.vtempusing where pr='total_mb';
	END
GO
GRANT EXECUTE
    ON OBJECT::[info].[usp_tempusing_total] TO [zabbix]
    AS [dbo];

