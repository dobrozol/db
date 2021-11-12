
	/* =============================================
	-- Author:		Ivanov Andrei
	-- Create date: 02.06.2016
	-- Description:	This function gets the PolicyScan for Update Statistics for the specified table and statistics.
				
	-- ============================================= */
	CREATE FUNCTION db_maintenance.uf_CheckTabList_US
	(
		@Tab NVARCHAR(1000), --Table name in []
		@Stat NVARCHAR(1000) --Stat name in []
	)
	RETURNS NVARCHAR(100)
	AS
	BEGIN
		declare @Return NVARCHAR(100);
		select @Return=PolicyScan 
		from db_maintenance.TabList_US
		where Tab=@Tab AND Stat=@Stat;
		IF @Return is null
			select @Return=PolicyScan 
			from db_maintenance.TabList_US
			where Tab=@Tab AND Stat IS NULL;
		return @Return;
	END