/* =============================================
-- Author:		Ivanov Andrei
-- Create date: 18.11.2021
-- Description:	This function gets the PolicyScan for Update Statistics based on size of data (parameter dataSizeMb).
				
-- ============================================= */
CREATE FUNCTION [db_maintenance].[uf_getRecomputePolicyScan]
(
	@dataSizeMb bigint
)
RETURNS TABLE AS RETURN
(
	SELECT TOP 1
		CASE 
			WHEN @dataSizeMb<=1024	THEN N'WITH FULLSCAN;'
			WHEN @dataSizeMb<=1536	THEN N'WITH SAMPLE 95 PERCENT;'
			WHEN @dataSizeMb<=2048	THEN N'WITH SAMPLE 90 PERCENT;'
			WHEN @dataSizeMb<=3072	THEN N'WITH SAMPLE 75 PERCENT;'
			WHEN @dataSizeMb<4096	THEN N'WITH SAMPLE 50 PERCENT;'
			WHEN @dataSizeMb<5120	THEN N'WITH SAMPLE 40 PERCENT;'
			WHEN @dataSizeMb<7168	THEN N'WITH SAMPLE 25 PERCENT;'
			WHEN @dataSizeMb<9216	THEN N'WITH SAMPLE 20 PERCENT;'
			WHEN @dataSizeMb<11264	THEN N'WITH SAMPLE 15 PERCENT;'
			WHEN @dataSizeMb<15360	THEN N'WITH SAMPLE 10 PERCENT;'
			WHEN @dataSizeMb<20480	THEN N'WITH SAMPLE 8 PERCENT;'
			WHEN @dataSizeMb<30720	THEN N'WITH SAMPLE 5 PERCENT;'
			WHEN @dataSizeMb<40960	THEN N'WITH SAMPLE 3 PERCENT;'
			WHEN @dataSizeMb<61440	THEN N'WITH SAMPLE 2 PERCENT;'
			WHEN @dataSizeMb<102400	THEN N'WITH SAMPLE 1 PERCENT;'
			ELSE NULL
		END as policyScan
)
