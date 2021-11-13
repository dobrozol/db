/* =============================================
-- Author:		Andrei N. Ivanov (sqland1c)
-- Create date: 13.11.2021 (1.0)
-- Description: Procedure for getting parameter max_dop, auto determine this parameter by edition, cores on server
-- Update:	

-- ============================================= */
CREATE PROCEDURE [db_maintenance].[usp_getMaxDop]
	@objectSizeInPages int=NULL	--one page=8kB
AS
BEGIN
	declare @maxDop smallint=1, @edition VARCHAR(3), @allCores smallint;
	--Getting edition for current SQL Server. MaxDop will be running in Enterprise only:
	set @edition = LEFT(CAST(SERVERPROPERTY('Edition') AS VARCHAR(128)),3);
	if @edition='Ent' begin
		--Determine available number of CPU cores for SQL Server
		select @allCores=count(*) from sys.dm_os_schedulers where [status]='VISIBLE ONLINE';
		--Maximum for max dop = 64 cores, minimum 1 core
		select @allCores = case
			when @allCores>64 then 64
			when isnull(@allCores,0)<1 then 1
		end;
		--defining maxdop depending on the size of the index or available CPU cores
		select @maxDop = case
			when @allCores between 5 and 8 or @objectSizeInPages between 640 and 3200 then 2 --max 25Mb
			when @allCores between 9 and 12 or @objectSizeInPages between 3201 and 12800 then 4 --max 100Mb
			when @allCores between 13 and 16 or @objectSizeInPages between 12801 and 64000 then 8 --max 500Mb
			when @allCores between 17 and 24 or @objectSizeInPages between 64001 and 640000 then 12 --max 5Gb
			when @allCores between 25 and 32 or @objectSizeInPages between 640001 and 6400000 then 16 --max 50Gb
			when @allCores between 33 and 48 or @objectSizeInPages between 6400001 and 9600000 then 24 --max 75Gb
			when @allCores between 49 and 64 or @objectSizeInPages between 9600001 and 12800000 then 32 --max 100Gb
			when @allCores between 65 and 96 or @objectSizeInPages between 12800001 and 16000000 then 48 --max 125Gb
			when @allCores > 96 or @objectSizeInPages > 16000000 then 64 --more than 125Gb
			else 1
		end
	end;
	return @maxDop
END
