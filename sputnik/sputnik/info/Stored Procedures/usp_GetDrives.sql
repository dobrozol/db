
/* =============================================
-- Author:		Андрей Иванов (sqland1c)
-- Create date: 18.02.2014 (1.0)
-- Description:	Эта функция возвращает информацию о локальных дисках и свободном месте в Гб на Них в виде Таблицы!
				
-- Update:		
				06.04.2014 (1.01)
				Новый параметр @GetMaxFree - если задан 1, тогда в выходном параметре @MaxFreeDrive будет буква диска,
				где больше всего свободного места.
				17.09.2015 (1.02)
				Новый параметр @DiskFilter показывает информацию только по указанному диску (нужно передавать только 1 букву диска)
-- ============================================= */
CREATE PROCEDURE info.usp_GetDrives 
	@GetMaxFree bit = 0, 
	@MaxFreeDrive char(1)=NULL OUTPUT,
	@DiskFilter varchar(1)=NULL
AS
BEGIN
	set nocount ON;
	declare  @Temp TABLE (Drive varchar(5),MB_Free decimal(12,3));

	insert into @Temp
	exec xp_fixeddrives

	IF @GetMaxFree=0
		select Drive, cast (MB_Free / 1024.000 as decimal (12,3)) as GB_Free
		from @Temp
		where (@DiskFilter is null OR Drive=@DiskFilter)
	ELSE
		SELECT TOP 1 @MaxFreeDrive=Drive
		FROM @Temp
		WHERE MB_Free= (SELECT MAX(MB_Free) FROM @Temp);
END