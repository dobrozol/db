
	/* =============================================
	-- Author:		Андрей Иванов (sqland1c)
	-- Create date: 03.08.2016 (1.0)
	-- Description:	Новая процедура, возвращает информацию о локальных дисках и свободном месте в Гб на Них в виде Таблицы!
					В отличии от первой версии (info.usp_GetDrives) эта процедура возвращает наиболее
					полную информацию (метка диска, весь объем диска).
	-- Update:		07.11.2016 (1.050)
					Добавлен новый параметр @DiskFilter - отбор по букве диска (необязательный).
	-- ============================================= */
	CREATE PROCEDURE info.usp_GetDrives2 
		@Details bit=1,
		@DiskFilter varchar(1)=NULL
	AS
	BEGIN
		set nocount ON;
		declare @sql varchar(400);
		declare @DiskFilter_ps varchar(300)='';
		IF @DiskFilter IS NOT NULL 
			set @DiskFilter_ps=' WHERE {$_.Name -eq '''+@DiskFilter+':\''} |';
		--declare @svrName varchar(255)
		--По умолчанию выводим информацию для текущего Компьютера. НО также можно указать другой компьютер в параметре -ComputerName
		--set @svrName = CAST(SERVERPROPERTY('MachineName') as varchar(255));
		--set @sql = 'powershell.exe -c "Get-WmiObject -ComputerName ' + QUOTENAME(@svrName,'''') + ' -Class Win32_Volume -Filter ''DriveType = 3'' | select name,capacity,freespace,label | foreach{$_.name+''|''+$_.capacity/1048576+''%''+$_.freespace/1048576+''*''+$_.label+''@''}"'

		/*--ТипДиска:
				Value	Meaning
				0 (0x0) Unknown
				1 (0x1)	No Root Directory
				2 (0x2)	Removable Disk
				3 (0x3)	Local Disk
				4 (0x4)	Network Drive
				5 (0x5)	Compact Disk
				6 (0x6)	RAM Disk
		*/
			set @sql = 'powershell.exe -c "Get-WmiObject -Class Win32_Volume |'+@DiskFilter_ps+' select name,capacity,freespace,label,FileSystem,DriveType | foreach{$_.name+''|''+$_.capacity/1048576+''%''+$_.freespace/1048576+''*''+$_.label+''@''+$_.FileSystem+''#''+$_.DriveType+''!''}"';
			if object_id('tempdb.dbo.#output') is not null
				drop table #output;
			CREATE TABLE #output
			(line varchar(255))
			insert #output
			EXEC xp_cmdshell @sql
			--select * from #output
			----script to retrieve the values in MB from PS Script output
			--select rtrim(ltrim(SUBSTRING(line,1,CHARINDEX('|',line) -1))) as drivename
			--      ,round(cast(rtrim(ltrim(SUBSTRING(line,CHARINDEX('|',line)+1,
			--      (CHARINDEX('%',line) -1)-CHARINDEX('|',line)) )) as Float),0) as 'capacity(MB)'
			--      ,round(cast(rtrim(ltrim(SUBSTRING(line,CHARINDEX('%',line)+1,
			--      (CHARINDEX('*',line) -1)-CHARINDEX('%',line)) )) as Float),0) as 'freespace(MB)'
			--from #output
			--where line like '[A-Z][:]%'
			--order by drivename
			--script to retrieve the values in GB from PS Script output
			IF @Details=1 
				select rtrim(ltrim(SUBSTRING(line,1,CHARINDEX('|',line) -1))) as Drive
					  ,
					  CASE rtrim(ltrim(SUBSTRING(line,CHARINDEX('#',line)+1,(CHARINDEX('!',line) -1)-CHARINDEX('#',line))))
						WHEN 0 THEN 'Unknown' 
						WHEN 1 THEN 'No_Root_Directory' 
						WHEN 2 THEN 'Removable' 
						WHEN 3 THEN 'Fixed'
						WHEN 4 THEN 'Network'
						WHEN 5 THEN 'CD-ROM'
						WHEN 6 THEN 'RAM_Disk'
					  END as DriveType		
					  ,rtrim(ltrim(SUBSTRING(line,CHARINDEX('*',line)+1,(CHARINDEX('@',line) -1)-CHARINDEX('*',line)))) as Label
					  ,rtrim(ltrim(SUBSTRING(line,CHARINDEX('@',line)+1,(CHARINDEX('#',line) -1)-CHARINDEX('@',line)))) as FileSystem
					  ,round(cast(rtrim(ltrim(SUBSTRING(line,CHARINDEX('|',line)+1,
					  (CHARINDEX('%',line) -1)-CHARINDEX('|',line)) )) as Float)/1024,0) as 'GB_Capacity'
					  ,round(cast(rtrim(ltrim(SUBSTRING(line,CHARINDEX('%',line)+1,
					  (CHARINDEX('*',line) -1)-CHARINDEX('%',line)) )) as Float) /1024 ,0)as 'GB_Free'
				from #output
				where line like '[A-Z][:]%'
				order by Drive
			ELSE
				select LEFT(rtrim(ltrim(SUBSTRING(line,1,CHARINDEX('|',line) -1))),1) as Drive
					  ,round(cast(rtrim(ltrim(SUBSTRING(line,CHARINDEX('|',line)+1,
					  (CHARINDEX('%',line) -1)-CHARINDEX('|',line)) )) as Float)/1024,0) as 'GB_Capacity'
					  ,round(cast(rtrim(ltrim(SUBSTRING(line,CHARINDEX('%',line)+1,
					  (CHARINDEX('*',line) -1)-CHARINDEX('%',line)) )) as Float) /1024 ,0)as 'GB_Free'
				from #output
				where line like '[A-Z][:]%'
					AND rtrim(ltrim(SUBSTRING(line,CHARINDEX('#',line)+1,(CHARINDEX('!',line) -1)-CHARINDEX('#',line))))=3
				order by Drive			
			if object_id('tempdb.dbo.#output') is not null
				drop table #output;
	END