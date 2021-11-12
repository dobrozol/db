
CREATE PROCEDURE [info].[usp_dbmail_MissingBackups]
AS
BEGIN
	SET NOCOUNT ON;

	if object_id('tempdb.dbo.#Missing_Backups') is not null
		drop table #Missing_Backups;

	create table #Missing_Backups (
		[DB Name] [varchar](1000) NOT NULL,
		[Type] [varchar] (5) NOT NULL,
		[Last Backup] [varchar](100) NULL,
		[RecoveryModel] NVARCHAR(20),
		create_date VARCHAR(20),
		dbowner NVARCHAR(300)
		
		)

	DECLARE @T TABLE ([Database] NVARCHAR(2000), [TYPE] VARCHAR(4), [Last Backup] DATETIME, [RecoveryModel] NVARCHAR(20),create_date VARCHAR(20), dbowner NVARCHAR(300));
	INSERT INTO @T([Database],[TYPE],[Last Backup], RecoveryModel, create_date, dbowner)	
	SELECT 
		d.name AS [Database],
		CASE WHEN b.type = 'D' THEN 'FULL' ELSE 'DIFF' END as [Type],
		b.backupdate as [Last Backup],
		CAST(d.recovery_model_desc  COLLATE Cyrillic_General_CI_AS as NVARCHAR(20)) as RecoveryModel,
		CONVERT(VARCHAR(20),d.create_date,120) as create_date,
		suser_sname(d.owner_sid) as DBOwner
	FROM sys.databases d
	LEFT JOIN (SELECT database_name,type,MAX(backup_finish_date) backupdate 
				FROM msdb.dbo.backupset
				WHERE type IN ('D','I')
				GROUP BY database_name,type
				) b
		ON d.name=b.database_name
	WHERE d.name <> 'tempdb' and d.state_desc='online';

	;with cte_src01 as(
		SELECT Mx.[Database],CASE WHEN Det.[Type] IS NULL THEN 'FULL' ELSE Det.[Type] END [Type],
			   ISNULL(CONVERT(VARCHAR,Mx.[Last Backup],120),'NEVER') AS [Last Backup],
			   Mx.RecoveryModel,Mx.create_date,Mx.dbowner
		FROM 
			(SELECT [Database], MAX([Last Backup]) [Last Backup],RecoveryModel,create_date, dbowner  FROM @T GROUP BY [Database],RecoveryModel,create_date, dbowner) AS Mx
		LEFT JOIN 
			@T as Det
		ON Mx.[Database]=Det.[Database]	AND Mx.[Last Backup]=Det.[Last Backup]
		WHERE (Mx.[Last Backup] IS NULL OR Det.[Last Backup] < getdate()-8)	--для Full/Diff бэкапов 8 дней.
		UNION ALL
		SELECT d.name AS [Database],'Log' as [Type],
			   ISNULL(CONVERT(VARCHAR,b.backupdate,120),'NEVER') AS [Last Backup],
			   CAST(d.recovery_model_desc  COLLATE Cyrillic_General_CI_AS as NVARCHAR(20)) as RecoveryModel,
				CONVERT(VARCHAR(20),d.create_date,120) AS create_date,
				suser_sname(d.owner_sid) as dbowner
		FROM sys.databases d
		LEFT JOIN (SELECT database_name,type,MAX(backup_finish_date) backupdate 
				   FROM msdb.dbo.backupset
				   WHERE type LIKE 'L'
				   GROUP BY database_name,type
				  ) b on d.name=b.database_name
		WHERE recovery_model = 1
		  AND (backupdate IS NULL OR backupdate < getdate()-1)	--для бэкапов Логов 1 день.
		  AND d.name NOT IN ('tempdb','model')
		  AND d.state_desc='online'
	)
	insert into #Missing_Backups([DB Name],[Type],[Last Backup], RecoveryModel, create_date, dbowner)
	SELECT mb.[Database],mb.[Type],mb.[Last Backup], mb.RecoveryModel, mb.create_date, mb.dbowner
	FROM cte_src01 mb
	--Дополнительно перед отправкой сверим список исключений в базе sputnik.
	LEFT JOIN sputnik.backups.NoBackupList NBL
	ON mb.[Database]=NBL.DBName
		AND (mb.[Type]=NBL.TypeBackup OR NBL.TypeBackup IS NULL)
		AND (NBL.ExpDate>cast(getdate() as date) OR NBL.ExpDate IS NULL)
	WHERE 
		NBL.DBName IS NULL	  
	;
	--select * from #Missing_Backups;
	declare @cnt int  
	select @cnt=COUNT(1) from #Missing_Backups    
	if (@cnt > 0)
	begin

		declare @strsubject varchar(100)
		select @strsubject='Check for missing backups on ' + @@SERVERNAME

		declare @tableHTML  nvarchar(max);
		set @tableHTML =
			N'<H1>Databases Missing Backups Listing - ' + @@SERVERNAME +'</H1>' +
			N'<table border="1">' +
			N'<tr><th>DB Name&nbsp;&nbsp;&nbsp;</th><th>Type&nbsp;&nbsp;&nbsp;</th>' +
			N'<th>Last Backup&nbsp;&nbsp;&nbsp;</th>' +
			N'<th>Recovery_Model&nbsp;&nbsp;&nbsp;</th>' +
			N'<th>create_date&nbsp;&nbsp;&nbsp;</th>' +
			N'<th>DB Owner&nbsp;&nbsp;&nbsp;</th></tr>' +
			CAST ( ( SELECT td = [DB Name], '',
							td = [Type], '',
							td = [Last Backup], '',
							td = RecoveryModel, '',
							td = create_date, '',
							td = dbowner
					  FROM #Missing_Backups mb
					  FOR XML PATH('tr'), TYPE 
			) AS NVARCHAR(MAX) ) +
			N'</table>' ;

		 EXEC msdb.dbo.sp_send_dbmail
		 --@from_address='test@test.com',
		 @recipients='dba-info@ntsmail.ru',
		 @subject = @strsubject,
		 @body = @tableHTML,
		 @body_format = 'HTML' ,
		 @profile_name='sql-info'
	end

	drop table #Missing_Backups
END