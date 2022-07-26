
	CREATE VIEW info.vGetQuickBackConf
	AS
		SELECT
			D.DB, D.state, D.RecoveryModel,
			CASE
				WHEN BF.Kind is NULL THEN 0
				ELSE 1
			END AS 'Full',
			CASE
				WHEN BD.Kind is NULL THEN 0
				ELSE 1
			END AS 'Diff',
			CASE
				WHEN BL.Kind is NULL THEN 0
				ELSE 1
			END AS 'Log',
			CASE
				WHEN BFW.Kind is NULL THEN 0
				ELSE 1
			END AS 'FullWeekly'
		FROM
		(
			SELECT name as DB, state_desc as state, recovery_model_desc as RecoveryModel
			FROM [master].SYS.DATABASES
			WHERE state=0 AND is_read_only=0 AND name not in (/*'model',*/ 'tempdb')
		) as D
		LEFT JOIN 
			(SELECT DBNAME, Kind
			 FROM	info.vGetAllBackConf
			 WHERE Kind='Full' AND WeekDay IS NULL AND isnull(DBName,'') != ''
			 GROUP BY DBNAME, Kind) BF
			ON D.DB=BF.DBName
		LEFT JOIN 
			(SELECT DBNAME, Kind
			 FROM	info.vGetAllBackConf
			 WHERE Kind='Full' AND WeekDay IS NOT NULL AND isnull(DBName,'') != ''
			 GROUP BY DBNAME, Kind) BFW
			ON D.DB=BFW.DBName
		LEFT JOIN 
			(SELECT DBNAME, Kind
			 FROM	info.vGetAllBackConf
			 WHERE  isnull(DBName,'') != ''
			 GROUP BY DBNAME, Kind) BD
			ON D.DB=BD.DBName AND BD.Kind='Diff'
		LEFT JOIN 
			(SELECT DBNAME, Kind
			 FROM	info.vGetAllBackConf
			 WHERE  isnull(DBName,'') != ''
			 GROUP BY DBNAME, Kind) BL
			ON D.DB=BL.DBName AND BL.Kind='Log'
		UNION 
		SELECT TOP 1
			d.DB, d.[state], d.RecoveryModel,
			CASE
				WHEN c.Kind = 'Full' and WeekDay is null THEN 1
				ELSE 0
			END AS 'Full',
			CASE
				WHEN c.Kind = 'Diff' THEN 1
				ELSE 0
			END AS 'Diff',
			CASE
				WHEN c.Kind = 'Log' THEN 1
				ELSE 0
			END AS 'Log',
			CASE
				WHEN exists (select top 1 1 from info.[vGetNullBackConf] where Kind = 'Full' and [WeekDay] is not null) THEN 1
				ELSE 0
			END AS 'FullWeekly'
		FROM (select '' as DB, 'ONLINE' as [state], 'FULL' as RecoveryModel) as d
		left join [info].[vGetNullBackConf] c
			on isnull(c.DBName,'') = ''
		ORDER BY isnull(c.[WeekDay],0)