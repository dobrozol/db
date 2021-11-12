
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
			FROM SYS.DATABASES
			WHERE state=0 AND is_read_only=0 AND name not in (/*'model',*/ 'tempdb')
				AND (name NOT LIKE '201%(%)%' and name NOT LIKE 'S201%(%)%')
		) as D
		LEFT JOIN 
			(SELECT DBNAME, Kind
			 FROM	info.vGetAllBackConf
			 WHERE Kind='Full' AND WeekDay IS NULL
			 GROUP BY DBNAME, Kind) BF
			ON D.DB=BF.DBName
		LEFT JOIN 
			(SELECT DBNAME, Kind
			 FROM	info.vGetAllBackConf
			 WHERE Kind='Full' AND WeekDay IS NOT NULL
			 GROUP BY DBNAME, Kind) BFW
			ON D.DB=BFW.DBName
		LEFT JOIN 
			(SELECT DBNAME, Kind
			 FROM	info.vGetAllBackConf
			 GROUP BY DBNAME, Kind) BD
			ON D.DB=BD.DBName AND BD.Kind='Diff'
		LEFT JOIN 
			(SELECT DBNAME, Kind
			 FROM	info.vGetAllBackConf
			 GROUP BY DBNAME, Kind) BL
			ON D.DB=BL.DBName AND BL.Kind='Log'