
	CREATE VIEW info.vGetAllBackConf
	AS
		SELECT
			DBName, LocalDir, LocalDays, NetDir, NetDays, Kind, WeekDay, MonthDay, FG, 1 as pri
			,LocalPolicy, NetPolicy
		FROM backups.BackConfWeekly
		WHERE KIND IN ('Full', 'Diff', 'Log')
			and isnull(DBNAME, '') > ''
		UNION
		SELECT
			DBName, LocalDir, LocalDays, NetDir, NetDays, Kind, NULL as WeekDay, NULL as MonthDay, FG, 2 as pri
			,LocalPolicy, NetPolicy
		FROM backups.BackConf
		WHERE KIND IN ('Full', 'Diff', 'Log')
			and isnull(DBNAME, '') > ''
		UNION
		SELECT DBName, LocalDir, LocalDays, NetDir, NetDays, Kind, WeekDay, MonthDay, NULL as FG, 101 as pri
			,LocalPolicy, NetPolicy
		FROM [info].[vGetNullBackConf]
		WHERE KIND IN ('Full', 'Diff', 'Log')
			and isnull(DBNAME, '') = ''
			AND ([WeekDay]>0 or [MonthDay]>0)
		UNION
		SELECT DBName, LocalDir, LocalDays, NetDir, NetDays, Kind, WeekDay, MonthDay, NULL as FG, 102 as pri
			,LocalPolicy, NetPolicy
		FROM [info].[vGetNullBackConf]
		WHERE KIND IN ('Full', 'Diff', 'Log') 
			and isnull(DBNAME, '') = ''
			AND (isnull([WeekDay],0)<=0 and isnull([MonthDay],0)<=0)