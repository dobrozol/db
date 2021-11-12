
	CREATE VIEW info.vGetAllBackConf
	AS
		SELECT
			DBName, LocalDir, LocalDays, NetDir, NetDays, Kind, NULL as WeekDay, NULL as MonthDay
			,LocalPolicy, NetPolicy
		FROM backups.BackConf
		WHERE KIND IN ('Full', 'Diff', 'Log')
		UNION
		SELECT
			DBName, LocalDir, LocalDays, NetDir, NetDays, Kind, WeekDay, MonthDay
			,LocalPolicy, NetPolicy
		FROM backups.BackConfWeekly
		WHERE KIND IN ('Full', 'Diff', 'Log');