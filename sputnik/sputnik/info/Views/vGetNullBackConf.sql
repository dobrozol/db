CREATE VIEW [info].[vGetNullBackConf]
AS 	
	SELECT
		'' as DBName, c.LocalDir, c.LocalDays, c.NetDir, c.NetDays, c.Kind, NULL as WeekDay, NULL as MonthDay
		,c.LocalPolicy, c.NetPolicy
	FROM backups.BackConf c
	WHERE c.Kind IN ('Full', 'Diff', 'Log')
		and isnull(c.DBName, '') = ''
	UNION
	SELECT
		'' as DBName, c.LocalDir, c.LocalDays, c.NetDir, c.NetDays, c.Kind, c.[WeekDay], c.MonthDay
		,c.LocalPolicy, c.NetPolicy
	FROM backups.BackConfWeekly c
	WHERE c.Kind IN ('Full', 'Diff', 'Log')
		and isnull(c.DBName, '') = ''