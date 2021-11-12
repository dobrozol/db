
/* =============================================
-- Author:		Андрей Иванов (sqland1c)
-- Create date: 25.04.2014
-- Description:	Эта функция получает наиболее подходящие настройки для Резервного копирования из БД 
				В зависимости от Базы, типа бэкапа и текущей даты это значение может быть различным.
				
-- Update:		31.07.2014 (1.1)
				Добавлен алгоритм, который учитывает, что мог быть сделан Полный бэкап без учёта дня месяца и дня недели (это параметр @OnlyFull в ХП usp_RunBack).

-- ============================================= */
CREATE FUNCTION info.uf_GetBackConf
(
	@DB NVARCHAR(100),	--Database Name
	@BT VARCHAR(4), --BackupType
	@DD DATETIME2(2) --Date of Backup
)
RETURNS @ReturnTable TABLE (
	LocalDir NVARCHAR(300), 
	NetDir NVARCHAR(300),
	LocalDays int,
	NetDays int
)
AS
BEGIN
	DECLARE @WeekDay TINYINT, @MonthDay TINYINT;
	DECLARE @T TABLE (LocalDir NVARCHAR(300), NetDir NVARCHAR(300), LocalDays INT, NetDays INT, Ord tinyint);

	SET @WeekDay=info.uf_GetWeekDay(@DD);
	SET @MonthDay=DATEPART ( DAY , @DD );

	INSERT INTO @T (LocalDir, NetDir, LocalDays, NetDays, Ord)
	SELECT TOP 1 LocalDir, NetDir, LocalDays, NetDays, 1 as Ord
	FROM backups.BackConfWeekly
	WHERE Kind=@BT and DBName=@DB and MonthDay=@MonthDay
	UNION ALL
	SELECT TOP 1 LocalDir, NetDir, LocalDays, NetDays, 2 as Ord
	FROM backups.BackConfWeekly
	WHERE Kind=@BT and DBName=@DB and WeekDay=@WeekDay
	UNION ALL
	SELECT	TOP 1 LocalDir, NetDir, LocalDays, NetDays, 3 as Ord
	FROM	backups.BackConf
	WHERE	DBName = @DB AND Kind = @BT;

	--Новый алгоритм (только для Полных бэкапов!): если использован параметр @OnlyFull в ХП usp_RunBack и стандартные настройки бэкапов не были найдены
	--тогда нужно найти подходящие настройки для Full из таблицы BackConfWeekly без учета дня месяца и дня недели!
	IF NOT EXISTS (SELECT LocalDir FROM  @T WHERE LocalDir IS NOT NULL) AND @BT='Full'
	BEGIN
		INSERT INTO @T (LocalDir, NetDir, LocalDays, NetDays, Ord)
		SELECT TOP 1 LocalDir, NetDir, LocalDays, NetDays, 1 AS Ord
		FROM backups.BackConfWeekly
		WHERE Kind='Full' and DBName=@DB and MonthDay BETWEEN 1 AND 31
		UNION ALL
		SELECT TOP 1 LocalDir, NetDir, LocalDays, NetDays, 2 AS Ord
		FROM backups.BackConfWeekly
		WHERE Kind='Full' and DBName=@DB and WeekDay BETWEEN 1 AND 7;
	END;

	INSERT @ReturnTable (LocalDir, NetDir, LocalDays, NetDays)
	SELECT LocalDir, NetDir, LocalDays, NetDays
	FROM @T
	WHERE Ord=(SELECT MIN(Ord) FROM @T) ;

	RETURN;
END