
/* =============================================
-- Author:		Andrey N. Ivanov
-- Create date: 25.04.2014
-- Description:	This feature gets the most appropriate backup settings from the backup configuration.
				Depending on the Base, backup type and current date, this value may differ.
				
-- Update:		31.07.2014 (1.1)
				Add algorithm that determines whether a Full backup was made without taking info about day of the month or day of the week 
				(this is the @OnlyFull parameter in usp_RunBack hp).

				06.10.2022 (1.20)
				The algorithm for determining settings has been changed: the vGetAllBackConf view has been used. 
				Also added support for NULL configuration backups.

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
	SELECT LocalDir, NetDir, LocalDays, NetDays, pri as Ord
	FROM info.vGetAllBackConf
	WHERE Kind=@BT and DBName in ('', @DB)
		and isnull([MonthDay],0) in (0, @MonthDay)
		and isnull([WeekDay],0) in (0, @WeekDay);

	IF NOT EXISTS (SELECT LocalDir FROM @T WHERE LocalDir IS NOT NULL) AND @BT='Full'
	BEGIN
		INSERT INTO @T (LocalDir, NetDir, LocalDays, NetDays, Ord)
		SELECT LocalDir, NetDir, LocalDays, NetDays, pri as Ord
		FROM info.vGetAllBackConf
		WHERE Kind='Full' and DBName in ('', @DB)
			and (MonthDay BETWEEN 1 AND 31 or WeekDay BETWEEN 1 AND 7);
	END;

	INSERT @ReturnTable (LocalDir, NetDir, LocalDays, NetDays)
	SELECT LocalDir, NetDir, LocalDays, NetDays
	FROM @T
	WHERE Ord=(SELECT MIN(Ord) FROM @T) ;

	RETURN;
END