/* =============================================
-- Author:		Андрей Иванов (sqland1c)
-- Create date: 15.03.2015
-- Description:	Эта процедура возвращает информацию из журналов SQL Server.
				Имеется множество входных параметров для применения различных отборов (фильтров).
				Обязательных параметров нет!
				@Filter1 и @Filter2 - основные фильтры для отбора по тексту события!
				@FilterOR - если задан, тогда фильтры @Filter1 и @Filter2 применяются как OR (ИЛИ), иначе AND (И).
				@FilterNotLike - дополнительный фильтр используется для отсечения ненужных значений (можно использовать %).
				@DateStart и @DateEnd - фильтры для отбора событий по датам (можно задавать как вместе так и по отдельности).
				@DateLastNHours - ещё один фильтр по дате (отбирает события за последние @DateLastNHours часов).
				@TOP - ограничение результирующиего набора по кол-ву строк (по умолчанию 4000).
				Для получения подобной информации используется системные расширенные процедуры.
-- Update:		20.03.2015 (1.05)
				Добавлены новые параметры @FilterSource и @FilterSourceNotLike и возможность фильтрация 
				по Источнику (столбец ProccessInfo). Также внесены исправления в код (устранение небольших ошибок).
				25.03.2015 (1.10)
				В результирующий запрос добавлена оконная функция ROW_NUMBER чтобы пронумеровать одинаковые строки (если время события и источник одинаковы).
				Причем уникальность по времени события в пределах 1 секунды (без учета милисек, как и в 1С).
				Необходимо, чтобы можно было нормально загружать в Хьюстон!
				Также добавлен параметр @WithRowNum, если он задан то ROW_NUMBER будет выведен. Если не задан, тогда запрос будет работать быстрее и без ROW_NUMBER.
				19.01.2016 (1.15)
				Добавлены новые параметры: @tt_tz_min - определяет часовой пояс в минутах, для компьютера откуда запущен запрос (например, сервер программы Хьюстон).
				@Convert_tz_InFilters - при отборах по датам @DateStart и @DateEnd  будет происходит конвертация часового пояса в местное время (с учетом заданного параметра @tt_tz_min).
				@Convert_tz_InResults - в результатах, при выводе даты события LogDate, будет происходит конвертация часового пояса из местного в заданный в параметре @tt_tz_min .
				19.10.2017 (1.200)
				Добавлен новый параметр: @GroupByText - группировать события по Тексту.
				Также табл.переменная заменена на временную таблицу #TLog.
				Также столбец ServerName теперь определяется по новому: учитывается имя сервера + имя экземпляра (отдельно).
				13.03.2018 (1.202)
				Для определения правильного имени сервера SQL теперь используется процедура info.usp_getHostname	
				31.01.2019 (1.250)
				Добавлен новый параметр @SortAZ-направление сортировки в результате.
				Также переделан алгоритм получения результата: вместо нескольких запросов сделан один динамический код sql.
				05.12.2021 (1.251)
				group by mode was fixed
-- ============================================= */
CREATE PROCEDURE [info].[usp_GetSqlLog]
	@Filter1 NVARCHAR(200) = NULL,
	@Filter2 NVARCHAR(200) = NULL,
	@FilterOR BIT = 0,
	@FilterNotLike NVARCHAR(200) = NULL,
	@FilterSource NVARCHAR(75) = NULL,
	@FilterSourceNotLike NVARCHAR(75) = NULL,
	@DateStart DATETIME2(0) = NULL,
	@DateEnd DATETIME2(0) = NULL,
	@DateLastNHours SMALLINT = 6,
	@TOP INT = 4000,
	@WithRowNum BIT=0,
	@tt_tz_min SMALLINT = NULL,
	@Convert_tz_InFilters BIT=1,
	@Convert_tz_InResults BIT=1,
	@GroupByText BIT=0,
	@SortAZ NVARCHAR(10) = 'DESC'
AS
BEGIN
	SET NOCOUNT ON;
	SET DATEFORMAT YMD;
	DECLARE @GD DATETIME2(0)=SYSDATETIME(), @Num TINYINT;
	declare @SQLServer nvarchar(510);
	declare @cmd nvarchar(max);
	exec info.usp_GetHostname @Servername=@SQLServer OUT;

	IF @DateStart IS NULL AND @DateEnd IS NULL
	BEGIN
		SET @DateEnd=@GD;
		SET @DateStart=DATEADD(HOUR,-@DateLastNHours,@GD);
	END
	ELSE
	BEGIN
		IF @Convert_tz_InFilters=1
		BEGIN
			SET @DateEnd=COALESCE(DATEADD(minute,datepart(TZoffset,SYSDATETIMEOFFSET())-@tt_tz_min,@DateEnd),@GD);
			SET @DateStart=COALESCE(DATEADD(minute,datepart(TZoffset,SYSDATETIMEOFFSET())-@tt_tz_min,@DateStart),CAST('1900-01-01' AS DATETIME2(0)));
		END 				
		ELSE
		BEGIN
			SET @DateEnd=COALESCE(@DateEnd,@GD);
			SET @DateStart=COALESCE(@DateStart,CAST('1900-01-01' AS DATETIME2(0)));	
		END
	END
	DECLARE @TLogNum TABLE ([Archive #] TINYINT, [Date] DATETIME2(0), [Log File Size (Byte)] bigint);
	INSERT INTO @TLogNum EXEC sys.xp_enumerrorlogs;
	DECLARE N CURSOR FOR
	SELECT [Archive #] as Num
	FROM @TLogNum
	WHERE [Date]>=(SELECT COALESCE(MAX([Date]),@DateStart) FROM @TLogNum WHERE [Date]<=@DateStart)
		AND [Date]<=(SELECT COALESCE(MIN([Date]),@DateEnd) FROM @TLogNum WHERE [Date]>=@DateEnd);
	OPEN N;
	FETCH NEXT FROM N INTO @Num;
	IF OBJECT_ID('tempdb.dbo.#TLog') IS NOT NULL
		DROP TABLE #TLog;
	CREATE TABLE #TLog (LogDate DATETIME, ProccessInfo VARCHAR(50),[Text] NVARCHAR(4000));
	WHILE @@FETCH_STATUS=0
	BEGIN
		IF @FilterOR=0
			INSERT INTO #TLog
			EXEC master.dbo.xp_readerrorlog @Num, 1, @Filter1, @Filter2, @DateStart, @DateEnd;
		ELSE
		BEGIN
			INSERT INTO #TLog
			EXEC master.dbo.xp_readerrorlog @Num, 1, @Filter1, null, @DateStart, @DateEnd;
			INSERT INTO #TLog
			EXEC master.dbo.xp_readerrorlog @Num, 1, @Filter2, null, @DateStart, @DateEnd;		
		END	
		FETCH NEXT FROM N INTO @Num;
	END
	CLOSE N;
	DEALLOCATE N;
	
	SET @cmd = '
	SELECT '+CASE WHEN isnull(@GroupByText,0)=1 THEN 'DISTINCT ' ELSE 'TOP ('+cast(@TOP as varchar(20))+')' END + '
		'''+@SQLServer+''' AS ServerName, 
		'+CASE 
				
			WHEN (isnull(@GroupByText,0)=0) AND @Convert_tz_InResults=1 THEN 'ISNULL(DATEADD(minute,'+cast(@tt_tz_min as varchar(20))+'-datepart(TZoffset,SYSDATETIMEOFFSET()),LogDate),LogDate)'
			WHEN (isnull(@GroupByText,0)=0) AND @Convert_tz_InResults<>1 THEN 'LogDate'
			WHEN (isnull(@GroupByText,0)=1) AND @Convert_tz_InResults=1 THEN 'max(ISNULL(DATEADD(minute,'+cast(@tt_tz_min as varchar(20))+'-datepart(TZoffset,SYSDATETIMEOFFSET()),LogDate),LogDate)) over (partition by [Text])'
			WHEN (isnull(@GroupByText,0)=1) AND @Convert_tz_InResults<>1 THEN 'Max(LogDate) over (partition by [Text])'
		END +' AS LogDate,
		'+CASE 
				
			WHEN (isnull(@GroupByText,0)=0) AND @Convert_tz_InResults=1 THEN 'CONVERT(VARCHAR(23),ISNULL(DATEADD(minute,'+cast(@tt_tz_min as varchar(20))+'-datepart(TZoffset,SYSDATETIMEOFFSET()),LogDate),LogDate),121)'
			WHEN (isnull(@GroupByText,0)=0) AND @Convert_tz_InResults<>1 THEN 'CONVERT(VARCHAR(23),LogDate,121)'
			WHEN (isnull(@GroupByText,0)=1) AND @Convert_tz_InResults=1 THEN 'CONVERT(VARCHAR(23),max(ISNULL(DATEADD(minute,'+cast(@tt_tz_min as varchar(20))+'-datepart(TZoffset,SYSDATETIMEOFFSET()),LogDate),LogDate)) over (partition by [Text]),121)'
			WHEN (isnull(@GroupByText,0)=1) AND @Convert_tz_InResults<>1 THEN 'CONVERT(VARCHAR(23),Max(LogDate) over (partition by [Text]),121)'
		END +' AS LogDate_str,
		'+CASE 
			WHEN (isnull(@GroupByText,0)=0) THEN 'ProccessInfo,'
			ELSE 'COUNT(*) over (partition by [Text]) as ProccessInfo,'
		END+' 
		'+case
			when @WithRowNum=1 and (isnull(@GroupByText,0)=0) then 'ROW_NUMBER() OVER (PARTITION BY CONVERT(VARCHAR(19),LogDate,121),ProccessInfo ORDER BY LogDate DESC) AS RowNum' 
			else 'NULL AS RowNum' 
		end+', 
		[Text]
	FROM #TLog
	WHERE 1=1
		'+CASE WHEN @FilterNotLike IS NULL THEN '' ELSE 'AND [Text] NOT LIKE '''+@FilterNotLike+'''' END+'
		'+CASE WHEN @FilterSource IS NULL THEN '' ELSE 'AND [ProccessInfo] LIKE '''+@FilterSource+'''' END+'
		'+CASE WHEN @FilterSourceNotLike IS NULL THEN '' ELSE 'AND [ProccessInfo] NOT LIKE '''+@FilterSourceNotLike+'''' END+' 
	
	ORDER BY '+CASE WHEN isnull(@GroupByText,0)=1 THEN '[ProccessInfo]' ELSE 'LogDate ' END +@SortAZ+';';	
	--PRINT (@cmd);
	EXEC (@cmd);
END
GO