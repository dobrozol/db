
	/* =============================================
	-- Author:		Андрей Иванов (sqland1c)
	-- Create date: 26.02.2014 (1.0)
	-- Description: Эта процедура используется для очистки исторических данных в базе sputnik!
					Что конкретно и как чистить определено в таблице config.cleanup_hs!
					На основании этих данных выполняется очистка старых данных.
	-- Update:		22.10.2015 (1.01)
					Добавлен параметр @top - для очистки только указанного числа строк!
					02.12.2015 (1.02)
					Для параметра @top задано значение по умолчанию=10000 (ранее было NULL) !
					10.03.2016 (1.03)
					Для параметра @top задано значение по умолчанию=2000 (ранее было 10000) !
					24.11.2017 (1.032)
					Добавлен новый алгоритм - для таблицы sql_handle_collect очищаем сразу по 20000 строк, в связи с тем что данные не успевают очищаться в этой таблице !
					09.02.2018 (1.033)
					Алгоритм агрессивной зачистки теперь действует для таблиц: sql_handle_collect, pfc_data, pfc_data_dyn
	-- ============================================= */
	CREATE PROCEDURE [config].[usp_cleanup_hs_run] 
		@only_print_command bit=0,
		@top int=2000
	AS
		SET NOCOUNT ON;
		DECLARE @SN nvarchar(300), @TN nvarchar(300), @CF nvarchar(300), @IT varchar(30), @I varchar(10), @sql_cmd nvarchar(1000), @top_cmd nvarchar(100)='';
		DECLARE @top_cmd_plus nvarchar(100)='';
		set @top_cmd_plus=' TOP (20000) ';
		if @top is not null
			set @top_cmd=' TOP ('+cast(@top as varchar(40))+') ';
		DECLARE Cl CURSOR FOR
		SELECT SchemaName,TableName,Column_filter,interval_type,interval
		FROM sputnik.config.cleanup_hs;
		OPEN Cl;
		FETCH NEXT FROM Cl INTO @SN, @TN, @CF, @IT, @I;
		WHILE @@FETCH_STATUS=0
		BEGIN
			SET @sql_cmd='IF OBJECT_ID(''['+@SN+'].['+@TN+']'') IS NOT NULL
		DELETE '+CASE WHEN @TN IN ('sql_handle_collect','pfc_data','pfc_data_dyn') THEN @top_cmd_plus ELSE @top_cmd END+' FROM ['+@SN+'].['+@TN+'] WHERE ['+@CF+'] < DATEADD('+@IT+',-'+@I+',SYSDATETIME())';
			IF @only_print_command=1
				PRINT @sql_cmd;
			ELSE
				EXEC(@sql_cmd);
			FETCH NEXT FROM Cl INTO @SN, @TN, @CF, @IT, @I;
		END
		CLOSE Cl;
		DEALLOCATE Cl;