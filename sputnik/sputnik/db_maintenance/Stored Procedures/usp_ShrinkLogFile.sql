
	/* =============================================
	-- Author:		Андрей Иванов (sqland1c)
	-- Create date: 29.11.2013 (1.0)
	-- Description: Процедура для обслуживания БД! Для указанной в параметре @db_name 
					базы данных производит shrinkfile и изменяет настройки 
					(размер файла, автоприращение).
					Необязательные параметры @SetSizeMb - до какого размера сжимать в Мб
					и  @FileGrowthMb - установка приращений в Мб.
					Параметр @truncate - позволяет выполнить зачистку транзакций в Log и сжать файл до нужного размера.
					По умолчанию выключен!
	-- Update:	
					17.03.2014 (1.1)
					Изменены значения по умолчанию для Приращения. А также максимальный размер @SetSizeMb установлен в 23 Гб.
					25.04.2014 (1.12)
					Ещё раз оптимизированы значения по умолчанию для всех параметров
						@FileGrowthMb=64 (минимум 4Гб), @SetSizeMb - максимум 10Гб. 
					15.04.2014 (1.14)
					Изменены значения по умолчанию: 512 Мб для @FileGrowthMb (шаг приращения) и 3072 Мб для @SetSizeMb (начальный размер).
					18.03.2016 (1.20)
					Новые алгоритмы определения конечного размера и размера приращения!
					Также переопределены значения по умолчанию!
					Также теперь выполняется CHECKPOINT перед сжатием!
					Также теперь сжатие и установка приращения выполняются в отдельных пакетах
					и только в том случае, если устанавливаемый размер файла журнала транзакций
					меньше чем текущий размер. Приращение устанавливается только если новое значение
					отличается от текущего.
	-- ============================================= */
	CREATE PROCEDURE db_maintenance.usp_ShrinkLogFile
		@db_name nvarchar(50) = null,
		@SetSizeMb int = null,
		@FileGrowthMb int = null,
		@truncate bit=null
	AS
	BEGIN
		SET NOCOUNT ON;
		IF @db_name is not null
		begin
			declare @name varchar(128), @size int, @max_size int, @growth int, @is_percent_growth bit, @tsql nvarchar(500), @RecoveryModel varchar(11);
			declare @set_min_size int;
			select 
				@RecoveryModel=recovery_model_desc 
			from sys.databases where name=@db_name

			declare CUR_LogFiles CURSOR
			FOR		select name, size/128, max_size, growth, is_percent_growth
					from sys.master_files
					where database_id=DB_ID(@db_name) and type=1 and state=0; --type=1 (LOG) и state=0 (ONLINE)
			open CUR_LogFiles
			fetch next from CUR_LogFiles
				into @name,@size,@max_size,@growth,@is_percent_growth
		
			while @@FETCH_STATUS=0
			begin
				if @SetSizeMb is null or @SetSizeMb<=0  --Если размер не задан, или задан неверно!
					set @SetSizeMb=	@size/4 --Устанавливаем 25% от текущего размера файла.
			
				--Определяем минимальный размер = 4% от текущего размера файла.
				--НО не меньше чем 32 Мб!
				select @set_min_size=
					CASE 
						WHEN @size/25>32 THEN @size/25
						ELSE 32	
					END;
				if @SetSizeMb < @set_min_size	--Устанавливаем минимально-возможный размер	
					set @SetSizeMb=	@set_min_size 

				if @SetSizeMb > 19*1024 ---Определяем максимальный размер	
					set @SetSizeMb=19*1024 --установить размер 19 Гб!
			
				/*
					--Старый алгоритм определения авто-приращения @FileGrowthMb
					if @FileGrowthMb is null or @FileGrowthMb<=0 
						set @FileGrowthMb=64--АвтоПриращение установить в размере 64 (по умолчанию).
					else if  @FileGrowthMb>1024
						set @FileGrowthMb=1024--АвтоПриращение установить в размере 1024 (максимум).
					else if @FileGrowthMb<8 --Если приращение меньше 8 Мб.
						set @FileGrowthMb=8 --установить минимальное приращение в 8 Мб (Минимум).
				*/

				--	Новый алгоритм определения авто-приращения @FileGrowthMb
				if @FileGrowthMb is null or @FileGrowthMb<=0 
				begin
					--Определяем АвтоПриращение по умолчанию.
					select @FileGrowthMb=
						CASE 
							--WHEN @db_name = 'tempdb' THEN 1024
							WHEN @size <= 128 THEN 32
							WHEN @size <= 512 THEN 128
							WHEN @size <= 2048 THEN 256
							WHEN @size <= 8192 THEN 512
							WHEN @size <= 16384 THEN 1024
							WHEN @size <= 32768 THEN 2048
							ELSE 3072
						END	
				end
				else if  @FileGrowthMb>3072
					set @FileGrowthMb=3072--Максимальное АвтоПриращение установить в размере 3 Гб.
				else if @FileGrowthMb<32 
					set @FileGrowthMb=32 --Минимальное АвтоПриращение установить в размере 32 Мб.

				if @truncate=1 and @RecoveryModel<>'SIMPLE'
				begin
					set @tsql='use [master]
					ALTER DATABASE ['+@db_name+'] SET RECOVERY SIMPLE WITH NO_WAIT'
					exec(@tsql);
				end
			
				if @SetSizeMb<@size
				begin
					set @tsql='use ['+@db_name+'];
					CHECKPOINT;
					WAITFOR DELAY ''00:00:02'';'
					exec(@tsql);

					set @tsql='use ['+@db_name+'];
					DBCC SHRINKFILE (N'''+@name+''' , '+cast(@SetSizeMb as varchar(25))+');';
					exec(@tsql);

					if (@FileGrowthMb<>@growth or @is_percent_growth=1)
					begin
						set @tsql='use [master];
						ALTER DATABASE ['+@db_name+'] MODIFY FILE ( NAME = N'''+@name+''', FILEGROWTH = '+cast(@FileGrowthMb as varchar(25))+'MB)';
						exec(@tsql);
					end
				end
						
				if @truncate=1 and @RecoveryModel<>'SIMPLE'
				begin
					set @tsql='use [master]
					ALTER DATABASE ['+@db_name+'] SET RECOVERY '+@RecoveryModel+' WITH NO_WAIT'
					exec(@tsql);
				end

				fetch next from CUR_LogFiles
					into @name,@size,@max_size,@growth,@is_percent_growth
			end
		CLOSE CUR_LogFiles;
		DEALLOCATE CUR_LogFiles;
		end
	END