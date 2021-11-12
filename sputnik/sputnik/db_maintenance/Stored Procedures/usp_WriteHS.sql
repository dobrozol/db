
	/* =============================================
	-- Author:		Андрей Иванов (sqland1c)
	-- Create date: 19.10.2016 (1.0)
	-- Description: The procedure is used for logging all database maintenance operations (writing to the HS table).
	-- Update:		
					21.10.2016 (1.005) Fixed bug number, now 55000 (50000 cannot be used for custom bugs).
					27.12.2017 (1.010) Instead of NULL for DB_ID, 0 will be written to the history table.
	-- ============================================= */
	CREATE PROCEDURE db_maintenance.usp_WriteHS
		@DB_ID int,
		@Object_ID int=null,
		@Index_Stat_ID int=null,
		@Index_Stat_Type bit=null,
		@Command_Type tinyint,
		@Command_Text_1000 varchar(8000),
		@tt_start datetime2(2),
		@tt_end datetime2(2)=null,
		@Status bit,
		@Error_Text_1000 varchar(8000)=null
	as
	begin
		set nocount on;
		DECLARE @StrErr VARCHAR(2048);
		if @tt_end is null
			set @tt_end=CAST(SYSDATETIME() as datetime2(2));
		if LEN(@Command_Text_1000)>1000
			set @Command_Text_1000=LEFT(@Command_Text_1000,1000);
		if @Error_Text_1000 is not null AND LEN(@Error_Text_1000)>1000
			set @Error_Text_1000=LEFT(@Error_Text_1000,1000);

		BEGIN TRY
			insert into sputnik.db_maintenance.HS ([DB_ID],[Object_ID],[Index_Stat_ID],[Index_Stat_Type],[Command_Type],[Command_Text_1000],[tt_start],[tt_end],[Status],[Error_Text_1000])
			select COALESCE(@DB_ID,0), @Object_ID, @Index_Stat_ID, @Index_Stat_Type, @Command_Type, @Command_Text_1000, @tt_start, @tt_end, @Status, @Error_Text_1000 ;
		END TRY
		BEGIN CATCH
			SET @StrErr=N'An error occurred while writing to the sputnik.db_maintenance.HS table via the [usp_WriteHS] procedure! Error text: '+COALESCE(ERROR_MESSAGE(),'null *Error message is not defined');
			--print @StrErr;
			--RAISERROR(@StrErr,11,1) WITH LOG;
			EXEC xp_logevent 55000, @StrErr, ERROR; 
		END CATCH
		
	end