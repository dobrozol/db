
	CREATE PROC dbo.[usp_WriteToFile]
		@msg VARCHAR(7999),
		@file VARCHAR(300),
		@overwrite BIT = 0
	AS
	/* =============================================
	-- Author:		Андрей Иванов (sqland1c)
	-- Create date: 20.02.2019 (1.0)
	-- Description: Эта процедура производит построчную запись в файл.
					С помощью неё можно организовать файл скрипта powershell.
					Для записи в файл используется процедура xp_cmdshell.
	*/
	BEGIN
		SET NOCOUNT ON;
		DECLARE @cmd VARCHAR(7999),@ErrMsg nvarchar(3000),@rc int;
		set @cmd = N'cmd /k chcp 1251 && echo | echo '+COALESCE(LTRIM(@msg),'-')+CASE WHEN (@overwrite = 1) THEN ' > ' ELSE ' >> ' END +RTRIM(@file);
	--	SET @cmd = RTRIM('echo ' + COALESCE(LTRIM(@msg),'-') + CASE WHEN (@overwrite = 1) THEN ' > ' ELSE ' >> ' END + RTRIM(@file))
		EXEC @rc=[master].dbo.xp_cmdshell @cmd , no_output
		if @rc=1
		BEGIN
			set @ErrMsg=N'Ошибка при записи в файл '+@file;
			RAISERROR(@ErrMsg,11,1) WITH LOG;
		END;
		SET NOCOUNT OFF;
	END