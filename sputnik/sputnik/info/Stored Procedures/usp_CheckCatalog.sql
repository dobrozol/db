
/* =============================================
-- Author:		Андрей Иванов (sqland1c)
-- Create date: 26.07.2014
-- Description:	Эта процедура проверяет наличие (существование) указанного каталога.
				Если каталог существует, будет возвращен 1. Иначе 2.
				Параметр @Catalog - полный путь к файлу, который нужно проверить.	
-- Update:		
-- ============================================= */
CREATE PROCEDURE info.usp_CheckCatalog
(
	@Catalog nvarchar(500)
)
AS
BEGIN
	SET NOCOUNT ON;
	declare @Return tinyint=0;
	Declare @DIR NVARCHAR(550);
	declare @TableCheck TABLE ([File Exists] bit, [File is a Directory] bit, [Parent Directory Exists] bit);
	--Сначала проверим существование каталога;
	INSERT INTO @TableCheck
	EXEC xp_fileexist @Catalog;
	SELECT @Return=[File is a Directory] FROM @TableCheck;
	if @Return=0
		SELECT @Return=[Parent Directory Exists] FROM @TableCheck;
	--Теперь, если каталог не существует попробуем его создать!	
	if @Return=0
	begin
		SET @DIR='MKDIR '+@Catalog;
		exec @Return= xp_cmdshell @DIR , NO_OUTPUT;
		IF @Return=0 
			SET @Return=1;
		ELSE
			SET @Return=2;
	end
	else
		if @Return=0
			SET @Return=2;
	SELECT @Catalog AS 'Dir', @Return AS 'Exist';
END