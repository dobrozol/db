
/* =============================================
-- Author:		Андрей Иванов (sqland1c)
-- Create date: 23.07.2014 (1.0)
-- Description:	Эта процедура возвращает IP адрес сервера (где выполняется).
				Для получения нужных данных используется команда операционной системы IPConfig.
-- Update:		18.08.2014 (1.01)
				Исправлена строка с получением IP адреса из результатов команды IPConfig:
				вместо '%IPv4%' теперь получаем '   IPv4%' (чтобы получить именно IPv4 адрес).
-- ============================================= */
CREATE PROCEDURE info.usp_GetIP
AS
BEGIN
	SET NOCOUNT ON;
	DECLARE @T TABLE (Output NVARCHAR(300));
	DECLARE @str NVARCHAR(20);
	INSERT INTO @T
	EXEC xp_cmdshell 'IPCONFIG';
	SELECT TOP 1 @str=SUBSTRING(Output,CHARINDEX(':',Output)+1,20) FROM @T
	WHERE Output LIKE '   IPv4%'
	SELECT LTRIM(@str) as IP;
END