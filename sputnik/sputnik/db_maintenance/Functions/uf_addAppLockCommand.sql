/* =============================================
-- Author:		Andrei N. Ivanov (sqland1c)
-- Create date: 18.11.2021 (1.0)
-- Description: Function for getting t-sql command with app lock (sp_getapplock)
-- Update:	

-- ============================================= */
CREATE FUNCTION [db_maintenance].[uf_addAppLockCommand]
(
	@dbName varchar(500)='',
	@schemaName varchar(500)='',
	@objectName varchar(1000),
	@command varchar(4000),
	@lockMessage varchar(500)='This object is already locked by another process'
)
RETURNS TABLE AS RETURN
(
	select top (1)
		concat(mtHead, QUOTENAME(objectHash, ''''), mtBody, @command, mtEnd) as resultCommand
	from (
	--hashing the object name to make the lock name unique and short
		select
			objectHash = convert(VARCHAR(32), HashBytes('MD5', concat(@dbName, '.', @schemaName, '.', @objectName)), 2),	
		--preparing request text for checking and blocking
			mtHead = '
declare @lockResult int;
begin try
	begin tran
		exec @lockResult = sp_getapplock ',
	
			mtBody = ', ''Exclusive'', ''Transaction'', 0;
		if @lockResult<0
			throw 60000, '+QUOTENAME(@lockMessage,'''')+', 0
		else begin
			',
			mtEnd = '
		end
	commit;
end try
begin catch
	if @@TRANCOUNT>0
		rollback;
	throw
end catch
'
	)a
)
