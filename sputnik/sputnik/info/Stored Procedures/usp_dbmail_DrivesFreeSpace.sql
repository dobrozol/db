﻿
CREATE PROCEDURE [info].[usp_dbmail_DrivesFreeSpace]
AS
BEGIN
	SET NOCOUNT ON;
	create table #DriveSpaceLeft (Drive varchar(10),
								  [MB Free] bigint )

	insert #DriveSpaceLeft (Drive, [MB Free])
	   EXEC master.dbo.xp_fixeddrives;

	create table #DrivesWithIssue(Drive varchar(10),
								  [MB Free] bigint )

	insert into #DrivesWithIssue
	  select Drive, [MB Free] from #DriveSpaceLeft
	  where [MB Free] < 10*1024

	drop table #DriveSpaceLeft

	declare @cnt int  
	select @cnt=COUNT(1) from #DrivesWithIssue
	if (@cnt > 0)
	begin

		declare @strsubject varchar(100)
		select @strsubject='Check drive space on ' + @@SERVERNAME

		declare @tableHTML  nvarchar(max);
		set @tableHTML =
			N'<H1>Drives with less that 10GB Free  - ' + @@SERVERNAME + '</H1>' +
			N'<table border="1">' +
			N'<tr><th>Drive</th>' +
			N'<th>MB Free</th></tr>' +
			CAST ( ( SELECT td = [Drive], '',
							td = [MB Free]
					  FROM #DrivesWithIssue
					  FOR XML PATH('tr'), TYPE 
			) AS NVARCHAR(MAX) ) +
			N'</table>' ;

		 EXEC msdb.dbo.sp_send_dbmail
		 --@from_address='test@test.com',
		 @recipients='dba-info@ntsmail.ru',
		 @subject = @strsubject,
		 @body = @tableHTML,
		 @body_format = 'HTML' ,
		 @profile_name='sql-info'
	end

	drop table #DrivesWithIssue;
END