/* =============================================
-- Author:		Андрей Иванов (sqland1c)
-- Create date: 14.11.2018 (1.000)
-- Description: 
-- Update:		Эта процедура используется для уведомлений по эл.почте при создании новой базы данных и при удалении БД!
				
				
-- ============================================= */
create procedure [adt].[usp_mail_eventdb]
	@dbname nvarchar(2000),
	@tt datetime2(2),
	@login nvarchar(600),
	@hostname nvarchar(300)=null,
	@program nvarchar(300)=null,
	@sqlcommand nvarchar(max)='',
	@dropdb bit = 0
as
begin
	set nocount on;
	declare @strsubject varchar(100),@dbowner nvarchar(200),@StrErr NVARCHAR(3000);
	
	begin try
		select @strsubject=case when @dropdb=0 then 'New' else 'DROP' end +' database detected on ' + @@SERVERNAME;
		select @dbowner=suser_sname(owner_sid) from sys.databases where name=@dbname;

		declare @tableHTML  nvarchar(max);
		set @tableHTML =
			N'<H1>'+case when @dropdb=0 then 'Create new' else 'Drop' end + ' database on - ' + @@SERVERNAME +'</H1>' +
			N'<table border="1">' +
			N'<tr><th>Create date&nbsp;&nbsp;&nbsp;</th>'+
			N'<th>DB Name&nbsp;&nbsp;&nbsp;</th><th>Creator&nbsp;&nbsp;&nbsp;</th>' +
			N'<th>Owner&nbsp;&nbsp;&nbsp;</th>' +
			N'<th>Hostname&nbsp;&nbsp;&nbsp;</th>' +
			N'<th>Program&nbsp;&nbsp;&nbsp;</th>' +
			N'<th>SQL command&nbsp;&nbsp;&nbsp;</th></tr>' +
			CAST ( ( SELECT td = @tt, '',
							td = @dbname, '',
							td = @login, '',
							td = coalesce(@dbowner,''), '',
							td = coalesce(@hostname,''), '',
							td = coalesce(@program,''), '',
							td = coalesce(@sqlcommand,'') 
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
	end try
	begin catch
		SET @StrErr=N'Ошибка при отправке уведомления через e-mail, процедура [adt].[usp_mail_eventdb]. Имя БД: ['+@dbname+']; создатель: ['+@login+'], время события(tt): ['+convert(varchar(30),@tt,120)+']; текущий suser_name: ['+cast(suser_name() as nvarchar(500))+/*']; текущий suser_Sname: ['+cast(suser_sname() as nvarchar(500))+*/']. Текст ошибки: '+ERROR_MESSAGE();
				--В этом случае не будем заканчивать Диалог принудительно, чтобы другие бэкапы были выполнены до конца!
				--Просто пишем ошибку с подробностями в журнал SQL Server (через print) и заканчиваем выполнения (break):
		PRINT(@StrErr);
	end catch
end