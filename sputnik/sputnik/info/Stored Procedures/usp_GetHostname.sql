
	/* =============================================
	-- Author:		Андрей Иванов (sqland1c)
	-- Create date: 23.02.2018
	-- Description:	Эта процедура возращает имя сервера (Hostname). Причём Full computername, то есть даже если имя сервера будет более 15 символов, то вернёт полное имя сервера (а не netbios computername)!

	-- Update:		23.03.2018 (1.010)
					Добавлен алгоритм проверки доступна ли роль sysadmin. Если недоступна, то получаем имя сервера старым методом (без вызова xp_cmdshell).
	-- ============================================= */
	CREATE PROC [info].[usp_GetHostname] 
		@Hostname nvarchar(255)='' OUTPUT,
		@Instancename nvarchar(255)='' OUTPUT,
		@Servername nvarchar(510)='' OUTPUT
	AS
	BEGIN
		set nocount on;
		if is_srvrolemember('sysadmin')=1
		begin
			declare @output table (line varchar(255));
			insert @output(line) 
			exec xp_cmdshell 'powershell "[System.Net.Dns]::GetHostName()"';
			select top 1 @Hostname=[line] from @output where [line] is not null;
		end
		else
			set @Hostname=cast(serverproperty('MachineName') as nvarchar(255));
		set @Instancename=COALESCE( cast(SERVERPROPERTY('InstanceName') as nvarchar(255)),'');
		set @Servername=COALESCE(@Hostname,'')+CASE WHEN @InstanceName > '' THEN '\' ELSE '' END +@Instancename;
	END