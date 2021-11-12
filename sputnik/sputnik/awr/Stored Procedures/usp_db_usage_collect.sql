
CREATE procedure [awr].[usp_db_usage_collect]
	@dbfilter nvarchar(1000)=null
as
begin
	SET NOCOUNT ON;
	IF OBJECT_ID('Tempdb..#T') IS NOT NULL
		DROP TABLE #T;
	CREATE TABLE #T
		(DB nvarchar(300), CreateDate datetime2(2), 
			UsedType varchar(4), LastUsed datetime2(2), Cnt numeric(19,0));

	declare @DB nvarchar(300);

	declare DB cursor for
	select distinct
		d.[name] AS DB
	from
		sys.databases d
	where
		d.database_id>4
		and d.name<>'sputnik'
		and d.[state]=0
		and (d.name like @dbfilter or @dbfilter is null)
	;

	open DB;
	fetch next from DB into @DB;
	while @@FETCH_STATUS=0
	BEGIN
		INSERT INTO #T (DB, CreateDate , UsedType , LastUsed , Cnt )
		EXEC(N'
			SET NOCOUNT ON;
			USE ['+@DB+'];

			--Затем получаем информацию об обращаениях к таблицам БД!
			IF OBJECT_ID(''tempdb..#T_LastUsed'') IS NOT NULL
				DROP TABLE #T_LastUsed;
			SELECT DISTINCT
				max(coalesce(last_user_seek,''19000101 00:00:00'')) over() as Seek,
				max(coalesce(last_user_scan,''19000101 00:00:00'')) over() as Scan,
				max(coalesce(last_user_lookup,''19000101 00:00:00'')) over() as Lkp,
				max(coalesce(last_user_update,''19000101 00:00:00'')) over() as Upd,
				sum(coalesce(user_seeks,0)) over () as Seek_cnt,
				sum(coalesce(user_scans,0)) over () as Scan_cnt,
				sum(coalesce(user_lookups,0)) over () as Lkp_cnt,
				sum(coalesce(user_updates,0)) over () as Upd_cnt
			INTO #T_LastUsed
			FROM 
				sys.dm_db_index_usage_stats AS stat 
				INNER JOIN sys.objects AS o
			ON stat.[object_id] = o.[object_id]
			WHERE [database_id] = DB_ID()
			AND [type]=''U'';

			-- select * from #T_LastUsed

	
			;WITH cte_1 AS (
				SELECT
					'''+@DB+''' AS DB, 
					UsedType,
					LastUsed,
					Cnt
				FROM #T_LastUsed
				UNPIVOT (
						LastUsed FOR UsedType IN ([Seek], [Scan], [Lkp], [Upd])
					) unpvt_1
				UNPIVOT (
						Cnt FOR UsedType_cnt IN ([Seek_cnt], [Scan_cnt], [Lkp_cnt], [Upd_cnt])
					) unpvt_2
				WHERE UsedType+''_cnt''=UsedType_cnt
			)
			SELECT 
				d.name as DB,
				d.create_date as CreateDate,
				c.UsedType,
				case when c.LastUsed is null then ''19000101 00:00:00'' else c.LastUsed end as LastUsed,
				case when c.Cnt is null then 0 else c.Cnt end as cnt
			FROM sys.databases d 
			LEFT JOIN cte_1 c
				ON d.name=c.DB
			WHERE d.name='''+@DB+''';
		');
		fetch next from DB into @DB;
	END
	close DB;
	deallocate DB;
	declare @tt datetime2(2)=getdate();
	;MERGE [awr].[db_usage_stats] as target
	USING (
		SELECT DISTINCT 
			@tt as tt,
			(select cast(create_date as datetime2(2)) from sys.databases where name='tempdb') as StartUp,
			DB,
			db_id(DB) as [dbid],
			CreateDate,
			max(LastUsed) over (partition by DB) as LastUsed,
			sum(Cnt) over (partition by DB) as cntcalls
		FROM #T
	) as source (tt, StartUp, DB, [dbid], CreateDate, LastUsed, cntcalls)
	ON (target.StartUp=source.StartUp and target.[dbid]=source.[dbid] and target.CreateDate=source.CreateDate)
	WHEN MATCHED THEN 
		UPDATE SET LastUsed=source.LastUsed, cntcalls=source.cntcalls, tt=source.tt
	WHEN NOT MATCHED BY TARGET THEN
		INSERT (tt, StartUp, DB, [dbid], CreateDate, LastUsed, cntcalls)
		VALUES (source.tt, source.StartUp, source.DB, source.[dbid], source.CreateDate, source.LastUsed, source.cntcalls)
	;
end