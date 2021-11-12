
	CREATE VIEW info.vtempusing
	AS
	--Мониторинг использованного места в TempDB (по файлам)
	-- https://docs.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-db-file-space-usage-transact-sql
	with cte01 as(
		select 
			cast(sum(version_store_reserved_page_count)/128 as numeric(19,0)) as rowver_mb,
			cast(sum(user_object_reserved_page_count)/128 as numeric(19,0)) as user_mb,
			cast(sum(internal_object_reserved_page_count)/128 as numeric(19,0)) as internal_mb,
			cast(sum(mixed_extent_page_count)/128 as numeric(19,0)) as mixed_mb
		from tempdb.sys.dm_db_file_space_usage
	),
	cte02 as(
		select 
			*, cast(rowver_mb+user_mb+internal_mb+mixed_mb as numeric(19,0)) as total_mb
		from cte01
	)
		select 
			pr, vl
		from cte02 as src
		unpivot 
		(vl for pr in (rowver_mb,user_mb,internal_mb,mixed_mb,total_mb)) as unpvt;
GO
GRANT SELECT
    ON OBJECT::[info].[vtempusing] TO [zabbix]
    AS [dbo];

