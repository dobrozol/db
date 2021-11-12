
	CREATE VIEW info.vtempusing_ss
	AS
	--Мониторинг использованного места в TempDB (по запросам):
	-- https://docs.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-db-session-space-usage-transact-sql
	with cte01 as(
		SELECT
			ss.session_id as spid, 
			ss.user_objects_alloc_page_count+COALESCE(ts.user_objects_alloc_page_count,0) as user_objects_alloc_page_count,
			ss.user_objects_dealloc_page_count+COALESCE(ts.user_objects_dealloc_page_count,0) as user_objects_dealloc_page_count,
			ss.internal_objects_alloc_page_count+COALESCE(ts.internal_objects_alloc_page_count,0) as internal_objects_alloc_page_count,
			ss.internal_objects_dealloc_page_count+COALESCE(ts.internal_objects_dealloc_page_count,0) as internal_objects_dealloc_page_count
		FROM tempdb.sys.dm_db_session_space_usage ss
		LEFT JOIN tempdb.sys.dm_db_task_space_usage ts
			on ss.session_id=ts.session_id and (ts.user_objects_alloc_page_count>0 or ts.user_objects_dealloc_page_count>0 or ts.internal_objects_alloc_page_count>0 or ts.internal_objects_dealloc_page_count>0)
		WHERE ss.user_objects_alloc_page_count>0 or ss.user_objects_dealloc_page_count>0 or ss.internal_objects_alloc_page_count>0 or ss.internal_objects_dealloc_page_count>0 
	), cte02 as(
		SELECT 
			spid, 
			cast(SUM(user_objects_alloc_page_count)/128 as numeric(9,0)) AS user_alloc_mb,
			cast(SUM(user_objects_dealloc_page_count)/128 as numeric(9,0)) AS user_dealloc_mb,
			cast(SUM(internal_objects_alloc_page_count)/128 as numeric(9,0)) AS internal_alloc_mb,
			cast(SUM(internal_objects_dealloc_page_count)/128 as numeric(9,0)) AS internal_dealloc_mb
		FROM cte01
		GROUP BY spid
	)
	select 
		t.spid,
		ss.login_time,
		ss.last_request_start_time as rq_start_time,
		CASE WHEN t.user_alloc_mb-t.user_dealloc_mb<0 THEN 0 ELSE t.user_alloc_mb-t.user_dealloc_mb END as user_mb,
		CASE WHEN t.internal_alloc_mb-t.internal_dealloc_mb<0 THEN 0 ELSE t.internal_alloc_mb-t.internal_dealloc_mb END as internal_mb,
		ss.[status],
		CASE WHEN ss.[status]='sleeping' THEN datediff(second,ss.last_request_end_time,sysdatetime()) ELSE datediff(second,ss.last_request_start_time,sysdatetime()) END as elapsed_sec,
		ss.open_transaction_count as open_tran_cnt
	from cte02 as t
	inner join sys.dm_exec_sessions as ss
		on t.spid=ss.session_id
	where (ss.[status]<>'sleeping' or ss.open_transaction_count>0)
		and user_alloc_mb+internal_alloc_mb-user_dealloc_mb-internal_dealloc_mb>=300