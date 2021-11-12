
	CREATE VIEW info.vtempusing_ver
	AS
	--Мониторинг длительных транзакций, использующих row-verions store (snapshots) в tempdb:
	SELECT t.session_id as spid, ss.login_time, t.elapsed_time_seconds as tran_active_sec, ss.[status], ss.open_transaction_count as open_tran_cnt
	FROM sys.dm_tran_active_snapshot_database_transactions t
	inner join sys.dm_exec_sessions as ss
		on t.session_id=ss.session_id
	WHERE elapsed_time_seconds>180