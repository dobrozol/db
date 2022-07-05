CREATE VIEW [info].[vAlwaysOnMon]
AS
	select  
		DB_NAME(repl.database_id) as DB,
		repl.log_send_queue_size * 1024 AS Send_queue_size_B,
		repl.log_send_rate * 1024 AS Send_Rate_BSec,
		repl.last_received_time,
		repl.last_commit_time,
		CASE 
			WHEN main.last_commit_lsn>repl.last_commit_lsn THEN
				datediff(second,repl.last_commit_time,main.last_commit_time) 
			WHEN main.last_commit_lsn=repl.last_commit_lsn THEN 
				0
			ELSE NULL	
		END AS SyncWait_sec,
		repl.redo_queue_size * 1024 redo_queue_size_B,
		repl.redo_rate * 1024 AS Redo_Rate_BSec
	from sys.dm_hadr_database_replica_states repl
	left join sys.dm_hadr_database_replica_states main
		on repl.group_id=main.group_id 
			and repl.group_database_id=main.group_database_id
			and main.is_local=1
	where
		repl.is_local=0
GO
