
CREATE VIEW info.vGetLse
AS
	select 
		tc.ServerSource,
		tc.DBNameSource, 
		tc.DBNameTarget, 
		case tc.[Suspend]
			when 1 then 'Pause'
			else 'Run'
		end as [Status], 
		tc.InitDate,
		hs.LastSyncTime,
		DATEDIFF(MI,hs.LastSyncTime,sysdatetime()) as LastSyncInMin,
		hs.AvgSyncDurationSec,
		hs.CountRestoredLogBackups,
		hs.LastRestoredBackupId
	from lse.TargetConfig as tc
	left join (
		select distinct
			config_id,
			max(StartRestore) as LastSyncTime,
			avg(datediff(ss,StartRestore,CompleteRestore)) as AvgSyncDurationSec,
			count(*) as CountRestoredLogBackups,
			max(BackupHS_id) as LastRestoredBackupId
		 from lse.HS
		 group by config_id
	) hs
		on tc.id=hs.config_id