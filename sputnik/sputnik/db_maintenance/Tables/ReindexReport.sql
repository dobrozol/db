CREATE TABLE [db_maintenance].[ReindexReport]
(
	[reportTime]		datetime2(2)	NOT NULL,
	[metricName]		varchar(30)		NOT NULL,
	[metricRange]		varchar(10)		NOT NULL,
	[countDb]			smallint		NULL,
	[countTable]		int				NULL,
	[countIndex]		int				NULL,
	[avgFragmIndex%]	tinyint			NULL,
	[medFragmIndex%]	tinyint			NULL,
	[avgPageUsed%]		tinyint			NULL,
	[medPageUsed%]		tinyint			NULL,
	[avgSizeMb]			int				NULL,
	[medSizeMb]			int				NULL,
	[sumSizeMb]			int				NULL,
	[avgReindexCnt]		int				NULL,
	[medReindexCnt]		int				NULL,
	[sumReindexCnt]		int				NULL,

	constraint [ReindexReport_PK] primary key ([reportTime],[metricName],[metricRange])

)
