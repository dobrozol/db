﻿CREATE QUEUE [pp].[ProcessorQueueHard]
    WITH ACTIVATION (STATUS = ON, PROCEDURE_NAME = [pp].[usp_ExecProcessorHard], MAX_QUEUE_READERS = 2, EXECUTE AS OWNER);

