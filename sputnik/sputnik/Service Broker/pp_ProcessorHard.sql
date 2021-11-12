CREATE SERVICE [pp_ProcessorHard]
    AUTHORIZATION [dbo]
    ON QUEUE [pp].[ProcessorQueueHard]
    ([pp_lse_Contract], [pp_back_Contract]);

